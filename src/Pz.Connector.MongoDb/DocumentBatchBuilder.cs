using System.Globalization;
using Apache.Arrow;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.MongoDb;

/// <summary>Documents in, Arrow batches out, one column per <see cref="ColumnSpec"/>. Values are
/// read off the document by path and converted per the column's kind; a value the kind cannot hold
/// losslessly -- a fraction in an integer column, a string where a number is planned, a decimal
/// with more fraction digits than the column's scale -- fails the read naming the field and the
/// document, because a silently stringified or truncated column is worse than a stopped run.
/// Batches come from the ABI's pooled builder, so every yielded batch is a fresh instance the engine
/// owns outright.</summary>
internal sealed class DocumentBatchBuilder
{
    private static readonly decimal DecimalScaleFactor = 1_000_000_000m;

    private readonly ColumnPlan _plan;
    private readonly ArrowBatchBuilder _inner;
    private readonly string _dataset;
    private readonly MongoRedactor _redactor;
    private readonly object?[] _row;

    public DocumentBatchBuilder(ColumnPlan plan, BatchOptions options, string dataset, MongoRedactor redactor)
    {
        _plan = plan;
        _inner = new ArrowBatchBuilder(plan.Schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        _dataset = dataset;
        _redactor = redactor;
        _row = new object?[plan.Columns.Count];
    }

    public int PendingRows => _inner.PendingRows;

    public void Append(BsonDocument document)
    {
        var columns = _plan.Columns;
        for (var c = 0; c < columns.Count; c++)
        {
            _row[c] = Convert(columns[c], document);
        }

        _inner.AppendRow(_row);
    }

    /// <summary>The document's <c>_id</c> as a refusal names it. Formatted only on refusal: a
    /// per-document string on the happy path would be a million allocations per million rows.</summary>
    internal static string IdOf(BsonDocument document) => document.TryGetValue("_id", out var id) ? Describe(id) : "?";

    public bool TryTakeBatch(out RecordBatch? batch) => _inner.TryTakeBatch(out batch);

    public RecordBatch? Flush() => _inner.Flush();

    internal object? Convert(ColumnSpec column, BsonDocument document)
    {
        var id = document;
        if (!TryGetPath(document, column.Path, out var value) || value.IsBsonNull || value.BsonType == BsonType.Undefined)
        {
            return null;
        }

        return column.Kind switch
        {
            ColumnKind.Json => MongoSerialization.ToCanonicalJson(value),
            ColumnKind.String => ToText(value),
            ColumnKind.ObjectId => value.BsonType switch
            {
                BsonType.ObjectId => value.AsObjectId.ToString(),
                BsonType.String => value.AsString,
                _ => throw Refuse(column, id, $"holds {Describe(value)} where an ObjectId is planned"),
            },
            ColumnKind.Int32 => ToInt32(column, value, id),
            ColumnKind.Int64 => ToInt64(column, value, id),
            ColumnKind.Double => ToDouble(column, value, id),
            ColumnKind.Decimal => ToDecimal(column, value, id),
            ColumnKind.Boolean => value.BsonType == BsonType.Boolean
                ? value.AsBoolean
                : throw Refuse(column, id, $"holds {Describe(value)} where a boolean is planned"),
            ColumnKind.Timestamp => ToTimestamp(column, value, id),
            ColumnKind.Date => value.BsonType == BsonType.DateTime
                ? DateOnly.FromDateTime(ToTimestamp(column, value, id).UtcDateTime)
                : throw Refuse(column, id, $"holds {Describe(value)} where a date is planned"),
            _ => throw new InvalidOperationException($"unexpected column kind {column.Kind}"),
        };
    }

    /// <summary>Walks <paramref name="path"/> into nested documents. A step through anything that is
    /// not a document is a missing value: the plan was made from documents shaped one way and this
    /// one is shaped another, which a nullable column absorbs.</summary>
    internal static bool TryGetPath(BsonDocument document, string[] path, out BsonValue value)
    {
        BsonValue current = document;
        foreach (var segment in path)
        {
            if (current is not BsonDocument doc || !doc.TryGetValue(segment, out var next))
            {
                value = BsonNull.Value;
                return false;
            }

            current = next;
        }

        value = current;
        return true;
    }

    /// <summary>The text column is the lossless escape: any scalar has a text spelling, and a
    /// document or array is its canonical JSON.</summary>
    private static string ToText(BsonValue value) => value.BsonType switch
    {
        BsonType.String => value.AsString,
        BsonType.Symbol => value.AsBsonSymbol.Name,
        BsonType.ObjectId => value.AsObjectId.ToString(),
        BsonType.Int32 => value.AsInt32.ToString(CultureInfo.InvariantCulture),
        BsonType.Int64 => value.AsInt64.ToString(CultureInfo.InvariantCulture),
        BsonType.Double => value.AsDouble.ToString("R", CultureInfo.InvariantCulture),
        BsonType.Decimal128 => value.AsDecimal128.ToString(),
        BsonType.Boolean => value.AsBoolean ? "true" : "false",
        BsonType.DateTime => DateTimeOffset.FromUnixTimeMilliseconds(value.AsBsonDateTime.MillisecondsSinceEpoch)
            .ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture),
        _ => MongoSerialization.ToCanonicalJson(value),
    };

    private int ToInt32(ColumnSpec column, BsonValue value, BsonDocument id)
    {
        var l = ToInt64(column, value, id);
        if (l is < int.MinValue or > int.MaxValue)
        {
            throw Refuse(column, id, $"holds {l}, outside the planned 32-bit range");
        }

        return (int)l;
    }

    private long ToInt64(ColumnSpec column, BsonValue value, BsonDocument id)
    {
        switch (value.BsonType)
        {
            case BsonType.Int32:
                return value.AsInt32;
            case BsonType.Int64:
                return value.AsInt64;
            case BsonType.Double:
                var d = value.AsDouble;
                // long.MaxValue rounds UP to 2^63 as a double, so the upper check is strict.
                if (double.IsInteger(d) && d >= -9223372036854775808.0 && d < 9223372036854775808.0)
                {
                    return (long)d;
                }

                break;
            case BsonType.Decimal128:
                if (TryToDecimal(value.AsDecimal128, out var m) && decimal.IsInteger(m) && m is >= long.MinValue and <= long.MaxValue)
                {
                    return (long)m;
                }

                break;
        }

        throw Refuse(column, id, $"holds {Describe(value)} where an integer is planned");
    }

    private double ToDouble(ColumnSpec column, BsonValue value, BsonDocument id) => value.BsonType switch
    {
        BsonType.Int32 => value.AsInt32,
        BsonType.Int64 => value.AsInt64,
        BsonType.Double => value.AsDouble,
        BsonType.Decimal128 => Decimal128.ToDouble(value.AsDecimal128),
        _ => throw Refuse(column, id, $"holds {Describe(value)} where a double is planned"),
    };

    private decimal ToDecimal(ColumnSpec column, BsonValue value, BsonDocument id)
    {
        decimal result;
        switch (value.BsonType)
        {
            case BsonType.Int32:
                result = value.AsInt32;
                break;
            case BsonType.Int64:
                result = value.AsInt64;
                break;
            case BsonType.Double:
                var d = value.AsDouble;
                if (!double.IsFinite(d) || d is < -7.9e28 or > 7.9e28)
                {
                    throw Refuse(column, id, $"holds {Describe(value)}, which a decimal cannot hold");
                }

                result = (decimal)d;
                break;
            case BsonType.Decimal128:
                if (!TryToDecimal(value.AsDecimal128, out result))
                {
                    throw Refuse(column, id, $"holds {Describe(value)}, outside the planned decimal({ColumnPlan.DecimalPrecision},{ColumnPlan.DecimalScale}) range");
                }

                break;
            default:
                throw Refuse(column, id, $"holds {Describe(value)} where a decimal is planned");
        }

        if (result.Scale > ColumnPlan.DecimalScale)
        {
            var truncated = decimal.Truncate(result * DecimalScaleFactor) / DecimalScaleFactor;
            if (truncated != result)
            {
                throw Refuse(column, id, $"holds {Describe(value)}, more than {ColumnPlan.DecimalScale} fraction digits; declare the field as string or double under fields:");
            }

            result = truncated;
        }

        return result;
    }

    private DateTimeOffset ToTimestamp(ColumnSpec column, BsonValue value, BsonDocument id)
    {
        if (value.BsonType != BsonType.DateTime)
        {
            throw Refuse(column, id, $"holds {Describe(value)} where a timestamp is planned");
        }

        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(value.AsBsonDateTime.MillisecondsSinceEpoch);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw Refuse(column, id, $"holds {Describe(value)}, outside the timestamp range");
        }
    }

    /// <summary>System.Decimal holds 28-29 digits; a wider or finer Decimal128 overflows it.</summary>
    private static bool TryToDecimal(Decimal128 value, out decimal result)
    {
        try
        {
            result = Decimal128.ToDecimal(value);
            return true;
        }
        catch (OverflowException)
        {
            result = 0;
            return false;
        }
    }

    /// <summary>A value as a reader would write it: strings quoted, scalars in their plain
    /// spelling, documents and arrays as canonical JSON.</summary>
    internal static string Describe(BsonValue value) => value.BsonType switch
    {
        BsonType.String => $"\"{value.AsString}\"",
        BsonType.Document or BsonType.Array => MongoSerialization.ToCanonicalJson(value),
        _ => ToText(value),
    };

    private PzConnectorException Refuse(ColumnSpec column, BsonDocument document, string what) =>
        MongoErrors.Fatal($"dataset '{_dataset}': field '{column.Name}' of document {IdOf(document)} {what}", _redactor);
}

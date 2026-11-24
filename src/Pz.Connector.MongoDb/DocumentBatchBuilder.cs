using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Formats;

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
        if (!TryGetPath(document, column.Path, out var value) || value.IsBsonNull || value.BsonType == BsonType.Undefined)
        {
            return null;
        }

        return column.Kind switch
        {
            ColumnKind.Nested => ToNested(column, value, column.NestedType!, document),
            ColumnKind.Json => MongoSerialization.ToCanonicalJson(value),
            ColumnKind.String => ToText(value),
            ColumnKind.ObjectId => value.BsonType switch
            {
                BsonType.ObjectId => value.AsObjectId.ToString(),
                BsonType.String => value.AsString,
                _ => throw Refuse(column, document, $"holds {Describe(value)} where an ObjectId is planned"),
            },
            ColumnKind.Int32 => ToInt32(column, value, document),
            ColumnKind.Int64 => ToInt64(column, value, document),
            ColumnKind.Double => ToDouble(column, value, document),
            ColumnKind.Decimal => ToDecimal(column, value, document),
            ColumnKind.Boolean => value.BsonType == BsonType.Boolean
                ? value.AsBoolean
                : throw Refuse(column, document, $"holds {Describe(value)} where a boolean is planned"),
            ColumnKind.Timestamp => ToTimestamp(column, value, document),
            ColumnKind.Date => value.BsonType == BsonType.DateTime
                ? DateOnly.FromDateTime(ToTimestamp(column, value, document).UtcDateTime)
                : throw Refuse(column, document, $"holds {Describe(value)} where a date is planned"),
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

    /// <summary>A BSON sub-document or array as the boxed shape <see cref="ArrowBatchBuilder"/> takes for
    /// <paramref name="type"/>: a struct from a document (a missing field is null), a list from an array,
    /// a map from a document keyed by its field names. Leaves go through the flat converters, so a
    /// date, decimal or integer is checked exactly as a top-level column of that type is; a value of
    /// the wrong shape fails the read naming the column and the document.</summary>
    private object? ToNested(ColumnSpec column, BsonValue value, IArrowType type, BsonDocument document)
    {
        if (value.IsBsonNull || value.BsonType == BsonType.Undefined)
        {
            return null;
        }

        switch (type)
        {
            case StructType @struct:
            {
                if (value is not BsonDocument sub)
                {
                    throw Refuse(column, document, $"holds {Describe(value)} where a struct is planned");
                }

                var fields = new Dictionary<string, object?>(@struct.Fields.Count, StringComparer.Ordinal);
                foreach (var field in @struct.Fields)
                {
                    fields[field.Name] = sub.TryGetValue(field.Name, out var inner)
                        ? ToNested(column, inner, field.DataType, document)
                        : null;
                }

                return fields;
            }

            case ListType list:
                return ToList(column, value, list.ValueDataType, document);
            case FixedSizeListType fixedList:
                return ToList(column, value, fixedList.ValueDataType, document);
            case MapType map:
            {
                if (value is not BsonDocument sub)
                {
                    throw Refuse(column, document, $"holds {Describe(value)} where a map is planned");
                }

                var entries = new Dictionary<object, object?>(sub.ElementCount);
                foreach (var element in sub)
                {
                    entries[MapKey(column, element.Name, map.KeyField.DataType, document)] =
                        ToNested(column, element.Value, map.ValueField.DataType, document);
                }

                return entries;
            }

            default:
                return ToLeaf(column, value, type, document);
        }
    }

    private List<object?> ToList(ColumnSpec column, BsonValue value, IArrowType item, BsonDocument document)
    {
        if (value is not BsonArray array)
        {
            throw Refuse(column, document, $"holds {Describe(value)} where an array is planned");
        }

        var items = new List<object?>(array.Count);
        foreach (var element in array)
        {
            items.Add(ToNested(column, element, item, document));
        }

        return items;
    }

    private object ToLeaf(ColumnSpec column, BsonValue value, IArrowType type, BsonDocument document) => type switch
    {
        Int32Type => ToInt32(column, value, document),
        Int64Type => ToInt64(column, value, document),
        DoubleType => ToDouble(column, value, document),
        Decimal128Type => ToDecimal(column, value, document),
        StringType => ToText(value),
        BooleanType => value.BsonType == BsonType.Boolean
            ? value.AsBoolean
            : throw Refuse(column, document, $"holds {Describe(value)} where a boolean is planned"),
        Date32Type => value.BsonType == BsonType.DateTime
            ? DateOnly.FromDateTime(ToTimestamp(column, value, document).UtcDateTime)
            : throw Refuse(column, document, $"holds {Describe(value)} where a date is planned"),
        TimestampType => ToTimestamp(column, value, document),
        _ => throw new InvalidOperationException($"unexpected nested leaf type {type}"),
    };

    /// <summary>A sub-document's field name as the map's key type: BSON field names are text, so a
    /// non-text key type parses it and a name that does not parse fails the read.</summary>
    private object MapKey(ColumnSpec column, string name, IArrowType keyType, BsonDocument document)
    {
        try
        {
            return keyType switch
            {
                Int32Type => int.Parse(name, NumberStyles.Integer, CultureInfo.InvariantCulture),
                Int64Type => long.Parse(name, NumberStyles.Integer, CultureInfo.InvariantCulture),
                DoubleType => double.Parse(name, NumberStyles.Float, CultureInfo.InvariantCulture),
                Decimal128Type => decimal.Parse(name, NumberStyles.Number, CultureInfo.InvariantCulture),
                BooleanType => bool.Parse(name),
                Date32Type => DateOnly.ParseExact(name, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                TimestampType => DateTimeOffset.Parse(name, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
                _ => name,
            };
        }
        catch (Exception ex) when (ex is FormatException or OverflowException)
        {
            throw Refuse(column, document, $"has the map key \"{name}\", which is not a {ColumnContractType.ToDuckDbName(keyType).ToLowerInvariant()}");
        }
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

    private int ToInt32(ColumnSpec column, BsonValue value, BsonDocument document)
    {
        var l = ToInt64(column, value, document);
        if (l is < int.MinValue or > int.MaxValue)
        {
            throw Refuse(column, document, $"holds {l}, outside the planned 32-bit range");
        }

        return (int)l;
    }

    private long ToInt64(ColumnSpec column, BsonValue value, BsonDocument document)
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

        throw Refuse(column, document, $"holds {Describe(value)} where an integer is planned");
    }

    private double ToDouble(ColumnSpec column, BsonValue value, BsonDocument document) => value.BsonType switch
    {
        BsonType.Int32 => value.AsInt32,
        BsonType.Int64 => value.AsInt64,
        BsonType.Double => value.AsDouble,
        BsonType.Decimal128 => Decimal128.ToDouble(value.AsDecimal128),
        _ => throw Refuse(column, document, $"holds {Describe(value)} where a double is planned"),
    };

    private decimal ToDecimal(ColumnSpec column, BsonValue value, BsonDocument document)
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
                    throw Refuse(column, document, $"holds {Describe(value)}, which a decimal cannot hold");
                }

                result = (decimal)d;
                break;
            case BsonType.Decimal128:
                if (!TryToDecimal(value.AsDecimal128, out result))
                {
                    throw Refuse(column, document, $"holds {Describe(value)}, outside the planned decimal({ColumnPlan.DecimalPrecision},{ColumnPlan.DecimalScale}) range");
                }

                break;
            default:
                throw Refuse(column, document, $"holds {Describe(value)} where a decimal is planned");
        }

        if (result.Scale > ColumnPlan.DecimalScale)
        {
            var truncated = decimal.Truncate(result * DecimalScaleFactor) / DecimalScaleFactor;
            if (truncated != result)
            {
                throw Refuse(column, document, $"holds {Describe(value)}, more than {ColumnPlan.DecimalScale} fraction digits; declare the field as string or double under fields:");
            }

            result = truncated;
        }

        return result;
    }

    private DateTimeOffset ToTimestamp(ColumnSpec column, BsonValue value, BsonDocument document)
    {
        if (value.BsonType != BsonType.DateTime)
        {
            throw Refuse(column, document, $"holds {Describe(value)} where a timestamp is planned");
        }

        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(value.AsBsonDateTime.MillisecondsSinceEpoch);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw Refuse(column, document, $"holds {Describe(value)}, outside the timestamp range");
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

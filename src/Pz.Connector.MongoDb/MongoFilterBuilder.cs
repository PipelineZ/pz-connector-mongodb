using System.Globalization;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>The <c>find</c> filter: the user's <c>filter:</c> and the engine's watermark bounds,
/// combined under <c>$and</c> so neither is re-modelled. The bounds arrive as pz's canonical
/// strings (invariant digits, <c>yyyy-MM-dd</c>, <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> UTC) and are
/// typed from the cursor column's kind, because MongoDB compares a string bound against a date or
/// number field as a different BSON type -- never as a range.</summary>
internal static class MongoFilterBuilder
{
    private static readonly ColumnKind[] CursorKinds =
        [ColumnKind.Int32, ColumnKind.Int64, ColumnKind.Double, ColumnKind.Decimal, ColumnKind.Timestamp, ColumnKind.Date];

    public static BsonDocument Build(MongoDatasetConfig dataset, ColumnPlan plan, DatasetSpec spec, MongoRedactor redactor)
    {
        var user = dataset.Filter;
        var bounds = CursorBounds(plan, spec, redactor);
        if (user is null && bounds is null)
        {
            return [];
        }

        if (user is null)
        {
            return bounds!;
        }

        if (bounds is null)
        {
            return (BsonDocument)user.DeepClone();
        }

        return new BsonDocument("$and", new BsonArray { user.DeepClone(), bounds });
    }

    /// <summary>The engine's bounds as one range clause on the cursor, or null when there is no
    /// watermark yet. Gated on <see cref="DatasetSpec.WatermarkValue"/>, not the cursor name alone:
    /// the name is stamped on every incremental spec, including a first run with nothing stored.</summary>
    public static BsonDocument? CursorBounds(ColumnPlan plan, DatasetSpec spec, MongoRedactor redactor)
    {
        if (spec.WatermarkCursor is not { } cursor)
        {
            return null;
        }

        var lower = spec.WatermarkValue;
        var upper = spec.WatermarkUpperBound;
        if (lower is null && upper is null)
        {
            return null;
        }

        var column = ValidateCursor(plan, spec, redactor);
        var range = new BsonDocument();
        if (lower is not null)
        {
            range[spec.WatermarkLowerInclusive ? "$gte" : "$gt"] = Typed(column, lower, spec, redactor);
        }

        if (upper is not null)
        {
            range["$lte"] = Typed(column, upper, spec, redactor);
        }

        return new BsonDocument(column.Name, range);
    }

    /// <summary>A watermark cursor must be a column a range can bound: numeric, date, or
    /// timestamp. Checked whenever a cursor is named -- a first run has no value yet but the same
    /// misconfiguration.</summary>
    public static ColumnSpec ValidateCursor(ColumnPlan plan, DatasetSpec spec, MongoRedactor redactor)
    {
        var cursor = spec.WatermarkCursor ?? throw new InvalidOperationException("no cursor");
        var column = plan.Columns.FirstOrDefault(c => string.Equals(c.Name, cursor, StringComparison.Ordinal))
            ?? throw MongoErrors.Fatal($"dataset '{spec.Dataset}': watermark cursor '{cursor}' is not a column of the dataset", redactor);
        if (!CursorKinds.Contains(column.Kind))
        {
            throw MongoErrors.Fatal(
                $"dataset '{spec.Dataset}': watermark cursor '{cursor}' is a {column.Kind.ToString().ToLowerInvariant()} column, which a range cannot bound; " +
                "use a numeric, date, or timestamp field", redactor);
        }

        return column;
    }

    internal static BsonValue Typed(ColumnSpec column, string text, DatasetSpec spec, MongoRedactor redactor)
    {
        switch (column.Kind)
        {
            case ColumnKind.Int32 or ColumnKind.Int64:
                if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var l))
                {
                    return new BsonInt64(l);
                }

                break;
            case ColumnKind.Double:
                if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                {
                    return new BsonDouble(d);
                }

                break;
            case ColumnKind.Decimal:
                if (Decimal128.TryParse(text, out var m))
                {
                    return new BsonDecimal128(m);
                }

                break;
            case ColumnKind.Timestamp:
                if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
                {
                    return new BsonDateTime(ts.ToUnixTimeMilliseconds());
                }

                break;
            case ColumnKind.Date:
                if (DateOnly.TryParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day))
                {
                    return new BsonDateTime(new DateTimeOffset(day.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc)).ToUnixTimeMilliseconds());
                }

                break;
        }

        throw MongoErrors.Fatal(
            $"dataset '{spec.Dataset}': watermark bound '{text}' is not a {column.Kind.ToString().ToLowerInvariant()} value for cursor '{column.Name}'", redactor);
    }
}

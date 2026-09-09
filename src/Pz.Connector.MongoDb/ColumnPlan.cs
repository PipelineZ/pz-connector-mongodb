using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;

namespace Pz.Connector.MongoDb;

/// <summary>How a column's value is read out of a document and spelled in Arrow. <see cref="Json"/>
/// takes any BSON value as canonical extended JSON text; <see cref="ObjectId"/> is a text column
/// that reads an ObjectId as its 24-hex spelling.</summary>
internal enum ColumnKind { String, ObjectId, Int32, Int64, Double, Decimal, Boolean, Timestamp, Date, Json }

/// <summary>One column: its name (the dotted BSON path), the path inside the document, and its kind.</summary>
internal sealed record ColumnSpec(string Name, string[] Path, ColumnKind Kind)
{
    public static ColumnSpec Of(string path, ColumnKind kind) => new(path, path.Split('.'), kind);
}

/// <summary>The dataset's Arrow schema and, per column, how to fill it. <c>_id</c> is the one
/// column MongoDB guarantees present, hence the one non-nullable field.</summary>
internal sealed class ColumnPlan
{
    public const int DecimalPrecision = 38;
    public const int DecimalScale = 9;

    public ColumnPlan(IReadOnlyList<ColumnSpec> columns)
    {
        Columns = columns;
        Schema = new Schema(columns.Select(c => new Field(c.Name, ArrowType(c.Kind), c.Name != "_id")).ToList(), null);
    }

    public IReadOnlyList<ColumnSpec> Columns { get; }

    public Schema Schema { get; }

    /// <summary>The <c>find</c> projection: every column's path. <c>_id</c> is left in even when no
    /// column wants it (the server includes it by default): a value the plan cannot hold is
    /// reported by document, and twelve bytes per document is what naming it costs.</summary>
    public BsonDocument Projection()
    {
        var projection = new BsonDocument();
        foreach (var column in Columns)
        {
            projection[column.Name] = 1;
        }

        return projection;
    }

    /// <summary>Narrows to the named columns, in this plan's own order. A name this plan does not
    /// have means the hint is unusable, and the full plan is returned -- the engine then drops the
    /// hint the same way and the pipeline's SQL reports the unknown column.</summary>
    public ColumnPlan Project(IReadOnlyList<string>? columns)
    {
        if (columns is not { Count: > 0 })
        {
            return this;
        }

        var wanted = new HashSet<string>(columns, StringComparer.OrdinalIgnoreCase);
        var kept = Columns.Where(c => wanted.Contains(c.Name)).ToList();
        return kept.Count == wanted.Count ? new ColumnPlan(kept) : this;
    }

    public static IArrowType ArrowType(ColumnKind kind) => kind switch
    {
        ColumnKind.Int32 => Int32Type.Default,
        ColumnKind.Int64 => Int64Type.Default,
        ColumnKind.Double => DoubleType.Default,
        ColumnKind.Decimal => new Decimal128Type(DecimalPrecision, DecimalScale),
        ColumnKind.Boolean => BooleanType.Default,
        ColumnKind.Timestamp => new TimestampType(TimeUnit.Microsecond, "UTC"),
        ColumnKind.Date => Date32Type.Default,
        _ => StringType.Default,
    };

    /// <summary>The <c>fields:</c> spellings, with the SQL-flavoured aliases a pz author is likely
    /// to reach for.</summary>
    public static bool TryParseKind(string text, out ColumnKind kind)
    {
        switch (text.Trim().ToLowerInvariant())
        {
            case "string" or "varchar" or "text": kind = ColumnKind.String; return true;
            case "objectid" or "oid": kind = ColumnKind.ObjectId; return true;
            case "int32" or "int" or "integer": kind = ColumnKind.Int32; return true;
            case "int64" or "bigint" or "long": kind = ColumnKind.Int64; return true;
            case "double" or "float" or "float8": kind = ColumnKind.Double; return true;
            case "decimal" or "decimal128" or "numeric": kind = ColumnKind.Decimal; return true;
            case "bool" or "boolean": kind = ColumnKind.Boolean; return true;
            case "timestamp" or "datetime": kind = ColumnKind.Timestamp; return true;
            case "date": kind = ColumnKind.Date; return true;
            case "json": kind = ColumnKind.Json; return true;
            default: kind = default; return false;
        }
    }

    public const string KindNames = "string, objectid, int32, int64, double, decimal, bool, timestamp, date, json";

    /// <summary>More columns than this is not a table but a map with dynamic keys (a per-user
    /// bag, an attribute dictionary); such a field belongs under <c>fields:</c> as <c>json</c>.</summary>
    public const int MaxColumns = 2000;

    /// <summary>Column names are BSON paths and DuckDB identifiers at once: one must not be a
    /// prefix path of another (the projection would clash, and the sink could not nest them), and
    /// two must not differ only by case (DuckDB would fold them together).</summary>
    public static void ValidateNames(IReadOnlyList<string> names, string prefix, List<string> errors)
    {
        if (names.Count > MaxColumns)
        {
            errors.Add($"{prefix}: {names.Count} columns is more than the {MaxColumns} a dataset may have; a field with dynamic keys should be declared as json under fields:");
            return;
        }

        var seen = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var exact = new HashSet<string>(names, StringComparer.Ordinal);
        foreach (var name in names)
        {
            if (seen.TryGetValue(name, out var other) && !string.Equals(other, name, StringComparison.Ordinal))
            {
                errors.Add($"{prefix}: fields '{other}' and '{name}' differ only by case, which SQL cannot tell apart; declare one of them under fields: with another name or drop it");
            }

            seen.TryAdd(name, name);

            // Every proper dotted prefix of this name is a parent path; one that is also a name is
            // a value and a parent at once. Linear in the total path length, not quadratic.
            for (var dot = name.IndexOf('.'); dot > 0; dot = name.IndexOf('.', dot + 1))
            {
                var parent = name[..dot];
                if (exact.Contains(parent))
                {
                    errors.Add($"{prefix}: field '{parent}' is both a value and the parent of '{name}'");
                    break;
                }
            }
        }
    }
}

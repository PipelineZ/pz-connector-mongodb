using MongoDB.Bson;

namespace Pz.Connector.MongoDb;

/// <summary>Turns a sample of documents into a <see cref="ColumnPlan"/>: every scalar leaf is a
/// column named by its dotted path, in first-seen order; nested documents flatten; arrays, values
/// of no scalar kind, and fields whose kind varies across the sample land as JSON text; numerics
/// widen (int32 -> int64 -> double, anything with a decimal -> decimal); <c>_id</c> is the one
/// trailing column. Null and missing values carry no kind, so a field that is sometimes absent is
/// just nullable. The sample is deterministic (ascending <c>_id</c>), so two reads of the same head
/// of a collection plan the same schema. A field whose own name contains a dot is refused: the
/// column named <c>a.b</c> is the path into a nested document, and a literal <c>"a.b"</c> field
/// would share its name and lose its values silently.</summary>
internal sealed class SchemaInference(string datasetName, MongoRedactor redactor)
{
    [Flags]
    internal enum Seen
    {
        None = 0, String = 1, ObjectId = 2, Int32 = 4, Int64 = 8, Double = 16, Decimal = 32,
        Boolean = 64, DateTime = 128, Document = 256, Array = 512, Other = 1024,
    }

    private const Seen Numeric = Seen.Int32 | Seen.Int64 | Seen.Double | Seen.Decimal;

    private readonly Dictionary<string, Seen> _seen = new(StringComparer.Ordinal);
    private readonly List<string> _order = [];
    private readonly HashSet<string> _parents = new(StringComparer.Ordinal);
    private Seen _id;

    public int Documents { get; private set; }

    public void Observe(BsonDocument document)
    {
        Documents++;
        foreach (var element in document)
        {
            if (element.Name == "_id")
            {
                _id |= KindOf(element.Value);
                continue;
            }

            Walk(null, element.Name, element.Value, document);
        }
    }

    private void Walk(string? parent, string name, BsonValue value, BsonDocument document)
    {
        if (name.Contains('.'))
        {
            throw MongoErrors.Fatal(
                $"dataset '{datasetName}': field '{name}' of document {DocumentBatchBuilder.IdOf(document)} has a dot in its own name, which a column path cannot tell from a nested field; " +
                "declare its parent as json under fields:", redactor);
        }

        var kind = KindOf(value);
        if (kind == Seen.None)
        {
            return;
        }

        var path = parent is null ? name : parent + "." + name;
        if (!_seen.TryGetValue(path, out var seen))
        {
            _order.Add(path);
            if (_order.Count > ColumnPlan.MaxColumns)
            {
                throw MongoErrors.Fatal(
                    $"dataset '{datasetName}': more than {ColumnPlan.MaxColumns} distinct field paths in the sample; a field with dynamic keys should be declared as json under fields:", redactor);
            }

            if (parent is not null)
            {
                _parents.Add(parent);
            }
        }

        _seen[path] = seen | kind;
        if (kind == Seen.Document)
        {
            foreach (var element in value.AsBsonDocument)
            {
                Walk(path, element.Name, element.Value, document);
            }
        }
    }

    public ColumnPlan Plan()
    {
        // A path lands as JSON when it is ever an array, a value of no scalar kind, or a mix of a
        // document and something else; it is a column of its own kind otherwise. A document-only
        // path is not a column -- its leaves are -- unless it never had any (an empty object).
        var json = new HashSet<string>(StringComparer.Ordinal);
        var columns = new List<ColumnSpec>();
        foreach (var path in _order)
        {
            if (HasJsonAncestor(path, json))
            {
                continue;
            }

            var seen = _seen[path];
            if (seen == Seen.Document)
            {
                if (!_parents.Contains(path))
                {
                    columns.Add(ColumnSpec.Of(path, ColumnKind.Json));
                }

                continue;
            }

            var kind = Resolve(seen);
            if (kind == ColumnKind.Json)
            {
                json.Add(path);
            }

            columns.Add(ColumnSpec.Of(path, kind));
        }

        columns.Add(ColumnSpec.Of("_id", _id == Seen.None ? ColumnKind.ObjectId : Resolve(_id)));

        var errors = new List<string>();
        ColumnPlan.ValidateNames(columns.Select(c => c.Name).ToList(), $"dataset '{datasetName}'", errors);
        if (errors.Count > 0)
        {
            throw MongoErrors.Fatal(string.Join("; ", errors), redactor);
        }

        return new ColumnPlan(columns);
    }

    private static bool HasJsonAncestor(string path, HashSet<string> json)
    {
        for (var dot = path.IndexOf('.'); dot > 0; dot = path.IndexOf('.', dot + 1))
        {
            if (json.Contains(path[..dot]))
            {
                return true;
            }
        }

        return false;
    }

    internal static ColumnKind Resolve(Seen seen)
    {
        if ((seen & (Seen.Array | Seen.Other | Seen.Document)) != 0)
        {
            return ColumnKind.Json;
        }

        if ((seen & ~Numeric) == 0)
        {
            if ((seen & Seen.Decimal) != 0) return ColumnKind.Decimal;
            if ((seen & Seen.Double) != 0) return ColumnKind.Double;
            if ((seen & Seen.Int64) != 0) return ColumnKind.Int64;
            return ColumnKind.Int32;
        }

        return seen switch
        {
            Seen.String => ColumnKind.String,
            Seen.ObjectId => ColumnKind.ObjectId,
            Seen.String | Seen.ObjectId => ColumnKind.String,
            Seen.Boolean => ColumnKind.Boolean,
            Seen.DateTime => ColumnKind.Timestamp,
            _ => ColumnKind.Json,
        };
    }

    private static Seen KindOf(BsonValue value) => value.BsonType switch
    {
        BsonType.Null or BsonType.Undefined => Seen.None,
        BsonType.String or BsonType.Symbol => Seen.String,
        BsonType.ObjectId => Seen.ObjectId,
        BsonType.Int32 => Seen.Int32,
        BsonType.Int64 => Seen.Int64,
        BsonType.Double => Seen.Double,
        BsonType.Decimal128 => Seen.Decimal,
        BsonType.Boolean => Seen.Boolean,
        BsonType.DateTime => Seen.DateTime,
        BsonType.Document => Seen.Document,
        BsonType.Array => Seen.Array,
        _ => Seen.Other,
    };
}

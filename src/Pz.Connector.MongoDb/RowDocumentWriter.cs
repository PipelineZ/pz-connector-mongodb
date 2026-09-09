using Apache.Arrow;
using MongoDB.Bson;

namespace Pz.Connector.MongoDb;

/// <summary>One row of an Arrow batch as a <see cref="BsonDocument"/>: field order = column order,
/// a dotted column name nested into sub-documents (the inverse of the source's flattening), and
/// every value in its BSON type. Hand-written over the v0 type matrix: no reflection, and the
/// choices are a documented contract -- a Decimal128 column keeps every digit through its text
/// spelling, a timestamp keeps milliseconds (BSON dates have no finer unit), a date is midnight
/// UTC, and a text column listed under <c>object_ids</c> becomes an ObjectId when its value is 24
/// hex characters.</summary>
internal sealed class RowDocumentWriter
{
    private readonly string[][] _paths;
    private readonly bool[] _objectId;

    public RowDocumentWriter(Schema schema, IReadOnlySet<string> objectIds)
    {
        _paths = schema.FieldsList.Select(f => f.Name.Split('.')).ToArray();
        _objectId = schema.FieldsList.Select(f => objectIds.Contains(f.Name)).ToArray();
    }

    public BsonDocument Write(RecordBatch batch, int row)
    {
        var document = new BsonDocument();
        for (var c = 0; c < _paths.Length; c++)
        {
            var value = Value(batch.Column(c), row, _objectId[c]);
            var path = _paths[c];
            var target = document;
            for (var i = 0; i < path.Length - 1; i++)
            {
                if (!target.TryGetValue(path[i], out var next) || next is not BsonDocument nested)
                {
                    nested = [];
                    target[path[i]] = nested;
                }

                target = nested;
            }

            target[path[^1]] = value;
        }

        return document;
    }

    /// <summary>The value at (column, row), for a merge filter as well as the document itself.</summary>
    public static BsonValue Value(IArrowArray column, int row, bool asObjectId)
    {
        if (column.IsNull(row))
        {
            return BsonNull.Value;
        }

        return column switch
        {
            Int32Array a => new BsonInt32(a.GetValue(row)!.Value),
            Int64Array a => new BsonInt64(a.GetValue(row)!.Value),
            DoubleArray a => new BsonDouble(a.GetValue(row)!.Value),
            BooleanArray a => new BsonBoolean(a.GetValue(row)!.Value),
            // Through SqlDecimal, never GetValue: System.Decimal holds 28-29 significant digits, and
            // for a wider Decimal128 GetValue returns the value with the excess digits dropped rather
            // than throwing. SqlDecimal's text is culture-independent and carries every stored digit.
            Decimal128Array a => new BsonDecimal128(Decimal128.Parse(a.GetSqlDecimal(row)!.Value.ToString())),
            Date32Array a => new BsonDateTime(new DateTimeOffset(a.GetDateOnly(row)!.Value.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc)).ToUnixTimeMilliseconds()),
            TimestampArray a => new BsonDateTime(a.GetTimestamp(row)!.Value.ToUnixTimeMilliseconds()),
            StringArray a => Text(a.GetString(row)!, asObjectId),
            _ => throw new NotSupportedException($"column type {column.Data.DataType.TypeId} is outside pz's type matrix"),
        };
    }

    private static BsonValue Text(string text, bool asObjectId) =>
        asObjectId && text.Length == 24 && ObjectId.TryParse(text, out var id) ? new BsonObjectId(id) : new BsonString(text);
}

using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>Per-output write options: <c>collection</c> (defaults to the entity name),
/// <c>batch_size</c> documents per insert or bulk request, and <c>object_ids</c>, the text columns
/// whose 24-hex values are written as ObjectId (default <c>_id</c>). Column checks against the real
/// schema happen at BeginWriteAsync (<see cref="ValidateSchema"/>); Parse only knows the option shapes.</summary>
internal sealed record MongoOutputConfig(string Collection, int BatchSize, IReadOnlySet<string> ObjectIds)
{
    public const int DefaultBatchSize = 1000;
    public const int MaxBatchSize = 100_000;

    private static readonly string[] KnownKeys = ["collection", "batch_size", "object_ids"];
    private static readonly string[] Modes = ["append", "merge", "replace"];

    /// <summary>Exactly what <see cref="RowDocumentWriter"/> can spell -- pz's v0 type matrix.</summary>
    private static readonly ArrowTypeId[] DocumentTypes =
    [
        ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Double,
        ArrowTypeId.Decimal128, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp,
    ];

    public static MongoOutputConfig? Parse(OutputSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"output '{spec.Output}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown write option '{key}'; known: {string.Join(", ", KnownKeys)}");
        }

        if (!Modes.Contains(spec.Mode, StringComparer.Ordinal))
        {
            errors.Add($"{prefix}: mode '{spec.Mode}' is not supported; mongodb supports append, merge, and replace");
        }

        var collection = spec.Output;
        if (spec.Options.TryGetValue("collection", out var collectionRaw))
        {
            collection = collectionRaw?.ToString() ?? "";
            if (collection.Length == 0)
            {
                errors.Add($"{prefix}: 'collection' must be a non-empty string");
            }
        }

        if (collection.Contains('$') || collection.StartsWith("system.", StringComparison.Ordinal))
        {
            errors.Add($"{prefix}: 'collection' '{collection}' is not a name MongoDB accepts ('$' and the system. prefix are reserved)");
        }

        var batchSize = Options.Int(spec.Options, "batch_size", DefaultBatchSize, 1, MaxBatchSize, prefix, errors);
        var objectIds = Options.Strings(spec.Options, "object_ids", prefix, errors) ?? ["_id"];

        return errors.Count == start
            ? new MongoOutputConfig(collection, batchSize, new HashSet<string>(objectIds, StringComparer.Ordinal))
            : null;
    }

    /// <summary>Every column must be spellable as BSON and nameable as a field, a merge output's keys
    /// must be present columns (the filter is built from them), and an <c>object_ids</c> entry that
    /// names a present column must name a text one.</summary>
    public static void ValidateSchema(OutputSpec spec, MongoOutputConfig output, Schema schema, List<string> errors)
    {
        var prefix = $"output '{spec.Output}'";
        foreach (var field in schema.FieldsList)
        {
            if (!DocumentTypes.Contains(field.DataType.TypeId))
            {
                errors.Add($"{prefix}: column '{field.Name}' is {field.DataType.TypeId}, which a BSON document cannot carry; "
                    + $"allowed: {string.Join(", ", DocumentTypes)}. Drop it from the pipeline's projection or cast it");
            }

            if (field.Name.Length == 0 || field.Name.Split('.').Any(s => s.Length == 0 || s.StartsWith('$')))
            {
                errors.Add($"{prefix}: column '{field.Name}' is not a field name MongoDB accepts (no empty segments, no '$' prefix)");
            }

            if (output.ObjectIds.Contains(field.Name) && field.DataType.TypeId != ArrowTypeId.String)
            {
                errors.Add($"{prefix}: object_ids entry '{field.Name}' is a {field.DataType.TypeId} column; only a varchar column can hold a 24-hex ObjectId");
            }
        }

        ColumnPlan.ValidateNames(schema.FieldsList.Select(f => f.Name).ToList(), prefix, errors);

        if (spec.Mode == "merge")
        {
            if (spec.Keys.Count == 0)
            {
                errors.Add($"{prefix}: mode merge needs 'keys:' -- the upsert filter is built from them");
            }

            foreach (var key in spec.Keys.Where(k => schema.FieldsList.All(f => f.Name != k)))
            {
                errors.Add($"{prefix}: key column '{key}' is not in the pipeline's output");
            }
        }
    }
}

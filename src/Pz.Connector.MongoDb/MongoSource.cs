using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>A collection as one table-shaped dataset: the schema is declared under <c>fields:</c>
/// or inferred from a sample, the engine's watermark bounds become a range on the cursor, and
/// pruning narrows the projection. One partition per dataset. No native scan: DuckDB cannot speak
/// the MongoDB wire protocol.</summary>
internal sealed class MongoSource(MongoConnectionConfig connection, MongoClient client, ILogger logger) : ISource
{
    private readonly IMongoDatabase _database = client.GetDatabase(connection.Database);
    private readonly Dictionary<string, ColumnPlan> _plans = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _planning = new(1, 1);

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var plan = await PlanAsync(dataset, spec, ct).ConfigureAwait(false);
        return new DatasetSchema(plan.Schema);
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var plan = await PlanAsync(dataset, spec, ct).ConfigureAwait(false);
        if (spec.WatermarkCursor is not null)
        {
            MongoFilterBuilder.ValidateCursor(plan, spec, connection.Redactor);
        }

        var filter = MongoFilterBuilder.Build(dataset, plan, spec, connection.Redactor);
        var projected = plan.Project(hints.Columns);
        logger.LogDebug("mongodb: dataset {Dataset}: {Columns} of {Total} columns, batch size {BatchSize}",
            spec.Dataset, projected.Columns.Count, plan.Columns.Count, dataset.BatchSize);
        return [new MongoPartition(connection, _database.GetCollection<BsonDocument>(dataset.Collection), dataset, projected, filter, spec, logger)];
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        _planning.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>A declared schema is the plan. Otherwise the first <c>sample_size</c> documents
    /// matching the user's filter (not the watermark bounds, so an incremental read plans the same
    /// columns as a full one), in ascending <c>_id</c>, are inferred from; an empty sample is a
    /// refusal, because a dataset with no columns is a misconfiguration, not an empty table. The
    /// engine asks for the schema and then plans the read; one sample serves both calls, so the
    /// staging table and the batches that land in it cannot disagree on the columns because a
    /// document arrived in between.</summary>
    private async Task<ColumnPlan> PlanAsync(MongoDatasetConfig dataset, DatasetSpec spec, CancellationToken ct)
    {
        if (dataset.Fields is { } fields)
        {
            return new ColumnPlan(fields);
        }

        var key = $"{spec.Dataset}\n{dataset.Collection}\n{dataset.SampleSize}\n{dataset.Filter}";
        await _planning.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (!_plans.TryGetValue(key, out var plan))
            {
                plan = await InferAsync(dataset, spec, ct).ConfigureAwait(false);
                _plans[key] = plan;
            }

            return plan;
        }
        finally
        {
            _planning.Release();
        }
    }

    private async Task<ColumnPlan> InferAsync(MongoDatasetConfig dataset, DatasetSpec spec, CancellationToken ct)
    {
        var redactor = connection.Redactor;
        var context = $"dataset '{spec.Dataset}'";
        var collection = _database.GetCollection<BsonDocument>(dataset.Collection);
        var inference = new SchemaInference(spec.Dataset, redactor);
        try
        {
            var options = new FindOptions<BsonDocument>
            {
                Sort = new BsonDocument("_id", 1),
                Limit = dataset.SampleSize,
                BatchSize = Math.Min(dataset.BatchSize, dataset.SampleSize),
            };
            using var cursor = await collection.FindAsync(dataset.Filter ?? [], options, ct).ConfigureAwait(false);
            while (await cursor.MoveNextAsync(ct).ConfigureAwait(false))
            {
                foreach (var document in cursor.Current)
                {
                    inference.Observe(document);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, redactor, $"{context}: sampling '{dataset.Collection}'");
        }

        if (inference.Documents == 0)
        {
            var exists = await CollectionExistsAsync(dataset.Collection, context, ct).ConfigureAwait(false);
            throw MongoErrors.Fatal(exists
                ? $"{context}: '{dataset.Collection}' has no document matching the filter to infer a schema from; declare the columns under fields:"
                : $"{context}: collection '{dataset.Collection}' does not exist in database '{connection.Database}'; create it or fix 'collection:'", redactor);
        }

        var plan = inference.Plan();
        logger.LogDebug("mongodb: dataset {Dataset}: inferred {Columns} columns from {Documents} documents", spec.Dataset, plan.Columns.Count, inference.Documents);
        return plan;
    }

    private async Task<bool> CollectionExistsAsync(string name, string context, CancellationToken ct)
    {
        try
        {
            var options = new ListCollectionNamesOptions { Filter = new BsonDocument("name", name) };
            using var names = await _database.ListCollectionNamesAsync(options, ct).ConfigureAwait(false);
            return await names.AnyAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, connection.Redactor, $"{context}: listing collections");
        }
    }

    private MongoDatasetConfig ParseDataset(DatasetSpec spec)
    {
        var errors = new List<string>();
        return MongoDatasetConfig.Parse(spec, errors) ?? throw MongoErrors.Fatal(string.Join("; ", errors), connection.Redactor);
    }
}

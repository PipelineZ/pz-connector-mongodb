using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>Document writes. <c>append</c> inserts, <c>merge</c> upserts each row as a whole
/// document under a filter built from its keys, <c>replace</c> writes a fresh collection and renames
/// it over the output in one server-side rename. No native copy (DuckDB cannot speak the wire
/// protocol), and <see cref="AbortSemantics.BestEffort"/>: an aborted replace drops its staging
/// collection, but inserts an aborted append or merge already sent are visible and cannot be unsent.</summary>
internal sealed class MongoSink(
    MongoConnectionConfig connection, MongoClient client, ILogger logger, TimeProvider time, Random random) : ISink
{
    private readonly IMongoDatabase _database = client.GetDatabase(connection.Database);

    public AbortSemantics AbortSemantics => AbortSemantics.BestEffort;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var errors = new List<string>();
        var output = MongoOutputConfig.Parse(spec, errors);
        if (output is not null)
        {
            MongoOutputConfig.ValidateSchema(spec, output, schema, errors);
        }

        if (output is null || errors.Count > 0)
        {
            // Parse and ValidateSchema already name the output in every message they add.
            throw MongoErrors.Fatal(string.Join("; ", errors), connection.Redactor);
        }

        return spec.Mode switch
        {
            "append" => new MongoWriteSession(connection, _database, output, schema, [], output.Collection, null, logger),
            "merge" => new MongoWriteSession(connection, _database, output, schema, spec.Keys, output.Collection, null, logger),
            _ => await BeginReplaceAsync(spec, schema, output, ct).ConfigureAwait(false),
        };
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>The staging collection is created up front so an empty write still renames an
    /// empty collection over the output, and carries the output's own indexes so the rename keeps
    /// the shape a reader relies on (a rename drops the target's indexes with the target). A view
    /// of the output name is refused: a rename cannot replace one.</summary>
    private async Task<MongoWriteSession> BeginReplaceAsync(OutputSpec spec, Schema schema, MongoOutputConfig output, CancellationToken ct)
    {
        var redactor = connection.Redactor;
        var context = $"output '{spec.Output}'";
        var staging = StagingName(output.Collection);
        try
        {
            BsonDocument? existing;
            using (var listed = await _database.ListCollectionsAsync(
                       new ListCollectionsOptions { Filter = new BsonDocument("name", output.Collection) }, ct).ConfigureAwait(false))
            {
                existing = await listed.FirstOrDefaultAsync(ct).ConfigureAwait(false);
            }

            if (existing is not null && existing.TryGetValue("type", out var type) && type == "view")
            {
                throw MongoErrors.Fatal($"{context}: '{output.Collection}' is a view; replace renames a collection over the output, and a view cannot be replaced", redactor);
            }

            await _database.CreateCollectionAsync(staging, cancellationToken: ct).ConfigureAwait(false);
            if (existing is not null)
            {
                await CopyIndexesAsync(output.Collection, staging, ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, redactor, $"{context}: preparing the staging collection '{staging}'");
        }

        logger.LogDebug("mongodb: output {Output}: replace stages into {Staging}", spec.Output, staging);
        return new MongoWriteSession(connection, _database, output, schema, [], staging, new ReplacePlan(output.Collection, staging), logger);
    }

    /// <summary>Every index of the output except the implicit <c>_id_</c>, recreated by spec on the
    /// staging collection through the <c>createIndexes</c> command -- the listing's own documents,
    /// minus the fields that belong to the old namespace, are the command's input.</summary>
    private async Task CopyIndexesAsync(string from, string to, CancellationToken ct)
    {
        var specs = new BsonArray();
        using (var indexes = await _database.GetCollection<BsonDocument>(from).Indexes.ListAsync(ct).ConfigureAwait(false))
        {
            while (await indexes.MoveNextAsync(ct).ConfigureAwait(false))
            {
                foreach (var index in indexes.Current)
                {
                    if (index.TryGetValue("name", out var name) && name == "_id_")
                    {
                        continue;
                    }

                    var copy = (BsonDocument)index.DeepClone();
                    copy.Remove("v");
                    copy.Remove("ns");
                    copy.Remove("background");
                    specs.Add(copy);
                }
            }
        }

        if (specs.Count == 0)
        {
            return;
        }

        await _database.RunCommandAsync<BsonDocument>(new BsonDocument { ["createIndexes"] = to, ["indexes"] = specs }, null, ct).ConfigureAwait(false);
    }

    private string StagingName(string collection)
    {
        var stamp = time.GetUtcNow().ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
        var suffix = random.Next(0, 0x1000000).ToString("x6", CultureInfo.InvariantCulture);
        return $"{collection}.pz_{stamp}_{suffix}";
    }
}

/// <summary>What a replace commit renames: the output collection and the staging collection that
/// takes its place.</summary>
internal sealed record ReplacePlan(string Collection, string Staging);

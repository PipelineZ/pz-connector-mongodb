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
            "replace" => await BeginReplaceAsync(spec, schema, output, ct).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"mode '{spec.Mode}' passed validation"),
        };
    }

    public ValueTask DisposeAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>The staging collection is created up front so an empty write still renames an
    /// empty collection over the output, and carries the output's own collection options
    /// (collation, validator) and indexes, so the rename keeps the shape a reader relies on -- a
    /// rename drops the target's options and indexes with the target, and a unique index recreated
    /// under another collation would enforce something else. An existing output that a rename
    /// cannot stand in for -- a view, a timeseries collection, or a capped collection, whose cap
    /// would silently keep only its last <c>max</c> rows -- is refused before the staging
    /// collection is even created. A failure after it exists drops it on the way out; nothing else
    /// would.</summary>
    private async Task<MongoWriteSession> BeginReplaceAsync(OutputSpec spec, Schema schema, MongoOutputConfig output, CancellationToken ct)
    {
        var redactor = connection.Redactor;
        var context = $"output '{spec.Output}'";
        var staging = StagingName(output.Collection);
        BsonDocument? existing;
        try
        {
            using (var listed = await _database.ListCollectionsAsync(
                       new ListCollectionsOptions { Filter = new BsonDocument("name", output.Collection) }, ct).ConfigureAwait(false))
            {
                existing = await listed.FirstOrDefaultAsync(ct).ConfigureAwait(false);
            }

            if (existing is not null)
            {
                RefuseIfNotReplaceable(context, output.Collection, existing, redactor);
            }

            var create = new BsonDocument("create", staging);
            if (existing is not null && existing.TryGetValue("options", out var options) && options is BsonDocument optionDocument)
            {
                create.AddRange(optionDocument);
            }

            await _database.RunCommandAsync<BsonDocument>(create, null, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, redactor, $"{context}: creating the staging collection '{staging}'");
        }

        try
        {
            if (existing is not null)
            {
                await CopyIndexesAsync(output.Collection, staging, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            await DropQuietlyAsync(staging, spec.Output).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            await DropQuietlyAsync(staging, spec.Output).ConfigureAwait(false);
            throw MongoErrors.Wrap(ex, redactor, $"{context}: copying the indexes of '{output.Collection}' to '{staging}'");
        }

        logger.LogDebug("mongodb: output {Output}: replace stages into {Staging}", spec.Output, staging);
        return new MongoWriteSession(connection, _database, output, schema, [], staging, new ReplacePlan(output.Collection, staging), logger);
    }

    /// <summary>A rename can only stand in a plain collection over the output: a capped collection
    /// would keep only its last <c>max</c> rows while every row still reports written, and a view
    /// or timeseries collection cannot be the target of a rename at all.</summary>
    private static void RefuseIfNotReplaceable(string context, string collection, BsonDocument existing, MongoRedactor redactor)
    {
        var capped = existing.TryGetValue("options", out var optionsValue) && optionsValue is BsonDocument options
            && options.TryGetValue("capped", out var cappedValue) && cappedValue.IsBoolean && cappedValue.AsBoolean;
        if (capped)
        {
            throw MongoErrors.Fatal(
                $"{context}: '{collection}' is a capped collection; replace renames a plain collection over the output and a cap cannot hold exactly what was written -- use append, or drop the cap",
                redactor);
        }

        if (existing.TryGetValue("type", out var typeValue) && typeValue.BsonType == BsonType.String && typeValue.AsString != "collection")
        {
            var type = typeValue.AsString;
            var noun = type == "timeseries" ? "a timeseries collection" : $"a {type}";
            throw MongoErrors.Fatal(
                $"{context}: '{collection}' is {noun}; replace renames a collection over the output, and {noun} cannot be replaced",
                redactor);
        }
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

    /// <summary>Best effort, on its own short budget: the staging collection was never visible
    /// under the output's name, and one that cannot be dropped is an orphan the operator can drop.</summary>
    private async Task DropQuietlyAsync(string staging, string output)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            await _database.DropCollectionAsync(staging, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning("mongodb: output {Output}: the staging collection {Staging} could not be dropped ({Reason}); drop it by hand",
                output, staging, connection.Redactor.Redact(ex.Message));
        }
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

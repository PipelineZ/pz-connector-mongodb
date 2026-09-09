using Apache.Arrow;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>One output's write: rows accumulate into documents that are sent whenever they reach the
/// output's batch bound (an ordered <c>insertMany</c> for append and replace, an ordered bulk of
/// upserting <c>replaceOne</c>s for merge), commit flushes the rest and for a replace renames the
/// staging collection over the output. Ordered bulks are what make a duplicate key inside one
/// session resolve last-writer-wins, and what make the first rejected document the one reported.</summary>
internal sealed class MongoWriteSession : ISinkWriteSession
{
    private readonly MongoRedactor _redactor;
    private readonly IMongoDatabase _database;
    private readonly IMongoCollection<BsonDocument> _target;
    private readonly MongoOutputConfig _output;
    private readonly ReplacePlan? _replace;
    private readonly ILogger _logger;
    private readonly RowDocumentWriter _writer;
    private readonly int[] _keyColumns;
    private readonly string[] _keyNames;
    private readonly List<BsonDocument> _documents = [];
    private readonly List<WriteModel<BsonDocument>> _models = [];
    private long _rows;
    private long _batches;
    private long _requests;
    private bool _committed;
    private bool _aborted;

    public MongoWriteSession(MongoConnectionConfig connection, IMongoDatabase database, MongoOutputConfig output, Schema schema,
        IReadOnlyList<string> keys, string target, ReplacePlan? replace, ILogger logger)
    {
        _redactor = connection.Redactor;
        _database = database;
        _target = database.GetCollection<BsonDocument>(target);
        _output = output;
        _replace = replace;
        _logger = logger;
        _writer = new RowDocumentWriter(schema, output.ObjectIds);
        _keyNames = keys.ToArray();
        _keyColumns = keys.Select(k => schema.FieldsList.ToList().FindIndex(f => f.Name == k)).ToArray();
    }

    /// <summary>The collection rows actually go to: the output's, or a replace's staging one.</summary>
    internal string TargetCollection => _target.CollectionNamespace.CollectionName;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ThrowIfFinished();
        for (var row = 0; row < batch.Length; row++)
        {
            ct.ThrowIfCancellationRequested();
            var document = _writer.Write(batch, row);
            if (_keyColumns.Length > 0)
            {
                _models.Add(new ReplaceOneModel<BsonDocument>(KeyFilter(batch, row), document) { IsUpsert = true });
            }
            else
            {
                _documents.Add(document);
            }

            _rows++;
            if (_documents.Count + _models.Count >= _output.BatchSize)
            {
                await FlushAsync(ct).ConfigureAwait(false);
            }
        }

        _batches++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        ThrowIfFinished();
        _committed = true;
        await FlushAsync(ct).ConfigureAwait(false);
        if (_replace is { } replace)
        {
            await RenameAsync(replace, ct).ConfigureAwait(false);
        }

        _logger.LogDebug("mongodb: output {Output}: committed {Rows} rows in {Batches} batches over {Requests} requests",
            _output.Collection, _rows, _batches, _requests);
        return new WriteResult(_rows, _batches);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        if (_committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        _aborted = true;
        _documents.Clear();
        _models.Clear();
        if (_replace is { } replace)
        {
            // The staging collection was never visible under the output's name; dropping it is the
            // whole cleanup. A failure here leaves an orphan the operator can drop, not a wrong result.
            try
            {
                await _database.DropCollectionAsync(replace.Staging, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning("mongodb: output {Output}: the staging collection {Staging} could not be dropped after abort ({Reason}); drop it by hand",
                    _output.Collection, replace.Staging, _redactor.Redact(ex.Message));
            }
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>A merge row's filter: every key column's value, by its path. A null key would match
    /// documents missing the field rather than one row, so it fails the write.</summary>
    private BsonDocument KeyFilter(RecordBatch batch, int row)
    {
        var filter = new BsonDocument();
        for (var k = 0; k < _keyColumns.Length; k++)
        {
            var value = RowDocumentWriter.Value(batch.Column(_keyColumns[k]), row, _output.ObjectIds.Contains(_keyNames[k]));
            if (value.IsBsonNull)
            {
                throw MongoErrors.Fatal($"output '{_output.Collection}': key column '{_keyNames[k]}' is null in row {_rows + 1}; a merge key must be present in every row", _redactor);
            }

            filter[_keyNames[k]] = value;
        }

        return filter;
    }

    private async Task FlushAsync(CancellationToken ct)
    {
        var count = _documents.Count + _models.Count;
        if (count == 0)
        {
            return;
        }

        var context = $"output '{_output.Collection}': writing {count} document(s) into '{TargetCollection}'";
        try
        {
            if (_models.Count > 0)
            {
                await _target.BulkWriteAsync(_models, new BulkWriteOptions { IsOrdered = true }, ct).ConfigureAwait(false);
            }
            else
            {
                await _target.InsertManyAsync(_documents, new InsertManyOptions { IsOrdered = true }, ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, _redactor, context);
        }
        finally
        {
            _documents.Clear();
            _models.Clear();
        }

        _requests++;
    }

    /// <summary>One <c>renameCollection</c> with <c>dropTarget</c>: the server swaps the namespaces
    /// atomically, so readers see the old collection or the new one, never a mix. A failure here
    /// leaves the staging collection in place, named, so the operator can finish or discard the swap.</summary>
    private async Task RenameAsync(ReplacePlan replace, CancellationToken ct)
    {
        try
        {
            await _database.RenameCollectionAsync(replace.Staging, replace.Collection, new RenameCollectionOptions { DropTarget = true }, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, _redactor,
                $"output '{_output.Collection}': renaming '{replace.Staging}' over '{replace.Collection}' (the staging collection holds the full write)");
        }
    }

    private void ThrowIfFinished()
    {
        if (_committed)
        {
            throw new InvalidOperationException("the session is already committed");
        }

        if (_aborted)
        {
            throw new InvalidOperationException("the session is aborted");
        }
    }
}

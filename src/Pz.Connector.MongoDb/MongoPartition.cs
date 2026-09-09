using System.Runtime.CompilerServices;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>The whole dataset as one pz partition: one <c>find</c> cursor over the filter and
/// projection, drained batch by batch in the server's natural order. The cursor keeps the server's
/// idle timeout (ten minutes by default): a cursor reaped while the engine holds the read back
/// fails with CursorNotFound, which is transient, and the engine's retry re-reads. Cancellation is
/// observed per server batch; a cancelled call is rethrown as cancellation, never classified as a
/// transient failure.</summary>
internal sealed class MongoPartition(
    MongoConnectionConfig connection, IMongoCollection<BsonDocument> collection, MongoDatasetConfig dataset, ColumnPlan plan,
    BsonDocument filter, DatasetSpec spec, ILogger logger) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var context = $"dataset '{spec.Dataset}': reading '{dataset.Collection}'";
        var builder = new DocumentBatchBuilder(plan, options, spec.Dataset, connection.Redactor);
        var findOptions = new FindOptions<BsonDocument> { Projection = plan.Projection(), BatchSize = dataset.BatchSize };
        using var cursor = await OpenAsync(findOptions, context, ct).ConfigureAwait(false);
        var pages = 0L;
        var rows = 0L;
        while (await MoveNextAsync(cursor, context, ct).ConfigureAwait(false))
        {
            pages++;
            foreach (var document in cursor.Current)
            {
                builder.Append(document);
                rows++;
                if (builder.TryTakeBatch(out var batch))
                {
                    yield return batch!;
                }
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }

        logger.LogDebug("mongodb: dataset {Dataset}: {Rows} documents in {Pages} server batches", spec.Dataset, rows, pages);
    }

    private async Task<IAsyncCursor<BsonDocument>> OpenAsync(FindOptions<BsonDocument> options, string context, CancellationToken ct)
    {
        try
        {
            return await collection.FindAsync(filter, options, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, connection.Redactor, context);
        }
    }

    private async Task<bool> MoveNextAsync(IAsyncCursor<BsonDocument> cursor, string context, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        try
        {
            return await cursor.MoveNextAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw MongoErrors.Wrap(ex, connection.Redactor, context);
        }
    }
}

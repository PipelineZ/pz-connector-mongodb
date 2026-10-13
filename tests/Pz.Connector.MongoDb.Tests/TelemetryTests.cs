using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Servers;
using MongoDB.Driver.Core.Connections;
using System.Net;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>What the connector reports through <see cref="ConnectorStats"/>. Request counts are taken at
/// the connector's own call sites, because the driver's command-monitoring events fire on driver
/// threads and carry an operation id the connector cannot know before it starts the operation.</summary>
[Collection("mongodb")]
public sealed class TelemetryTests
{
    private readonly MongoFixture _mongo;

    public TelemetryTests(MongoFixture mongo) => _mongo = mongo;

    private ConnectorConfig Config => new(_mongo.ConnectionConfig());

    [Fact]
    public void Connector_declares_ReportsStats()
    {
        Assert.True(new MongoConnector().Capabilities.HasFlag(ConnectorCapabilities.ReportsStats));
    }

    [SkippableFact]
    [Trait("Category", "Docker")]
    public async Task A_three_batch_cursor_read_counts_the_find_and_two_get_mores()
    {
        DockerFacts.SkipUnlessDocker();
        // 25 documents at batch_size 10: the find returns the first page, two getMores return the
        // rest, and the last getMore closes the cursor, so no further round trip follows.
        var collection = await _mongo.SeedAsync(MongoFixture.Rows(25));
        var spec = new DatasetSpec("mongodb", collection, new Dictionary<string, object?>
        {
            ["batch_size"] = 10,
            ["fields"] = new Dictionary<string, object?> { ["id"] = "int32", ["name"] = "string" },
        });

        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            ISourceConnector connector = new MongoConnector();
            await using var source = await connector.OpenAsync(Config, CancellationToken.None);
            foreach (var partition in await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))
            {
                await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
                {
                    batch.Dispose();
                }
            }
        }

        var s = acc.Snapshot();
        Assert.Equal(3, s.Requests);
        Assert.Equal(0, s.Throttled);
        Assert.Contains(new KeyValuePair<string, double>("mongodb.get_mores", 2), s.Extras);
        Assert.Contains(new KeyValuePair<string, double>("mongodb.documents", 25), s.Extras);
        Assert.Equal(StatsDrop.None, acc.Drops);
    }

    [SkippableFact]
    [Trait("Category", "Docker")]
    public async Task A_write_reports_one_request_per_flush_and_the_documents_sent()
    {
        DockerFacts.SkipUnlessDocker();
        var target = MongoFixture.NewName("tel");
        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, true)], null);
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (var i = 0; i < 5; i++)
        {
            ids.Append(i);
            names.Append("n" + i);
        }

        var spec = new OutputSpec("mongodb", target, "append", "fail_on_change", new Dictionary<string, object?> { ["batch_size"] = 2 });
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            ISinkConnector connector = new MongoConnector();
            await using var sink = await connector.OpenAsync(Config, CancellationToken.None);
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            using var batch = new RecordBatch(schema, [ids.Build(), names.Build()], 5);
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        // Flushes at 2, 4 and the final 1 on commit.
        var s = acc.Snapshot();
        Assert.Equal(3, s.Requests);
        Assert.Contains(new KeyValuePair<string, double>("mongodb.documents", 5), s.Extras);
        Assert.Equal(StatsDrop.None, acc.Drops);
    }

    [Fact]
    public void Error_16500_is_counted_as_throttled()
    {
        var acc = new ConnectorStatsAccumulator();
        var redactor = new MongoRedactor([]);
        using (ConnectorStats.Use(acc))
        {
            var wrapped = Assert.IsType<PzConnectorException>(
                MongoErrors.Wrap(Command(16500, "TooManyRequests"), redactor, "context"));
            Assert.Contains("code 16500", wrapped.Message);
        }

        Assert.Equal(1, acc.Snapshot().Throttled);
    }

    [Fact]
    public void Other_command_errors_are_not_throttled()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            MongoErrors.Wrap(Command(26, "NamespaceNotFound"), new MongoRedactor([]), "context");
        }

        Assert.Equal(0, acc.Snapshot().Throttled);
    }

    private static MongoCommandException Command(int code, string name)
    {
        var connection = new ConnectionId(new ServerId(new ClusterId(1), new DnsEndPoint("localhost", 27017)));
        var result = new BsonDocument { ["ok"] = 0, ["code"] = code, ["codeName"] = name, ["errmsg"] = "synthetic" };
        return new MongoCommandException(connection, "synthetic", new BsonDocument("find", "c"), result);
    }
}

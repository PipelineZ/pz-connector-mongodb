using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>TestKit sink contract. The suite writes a fixed (id Int64, name String) schema; the
/// connector stores each row as {_id, id, name}, so read-back reads the documents into the same two
/// columns. Fresh names per test-class instance (xunit instantiates per fact), so facts never see
/// each other's documents.</summary>
[Collection("mongodb")]
[Trait("Category", "Docker")]
public sealed class MongoSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly MongoFixture _mongo;
    private readonly string _append = MongoFixture.NewName("append");
    private readonly string _merge = MongoFixture.NewName("merge");
    private readonly string _replace = MongoFixture.NewName("replace");

    public MongoSinkAcceptance(MongoFixture mongo)
    {
        _mongo = mongo;
        DockerFacts.SkipUnlessDocker();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new MongoConnector();

    protected override ConnectorConfig ValidConfig => new(_mongo.ConnectionConfig());

    protected override OutputSpec SmallOutput => new("mongodb", _append, "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new OutputSpec("mongodb", _merge, "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override Task ResetMergeTargetAsync() => _mongo.DropAsync(_merge);

    protected override OutputSpec? ReplaceOutput => new("mongodb", _replace, "replace", "fail_on_change", new Dictionary<string, object?>());

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        if (!await _mongo.ExistsAsync(spec.Output))
        {
            return [];
        }

        var docs = await _mongo.AllAsync(spec.Output);
        if (docs.Count == 0)
        {
            return [];
        }

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var doc in docs)
        {
            ids.Append(doc["id"].ToInt64());
            names.Append(doc["name"].AsString);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return [new RecordBatch(schema, [ids.Build(), names.Build()], docs.Count)];
    }
}

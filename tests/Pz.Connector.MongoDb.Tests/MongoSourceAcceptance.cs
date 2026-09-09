using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>TestKit source contract against the Testcontainers server. SmallDataset is a collection
/// of 120 ~100-byte documents (>= 100 rows, >= 2 batches under the suite's 4KB target) whose first
/// inferred column is the int <c>id</c> the inclusive-watermark fact treats as the cursor (<c>_id</c>
/// trails). LargeDataset is 150k documents so mid-read cancellation is observable. BoundedWindowDataset
/// seeds ids 0..10. All three collections are seeded once per fixture on first use.</summary>
[Collection("mongodb")]
[Trait("Category", "Docker")]
public sealed class MongoSourceAcceptance : SourceConnectorAcceptanceTests
{
    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static string? _small;
    private static string? _large;
    private static string? _window;
    private readonly MongoFixture _mongo;

    public MongoSourceAcceptance(MongoFixture mongo)
    {
        _mongo = mongo;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override ConnectorConfig ValidConfig => new(_mongo.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("mongodb", _small!, new Dictionary<string, object?>());

    protected override DatasetSpec? LargeDataset => new("mongodb", _large!, new Dictionary<string, object?>());

    protected override DatasetSpec? BoundedWindowDataset =>
        new DatasetSpec("mongodb", _window!, new Dictionary<string, object?>())
        {
            WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7",
        };

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new MongoConnector();

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            _small ??= await _mongo.SeedAsync(MongoFixture.Rows(120), "small");
            _large ??= await _mongo.SeedAsync(MongoFixture.Rows(150_000), "large");
            _window ??= await _mongo.SeedAsync(MongoFixture.Rows(11), "window");
        }
        finally
        {
            Seed.Release();
        }
    }
}

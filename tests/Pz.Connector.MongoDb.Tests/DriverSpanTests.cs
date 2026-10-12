using System.Collections.Concurrent;
using System.Diagnostics;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>Pins the <see cref="ActivitySource"/> the MongoDB driver emits its command spans on, the
/// name <c>Program</c> opts in to, and that the spans carry no filter text.</summary>
[Collection("mongodb")]
[Trait("Category", "Docker")]
public sealed class DriverSpanTests
{
    private readonly MongoFixture _mongo;

    public DriverSpanTests(MongoFixture mongo) => _mongo = mongo;

    [SkippableFact]
    public async Task The_driver_activity_source_name_is_what_we_opt_in_to()
    {
        DockerFacts.SkipUnlessDocker();
        var collection = await _mongo.SeedAsync(MongoFixture.Rows(3));
        var sources = new ConcurrentBag<string>();
        var tags = new ConcurrentDictionary<string, string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s =>
            {
                sources.Add(s.Name);
                return true;
            },
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = a =>
            {
                if (a.Source.Name == MongoDriverTelemetry.ActivitySourceName)
                {
                    foreach (var tag in a.TagObjects)
                    {
                        tags[tag.Key] = tag.Value?.ToString() ?? "";
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        // A client built after the listener is attached, through the connector's own factory.
        var errors = new List<string>();
        var connection = MongoConnectionConfig.Parse(new(_mongo.ConnectionConfig()), errors)!;
        using var client = MongoClientFactory.Create(connection);
        var documents = client.GetDatabase(connection.Database).GetCollection<BsonDocument>(collection);
        using (var cursor = await documents.FindAsync(new BsonDocument("name", "n1")))
        {
            await cursor.ToListAsync();
        }

        Assert.Contains(MongoDriverTelemetry.ActivitySourceName, sources);
        // db.query.text (the command with its filter values) is off unless the client sets
        // TracingOptions.QueryTextMaxLength, which this connector never does; the summary names the
        // operation and namespace only.
        Assert.DoesNotContain("db.query.text", tags.Keys);
        Assert.False(tags.GetValueOrDefault("db.query.summary", "").Contains("n1", StringComparison.Ordinal), "summary: " + tags.GetValueOrDefault("db.query.summary"));
        Assert.Equal("find", tags["db.command.name"]);
    }
}

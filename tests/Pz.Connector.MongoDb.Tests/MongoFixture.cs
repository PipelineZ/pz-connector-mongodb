using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace Pz.Connector.MongoDb.Tests;

[CollectionDefinition("mongodb")]
public sealed class MongoCollection : ICollectionFixture<MongoFixture>;

/// <summary>One <c>mongo:8.0</c> per test run, with the image's root user (so every fact also
/// exercises SCRAM authentication). Collection names are unique per call so facts never share
/// state. Helpers talk to the server with the raw driver, deliberately not through the connector:
/// a fact that used the code under test to seed and verify would prove nothing.</summary>
public sealed class MongoFixture : IAsyncLifetime
{
    public const string Image = "mongo:8.0";
    public const string Database = "pz_tests";
    public const string Username = "pz";
    public const string Password = "pz-test-secret";

    private MongoDbContainer? _container;
    private MongoClient? _client;

    /// <summary>The credential-free uri; <see cref="ConnectionConfig"/> adds the user separately.</summary>
    public string Uri { get; private set; } = "";

    public MongoClient Client => _client ?? throw new InvalidOperationException("the server is not running");

    public IMongoDatabase Db => Client.GetDatabase(Database);

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        // Built here rather than in a field initializer: Build() resolves and pings the docker
        // endpoint, so a constructor that built it would throw before the probe above could no-op --
        // and a collection fixture that throws is a failed fixture, not a skip.
        _container = new MongoDbBuilder(Image).WithUsername(Username).WithPassword(Password).Build();
        await _container.StartAsync().ConfigureAwait(false);
        var url = MongoUrl.Create(_container.GetConnectionString());
        Uri = $"mongodb://{url.Server.Host}:{url.Server.Port}";
        MongoSerialization.EnsureRegistered();
        _client = new MongoClient(_container.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig() => new()
    {
        ["uri"] = Uri, ["database"] = Database, ["username"] = Username, ["password"] = Password,
    };

    public static string NewName(string prefix = "pz") => $"{prefix}_{Guid.NewGuid():N}"[..(prefix.Length + 13)];

    public IMongoCollection<BsonDocument> Collection(string name) => Db.GetCollection<BsonDocument>(name);

    /// <summary>Inserts documents in chunks of 1000 into a fresh or existing collection.</summary>
    public async Task InsertAsync(string collection, IEnumerable<BsonDocument> documents)
    {
        var target = Collection(collection);
        foreach (var chunk in documents.Chunk(1000))
        {
            await target.InsertManyAsync(chunk).ConfigureAwait(false);
        }
    }

    /// <summary>Seeds a fresh collection and returns its name.</summary>
    public async Task<string> SeedAsync(IEnumerable<BsonDocument> documents, string prefix = "pz")
    {
        var name = NewName(prefix);
        await InsertAsync(name, documents).ConfigureAwait(false);
        return name;
    }

    public Task<List<BsonDocument>> AllAsync(string collection) =>
        Collection(collection).Find(new BsonDocument()).Sort(new BsonDocument("_id", 1)).ToListAsync();

    public Task<long> CountAsync(string collection) => Collection(collection).CountDocumentsAsync(new BsonDocument());

    public async Task<bool> ExistsAsync(string collection)
    {
        using var names = await Db.ListCollectionNamesAsync(new ListCollectionNamesOptions { Filter = new BsonDocument("name", collection) }).ConfigureAwait(false);
        return await names.AnyAsync().ConfigureAwait(false);
    }

    public Task DropAsync(string collection) => Db.DropCollectionAsync(collection);

    public async Task<List<string>> IndexNamesAsync(string collection)
    {
        using var cursor = await Collection(collection).Indexes.ListAsync().ConfigureAwait(false);
        var list = await cursor.ToListAsync().ConfigureAwait(false);
        return list.Select(i => i["name"].AsString).Order(StringComparer.Ordinal).ToList();
    }

    /// <summary>Documents <c>{id, name, pad}</c> for <c>id</c> in [0, rows), <c>id</c> an int32.</summary>
    public static IEnumerable<BsonDocument> Rows(int rows, string namePrefix = "n") =>
        Enumerable.Range(0, rows).Select(i => new BsonDocument
        {
            ["id"] = i, ["name"] = namePrefix + i, ["pad"] = new string('x', 80),
        });
}

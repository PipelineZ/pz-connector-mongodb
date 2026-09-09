using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>MongoDB for pz: a collection is a table-shaped dataset typed from a declared or sampled
/// schema and read through one <c>find</c> cursor; a sink output is an insert, an upsert by keys,
/// or a fresh collection renamed over the output.</summary>
public sealed class MongoConnector : IConnector, ISourceConnector, ISinkConnector
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _time;
    private readonly Random _random;

    public MongoConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, TimeProvider.System, Random.Shared)
    {
    }

    internal MongoConnector(ILoggerFactory? loggerFactory, TimeProvider time, Random random)
    {
        MongoSerialization.EnsureRegistered();
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _time = time;
        _random = random;
    }

    public ConnectorInfo Info { get; } = new(
        "mongodb",
        typeof(MongoConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound
        | ConnectorCapabilities.Merge | ConnectorCapabilities.ReplaceWrites;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["uri"], "properties": {
            "uri": { "type": "string" },
            "database": { "type": "string" },
            "username": { "type": "string" },
            "password": { "type": "string" },
            "auth_source": { "type": "string" },
            "timeout": { "type": "integer", "minimum": 1 } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "collection": { "type": "string" },
            "filter": { "type": ["object", "string"] },
            "fields": { "type": "object", "additionalProperties": { "type": "string" } },
            "sample_size": { "type": "integer", "minimum": 1, "maximum": 100000 },
            "batch_size": { "type": "integer", "minimum": 1, "maximum": 100000 },
            "object_ids": { "type": "array", "items": { "type": "string" } } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        MongoConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : new ValidationResult(errors));
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        return ValueTask.FromResult<ISource>(new MongoSource(connection, MongoClientFactory.Create(connection), _loggerFactory.CreateLogger<MongoSource>()));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        return ValueTask.FromResult<ISink>(new MongoSink(connection, MongoClientFactory.Create(connection), _loggerFactory.CreateLogger<MongoSink>(), _time, _random));
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = MongoConnectionConfig.Parse(config, errors);
        if (connection is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        try
        {
            using var client = MongoClientFactory.Create(connection);
            var database = client.GetDatabase(connection.Database);
            // ping proves the credentials against the auth source; buildInfo names the server.
            await database.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1), null, ct).ConfigureAwait(false);
            var info = await database.RunCommandAsync<BsonDocument>(new BsonDocument("buildInfo", 1), null, ct).ConfigureAwait(false);
            var version = info.TryGetValue("version", out var v) ? v.ToString() : "unknown version";
            return new ConnectionCheck(true, $"MongoDB {version}, database '{connection.Database}'");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Every failure is a failed probe, never a crash. Cancellation is not a probe result
            // and still propagates.
            return new ConnectionCheck(false, MongoErrors.Wrap(ex, connection.Redactor, "checking the connection").Message);
        }
    }

    private static MongoConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return MongoConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException("mongodb: invalid connection config: " + string.Join("; ", errors), isTransient: false);
    }
}

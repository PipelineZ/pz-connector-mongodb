using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>The typed connection surface. One connection string (<c>mongodb://</c> or
/// <c>mongodb+srv://</c>); the database; credentials either inside the uri or as
/// <c>username</c>/<c>password</c>, never both; the auth source for the latter. Every credential
/// value is registered with the redactor by content.</summary>
internal sealed record MongoConnectionConfig(
    MongoUrl Url,
    string Database,
    string? Username,
    string? Password,
    string? AuthSource,
    int TimeoutSeconds,
    MongoRedactor Redactor)
{
    public const int DefaultTimeoutSeconds = 30;

    private static readonly string[] KnownKeys = ["uri", "database", "username", "password", "auth_source", "timeout"];

    public static MongoConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys)}");
        }

        MongoUrl? url = null;
        var uriText = config.GetString("uri");
        if (string.IsNullOrWhiteSpace(uriText))
        {
            errors.Add("'uri' is required (mongodb://host:27017 or mongodb+srv://cluster.example.net)");
        }
        else
        {
            try
            {
                url = MongoUrl.Create(uriText);
            }
            catch (Exception ex) when (ex is MongoConfigurationException or FormatException or ArgumentException)
            {
                // The driver's message may echo the whole uri; the redactor is not built yet, so
                // the uri's own password is masked by hand.
                errors.Add($"'uri' is not a valid MongoDB connection string: {MongoRedactor.None.Redact(ex.Message)}");
            }
        }

        if (url?.Password is { Length: > 0 } uriPassword)
        {
            secrets.Add(uriPassword);
        }

        var database = config.GetString("database");
        if (string.IsNullOrWhiteSpace(database))
        {
            database = url?.DatabaseName;
            if (string.IsNullOrWhiteSpace(database))
            {
                errors.Add("'database' is required (or name it in the uri path: mongodb://host/db)");
            }
        }

        var username = config.GetString("username");
        var password = config.GetString("password");
        var hasUser = !string.IsNullOrEmpty(username);
        var hasPassword = !string.IsNullOrEmpty(password);
        if (hasUser != hasPassword)
        {
            errors.Add("'username' and 'password' come together");
        }
        else if (hasUser && url?.Username is { Length: > 0 })
        {
            errors.Add("'username'/'password' and credentials inside the uri are exclusive; set one of them");
        }

        if (hasPassword)
        {
            secrets.Add(password!);
        }

        var authSource = config.GetString("auth_source");
        authSource = string.IsNullOrWhiteSpace(authSource) ? null : authSource;
        if (authSource is not null && !hasUser)
        {
            errors.Add("'auth_source' only applies with 'username'/'password'; inside the uri use ?authSource=");
        }

        var timeoutErrors = new List<string>();
        var timeout = Options.Int(config.Values, "timeout", DefaultTimeoutSeconds, 1, int.MaxValue, "", timeoutErrors);
        if (timeoutErrors.Count > 0)
        {
            errors.Add("'timeout' must be a positive integer number of seconds");
        }

        return errors.Count == start && url is not null && database is not null
            ? new MongoConnectionConfig(url, database, hasUser ? username : null, hasUser ? password : null, authSource, timeout, new MongoRedactor(secrets))
            : null;
    }
}

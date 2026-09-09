using MongoDB.Driver;

namespace Pz.Connector.MongoDb;

/// <summary>One client per opened source or sink, built from the parsed connection. The timeout
/// bounds server selection and connection establishment; a running operation keeps the driver's
/// unbounded socket timeout, because a long cursor read or a large bulk is not a hang.</summary>
internal static class MongoClientFactory
{
    public static MongoClient Create(MongoConnectionConfig connection)
    {
        var settings = MongoClientSettings.FromUrl(connection.Url);
        settings.ApplicationName ??= "pz";
        settings.ServerSelectionTimeout = TimeSpan.FromSeconds(connection.TimeoutSeconds);
        settings.ConnectTimeout = TimeSpan.FromSeconds(connection.TimeoutSeconds);
        if (connection.Username is not null)
        {
            // The default auth source for explicit credentials is 'admin', where MongoDB creates
            // root and most application users; the uri's own ?authSource= wins when set.
            var source = connection.AuthSource ?? connection.Url.AuthenticationSource ?? "admin";
            settings.Credential = MongoCredential.CreateCredential(source, connection.Username, connection.Password);
        }

        return new MongoClient(settings);
    }
}

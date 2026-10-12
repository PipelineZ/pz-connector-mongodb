using Pz.Connector.MongoDb;
using Pz.Connectors.Sdk;

MongoSerialization.EnsureRegistered();
return await PzConnectorHost.RunAsync(
    args,
    ctx => new MongoConnector(ctx.LoggerFactory),
    new PzConnectorHostOptions { ActivitySources = [MongoDriverTelemetry.ActivitySourceName] }).ConfigureAwait(false);

namespace Pz.Connector.MongoDb;

/// <summary>The driver's own <see cref="System.Diagnostics.ActivitySource"/>: one span per command,
/// emitted only while a listener is attached. <c>db.query.text</c> (the command with
/// its filter values) stays off unless the client asks for it, and the SDK strips it regardless.</summary>
internal static class MongoDriverTelemetry
{
    public const string ActivitySourceName = "MongoDB.Driver";
}

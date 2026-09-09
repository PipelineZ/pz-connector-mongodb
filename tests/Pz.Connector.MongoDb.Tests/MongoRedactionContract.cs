using Pz.Connectors.TestKit;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>The TestKit's credential shapes through this connector's redactor, seeded with the same
/// synthetic secret the suite embeds, exactly as a real config seeds it with the password.</summary>
public sealed class MongoRedactionContract : ErrorRedactionContractTests
{
    protected override string RedactErrorText(string thirdPartyMessage) =>
        new MongoRedactor(["pz-testkit-secret-value"]).Redact(thirdPartyMessage);
}

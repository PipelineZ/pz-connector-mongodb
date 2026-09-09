using System.Text.RegularExpressions;

namespace Pz.Connector.MongoDb;

/// <summary>Strips credentials from any text that may reach a PzConnectorException message, a log
/// line, or a ConnectionCheck: every configured secret value is replaced wherever it occurs (the
/// driver echoes the connection string into several of its messages), and the credential-bearing
/// shapes the driver and the environment print -- <c>mongodb://user:password@host</c>, a
/// <c>password=</c> pair -- are rewritten even when the value is not one of ours. Secrets shorter
/// than 3 characters are not matched; replacing them would shred unrelated text.</summary>
internal sealed partial class MongoRedactor
{
    public const string Mask = "***";

    public static readonly MongoRedactor None = new([]);

    private readonly string[] _secrets;

    public MongoRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = secrets.Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
            var escaped = Uri.EscapeDataString(secret);
            if (!string.Equals(escaped, secret, StringComparison.Ordinal))
            {
                text = text.Replace(escaped, Mask, StringComparison.Ordinal);
            }
        }

        text = UriCredentials().Replace(text, m => $"{m.Groups["scheme"].Value}{m.Groups["user"].Value}:{Mask}@");
        return CredentialPair().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
    }

    // mongodb://user:password@ and mongodb+srv://user:password@ -- the password runs to the '@'
    // that ends the userinfo; an unescaped '@' inside a password is not a valid connection string.
    [GeneratedRegex("""(?<scheme>mongodb(?:\+srv)?://)(?<user>[^:/@]*):(?<password>[^@]*)@""", RegexOptions.IgnoreCase)]
    private static partial Regex UriCredentials();

    // password=value / password="quoted value". The unquoted branch excludes ';', ',' and '&' so a
    // "key=value; key2=value2" dump or a query string does not get its separator swallowed.
    [GeneratedRegex("""(?<key>\b(?:password|passwd|pwd|access_key|secret_key)\b)=(?:"[^"]*"|[^\s;,&]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex CredentialPair();
}

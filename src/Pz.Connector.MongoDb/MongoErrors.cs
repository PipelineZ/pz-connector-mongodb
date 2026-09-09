using System.Net.Sockets;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>Turns driver outcomes into the engine's exception, classified for retry. Transient =
/// the server or the network may recover on its own: no connection at all, a server-selection or
/// operation timeout, a primary stepping down or a node recovering, a cursor that expired, a write
/// conflict, and a write-concern failure. Everything about credentials, authorization, namespaces,
/// malformed filters, and duplicate keys is not. Unmapped codes are non-transient: an unknown
/// failure retried is a failure hidden. Messages always pass the redactor -- the driver echoes the
/// connection string into several of its own messages.</summary>
internal static class MongoErrors
{
    /// <summary>Server error codes that a retry can outlive. The driver's own retryable-write list
    /// plus the cursor and lock timeouts a long read can hit.</summary>
    private static readonly HashSet<int> TransientCodes =
    [
        6,      // HostUnreachable
        7,      // HostNotFound
        24,     // LockTimeout
        43,     // CursorNotFound (idle cursor reaped; the engine's retry re-reads)
        46,     // LockBusy
        50,     // MaxTimeMSExpired
        64,     // WriteConcernFailed
        89,     // NetworkTimeout
        91,     // ShutdownInProgress
        112,    // WriteConflict
        133,    // FailedToSatisfyReadPreference
        134,    // ReadConcernMajorityNotAvailableYet
        189,    // PrimarySteppedDown
        262,    // ExceededTimeLimit
        9001,   // SocketException
        10107,  // NotWritablePrimary
        11600,  // InterruptedAtShutdown
        11602,  // InterruptedDueToReplStateChange
        13435,  // NotPrimaryNoSecondaryOk
        13436,  // NotPrimaryOrSecondary
    ];

    public const int DuplicateKey = 11000;

    public static bool IsTransientCode(int code) => TransientCodes.Contains(code);

    /// <summary>Classifies and wraps any exception raised by a driver call. Cancellation is not a
    /// failure and is never wrapped; an exception that is already ours passes through.</summary>
    public static Exception Wrap(Exception ex, MongoRedactor redactor, string context)
    {
        switch (ex)
        {
            case OperationCanceledException:
            case PzConnectorException:
                return ex;
            case MongoAuthenticationException auth:
                // A subclass of MongoConnectionException, so it must be matched first: a wrong
                // password retried is a wrong password.
                return Fatal($"{context}: authentication failed: {auth.Message}; check username, password and auth_source", redactor, ex);
            case MongoConnectionException connection:
                return Transient($"{context}: {connection.Message}", redactor, ex);
            case MongoConnectionPoolPausedException or MongoWaitQueueFullException or MongoDB.Driver.Core.MongoProxyConnectionException:
                // A pool paused after a prior network error, a pool with no free slot, a proxy
                // that dropped the connection: the next attempt gets a fresh connection.
                return Transient($"{context}: {ex.Message}", redactor, ex);
            case MongoConfigurationException configuration:
                return Fatal($"{context}: {configuration.Message}", redactor, ex);
            case MongoExecutionTimeoutException or MongoNodeIsRecoveringException or MongoNotPrimaryException:
                return Transient($"{context}: {ex.Message}", redactor, ex);
            case MongoCursorNotFoundException cursor:
                // The server reaped an idle cursor; the engine's retry re-reads from the start.
                return Transient($"{context}: {cursor.Message} (code 43 CursorNotFound)", redactor, ex);
            case MongoQueryException query:
                return Fatal($"{context}: {query.Message}", redactor, ex);
            case MongoBulkWriteException<MongoDB.Bson.BsonDocument> bulk:
                return FromBulk(bulk, redactor, context);
            case MongoWriteException write:
                return FromWriteError(write.WriteError?.Code, write.WriteError?.Message, write.WriteConcernError is not null, redactor, context, ex);
            case MongoWriteConcernException writeConcern:
                // MongoDuplicateKeyException derives from this one: the code decides, never the type.
                return FromWriteError(writeConcern.Code, writeConcern.Message, true, redactor, context, ex);
            case MongoCommandException command:
                return FromCode(command.Code, command.CodeName, command.ErrorMessage, redactor, context, ex);
            case TimeoutException timeout:
                // Server selection times out, rather than failing, when every hello is rejected
                // for bad credentials; the cluster description inside the message says so.
                return timeout.Message.Contains("MongoAuthenticationException", StringComparison.Ordinal)
                    || timeout.Message.Contains("Authentication failed", StringComparison.Ordinal)
                    ? Fatal($"{context}: authentication failed while selecting a server; check username, password and auth_source", redactor, ex)
                    : Transient($"{context}: {timeout.Message}", redactor, ex);
            case IOException or SocketException:
                return Transient($"{context}: {ex.Message}", redactor, ex);
            default:
                return Fatal($"{context}: {ex.Message}", redactor, ex);
        }
    }

    /// <summary>The one message shape for a server error: <c>mongodb: &lt;context&gt;: &lt;message&gt;
    /// (code n CodeName[; hint])</c>.</summary>
    public static PzConnectorException FromCode(int code, string? codeName, string? message, MongoRedactor redactor, string context,
        Exception? original = null)
    {
        var hint = code switch
        {
            13 => "; the user lacks a role for this operation",
            18 => "; check username, password and auth_source",
            26 => "; create the collection or fix 'collection:'",
            _ => "",
        };
        var name = string.IsNullOrEmpty(codeName) ? "" : $" {codeName}";
        var text = $"{context}: {message ?? "no message"} (code {code}{name}{hint})";
        return IsTransientCode(code) ? Transient(text, redactor, original) : Fatal(text, redactor, original);
    }

    private static PzConnectorException FromBulk(MongoBulkWriteException<MongoDB.Bson.BsonDocument> bulk, MongoRedactor redactor, string context)
    {
        var first = bulk.WriteErrors.Count > 0 ? bulk.WriteErrors[0] : null;
        if (first is null)
        {
            return bulk.WriteConcernError is not null
                ? Transient($"{context}: write concern not satisfied: {bulk.WriteConcernError.Message}", redactor, bulk)
                : Fatal($"{context}: {bulk.Message}", redactor, bulk);
        }

        var count = bulk.WriteErrors.Count;
        return FromWriteError(first.Code, $"{count} document(s) rejected; first (index {first.Index}): {first.Message}", false, redactor, context, bulk);
    }

    private static PzConnectorException FromWriteError(int? code, string? message, bool writeConcern, MongoRedactor redactor, string context,
        Exception original)
    {
        if (code is null or 0)
        {
            return writeConcern
                ? Transient($"{context}: write concern not satisfied: {original.Message}", redactor, original)
                : Fatal($"{context}: {original.Message}", redactor, original);
        }

        var hint = code == DuplicateKey ? "; a unique index on the collection rejects the row, use merge with that key or drop the duplicate" : "";
        var text = $"{context}: {message} (code {code}{hint})";
        return IsTransientCode(code.Value) ? Transient(text, redactor, original) : Fatal(text, redactor, original);
    }

    public static PzConnectorException Fatal(string message, MongoRedactor redactor, Exception? inner = null) =>
        new(Message(redactor, message), isTransient: false, innerException: inner);

    public static PzConnectorException Transient(string message, MongoRedactor redactor, Exception? inner = null) =>
        new(Message(redactor, message), isTransient: true, innerException: inner);

    /// <summary>The connector prefix goes on after redaction: a password that happens to be a
    /// substring of "mongodb" must not shred the one part of the message that is ours.</summary>
    public static string Message(MongoRedactor redactor, string text) => "mongodb: " + redactor.Redact(text);
}

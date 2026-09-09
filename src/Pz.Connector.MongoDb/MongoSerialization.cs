using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Pz.Connector.MongoDb;

/// <summary>The driver's wire protocol resolves a few serializers reflectively: decoding any reply
/// reads <see cref="BsonDefaults.DynamicArraySerializer"/>, whose default is a registry lookup for
/// <c>List&lt;object&gt;</c> that constructs a generic serializer through <c>Activator</c> -- which has
/// no metadata under Native AOT and throws before the first hello completes. Registering those
/// serializers as concrete instances, before any client exists, is what keeps the binary native.
/// Everything else this connector does goes through <see cref="BsonDocument"/>, whose serializer is
/// a static singleton.</summary>
internal static class MongoSerialization
{
    private static readonly object Gate = new();
    private static bool _registered;

    /// <summary>Canonical extended JSON: every BSON type keeps a lossless, unambiguous spelling
    /// (<c>{"$numberLong": "5"}</c>, <c>{"$date": {...}}</c>, <c>{"$oid": "..."}</c>).</summary>
    public static readonly JsonWriterSettings CanonicalJson = new() { OutputMode = JsonOutputMode.CanonicalExtendedJson };

    public static void EnsureRegistered()
    {
        lock (Gate)
        {
            if (_registered)
            {
                return;
            }

            var arrays = new EnumerableInterfaceImplementerSerializer<List<object>, object>(new ObjectSerializer());
            BsonSerializer.TryRegisterSerializer(typeof(List<object>), arrays);
            BsonDefaults.DynamicArraySerializer = arrays;
            BsonDefaults.DynamicDocumentSerializer = new ExpandoObjectSerializer();
            _registered = true;
        }
    }

    /// <summary>Any BSON value as canonical extended JSON text, through the static value serializer
    /// rather than the generic <c>ToJson&lt;T&gt;</c> extension (a registry lookup).</summary>
    public static string ToCanonicalJson(BsonValue value)
    {
        using var text = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        using var writer = new JsonWriter(text, CanonicalJson);
        var context = BsonSerializationContext.CreateRoot(writer);
        BsonValueSerializer.Instance.Serialize(context, value);
        return text.ToString();
    }
}

using System.Globalization;
using System.Text.Json;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb;

/// <summary>Per-dataset read options: <c>collection</c> (defaults to the entity name), <c>filter</c>
/// (a MongoDB query as a YAML mapping or an extended-JSON string), <c>fields</c> (a declared
/// schema, path to type), <c>sample_size</c> (documents inspected when inferring), and
/// <c>batch_size</c> (the cursor's batch size). A declared schema replaces inference entirely.</summary>
internal sealed record MongoDatasetConfig(
    string Collection, BsonDocument? Filter, IReadOnlyList<ColumnSpec>? Fields, int SampleSize, int BatchSize)
{
    public const int DefaultSampleSize = 1000;
    public const int MaxSampleSize = 100_000;
    public const int DefaultBatchSize = 1000;
    public const int MaxBatchSize = 100_000;

    private static readonly string[] KnownKeys = ["collection", "filter", "fields", "sample_size", "batch_size"];

    public static MongoDatasetConfig? Parse(DatasetSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"dataset '{spec.Dataset}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown read option '{key}'; known: {string.Join(", ", KnownKeys)}");
        }

        var collection = spec.Dataset;
        if (spec.Options.TryGetValue("collection", out var collectionRaw))
        {
            collection = collectionRaw?.ToString() ?? "";
            if (collection.Length == 0)
            {
                errors.Add($"{prefix}: 'collection' must be a non-empty string");
            }
        }

        BsonDocument? filter = null;
        if (spec.Options.TryGetValue("filter", out var filterRaw) && filterRaw is not null)
        {
            filter = ParseFilter(filterRaw, prefix, errors);
        }

        IReadOnlyList<ColumnSpec>? fields = null;
        if (spec.Options.TryGetValue("fields", out var fieldsRaw) && fieldsRaw is not null)
        {
            fields = ParseFields(fieldsRaw, prefix, errors);
        }

        var sampleSize = Options.Int(spec.Options, "sample_size", DefaultSampleSize, 1, MaxSampleSize, prefix, errors);
        var batchSize = Options.Int(spec.Options, "batch_size", DefaultBatchSize, 1, MaxBatchSize, prefix, errors);

        return errors.Count == start ? new MongoDatasetConfig(collection, filter, fields, sampleSize, batchSize) : null;
    }

    /// <summary>A YAML mapping becomes the equivalent JSON text and is read back as extended JSON,
    /// so <c>{ placed_at: { $gt: { $date: "2024-01-01T00:00:00Z" } } }</c> written in YAML is the
    /// same typed filter as the JSON string. The query itself is never interpreted here -- the
    /// server reports a malformed operator with its own reason at read time.</summary>
    internal static BsonDocument? ParseFilter(object raw, string prefix, List<string> errors)
    {
        string json;
        if (raw is string text)
        {
            json = text;
        }
        else if (raw is IEnumerable<KeyValuePair<string, object?>>)
        {
            json = YamlJson.Write(raw);
        }
        else
        {
            errors.Add($"{prefix}: 'filter' must be a mapping (a MongoDB query) or an extended-JSON string");
            return null;
        }

        try
        {
            return BsonDocument.Parse(json);
        }
        catch (Exception ex) when (ex is FormatException or MongoDB.Bson.BsonException or JsonException)
        {
            errors.Add($"{prefix}: 'filter' is not a valid extended-JSON document: {ex.Message}");
            return null;
        }
    }

    private static IReadOnlyList<ColumnSpec>? ParseFields(object raw, string prefix, List<string> errors)
    {
        if (raw is not IEnumerable<KeyValuePair<string, object?>> map)
        {
            errors.Add($"{prefix}: 'fields' must be a mapping of field path to type ({ColumnPlan.KindNames})");
            return null;
        }

        var start = errors.Count;
        var columns = new List<ColumnSpec>();
        foreach (var (name, typeRaw) in map)
        {
            if (string.IsNullOrEmpty(name) || name.Split('.').Any(segment => segment.Length == 0))
            {
                errors.Add($"{prefix}: 'fields' has an empty field path");
                continue;
            }

            var typeText = typeRaw?.ToString() ?? "";
            if (!ColumnPlan.TryParseKind(typeText, out var kind))
            {
                errors.Add($"{prefix}: field '{name}' has unknown type '{typeText}'; known: {ColumnPlan.KindNames}");
                continue;
            }

            columns.Add(ColumnSpec.Of(name, kind));
        }

        if (columns.Count == 0 && errors.Count == start)
        {
            errors.Add($"{prefix}: 'fields' must declare at least one field");
        }

        ColumnPlan.ValidateNames(columns.Select(c => c.Name).ToList(), prefix, errors);
        return errors.Count == start ? columns : null;
    }
}

/// <summary>Integer options as they arrive: YAML hands over ints and longs; the out-of-process
/// host's protobuf Struct has only doubles, so an integral double is an integer too.</summary>
internal static class Options
{
    public static int Int(IReadOnlyDictionary<string, object?> options, string option, int fallback, int min, int max, string prefix, List<string> errors)
    {
        if (!options.TryGetValue(option, out var raw) || raw is null)
        {
            return fallback;
        }

        if (raw is not bool && raw is IFormattable f && long.TryParse(f.ToString(null, CultureInfo.InvariantCulture), out var n)
            && n >= min && n <= max)
        {
            return (int)n;
        }

        errors.Add(max == int.MaxValue
            ? $"{prefix}: '{option}' must be an integer of at least {min}"
            : $"{prefix}: '{option}' must be an integer between {min} and {max}");
        return fallback;
    }

    public static List<string>? Strings(IReadOnlyDictionary<string, object?> options, string option, string prefix, List<string> errors)
    {
        if (!options.TryGetValue(option, out var raw) || raw is null)
        {
            return null;
        }

        if (raw is string || raw is not IEnumerable<object?> list)
        {
            errors.Add($"{prefix}: '{option}' must be a list of column names");
            return null;
        }

        var result = new List<string>();
        foreach (var item in list)
        {
            if (item?.ToString() is { Length: > 0 } name)
            {
                result.Add(name);
            }
            else
            {
                errors.Add($"{prefix}: '{option}' entries must be non-empty column names");
            }
        }

        return result;
    }
}

/// <summary>Writes the object graph a YAML loader hands over (mappings, lists, scalars) as JSON.
/// Scalars keep their YAML type: an unquoted 42 is a number, "42" a string.</summary>
internal static class YamlJson
{
    public static string Write(object? value)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteValue(writer, value);
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case double d: writer.WriteNumberValue(d); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case IEnumerable<KeyValuePair<string, object?>> map:
                writer.WriteStartObject();
                foreach (var (k, v) in map)
                {
                    writer.WritePropertyName(k);
                    WriteValue(writer, v);
                }

                writer.WriteEndObject();
                break;
            case IEnumerable<object?> list:
                writer.WriteStartArray();
                foreach (var item in list)
                {
                    WriteValue(writer, item);
                }

                writer.WriteEndArray();
                break;
            case IFormattable f: writer.WriteStringValue(f.ToString(null, CultureInfo.InvariantCulture)); break;
            default: writer.WriteStringValue(value.ToString()); break;
        }
    }
}

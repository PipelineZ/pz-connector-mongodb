using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb.Tests;

public sealed class MongoConnectionConfigTests
{
    private static MongoConnectionConfig? Parse(Dictionary<string, object?> values, out List<string> errors)
    {
        errors = [];
        return MongoConnectionConfig.Parse(new ConnectorConfig(values), errors);
    }

    [Fact]
    public void Uri_and_database_are_enough()
    {
        var config = Parse(new() { ["uri"] = "mongodb://localhost:27017", ["database"] = "shop" }, out var errors);
        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal("shop", config.Database);
        Assert.Null(config.Username);
        Assert.Equal(MongoConnectionConfig.DefaultTimeoutSeconds, config.TimeoutSeconds);
    }

    [Fact]
    public void Database_falls_back_to_the_uri_path()
    {
        var config = Parse(new() { ["uri"] = "mongodb://localhost/shop" }, out var errors);
        Assert.Empty(errors);
        Assert.Equal("shop", config!.Database);
    }

    [Fact]
    public void Missing_uri_and_database_are_both_reported()
    {
        Assert.Null(Parse(new(), out var errors));
        Assert.Contains(errors, e => e.StartsWith("'uri' is required", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.StartsWith("'database' is required", StringComparison.Ordinal));
    }

    [Fact]
    public void Malformed_uri_is_reported_without_echoing_a_password()
    {
        Assert.Null(Parse(new() { ["uri"] = "mongodb://u:hunter22@bad host:abc/db", ["database"] = "d" }, out var errors));
        var error = Assert.Single(errors);
        Assert.StartsWith("'uri' is not a valid MongoDB connection string", error);
        Assert.DoesNotContain("hunter22", error);
    }

    [Fact]
    public void Uri_password_is_a_secret()
    {
        var config = Parse(new() { ["uri"] = "mongodb://user:s3cret-pass@localhost/db" }, out var errors);
        Assert.Empty(errors);
        Assert.Equal("mongodb: mongodb://user:***@localhost failed", MongoErrors.Message(config!.Redactor, "mongodb://user:s3cret-pass@localhost failed"));
    }

    [Fact]
    public void Explicit_credentials_come_together_and_exclude_uri_credentials()
    {
        Assert.Null(Parse(new() { ["uri"] = "mongodb://localhost", ["database"] = "d", ["username"] = "u" }, out var errors));
        Assert.Contains("'username' and 'password' come together", errors);

        Assert.Null(Parse(new() { ["uri"] = "mongodb://a:b@localhost", ["database"] = "d", ["username"] = "u", ["password"] = "p" }, out errors));
        Assert.Contains(errors, e => e.Contains("exclusive", StringComparison.Ordinal));

        var config = Parse(new() { ["uri"] = "mongodb://localhost", ["database"] = "d", ["username"] = "u", ["password"] = "secret-pw", ["auth_source"] = "admin" }, out errors);
        Assert.Empty(errors);
        Assert.Equal("u", config!.Username);
        Assert.Equal("admin", config.AuthSource);
        Assert.Equal("***", config.Redactor.Redact("secret-pw"));
    }

    [Fact]
    public void Auth_source_needs_explicit_credentials()
    {
        Assert.Null(Parse(new() { ["uri"] = "mongodb://localhost", ["database"] = "d", ["auth_source"] = "admin" }, out var errors));
        Assert.Contains(errors, e => e.StartsWith("'auth_source' only applies", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0)]
    [InlineData("soon")]
    [InlineData(-5.0)]
    [InlineData(30.7)]
    [InlineData(true)]
    public void Timeout_must_be_a_positive_integer(object raw)
    {
        Assert.Null(Parse(new() { ["uri"] = "mongodb://localhost", ["database"] = "d", ["timeout"] = raw }, out var errors));
        Assert.Contains("'timeout' must be a positive integer number of seconds", errors);
    }

    [Fact]
    public void Timeout_accepts_the_process_hosts_double()
    {
        var config = Parse(new() { ["uri"] = "mongodb://localhost", ["database"] = "d", ["timeout"] = 45d }, out var errors);
        Assert.Empty(errors);
        Assert.Equal(45, config!.TimeoutSeconds);
    }

    [Fact]
    public void Unknown_keys_are_reported()
    {
        Assert.Null(Parse(new() { ["uri"] = "mongodb://localhost", ["database"] = "d", ["host"] = "x" }, out var errors));
        Assert.Contains(errors, e => e.StartsWith("unknown connection key 'host'", StringComparison.Ordinal));
    }
}

public sealed class MongoDatasetConfigTests
{
    private static MongoDatasetConfig? Parse(Dictionary<string, object?> options, out List<string> errors)
    {
        errors = [];
        return MongoDatasetConfig.Parse(new DatasetSpec("mongodb", "orders", options), errors);
    }

    [Fact]
    public void Defaults_come_from_the_entity_name()
    {
        var dataset = Parse(new(), out var errors);
        Assert.Empty(errors);
        Assert.Equal("orders", dataset!.Collection);
        Assert.Null(dataset.Filter);
        Assert.Null(dataset.Fields);
        Assert.Equal(MongoDatasetConfig.DefaultSampleSize, dataset.SampleSize);
        Assert.Equal(MongoDatasetConfig.DefaultBatchSize, dataset.BatchSize);
    }

    [Fact]
    public void Filter_as_yaml_mapping_reads_extended_json_wrappers()
    {
        var yaml = new Dictionary<string, object?>
        {
            ["status"] = "shipped",
            ["placed_at"] = new Dictionary<string, object?> { ["$gt"] = new Dictionary<string, object?> { ["$date"] = "2024-01-01T00:00:00Z" } },
            ["qty"] = new Dictionary<string, object?> { ["$in"] = new List<object?> { 1, 2L } },
        };
        var dataset = Parse(new() { ["filter"] = yaml }, out var errors);
        Assert.Empty(errors);
        Assert.Equal("shipped", dataset!.Filter!["status"].AsString);
        Assert.Equal(BsonType.DateTime, dataset.Filter["placed_at"]["$gt"].BsonType);
        Assert.Equal(2, dataset.Filter["qty"]["$in"].AsBsonArray.Count);
    }

    [Fact]
    public void Filter_as_json_string()
    {
        var dataset = Parse(new() { ["filter"] = """{"_id": {"$oid": "6aa1ae359e21d6563abe3c91"}}""" }, out var errors);
        Assert.Empty(errors);
        Assert.Equal(BsonType.ObjectId, dataset!.Filter!["_id"].BsonType);
    }

    [Theory]
    [InlineData("{not json")]
    [InlineData(42)]
    public void Malformed_filter_is_reported(object raw)
    {
        Assert.Null(Parse(new() { ["filter"] = raw }, out var errors));
        Assert.Contains(errors, e => e.Contains("'filter'", StringComparison.Ordinal));
    }

    [Fact]
    public void Fields_declare_the_columns_in_order()
    {
        var fields = new Dictionary<string, object?> { ["_id"] = "objectid", ["total"] = "decimal", ["address.city"] = "varchar", ["items"] = "json" };
        var dataset = Parse(new() { ["fields"] = fields }, out var errors);
        Assert.Empty(errors);
        var declared = dataset!.Fields!;
        Assert.Equal(["_id", "total", "address.city", "items"], declared.Select(f => f.Name));
        Assert.Equal([ColumnKind.ObjectId, ColumnKind.Decimal, ColumnKind.String, ColumnKind.Json], declared.Select(f => f.Kind));
        Assert.Equal(["address", "city"], declared[2].Path);
    }

    [Fact]
    public void Fields_reject_unknown_types_prefix_paths_and_case_twins()
    {
        var fields = new Dictionary<string, object?> { ["a"] = "string", ["a.b"] = "int32", ["Name"] = "string", ["name"] = "string", ["price"] = "money" };
        Assert.Null(Parse(new() { ["fields"] = fields }, out var errors));
        Assert.Contains(errors, e => e.Contains("unknown type 'money'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'a' is both a value and the parent of 'a.b'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("differ only by case", StringComparison.Ordinal));
    }

    [Fact]
    public void Fields_must_be_a_mapping()
    {
        Assert.Null(Parse(new() { ["fields"] = new List<object?> { "a" } }, out var errors));
        Assert.Contains(errors, e => e.Contains("'fields' must be a mapping", StringComparison.Ordinal));
    }

    [Fact]
    public void Integer_options_accept_doubles_and_reject_out_of_range()
    {
        var dataset = Parse(new() { ["sample_size"] = 50d, ["batch_size"] = 10 }, out var errors);
        Assert.Empty(errors);
        Assert.Equal(50, dataset!.SampleSize);
        Assert.Equal(10, dataset.BatchSize);

        Assert.Null(Parse(new() { ["sample_size"] = 0, ["batch_size"] = "many" }, out errors));
        Assert.Equal(2, errors.Count);
    }

    [Fact]
    public void Unknown_read_options_are_reported()
    {
        Assert.Null(Parse(new() { ["query"] = "x" }, out var errors));
        Assert.Contains(errors, e => e.StartsWith("dataset 'orders': unknown read option 'query'", StringComparison.Ordinal));
    }

    [Fact]
    public void Columns_option_from_the_engine_parses_with_no_error_and_is_not_listed_as_a_known_key()
    {
        var columns = new Dictionary<string, object?> { ["id"] = "bigint" };
        var dataset = Parse(new() { ["columns"] = columns }, out var errors);
        Assert.Empty(errors);
        Assert.NotNull(dataset);

        Assert.Null(Parse(new() { ["bogus"] = "x" }, out errors));
        var message = Assert.Single(errors);
        Assert.DoesNotContain("columns", message);
    }

    [Fact]
    public void Dataset_config_schema_declares_columns()
    {
        Assert.Contains("\"columns\"", new MongoConnector().DatasetConfigSchema);
    }
}

public sealed class MongoOutputConfigTests
{
    private static readonly Schema Fixed = new(
        [new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);

    private static OutputSpec Spec(string mode, Dictionary<string, object?>? options = null, params string[] keys) =>
        new("mongodb", "orders_out", mode, "fail_on_change", options ?? []) { Keys = keys };

    [Fact]
    public void Defaults_come_from_the_entity_name()
    {
        var errors = new List<string>();
        var output = MongoOutputConfig.Parse(Spec("append"), errors);
        Assert.Empty(errors);
        Assert.Equal("orders_out", output!.Collection);
        Assert.Equal(MongoOutputConfig.DefaultBatchSize, output.BatchSize);
        Assert.Equal(["_id"], output.ObjectIds);
    }

    [Fact]
    public void Options_are_typed()
    {
        var errors = new List<string>();
        var output = MongoOutputConfig.Parse(Spec("merge", new() { ["collection"] = "orders", ["batch_size"] = 5d, ["object_ids"] = new List<object?> { "customer_id" } }, "id"), errors);
        Assert.Empty(errors);
        Assert.Equal("orders", output!.Collection);
        Assert.Equal(5, output.BatchSize);
        Assert.Equal(["customer_id"], output.ObjectIds);
    }

    [Fact]
    public void Bad_mode_name_and_options_aggregate()
    {
        var errors = new List<string>();
        Assert.Null(MongoOutputConfig.Parse(Spec("upsert", new() { ["collection"] = "system.x", ["batch_size"] = 0, ["object_ids"] = "id", ["refresh"] = true }), errors));
        Assert.Equal(5, errors.Count);
    }

    [Fact]
    public void Schema_validation_refuses_types_outside_the_matrix_dollar_names_and_missing_keys()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, false),
            new Field("$bad", StringType.Default, true),
            new Field("blob", BinaryType.Default, true),
            new Field("a", StringType.Default, true),
            new Field("a.b", StringType.Default, true),
        ], null);
        var errors = new List<string>();
        var spec = Spec("merge", new() { ["object_ids"] = new List<object?> { "id" } }, "id", "missing");
        var output = MongoOutputConfig.Parse(spec, errors)!;
        MongoOutputConfig.ValidateSchema(spec, output, schema, errors);
        Assert.Contains(errors, e => e.Contains("column '$bad' is not a field name", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("column 'blob' is Binary", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'a' is both a value and the parent of 'a.b'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("key column 'missing' is not in the pipeline's output", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("object_ids entry 'id' is a Int64 column", StringComparison.Ordinal));
    }

    [Fact]
    public void Merge_needs_keys()
    {
        var errors = new List<string>();
        var spec = Spec("merge");
        var output = MongoOutputConfig.Parse(spec, errors)!;
        MongoOutputConfig.ValidateSchema(spec, output, Fixed, errors);
        Assert.Contains(errors, e => e.Contains("mode merge needs 'keys:'", StringComparison.Ordinal));
    }

    [Fact]
    public void An_undeclared_object_ids_entry_missing_from_the_schema_is_reported_only_when_declared()
    {
        var schema = new Schema([new Field("id", Int64Type.Default, false)], null);

        var declaredErrors = new List<string>();
        var declaredSpec = Spec("append", new() { ["object_ids"] = new List<object?> { "nope" } });
        var declaredOutput = MongoOutputConfig.Parse(declaredSpec, declaredErrors)!;
        MongoOutputConfig.ValidateSchema(declaredSpec, declaredOutput, schema, declaredErrors);
        Assert.Contains(declaredErrors, e => e.Contains("object_ids entry 'nope' is not a column of the pipeline's output", StringComparison.Ordinal));

        var defaultErrors = new List<string>();
        var defaultSpec = Spec("append");
        var defaultOutput = MongoOutputConfig.Parse(defaultSpec, defaultErrors)!;
        MongoOutputConfig.ValidateSchema(defaultSpec, defaultOutput, schema, defaultErrors);
        Assert.DoesNotContain(defaultErrors, e => e.Contains("is not a column of the pipeline's output", StringComparison.Ordinal));
    }
}

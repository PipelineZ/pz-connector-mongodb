using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb.Tests;

public sealed class MongoRedactorTests
{
    [Fact]
    public void Configured_secrets_are_masked_wherever_they_occur()
    {
        var redactor = new MongoRedactor(["s3cret", "ab"]);
        Assert.Equal("*** and *** and ab", redactor.Redact("s3cret and s3cret and ab"));
    }

    [Fact]
    public void Url_encoded_spelling_of_a_secret_is_masked_too()
    {
        var redactor = new MongoRedactor(["p@ss/word"]);
        Assert.Equal("mongodb://u:***@h", redactor.Redact("mongodb://u:p%40ss%2Fword@h"));
    }

    [Theory]
    [InlineData("mongodb://alice:hunter2@db.example:27017/shop", "mongodb://alice:***@db.example:27017/shop")]
    [InlineData("mongodb+srv://alice:hunter2@cluster0.example.net/?retryWrites=true", "mongodb+srv://alice:***@cluster0.example.net/?retryWrites=true")]
    [InlineData("settings: password=hunter2; host=x", "settings: password=***; host=x")]
    [InlineData("Password=\"quoted value\" more", "Password=*** more")]
    [InlineData("?authSource=admin&password=pw&x=1", "?authSource=admin&password=***&x=1")]
    public void Credential_shapes_are_rewritten_even_for_unknown_values(string input, string expected) =>
        Assert.Equal(expected, MongoRedactor.None.Redact(input));

    [Fact]
    public void Prefix_goes_on_after_redaction()
    {
        var redactor = new MongoRedactor(["mongo"]);
        Assert.Equal("mongodb: ***db is down", MongoErrors.Message(redactor, "mongodb is down"));
    }
}

public sealed class SchemaInferenceTests
{
    private static ColumnPlan Plan(params BsonDocument[] documents)
    {
        var inference = new SchemaInference("d", MongoRedactor.None);
        foreach (var document in documents)
        {
            inference.Observe(document);
        }

        return inference.Plan();
    }

    [Fact]
    public void Scalars_flatten_in_first_seen_order_with_id_trailing()
    {
        var plan = Plan(
            new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["name"] = "a", ["address"] = new BsonDocument { ["city"] = "x", ["zip"] = 1 } },
            new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["name"] = "b", ["extra"] = true });
        Assert.Equal(["name", "address.city", "address.zip", "extra", "_id"], plan.Columns.Select(c => c.Name));
        Assert.Equal([ColumnKind.String, ColumnKind.String, ColumnKind.Int32, ColumnKind.Boolean, ColumnKind.ObjectId], plan.Columns.Select(c => c.Kind));
        Assert.False(plan.Schema.FieldsList[^1].IsNullable);
        Assert.True(plan.Schema.FieldsList[0].IsNullable);
    }

    [Fact]
    public void Numerics_widen_and_decimal_wins()
    {
        var plan = Plan(
            new BsonDocument { ["a"] = 1, ["b"] = 1, ["c"] = 1, ["d"] = 1L },
            new BsonDocument { ["a"] = 2L, ["b"] = 2.5, ["c"] = new BsonDecimal128(new Decimal128(3m)), ["d"] = 2 });
        Assert.Equal([ColumnKind.Int64, ColumnKind.Double, ColumnKind.Decimal, ColumnKind.Int64], plan.Columns.Take(4).Select(c => c.Kind));
    }

    [Fact]
    public void Arrays_mixed_kinds_and_exotic_types_land_as_json()
    {
        var plan = Plan(
            new BsonDocument { ["tags"] = new BsonArray { "a" }, ["mixed"] = 1, ["bin"] = new BsonBinaryData([1, 2]), ["re"] = new BsonRegularExpression("x") },
            new BsonDocument { ["mixed"] = "one" });
        Assert.All(plan.Columns.Take(4), c => Assert.Equal(ColumnKind.Json, c.Kind));
    }

    [Fact]
    public void A_field_that_is_a_document_and_a_scalar_collapses_to_json_dropping_its_leaves()
    {
        var plan = Plan(
            new BsonDocument { ["meta"] = new BsonDocument { ["k"] = 1 } },
            new BsonDocument { ["meta"] = "none" });
        Assert.Equal(["meta", "_id"], plan.Columns.Select(c => c.Name));
        Assert.Equal(ColumnKind.Json, plan.Columns[0].Kind);
    }

    [Fact]
    public void An_always_empty_document_is_a_json_column()
    {
        var plan = Plan(new BsonDocument { ["meta"] = new BsonDocument() });
        Assert.Equal(["meta", "_id"], plan.Columns.Select(c => c.Name));
        Assert.Equal(ColumnKind.Json, plan.Columns[0].Kind);
    }

    [Fact]
    public void Nulls_and_missing_carry_no_kind()
    {
        var plan = Plan(new BsonDocument { ["a"] = BsonNull.Value }, new BsonDocument { ["a"] = 5 }, new BsonDocument());
        Assert.Equal(ColumnKind.Int32, plan.Columns[0].Kind);
    }

    [Fact]
    public void Only_null_fields_do_not_become_columns_and_an_unseen_id_is_objectid()
    {
        var plan = Plan(new BsonDocument { ["a"] = BsonNull.Value });
        Assert.Equal(["_id"], plan.Columns.Select(c => c.Name));
        Assert.Equal(ColumnKind.ObjectId, plan.Columns[0].Kind);
    }

    [Fact]
    public void Non_objectid_ids_keep_their_kind()
    {
        Assert.Equal(ColumnKind.Int64, Plan(new BsonDocument { ["_id"] = 5L }).Columns[0].Kind);
        Assert.Equal(ColumnKind.String, Plan(new BsonDocument { ["_id"] = "k" }, new BsonDocument { ["_id"] = ObjectId.GenerateNewId() }).Columns[0].Kind);
    }

    [Fact]
    public void Case_twins_are_refused()
    {
        var ex = Assert.Throws<PzConnectorException>(() => Plan(new BsonDocument { ["Name"] = "a", ["name"] = "b" }));
        Assert.Contains("differ only by case", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Projection_lists_every_column_and_keeps_the_id_for_error_messages()
    {
        var plan = Plan(new BsonDocument { ["a"] = 1, ["b"] = new BsonDocument { ["c"] = 2 } });
        Assert.Equal(new BsonDocument { ["a"] = 1, ["b.c"] = 1, ["_id"] = 1 }, plan.Projection());
        var projected = plan.Project(["b.c"]);
        Assert.Equal(new BsonDocument { ["b.c"] = 1 }, projected.Projection());
        Assert.Same(plan, plan.Project(["nope"]));
    }
}

public sealed class DocumentBatchBuilderTests
{
    private static object? Convert(ColumnKind kind, BsonValue value, string path = "f")
    {
        var plan = new ColumnPlan([ColumnSpec.Of(path, kind)]);
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "d", MongoRedactor.None);
        var document = new BsonDocument();
        var segments = path.Split('.');
        var target = document;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            var next = new BsonDocument();
            target[segments[i]] = next;
            target = next;
        }

        target[segments[^1]] = value;
        return builder.Convert(plan.Columns[0], document);
    }

    [Fact]
    public void Integers_convert_losslessly_across_bson_numerics()
    {
        Assert.Equal(5L, Convert(ColumnKind.Int64, 5));
        Assert.Equal(5L, Convert(ColumnKind.Int64, 5.0));
        Assert.Equal(5L, Convert(ColumnKind.Int64, new BsonDecimal128(new Decimal128(5m))));
        Assert.Equal(5, Convert(ColumnKind.Int32, 5L));
        Assert.Equal(5.0, Convert(ColumnKind.Double, 5));
        Assert.Equal(1.5, Convert(ColumnKind.Double, new BsonDecimal128(new Decimal128(1.5m))));
        Assert.Equal(1.5m, Convert(ColumnKind.Decimal, 1.5));
        Assert.Equal(7m, Convert(ColumnKind.Decimal, 7L));
    }

    [Theory]
    [InlineData(ColumnKind.Int64, 1.5)]
    [InlineData(ColumnKind.Int64, "5")]
    [InlineData(ColumnKind.Int32, 3_000_000_000L)]
    [InlineData(ColumnKind.Double, "1.0")]
    [InlineData(ColumnKind.Boolean, 1)]
    [InlineData(ColumnKind.Timestamp, "2024-01-01")]
    [InlineData(ColumnKind.Date, 5)]
    [InlineData(ColumnKind.ObjectId, 5)]
    internal void Lossy_values_are_refused_naming_field_and_document(ColumnKind kind, object raw)
    {
        var ex = Assert.Throws<PzConnectorException>(() => Convert(kind, BsonValue.Create(raw)));
        Assert.StartsWith("mongodb: dataset 'd': field 'f' of document ? holds", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Decimal_scale_beyond_nine_is_refused_but_trailing_zeros_are_fine()
    {
        Assert.Equal(1.5m, Convert(ColumnKind.Decimal, new BsonDecimal128(Decimal128.Parse("1.500000000000"))));
        var ex = Assert.Throws<PzConnectorException>(() => Convert(ColumnKind.Decimal, new BsonDecimal128(Decimal128.Parse("1.1234567891"))));
        Assert.Contains("more than 9 fraction digits", ex.Message);
    }

    [Fact]
    public void Timestamps_and_dates_come_from_bson_dates()
    {
        var at = new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero);
        Assert.Equal(at, Convert(ColumnKind.Timestamp, new BsonDateTime(at.ToUnixTimeMilliseconds())));
        Assert.Equal(new DateOnly(2024, 3, 5), Convert(ColumnKind.Date, new BsonDateTime(at.ToUnixTimeMilliseconds())));
    }

    [Fact]
    public void Text_is_the_lossless_escape()
    {
        var oid = ObjectId.GenerateNewId();
        Assert.Equal(oid.ToString(), Convert(ColumnKind.String, oid));
        Assert.Equal(oid.ToString(), Convert(ColumnKind.ObjectId, oid));
        Assert.Equal("5", Convert(ColumnKind.String, 5));
        Assert.Equal("true", Convert(ColumnKind.String, true));
        Assert.Equal("2024-03-05T06:07:08.123Z", Convert(ColumnKind.String, new BsonDateTime(new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero).ToUnixTimeMilliseconds())));
        Assert.Equal("""[{ "$numberInt" : "1" }, "a"]""", Convert(ColumnKind.String, new BsonArray { 1, "a" }));
    }

    [Fact]
    public void Json_is_canonical_extended_json()
    {
        Assert.Equal("""{ "n" : { "$numberLong" : "5" } }""", Convert(ColumnKind.Json, new BsonDocument("n", 5L)));
        Assert.Equal("""{ "$numberDecimal" : "1.5" }""", Convert(ColumnKind.Json, new BsonDecimal128(new Decimal128(1.5m))));
        Assert.Equal("\"s\"", Convert(ColumnKind.Json, "s"));
    }

    [Fact]
    public void Missing_null_and_a_scalar_in_the_way_of_a_path_are_null()
    {
        Assert.Null(Convert(ColumnKind.Int64, BsonNull.Value));
        var plan = new ColumnPlan([ColumnSpec.Of("a.b", ColumnKind.Int64)]);
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "d", MongoRedactor.None);
        Assert.Null(builder.Convert(plan.Columns[0], new BsonDocument("a", 5)));
        Assert.Null(builder.Convert(plan.Columns[0], []));
        Assert.Equal(5L, builder.Convert(plan.Columns[0], new BsonDocument("a", new BsonDocument("b", 5))));
    }

    [Fact]
    public void Batches_carry_the_plans_schema()
    {
        var plan = new ColumnPlan([ColumnSpec.Of("n", ColumnKind.Int32), ColumnSpec.Of("_id", ColumnKind.ObjectId)]);
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "d", MongoRedactor.None);
        builder.Append(new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["n"] = 1 });
        builder.Append(new BsonDocument { ["_id"] = ObjectId.GenerateNewId() });
        using var batch = builder.Flush()!;
        Assert.Equal(2, batch.Length);
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.True(batch.Column(0).IsNull(1));
        Assert.Equal(24, ((StringArray)batch.Column(1)).GetString(0).Length);
    }
}

public sealed class RowDocumentWriterTests
{
    private static RecordBatch Batch()
    {
        var schema = new Schema(
        [
            new Field("_id", StringType.Default, true),
            new Field("n", Int32Type.Default, true),
            new Field("big", Int64Type.Default, true),
            new Field("d", DoubleType.Default, true),
            new Field("b", BooleanType.Default, true),
            new Field("amount", new Decimal128Type(38, 9), true),
            new Field("day", Date32Type.Default, true),
            new Field("at", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
            new Field("address.city", StringType.Default, true),
            new Field("address.geo.lat", DoubleType.Default, true),
        ], null);
        var ids = new StringArray.Builder().Append("6aa1ae359e21d6563abe3c91").Append("not-an-oid").AppendNull();
        var n = new Int32Array.Builder().Append(1).Append(2).AppendNull();
        var big = new Int64Array.Builder().Append(1L << 40).AppendNull().AppendNull();
        var d = new DoubleArray.Builder().Append(1.5).AppendNull().AppendNull();
        var b = new BooleanArray.Builder().Append(true).AppendNull().AppendNull();
        var amount = new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append(12345678901234567890.123456789m).AppendNull().AppendNull();
        var day = new Date32Array.Builder().Append(new DateOnly(2024, 3, 5)).AppendNull().AppendNull();
        var at = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"))
            .Append(new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero)).AppendNull().AppendNull();
        var city = new StringArray.Builder().Append("Paris").AppendNull().AppendNull();
        var lat = new DoubleArray.Builder().Append(48.8).AppendNull().AppendNull();
        return new RecordBatch(schema, [ids.Build(), n.Build(), big.Build(), d.Build(), b.Build(), amount.Build(), day.Build(), at.Build(), city.Build(), lat.Build()], 3);
    }

    [Fact]
    public void Every_type_has_its_bson_spelling_and_dotted_columns_nest()
    {
        using var batch = Batch();
        var writer = new RowDocumentWriter(batch.Schema, new HashSet<string> { "_id" });
        var doc = writer.Write(batch, 0);
        Assert.Equal(["_id", "n", "big", "d", "b", "amount", "day", "at", "address"], doc.Names);
        Assert.Equal(ObjectId.Parse("6aa1ae359e21d6563abe3c91"), doc["_id"].AsObjectId);
        Assert.Equal(1, doc["n"].AsInt32);
        Assert.Equal(1L << 40, doc["big"].AsInt64);
        Assert.Equal(1.5, doc["d"].AsDouble);
        Assert.True(doc["b"].AsBoolean);
        Assert.Equal("12345678901234567890.123456789", doc["amount"].AsDecimal128.ToString());
        Assert.Equal(new DateTime(2024, 3, 5, 0, 0, 0, DateTimeKind.Utc), doc["day"].ToUniversalTime());
        Assert.Equal(new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero).ToUnixTimeMilliseconds(), doc["at"].AsBsonDateTime.MillisecondsSinceEpoch);
        Assert.Equal("Paris", doc["address"]["city"].AsString);
        Assert.Equal(48.8, doc["address"]["geo"]["lat"].AsDouble);
    }

    [Fact]
    public void Strings_that_are_not_object_ids_stay_strings_and_nulls_are_bson_null()
    {
        using var batch = Batch();
        var writer = new RowDocumentWriter(batch.Schema, new HashSet<string> { "_id" });
        var doc = writer.Write(batch, 1);
        Assert.Equal("not-an-oid", doc["_id"].AsString);
        Assert.True(doc["big"].IsBsonNull);
        Assert.True(doc["address"]["city"].IsBsonNull);
        var third = writer.Write(batch, 2);
        Assert.True(third["_id"].IsBsonNull);
    }

    [Fact]
    public void Object_id_conversion_is_opt_in_per_column()
    {
        using var batch = Batch();
        var writer = new RowDocumentWriter(batch.Schema, new HashSet<string>());
        Assert.Equal("6aa1ae359e21d6563abe3c91", writer.Write(batch, 0)["_id"].AsString);
    }
}

public sealed class MongoFilterBuilderTests
{
    private static readonly ColumnPlan Plan = new(
    [
        ColumnSpec.Of("n", ColumnKind.Int32), ColumnSpec.Of("d", ColumnKind.Double), ColumnSpec.Of("m", ColumnKind.Decimal),
        ColumnSpec.Of("at", ColumnKind.Timestamp), ColumnSpec.Of("day", ColumnKind.Date), ColumnSpec.Of("s", ColumnKind.String),
        ColumnSpec.Of("j", ColumnKind.Json), ColumnSpec.Of("_id", ColumnKind.ObjectId),
    ]);

    private static DatasetSpec Spec(string cursor, string? lower, string? upper = null, bool inclusive = false) =>
        new DatasetSpec("mongodb", "d", new Dictionary<string, object?>())
        {
            WatermarkCursor = cursor, WatermarkValue = lower, WatermarkUpperBound = upper, WatermarkLowerInclusive = inclusive,
        };

    [Fact]
    public void No_watermark_means_the_users_filter_alone()
    {
        var user = new BsonDocument("s", "x");
        var dataset = new MongoDatasetConfig("c", user, null, 1, 1);
        Assert.Equal(user, MongoFilterBuilder.Build(dataset, Plan, new DatasetSpec("mongodb", "d", new Dictionary<string, object?>()), MongoRedactor.None));
        Assert.Equal(user, MongoFilterBuilder.Build(dataset, Plan, Spec("n", null), MongoRedactor.None));
        Assert.Equal([], MongoFilterBuilder.Build(new MongoDatasetConfig("c", null, null, 1, 1), Plan, Spec("n", null), MongoRedactor.None));
    }

    [Fact]
    public void Bounds_are_typed_from_the_cursor_and_anded_with_the_users_filter()
    {
        var user = new BsonDocument("s", "x");
        var dataset = new MongoDatasetConfig("c", user, null, 1, 1);
        var filter = MongoFilterBuilder.Build(dataset, Plan, Spec("n", "3", "7"), MongoRedactor.None);
        var expected = new BsonDocument("$and", new BsonArray { user, new BsonDocument("n", new BsonDocument { ["$gt"] = 3L, ["$lte"] = 7L }) });
        Assert.Equal(expected, filter);
    }

    [Fact]
    public void Lower_inclusive_flag_switches_the_operator()
    {
        var bounds = MongoFilterBuilder.CursorBounds(Plan, Spec("n", "3", inclusive: true), MongoRedactor.None);
        Assert.Equal(new BsonDocument("n", new BsonDocument("$gte", 3L)), bounds);
    }

    [Theory]
    [InlineData("d", "2.5", BsonType.Double)]
    [InlineData("m", "12345678901234567890.5", BsonType.Decimal128)]
    [InlineData("at", "2024-03-05T06:07:08.123456", BsonType.DateTime)]
    [InlineData("day", "2024-03-05", BsonType.DateTime)]
    public void Each_cursor_kind_has_its_bson_type(string cursor, string value, BsonType expected)
    {
        var bounds = MongoFilterBuilder.CursorBounds(Plan, Spec(cursor, value), MongoRedactor.None)!;
        Assert.Equal(expected, bounds[cursor]["$gt"].BsonType);
    }

    [Fact]
    public void Timestamp_bounds_are_utc()
    {
        var bounds = MongoFilterBuilder.CursorBounds(Plan, Spec("at", "2024-03-05T06:07:08.123456"), MongoRedactor.None)!;
        Assert.Equal(new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero).ToUnixTimeMilliseconds(), bounds["at"]["$gt"].AsBsonDateTime.MillisecondsSinceEpoch);
    }

    [Theory]
    [InlineData("s")]
    [InlineData("j")]
    [InlineData("_id")]
    [InlineData("missing")]
    public void Unboundable_cursors_are_refused_even_before_a_value_exists(string cursor)
    {
        var ex = Assert.Throws<PzConnectorException>(() => MongoFilterBuilder.ValidateCursor(Plan, Spec(cursor, null), MongoRedactor.None));
        Assert.Contains($"watermark cursor '{cursor}'", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void A_bound_that_is_not_of_the_cursors_kind_is_refused()
    {
        var ex = Assert.Throws<PzConnectorException>(() => MongoFilterBuilder.CursorBounds(Plan, Spec("n", "three"), MongoRedactor.None));
        Assert.Contains("watermark bound 'three' is not a int32 value for cursor 'n'", ex.Message);
    }
}

public sealed class MongoErrorsTests
{
    [Fact]
    public void Connection_and_timeouts_are_transient_authentication_is_not()
    {
        var wrapped = (PzConnectorException)MongoErrors.Wrap(new TimeoutException("selecting a server"), MongoRedactor.None, "ctx");
        Assert.True(wrapped.IsTransient);
        Assert.Equal("mongodb: ctx: selecting a server", wrapped.Message);

        var io = (PzConnectorException)MongoErrors.Wrap(new IOException("reset"), MongoRedactor.None, "ctx");
        Assert.True(io.IsTransient);

        var config = (PzConnectorException)MongoErrors.Wrap(new MongoConfigurationException("bad"), MongoRedactor.None, "ctx");
        Assert.False(config.IsTransient);
    }

    [Theory]
    [InlineData(11000, false)]
    [InlineData(13, false)]
    [InlineData(26, false)]
    [InlineData(189, true)]
    [InlineData(10107, true)]
    [InlineData(43, true)]
    [InlineData(112, true)]
    public void Server_codes_classify(int code, bool transient)
    {
        var ex = MongoErrors.FromCode(code, "Name", "boom", MongoRedactor.None, "ctx");
        Assert.Equal(transient, ex.IsTransient);
        Assert.StartsWith($"mongodb: ctx: boom (code {code} Name", ex.Message);
    }

    [Fact]
    public void Hints_name_the_next_step()
    {
        Assert.Contains("check username, password and auth_source", MongoErrors.FromCode(18, "AuthenticationFailed", "x", MongoRedactor.None, "c").Message);
        Assert.Contains("create the collection or fix 'collection:'", MongoErrors.FromCode(26, "NamespaceNotFound", "x", MongoRedactor.None, "c").Message);
    }

    [Fact]
    public void Cancellation_and_own_exceptions_pass_through()
    {
        var cancelled = new OperationCanceledException();
        Assert.Same(cancelled, MongoErrors.Wrap(cancelled, MongoRedactor.None, "c"));
        var own = new PzConnectorException("x", false);
        Assert.Same(own, MongoErrors.Wrap(own, MongoRedactor.None, "c"));
    }

    [Fact]
    public void A_server_selection_timeout_caused_by_bad_credentials_is_not_transient()
    {
        var ex = (PzConnectorException)MongoErrors.Wrap(new TimeoutException("A timeout occurred ... MongoAuthenticationException: Unable to authenticate"), MongoRedactor.None, "c");
        Assert.False(ex.IsTransient);
        Assert.Contains("check username, password and auth_source", ex.Message);
    }

    [Fact]
    public void Messages_are_redacted()
    {
        var ex = (PzConnectorException)MongoErrors.Wrap(new TimeoutException("mongodb://u:pw123@h timed out"), new MongoRedactor(["pw123"]), "c");
        Assert.Equal("mongodb: c: mongodb://u:***@h timed out", ex.Message);
    }
}

public sealed class MongoConnectorTests
{
    [Fact]
    public void Info_and_capabilities()
    {
        var connector = new MongoConnector();
        Assert.Equal("mongodb", connector.Info.Name);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.ColumnPruning));
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.BoundedWindow));
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.InclusiveWatermarkBound));
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.Merge));
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.ReplaceWrites));
        Assert.False(connector.Capabilities.HasFlag(ConnectorCapabilities.NativeScan));
    }

    [Fact]
    public async Task Validate_reports_every_error()
    {
        var result = await new MongoConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?> { ["timeout"] = 0 }), CancellationToken.None);
        Assert.False(result.IsValid);
        Assert.Equal(3, result.Errors.Count);
    }

    [Fact]
    public async Task Check_connection_with_an_invalid_config_is_a_failed_probe()
    {
        var check = await new MongoConnector().CheckConnectionAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.Contains("'uri' is required", check.Message);
    }

    [Fact]
    public async Task Check_connection_to_nowhere_fails_fast_and_redacted()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["uri"] = "mongodb://user:topsecret@127.0.0.1:1/db", ["timeout"] = 1,
        });
        var check = await new MongoConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("mongodb: checking the connection:", check.Message);
        Assert.DoesNotContain("topsecret", check.Message);
    }

    [Fact]
    public async Task Opening_with_an_invalid_config_throws_non_transient()
    {
        ISourceConnector connector = new MongoConnector();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await connector.OpenAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("mongodb: invalid connection config", ex.Message);
    }
}

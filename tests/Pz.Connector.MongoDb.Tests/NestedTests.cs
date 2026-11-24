using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>Three rows (the middle one all-null) over every nested shape: struct, list, fixed-size list,
/// map, and a list of structs.</summary>
internal static class NestedBatch
{
    public const int Rows = 3;

    public static RecordBatch Build()
    {
        var types = new (string Name, string Type)[]
        {
            ("rec", "struct(n int, tags varchar[])"), ("xs", "bigint[]"), ("pair", "int[2]"),
            ("m", "map(varchar, int)"), ("items", "struct(k varchar, v double)[]"),
        };
        var schema = new Schema(
            [new Field("id", Int64Type.Default, false), .. types.Select(t => new Field(t.Name, ColumnContractType.ToArrowType(t.Type, t.Name), true))], null);
        var builder = new ArrowBatchBuilder(schema, 1 << 20);
        builder.AppendRow([0L,
            new Dictionary<string, object?> { ["n"] = 7, ["tags"] = new List<object?> { "a", "b" } },
            new List<object?> { 1L, 2L }, new List<object?> { 1, 2 },
            new Dictionary<object, object?> { ["x"] = 1, ["y"] = null },
            new List<object?> { new Dictionary<string, object?> { ["k"] = "x", ["v"] = 1.5 } }]);
        builder.AppendRow([1L, null, null, null, null, null]);
        builder.AppendRow([2L,
            new Dictionary<string, object?> { ["n"] = null, ["tags"] = new List<object?>() },
            new List<object?>(), new List<object?> { 3, null },
            new Dictionary<object, object?>(), new List<object?>()]);
        return builder.Flush()!;
    }
}

public sealed class NestedUnitTests
{
    private const string Contract = "struct(name varchar, tags varchar[], at timestamp, born date, price decimal, scores map(varchar, bigint), pair int[2])";

    private static ColumnSpec Nested(string text) => ColumnSpec.OfNested("v", ColumnContractType.ToArrowType(text, "v"));

    private static RecordBatch Read(string text, BsonValue value, out object?[] row)
    {
        var plan = new ColumnPlan([Nested(text)]);
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "d", MongoRedactor.None);
        builder.Append(new BsonDocument { ["_id"] = 7, ["v"] = value });
        var batch = builder.Flush()!;
        row = [ArrowCell.GetValue(batch.Column(0), 0)];
        return batch;
    }

    [Fact]
    public void Fields_accept_the_contract_grammar_for_nested_types()
    {
        var errors = new List<string>();
        var dataset = MongoDatasetConfig.Parse(new DatasetSpec("mongodb", "c", new Dictionary<string, object?>
        {
            ["fields"] = new Dictionary<string, object?> { ["_id"] = "objectid", ["tags"] = "varchar[]", ["a.geo"] = "struct(lat double, lon double)", ["m"] = "map(varchar, bigint)" },
        }), errors);
        Assert.Empty(errors);
        var fields = dataset!.Fields!;
        Assert.All(fields.Skip(1), f => Assert.Equal(ColumnKind.Nested, f.Kind));
        Assert.IsType<ListType>(fields[1].NestedType);
        Assert.IsType<StructType>(fields[2].NestedType);
        Assert.Equal(["a", "geo"], fields[2].Path);
        Assert.IsType<MapType>(fields[3].NestedType);
    }

    [Fact]
    public void A_malformed_nested_type_is_still_an_unknown_type()
    {
        var errors = new List<string>();
        Assert.Null(MongoDatasetConfig.Parse(new DatasetSpec("mongodb", "c", new Dictionary<string, object?>
        {
            ["fields"] = new Dictionary<string, object?> { ["x"] = "struct(a" },
        }), errors));
        Assert.Contains(errors, e => e.Contains("field 'x' has unknown type 'struct(a'", StringComparison.Ordinal));
    }

    [Fact]
    public void A_sub_document_reads_into_a_typed_struct_with_dates_and_decimals()
    {
        var at = new DateTime(2024, 3, 5, 6, 7, 8, 123, DateTimeKind.Utc);
        var document = new BsonDocument
        {
            ["name"] = "ada",
            ["tags"] = new BsonArray { "a", BsonNull.Value, "c" },
            ["at"] = new BsonDateTime(at),
            ["born"] = new BsonDateTime(new DateTime(1815, 12, 10, 0, 0, 0, DateTimeKind.Utc)),
            ["price"] = new BsonDecimal128(Decimal128.Parse("12.345000000")),
            ["scores"] = new BsonDocument { ["x"] = 1, ["y"] = 2L },
            ["pair"] = new BsonArray { 1, 2 },
            ["ignored"] = "not in the type",
        };
        using var batch = Read(Contract, document, out var row);
        var fields = Assert.IsType<Dictionary<string, object?>>(row[0]);
        Assert.Equal("ada", fields["name"]);
        Assert.Equal(new List<object?> { "a", null, "c" }, Assert.IsType<List<object?>>(fields["tags"]));
        Assert.Equal(new DateTimeOffset(at), fields["at"]);
        Assert.Equal(new DateOnly(1815, 12, 10), fields["born"]);
        Assert.Equal(12.345m, fields["price"]);
        var scores = Assert.IsType<List<KeyValuePair<object, object?>>>(fields["scores"]);
        Assert.Equal([("x", 1L), ("y", 2L)], scores.Select(e => ((string)e.Key, (long)e.Value!)));
        Assert.Equal(new List<object?> { 1, 2 }, Assert.IsType<List<object?>>(fields["pair"]));
    }

    [Fact]
    public void A_missing_struct_field_is_null_and_a_missing_column_value_is_null()
    {
        using var batch = Read("struct(a bigint, b varchar)", new BsonDocument { ["a"] = 1 }, out var row);
        var fields = Assert.IsType<Dictionary<string, object?>>(row[0]);
        Assert.Equal(1L, fields["a"]);
        Assert.Null(fields["b"]);

        var plan = new ColumnPlan([Nested("varchar[]")]);
        var builder = new DocumentBatchBuilder(plan, BatchOptions.Default, "d", MongoRedactor.None);
        builder.Append(new BsonDocument { ["_id"] = 1 });
        builder.Append(new BsonDocument { ["_id"] = 2, ["v"] = BsonNull.Value });
        using var nulls = builder.Flush()!;
        Assert.True(nulls.Column(0).IsNull(0));
        Assert.True(nulls.Column(0).IsNull(1));
    }

    [Fact]
    public void Arrays_of_documents_read_as_lists_of_structs()
    {
        using var batch = Read("struct(k varchar, v double)[]",
            new BsonArray { new BsonDocument { ["k"] = "a", ["v"] = 1.5 }, new BsonDocument { ["k"] = "b" } }, out var row);
        var items = Assert.IsType<List<object?>>(row[0]);
        Assert.Equal(1.5, ((Dictionary<string, object?>)items[0]!)["v"]);
        Assert.Null(((Dictionary<string, object?>)items[1]!)["v"]);
    }

    [Theory]
    [InlineData("varchar[]", "a string", "where an array is planned")]
    [InlineData("struct(a bigint)", "[1]", "where a struct is planned")]
    [InlineData("map(varchar, bigint)", "[1]", "where a map is planned")]
    [InlineData("bigint[]", "[1.5]", "where an integer is planned")]
    [InlineData("map(bigint, bigint)", "{\"x\": 1}", "which is not a bigint")]
    public void A_value_of_the_wrong_shape_fails_the_read_naming_field_and_document(string type, string json, string expected)
    {
        BsonValue value = json[0] switch
        {
            '[' => BsonSerializer.Deserialize<BsonArray>(json),
            '{' => BsonDocument.Parse(json),
            _ => new BsonString(json),
        };
        var ex = Assert.Throws<PzConnectorException>(() => Read(type, value, out _));
        Assert.Contains("field 'v' of document 7", ex.Message, StringComparison.Ordinal);
        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_sink_accepts_nested_columns_and_refuses_nested_leaves_outside_the_matrix()
    {
        var spec = new OutputSpec("mongodb", "o", "append", "fail_on_change", new Dictionary<string, object?>());
        var output = MongoOutputConfig.Parse(spec, []);
        var ok = new Schema([new Field("rec", ColumnContractType.ToArrowType(Contract, "rec"), true)], null);
        var errors = new List<string>();
        MongoOutputConfig.ValidateSchema(spec, output!, ok, errors);
        Assert.Empty(errors);

        var bad = new Schema([new Field("rec", new ListType(new Field("item", Int16Type.Default, true)), true)], null);
        MongoOutputConfig.ValidateSchema(spec, output!, bad, errors);
        Assert.Contains(errors, e => e.Contains("column 'rec' is List", StringComparison.Ordinal));
    }

    [Fact]
    public void Nested_cells_become_documents_arrays_and_key_text_documents()
    {
        using var batch = NestedBatch.Build();
        var writer = new RowDocumentWriter(batch.Schema, new HashSet<string>());
        var docs = Enumerable.Range(0, batch.Length).Select(r => writer.Write(batch, r)).ToList();

        Assert.All(docs, d => Assert.Equal(["id", "rec", "xs", "pair", "m", "items"], d.Names));
        Assert.Equal(BsonType.Document, docs[0]["rec"].BsonType);
        Assert.Equal(7, docs[0]["rec"]["n"].AsInt32);
        Assert.Equal(BsonType.Array, docs[0]["rec"]["tags"].BsonType);
        Assert.Equal(BsonType.Array, docs[0]["xs"].BsonType);
        Assert.Equal(BsonType.Array, docs[0]["pair"].BsonType);
        Assert.Equal(BsonType.Document, docs[0]["m"].BsonType);
        Assert.Equal(BsonType.Array, docs[0]["items"].BsonType);
        Assert.Equal(BsonType.Null, docs[1]["rec"].BsonType);

        // The doc must be a valid BSON document: serialising it throws on any value the driver rejects.
        Assert.All(docs, d => Assert.NotEmpty(d.ToBson()));
    }

    [Fact]
    public void Leaves_inside_nested_values_keep_their_flat_spellings()
    {
        var type = ColumnContractType.ToArrowType("struct(at timestamp, born date, price decimal, big bigint)", "v");
        var builder = new ArrowBatchBuilder(new Schema([new Field("v", type, true)], null), 1 << 20);
        builder.AppendRow([new Dictionary<string, object?>
        {
            ["at"] = new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero),
            ["born"] = new DateOnly(2024, 3, 5),
            ["price"] = 12345678901234567890.123456789m,
            ["big"] = 1L << 40,
        }]);
        using var batch = builder.Flush()!;
        var doc = new RowDocumentWriter(batch.Schema, new HashSet<string>()).Write(batch, 0)["v"].AsBsonDocument;
        Assert.Equal(new DateTimeOffset(2024, 3, 5, 6, 7, 8, 123, TimeSpan.Zero).ToUnixTimeMilliseconds(), doc["at"].AsBsonDateTime.MillisecondsSinceEpoch);
        Assert.Equal(new DateTimeOffset(2024, 3, 5, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds(), doc["born"].AsBsonDateTime.MillisecondsSinceEpoch);
        Assert.Equal(Decimal128.Parse("12345678901234567890.123456789"), doc["price"].AsDecimal128);
        Assert.Equal(1L << 40, doc["big"].AsInt64);
    }
}

[Collection("mongodb")]
[Trait("Category", "Docker")]
public sealed class NestedDockerFacts
{
    private readonly MongoFixture _mongo;

    public NestedDockerFacts(MongoFixture mongo)
    {
        _mongo = mongo;
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task A_declared_nested_contract_reads_sub_documents_and_arrays_as_typed_values()
    {
        var at = new DateTime(2024, 3, 5, 6, 7, 8, 123, DateTimeKind.Utc);
        var collection = await _mongo.SeedAsync(
        [
            new BsonDocument
            {
                ["_id"] = 1,
                ["address"] = new BsonDocument { ["city"] = "Paris", ["geo"] = new BsonDocument { ["lat"] = 48.8, ["lon"] = 2.35 } },
                ["tags"] = new BsonArray { "x", "y" },
                ["lines"] = new BsonArray { new BsonDocument { ["sku"] = "a", ["at"] = new BsonDateTime(at), ["price"] = new BsonDecimal128(Decimal128.Parse("9.5")) } },
                ["attrs"] = new BsonDocument { ["k"] = 3 },
            },
            new BsonDocument { ["_id"] = 2 },
        ]);
        var spec = new DatasetSpec("mongodb", collection, new Dictionary<string, object?>
        {
            ["fields"] = new Dictionary<string, object?>
            {
                ["_id"] = "int64",
                ["address"] = "struct(city varchar, geo struct(lat double, lon double))",
                ["tags"] = "varchar[]",
                ["lines"] = "struct(sku varchar, at timestamp, price decimal)[]",
                ["attrs"] = "map(varchar, bigint)",
            },
        });

        ISourceConnector connector = new MongoConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(_mongo.ConnectionConfig()), CancellationToken.None);
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        Assert.IsType<StructType>(schema.FieldsList[1].DataType);
        Assert.IsType<ListType>(schema.FieldsList[2].DataType);
        Assert.IsType<MapType>(schema.FieldsList[4].DataType);

        var json = new List<string[]>();
        foreach (var partition in await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    json.Add(Enumerable.Range(1, 4).Select(c => ArrowCell.ToJson(batch.Column(c), r)).ToArray());
                }

                batch.Dispose();
            }
        }

        json.Sort((a, b) => string.CompareOrdinal(a[1], b[1]));
        var row = json.Single(j => j[1] != "null");
        Assert.Equal("{\"city\":\"Paris\",\"geo\":{\"lat\":48.8,\"lon\":2.35}}", row[0]);
        Assert.Equal("[\"x\",\"y\"]", row[1]);
        Assert.Equal("[{\"sku\":\"a\",\"at\":\"2024-03-05T06:07:08.123000Z\",\"price\":9.500000000}]", row[2]);
        Assert.Equal("{\"k\":3}", row[3]);
        Assert.Equal(["null", "null", "null", "null"], json.Single(j => j[1] == "null"));
    }

    [SkippableFact]
    public async Task Nested_columns_are_stored_as_sub_documents_and_arrays()
    {
        var collection = MongoFixture.NewName("nested");
        ISinkConnector connector = new MongoConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(_mongo.ConnectionConfig()), CancellationToken.None);
        var batch = NestedBatch.Build();
        var spec = new OutputSpec("mongodb", collection, "append", "fail_on_change", new Dictionary<string, object?>());
        await using (var session = await sink.BeginWriteAsync(spec, batch.Schema, CancellationToken.None))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var expected = Enumerable.Range(0, batch.Length)
            .Select(r => ArrowCell.ToJson(batch.Column(1), r)).ToList();
        batch.Dispose();

        var docs = await _mongo.AllAsync(collection);
        Assert.Equal(NestedBatch.Rows, docs.Count);
        var first = docs[0];
        Assert.Equal(BsonType.Document, first["rec"].BsonType);
        Assert.Equal(7, first["rec"]["n"].AsInt32);
        Assert.Equal(BsonType.Array, first["xs"].BsonType);
        Assert.Equal(BsonType.Document, first["m"].BsonType);
        Assert.Equal(BsonType.Array, first["items"].BsonType);
        Assert.Equal(BsonType.Null, docs[1]["rec"].BsonType);
        Assert.Equal(expected.Count, docs.Count);
    }
}

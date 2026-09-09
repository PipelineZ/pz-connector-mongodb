using Apache.Arrow;
using Apache.Arrow.Types;
using MongoDB.Bson;
using MongoDB.Driver;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MongoDb.Tests;

/// <summary>The MongoDB-specific behaviour the TestKit contract does not cover: inference over
/// real documents, declared schemas, ObjectId and decimal round trips, typed watermark bounds,
/// replace keeping indexes, merge by <c>_id</c>, and the error messages a misconfiguration earns.</summary>
[Collection("mongodb")]
[Trait("Category", "Docker")]
public sealed class MongoBehaviorFacts
{
    private readonly MongoFixture _mongo;

    public MongoBehaviorFacts(MongoFixture mongo)
    {
        _mongo = mongo;
        DockerFacts.SkipUnlessDocker();
    }

    private ConnectorConfig Config => new(_mongo.ConnectionConfig());

    private static DatasetSpec Dataset(string collection, Dictionary<string, object?>? options = null) =>
        new("mongodb", collection, options ?? []);

    private static OutputSpec Output(string collection, string mode, Dictionary<string, object?>? options = null, params string[] keys) =>
        new OutputSpec("mongodb", collection, mode, "fail_on_change", options ?? []) { Keys = keys };

    private async Task<(Schema Schema, List<object?[]> Rows)> ReadAsync(DatasetSpec spec, ReadHints? hints = null)
    {
        ISourceConnector connector = new MongoConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var rows = new List<object?[]>();
        foreach (var partition in await source.PlanReadAsync(spec, hints ?? ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    rows.Add(batch.Schema.FieldsList.Select((_, c) => Scalar(batch.Column(c), r)).ToArray());
                }

                batch.Dispose();
            }
        }

        return (schema, rows);
    }

    private static object? Scalar(IArrowArray column, int row) => column.IsNull(row) ? null : column switch
    {
        Int32Array a => a.GetValue(row),
        Int64Array a => a.GetValue(row),
        DoubleArray a => a.GetValue(row),
        BooleanArray a => a.GetValue(row),
        Decimal128Array a => a.GetValue(row),
        Date32Array a => a.GetDateOnly(row),
        TimestampArray a => a.GetTimestamp(row),
        StringArray a => a.GetString(row),
        _ => throw new NotSupportedException(column.GetType().Name),
    };

    private async Task WriteAsync(OutputSpec spec, Schema schema, params RecordBatch[] batches)
    {
        ISinkConnector connector = new MongoConnector();
        await using var sink = await connector.OpenAsync(Config, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            batch.Dispose();
        }

        await session.CommitAsync(CancellationToken.None);
    }

    private static readonly Schema IdName = new([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, true)], null);

    private static RecordBatch IdNameBatch(params (long Id, string? Name)[] rows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (id, name) in rows)
        {
            ids.Append(id);
            if (name is null) names.AppendNull(); else names.Append(name);
        }

        return new RecordBatch(IdName, [ids.Build(), names.Build()], rows.Length);
    }

    [SkippableFact]
    public async Task Inference_flattens_nested_documents_and_lands_arrays_as_json()
    {
        var oid = ObjectId.GenerateNewId();
        var at = new DateTime(2024, 3, 5, 6, 7, 8, DateTimeKind.Utc);
        var collection = await _mongo.SeedAsync(
        [
            new BsonDocument
            {
                ["_id"] = oid, ["name"] = "a", ["qty"] = 3, ["total"] = new BsonDecimal128(Decimal128.Parse("12.50")), ["ok"] = true,
                ["placed_at"] = new BsonDateTime(at), ["address"] = new BsonDocument { ["city"] = "Paris", ["geo"] = new BsonDocument("lat", 48.8) },
                ["tags"] = new BsonArray { "x", "y" },
            },
            new BsonDocument { ["_id"] = ObjectId.GenerateNewId(), ["name"] = "b", ["qty"] = 4L },
        ]);

        var (schema, rows) = await ReadAsync(Dataset(collection));
        Assert.Equal(["name", "qty", "total", "ok", "placed_at", "address.city", "address.geo.lat", "tags", "_id"], schema.FieldsList.Select(f => f.Name));
        Assert.Equal(ArrowTypeId.Int64, schema.FieldsList[1].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Decimal128, schema.FieldsList[2].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Timestamp, schema.FieldsList[4].DataType.TypeId);
        Assert.Equal(2, rows.Count);
        var first = rows.Single(r => (string?)r[0] == "a");
        Assert.Equal(3L, first[1]);
        Assert.Equal(12.5m, first[2]);
        Assert.Equal(true, first[3]);
        Assert.Equal(new DateTimeOffset(at), first[4]);
        Assert.Equal("Paris", first[5]);
        Assert.Equal(48.8, first[6]);
        Assert.Equal("""["x", "y"]""", first[7]);
        Assert.Equal(oid.ToString(), first[8]);
        var second = rows.Single(r => (string?)r[0] == "b");
        Assert.Null(second[5]);
        Assert.Null(second[7]);
    }

    [SkippableFact]
    public async Task Declared_fields_replace_inference_and_project_only_themselves()
    {
        var collection = await _mongo.SeedAsync([new BsonDocument { ["a"] = 1, ["b"] = "x", ["c"] = new BsonDocument("d", 2.5) }]);
        var fields = new Dictionary<string, object?> { ["b"] = "string", ["c.d"] = "double", ["a"] = "string" };
        var (schema, rows) = await ReadAsync(Dataset(collection, new() { ["fields"] = fields }));
        Assert.Equal(["b", "c.d", "a"], schema.FieldsList.Select(f => f.Name));
        Assert.Equal(["x", 2.5, "1"], rows.Single());
    }

    [SkippableFact]
    public async Task Column_pruning_narrows_the_projection()
    {
        var collection = await _mongo.SeedAsync(MongoFixture.Rows(3));
        var (_, rows) = await ReadAsync(Dataset(collection), new ReadHints(Columns: ["name"]));
        Assert.All(rows, r => Assert.Single(r));
        Assert.Equal(["n0", "n1", "n2"], rows.Select(r => (string?)r[0]).Order());
    }

    [SkippableFact]
    public async Task Filter_and_timestamp_watermark_bounds_combine()
    {
        var day = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var collection = await _mongo.SeedAsync(Enumerable.Range(0, 10).Select(i => new BsonDocument
        {
            ["n"] = i, ["kind"] = i % 2 == 0 ? "even" : "odd", ["at"] = new BsonDateTime(day.AddHours(i)),
        }));
        var spec = new DatasetSpec("mongodb", collection, new Dictionary<string, object?> { ["filter"] = new Dictionary<string, object?> { ["kind"] = "even" } })
        {
            WatermarkCursor = "at", WatermarkValue = "2024-01-01T02:00:00.000000", WatermarkUpperBound = "2024-01-01T08:00:00.000000",
        };
        var (_, rows) = await ReadAsync(spec);
        Assert.Equal([4, 6, 8], rows.Select(r => (int)r[0]!).Order());

        var inclusive = spec with { WatermarkLowerInclusive = true };
        (_, rows) = await ReadAsync(inclusive);
        Assert.Equal([2, 4, 6, 8], rows.Select(r => (int)r[0]!).Order());
    }

    [SkippableFact]
    public async Task Missing_and_empty_collections_earn_distinct_messages()
    {
        ISourceConnector connector = new MongoConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var missing = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.GetSchemaAsync(Dataset("nope_" + Guid.NewGuid().ToString("N")), CancellationToken.None));
        Assert.Contains("does not exist in database", missing.Message);
        Assert.False(missing.IsTransient);

        var empty = MongoFixture.NewName("empty");
        await _mongo.Db.CreateCollectionAsync(empty);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.GetSchemaAsync(Dataset(empty), CancellationToken.None));
        Assert.Contains("declare the columns under fields:", ex.Message);

        // Declared fields make an empty collection a legitimate empty table.
        var (schema, rows) = await ReadAsync(Dataset(empty, new() { ["fields"] = new Dictionary<string, object?> { ["a"] = "int64" } }));
        Assert.Single(schema.FieldsList);
        Assert.Empty(rows);
    }

    [SkippableFact]
    public async Task A_lossy_value_fails_the_read_naming_field_and_document()
    {
        var collection = await _mongo.SeedAsync([new BsonDocument { ["_id"] = 1, ["n"] = 1 }, new BsonDocument { ["_id"] = 2, ["n"] = 2.5 }]);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await ReadAsync(Dataset(collection, new() { ["fields"] = new Dictionary<string, object?> { ["n"] = "int64" } })));
        Assert.Contains("field 'n' of document 2 holds 2.5 where an integer is planned", ex.Message);
    }

    [SkippableFact]
    public async Task Append_flushes_by_batch_size_and_round_trips_types()
    {
        var target = MongoFixture.NewName("types");
        var schema = new Schema(
        [
            new Field("_id", StringType.Default, true),
            new Field("amount", new Decimal128Type(38, 9), true),
            new Field("day", Date32Type.Default, true),
            new Field("at", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
            new Field("address.city", StringType.Default, true),
        ], null);
        var oids = Enumerable.Range(0, 5).Select(_ => ObjectId.GenerateNewId().ToString()).ToArray();
        var ids = new StringArray.Builder();
        var amounts = new Decimal128Array.Builder(new Decimal128Type(38, 9));
        var days = new Date32Array.Builder();
        var ats = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var cities = new StringArray.Builder();
        for (var i = 0; i < 5; i++)
        {
            ids.Append(oids[i]);
            amounts.Append(1.5m * i);
            days.Append(new DateOnly(2024, 1, 1 + i));
            ats.Append(new DateTimeOffset(2024, 1, 1, i, 0, 0, TimeSpan.Zero));
            cities.Append("c" + i);
        }

        var batch = new RecordBatch(schema, [ids.Build(), amounts.Build(), days.Build(), ats.Build(), cities.Build()], 5);
        await WriteAsync(Output(target, "append", new() { ["batch_size"] = 2 }), schema, batch);

        var docs = await _mongo.AllAsync(target);
        Assert.Equal(5, docs.Count);
        Assert.Equal(oids.Order(), docs.Select(d => d["_id"].AsObjectId.ToString()).Order());
        var doc = docs.Single(d => d["_id"].AsObjectId.ToString() == oids[1]);
        Assert.Equal("1.500000000", doc["amount"].AsDecimal128.ToString());
        Assert.Equal(new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc), doc["day"].ToUniversalTime());
        Assert.Equal(new DateTime(2024, 1, 1, 1, 0, 0, DateTimeKind.Utc), doc["at"].ToUniversalTime());
        Assert.Equal("c1", doc["address"]["city"].AsString);

        // And the source reads the same shape back: the ObjectId as hex, the nested field flattened.
        var (readSchema, rows) = await ReadAsync(Dataset(target));
        Assert.Equal(["amount", "day", "at", "address.city", "_id"], readSchema.FieldsList.Select(f => f.Name));
        Assert.Equal(oids.Order(), rows.Select(r => (string?)r[4]).Order());
    }

    [SkippableFact]
    public async Task Merge_by_object_id_key_updates_in_place()
    {
        var target = MongoFixture.NewName("byid");
        var schema = new Schema([new Field("_id", StringType.Default, false), new Field("name", StringType.Default, true)], null);
        var oid = ObjectId.GenerateNewId().ToString();
        RecordBatch Batch(string name) =>
            new(schema, [new StringArray.Builder().Append(oid).Build(), new StringArray.Builder().Append(name).Build()], 1);

        var spec = Output(target, "merge", null, "_id");
        await WriteAsync(spec, schema, Batch("first"));
        await WriteAsync(spec, schema, Batch("second"));
        var docs = await _mongo.AllAsync(target);
        var doc = Assert.Single(docs);
        Assert.Equal(BsonType.ObjectId, doc["_id"].BsonType);
        Assert.Equal("second", doc["name"].AsString);
    }

    [SkippableFact]
    public async Task Merge_with_a_null_key_fails_the_write()
    {
        var target = MongoFixture.NewName("nullkey");
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);
        var batch = new RecordBatch(schema, [new Int64Array.Builder().AppendNull().Build(), new StringArray.Builder().Append("x").Build()], 1);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await WriteAsync(Output(target, "merge", null, "id"), schema, batch));
        Assert.Contains("key column 'id' is null in row 1", ex.Message);
    }

    [SkippableFact]
    public async Task Replace_keeps_the_outputs_indexes_and_swaps_atomically()
    {
        var target = MongoFixture.NewName("replace");
        await _mongo.InsertAsync(target, [new BsonDocument { ["id"] = 1L, ["name"] = "old" }]);
        await _mongo.Collection(target).Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(
            new BsonDocument("id", 1), new CreateIndexOptions { Unique = true, Name = "id_unique" }));

        await WriteAsync(Output(target, "replace"), IdName, IdNameBatch((7, "new"), (8, "newer")));

        var docs = await _mongo.AllAsync(target);
        Assert.Equal([7L, 8L], docs.Select(d => d["id"].AsInt64));
        Assert.Equal(["_id_", "id_unique"], await _mongo.IndexNamesAsync(target));
        // The unique index is live on the new collection.
        await Assert.ThrowsAnyAsync<MongoException>(async () => await _mongo.InsertAsync(target, [new BsonDocument { ["id"] = 7L }]));
        // No staging collection is left behind.
        using var names = await _mongo.Db.ListCollectionNamesAsync();
        Assert.DoesNotContain((await names.ToListAsync()), n => n.StartsWith(target + ".pz_", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Replace_of_an_empty_write_leaves_an_empty_collection_and_abort_drops_staging()
    {
        var target = MongoFixture.NewName("replace");
        await _mongo.InsertAsync(target, [new BsonDocument { ["id"] = 1L }]);
        await WriteAsync(Output(target, "replace"), IdName);
        Assert.Equal(0, await _mongo.CountAsync(target));

        ISinkConnector connector = new MongoConnector();
        await using var sink = await connector.OpenAsync(Config, CancellationToken.None);
        var session = (MongoWriteSession)await sink.BeginWriteAsync(Output(target, "replace"), IdName, CancellationToken.None);
        Assert.True(await _mongo.ExistsAsync(session.TargetCollection));
        await session.AbortAsync(CancellationToken.None);
        Assert.False(await _mongo.ExistsAsync(session.TargetCollection));
    }

    [SkippableFact]
    public async Task Replace_refuses_a_view()
    {
        var backing = await _mongo.SeedAsync([new BsonDocument("id", 1L)]);
        var view = MongoFixture.NewName("view");
        await _mongo.Db.CreateViewAsync<BsonDocument, BsonDocument>(view, backing, new EmptyPipelineDefinition<BsonDocument>());
        ISinkConnector connector = new MongoConnector();
        await using var sink = await connector.OpenAsync(Config, CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(Output(view, "replace"), IdName, CancellationToken.None));
        Assert.Contains("is a view", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [SkippableFact]
    public async Task Duplicate_key_on_append_is_non_transient_with_a_hint()
    {
        var target = MongoFixture.NewName("dup");
        await _mongo.Db.CreateCollectionAsync(target);
        await _mongo.Collection(target).Indexes.CreateOneAsync(new CreateIndexModel<BsonDocument>(new BsonDocument("id", 1), new CreateIndexOptions { Unique = true }));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await WriteAsync(Output(target, "append"), IdName, IdNameBatch((1, "a"), (1, "b"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("code 11000", ex.Message);
        Assert.Contains("unique index", ex.Message);
    }

    [SkippableFact]
    public async Task Check_connection_reports_the_server_and_a_wrong_password_redacted()
    {
        var connector = new MongoConnector();
        var ok = await connector.CheckConnectionAsync(Config, CancellationToken.None);
        Assert.True(ok.Ok, ok.Message);
        Assert.StartsWith("MongoDB 8.", ok.Message);

        var bad = new Dictionary<string, object?>(_mongo.ConnectionConfig()) { ["password"] = "wrong-password-value", ["timeout"] = 5 };
        var failed = await connector.CheckConnectionAsync(new ConnectorConfig(bad), CancellationToken.None);
        Assert.False(failed.Ok);
        Assert.DoesNotContain("wrong-password-value", failed.Message);
        Assert.StartsWith("mongodb: checking the connection", failed.Message);
    }

    [SkippableFact]
    public async Task Wrong_password_on_read_is_non_transient()
    {
        var bad = new Dictionary<string, object?>(_mongo.ConnectionConfig()) { ["password"] = "wrong-password-value", ["timeout"] = 5 };
        ISourceConnector connector = new MongoConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(bad), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await source.GetSchemaAsync(Dataset("x"), CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.DoesNotContain("wrong-password-value", ex.Message);
    }

    [SkippableFact]
    public async Task Credentials_inside_the_uri_work_too()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["uri"] = _mongo.Uri.Replace("mongodb://", $"mongodb://{MongoFixture.Username}:{MongoFixture.Password}@") + "/?authSource=admin",
            ["database"] = MongoFixture.Database,
        });
        var check = await new MongoConnector().CheckConnectionAsync(config, CancellationToken.None);
        Assert.True(check.Ok, check.Message);
    }

    [SkippableFact]
    public async Task Merge_with_a_dotted_key_upserts_by_the_nested_field()
    {
        var target = MongoFixture.NewName("dotted");
        var schema = new Schema([new Field("address.city", StringType.Default, true), new Field("n", Int64Type.Default, true)], null);
        RecordBatch Batch(params (string City, long N)[] rows)
        {
            var cities = new StringArray.Builder();
            var ns = new Int64Array.Builder();
            foreach (var (city, n) in rows)
            {
                cities.Append(city);
                ns.Append(n);
            }

            return new RecordBatch(schema, [cities.Build(), ns.Build()], rows.Length);
        }

        var spec = Output(target, "merge", null, "address.city");
        await WriteAsync(spec, schema, Batch(("Paris", 1)));
        await WriteAsync(spec, schema, Batch(("Paris", 2), ("Rome", 3)));

        var docs = await _mongo.AllAsync(target);
        Assert.Equal(2, docs.Count);
        var paris = docs.Single(d => d["address"]["city"].AsString == "Paris");
        Assert.Equal(BsonType.Document, paris["address"].BsonType);
        Assert.Equal(2L, paris["n"].AsInt64);
        Assert.False(paris.Contains("address.city"));
        var rome = docs.Single(d => d["address"]["city"].AsString == "Rome");
        Assert.Equal(3L, rome["n"].AsInt64);
    }

    [SkippableFact]
    public async Task Schema_and_read_use_one_sample_even_when_a_document_lands_in_between()
    {
        var collection = await _mongo.SeedAsync(Enumerable.Range(0, 3).Select(_ => new BsonDocument("a", 1)));
        var spec = Dataset(collection);

        ISourceConnector connector = new MongoConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var declared = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;

        await _mongo.InsertAsync(collection, [new BsonDocument { ["a"] = 1, ["b"] = "new" }]);

        var rows = 0;
        foreach (var partition in await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                Assert.Equal(declared.FieldsList.Select(f => f.Name), batch.Schema.FieldsList.Select(f => f.Name));
                rows += batch.Length;
                batch.Dispose();
            }
        }

        Assert.Equal(4, rows);

        // A second source instance samples fresh: the memo lives on the MongoSource, not globally.
        ISourceConnector connector2 = new MongoConnector();
        await using var source2 = await connector2.OpenAsync(Config, CancellationToken.None);
        var resampled = (await source2.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        Assert.Contains("b", resampled.FieldsList.Select(f => f.Name));
    }

    [SkippableFact]
    public async Task Replace_keeps_the_outputs_collation_and_validator()
    {
        var target = MongoFixture.NewName("collate");
        await _mongo.Db.CreateCollectionAsync(target, new CreateCollectionOptions { Collation = new Collation("en", strength: CollationStrength.Secondary) });
        await _mongo.Db.RunCommandAsync<BsonDocument>(new BsonDocument
        {
            ["collMod"] = target,
            ["validator"] = new BsonDocument("$jsonSchema", new BsonDocument { ["bsonType"] = "object", ["required"] = new BsonArray { "id" } }),
        });

        await WriteAsync(Output(target, "replace"), IdName, IdNameBatch((1, "a"), (2, "b")));

        using var listed = await _mongo.Db.ListCollectionsAsync(new ListCollectionsOptions { Filter = new BsonDocument("name", target) });
        var info = await listed.FirstAsync();
        var options = info["options"].AsBsonDocument;
        Assert.Equal("en", options["collation"]["locale"].AsString);
        Assert.True(options.Contains("validator"));

        // A row that violates the validator fails the write, non-transiently; the staging
        // collection the replace never got to rename over the output is dropped on abort.
        var nameOnly = new Schema([new Field("name", StringType.Default, true)], null);
        var nameValues = new StringArray.Builder();
        nameValues.Append("x");
        var badBatch = new RecordBatch(nameOnly, [nameValues.Build()], 1);

        ISinkConnector connector = new MongoConnector();
        await using var sink = await connector.OpenAsync(Config, CancellationToken.None);
        var session = await sink.BeginWriteAsync(Output(target, "replace"), nameOnly, CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await session.WriteBatchAsync(badBatch, CancellationToken.None);
            badBatch.Dispose();
            await session.CommitAsync(CancellationToken.None);
        });
        Assert.False(ex.IsTransient);
        await session.AbortAsync(CancellationToken.None);

        using var namesAfterAbort = await _mongo.Db.ListCollectionNamesAsync();
        Assert.DoesNotContain(await namesAfterAbort.ToListAsync(), n => n.StartsWith(target + ".pz_", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Cursor_not_found_is_reported_transient()
    {
        var collection = await _mongo.SeedAsync(MongoFixture.Rows(5000));
        ISourceConnector connector = new MongoConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var spec = Dataset(collection, new() { ["batch_size"] = 10 });
        var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        // maxRowsPerBatch: 1 forces an Arrow batch after every document, well inside the driver's
        // first 10-document page (batch_size: 10), so the first yielded batch needs no round trip.
        var enumerator = partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None).GetAsyncEnumerator();
        try
        {
            Assert.True(await enumerator.MoveNextAsync());
            enumerator.Current.Dispose();

            // Kills every session on the server but the one issuing the command -- which drops the
            // cursors pinned to them, including the source's still-open find cursor.
            await _mongo.Client.GetDatabase("admin").RunCommandAsync<BsonDocument>(new BsonDocument("killAllSessions", new BsonArray()));

            PzConnectorException? caught = null;
            try
            {
                while (await enumerator.MoveNextAsync())
                {
                    enumerator.Current.Dispose();
                }
            }
            catch (PzConnectorException ex)
            {
                caught = ex;
            }

            if (caught is null)
            {
                Skip.If(true, "killAllSessions did not surface as a CursorNotFound failure on this server; no way to reap the connector's cursor from outside");
                return;
            }

            Assert.True(caught.IsTransient);
        }
        finally
        {
            try
            {
                await enumerator.DisposeAsync();
            }
            catch
            {
                // The killed cursor cannot be closed cleanly server-side; nothing more to do.
            }
        }
    }
}

# Pz.Connector.MongoDb

MongoDB source and sink for [PipelineZ](https://pipelinez.dev) (`pz`), served out of process.
A collection reads as a **table**: the schema is declared or inferred from a sample, documents
stream through one `find` cursor, and the engine's incremental watermarks become a typed range on
the cursor field. A sink output **appends**, **merges** by keys, or **replaces** the collection
atomically.

## Installation

```yaml
# project.yml
connectors:
  - package: Pz.Connector.MongoDb
    version: 0.1.0
```

`pz restore` installs the Native AOT binary for your platform (linux-x64, linux-arm64, osx-arm64,
win-x64) and `pz run` spawns it. Needs pz 0.6.0 or newer. Built on the official `MongoDB.Driver`
3.x; tested against MongoDB 8.0.

| RID | status |
|---|---|
| `linux-x64` | the platform every test and the packaging proof run on |
| `linux-arm64`, `osx-arm64`, `win-x64` | shipped, never exercised by this connector's own CI |

## Connection

```yaml
# connections.yml
docs:
  connector: mongodb
  uri: mongodb://db.example:27017    # required; mongodb:// or mongodb+srv://, any driver option as ?query
  database: shop                      # required unless the uri names it: mongodb://host/shop
  username: ${MONGO_USER}             # optional; when the credentials are not inside the uri
  password: ${MONGO_PASSWORD}
  auth_source: admin                  # optional; the database holding the user (default admin)
  timeout: 30                         # optional; seconds for server selection and connecting
```

Credentials go either inside the uri (`mongodb://user:pass@host/?authSource=admin`) or as
`username`/`password`, not both. Every password is redacted from every error, and the
`mongodb://user:password@` shape is masked even when it is not ours.

## Reading a collection

```yaml
  entities:
    orders:
      read:
        collection: orders          # optional; defaults to the entity name
        filter:                     # optional; a MongoDB query, as YAML or an extended-JSON string
          status: shipped
          placed_at: { $gt: { $date: "2024-01-01T00:00:00Z" } }
        fields:                     # optional; declares the columns and skips inference
          _id: objectid
          total: decimal
          placed_at: timestamp
          address.city: string
          items: json
        sample_size: 1000           # optional; documents inspected when inferring (1..100000)
        batch_size: 1000            # optional; documents per server batch
```

**Declared schema.** `fields:` maps a field path (dotted for nested documents) to a type:
`string`, `objectid`, `int32`, `int64`, `double`, `decimal`, `bool`, `timestamp`, `date`, `json`
(SQL spellings such as `varchar`, `bigint`, `boolean` work too). The columns are exactly those, in
that order; a pipeline that must not change shape when the data does declares them.

**Inferred schema.** Without `fields:`, the first `sample_size` documents matching `filter`, in
ascending `_id`, are inspected. Every scalar field becomes a column named by its path, nested
documents flatten into `parent.child` columns, and `_id` is one trailing column:

| BSON in the sample | column type |
|---|---|
| `string`, `symbol` | varchar |
| `objectId` | varchar, the 24-hex spelling |
| `int32` | integer; `int64` in the mix widens to bigint |
| `int64` | bigint |
| `double`, or any int mixed with a double | double |
| `decimal128`, or any number mixed with one | decimal(38,9) |
| `bool` | boolean |
| `date` | timestamp (UTC, milliseconds) |
| a document | flattened; its leaves are the columns |
| an array, `binary`, `regex`, `timestamp`, `javascript`, `minKey`/`maxKey`, an always-empty document, or a field that is a document in one document and a scalar in another | varchar, the field's canonical extended JSON |

Null and missing values carry no type, so a field that is sometimes absent is simply nullable. Two
field names that differ only by case, which SQL cannot tell apart, are refused; declare one under
`fields:` with another name. A field whose own name contains a dot -- a document literally keyed
`"a.b"` -- is refused, naming the field and the document's `_id`: the column named `a.b` is the
path into a nested document, and the literal field would share that name and lose its values
silently. More than 2000 distinct field paths, declared or seen while sampling, is a refusal too;
a field with dynamic keys (a per-user bag, an attribute dictionary) belongs under `fields:` as
`json`, not as one column per key. An empty collection cannot be inferred from (declare `fields:`),
and a missing collection is reported as missing rather than empty.

**Values.** A document's value must fit its column losslessly: an integral double or decimal fits
an integer column, a `decimal128` with more than nine fraction digits or a non-numeric value in a
number column **fails the read** naming the field and the document's `_id`, rather than landing
truncated or stringified. `string` accepts any scalar in its text spelling, and `json` accepts
anything -- those are the escapes for a field whose type varies. `objectid` accepts an ObjectId or a
string.

**Incremental reads.** Declare the cursor in SQL as for any pz source:

```sql
select * from {{ source('docs', 'orders') }}
where placed_at > {{ watermark('docs', 'orders') }}
```

The cursor must be a numeric, `date`, or `timestamp` column; the bound (and a bounded window's
upper bound) becomes `{ placed_at: { $gt: ISODate(...), $lte: ... } }` typed from the column, and
is combined with your `filter:` under `$and`. Column pruning is honoured through the projection;
there is no SQL predicate pushdown -- `filter:` is the explicit lever for that.

One `find` cursor per read, in the server's natural order, never sorted. The cursor keeps the
server's idle timeout (ten minutes by default): a read the engine holds back longer than that is
reported as transient and retried from the start.

## Writing a collection

```yaml
  entities:
    orders_out:
      write:
        collection: orders          # optional; defaults to the entity name
        strategy: append            # append | merge | replace
        keys: [tenant, order_id]    # merge only
        batch_size: 1000            # optional; documents per insert or bulk request
        object_ids: [_id, customer_id]  # optional; varchar columns whose 24-hex values are written as ObjectId (default [_id])
```

Every row becomes one document of every column, in column order: integers, doubles, and booleans
as themselves, decimals as `decimal128` (every digit), dates as midnight UTC, timestamps as BSON
dates (milliseconds; finer digits are dropped), nulls as `null`. A dotted column name nests:
`address.city` becomes `{ address: { city: ... } }`, the inverse of the source's flattening. A varchar
column listed under `object_ids` whose value is 24 hex characters is written as an ObjectId, so a
`_id` read by this connector round-trips. Columns outside pz's type matrix, names starting with `$`,
and a column that is both a value and the parent of another (`a` and `a.b`) are refused before a
request is sent.

- **`append`**: an ordered `insertMany` per batch; MongoDB assigns `_id` unless the pipeline
  provides one. At-least-once across runs, as for every `append` output.
- **`merge`**: each row is an upserting `replaceOne` whose filter is the key columns' values, so the
  row replaces the document. When `_id` is the key the filter is on `_id`. A null key fails the write.
- **`replace`**: the write goes to a fresh collection named `<collection>.pz_<timestamp>_<suffix>`,
  created up front carrying the output's own collection options (collation, validator, capped
  bounds) and indexes (a bare rename would otherwise drop both with the old collection); commit
  issues one `renameCollection` with `dropTarget`, which the server applies atomically -- readers
  see the old collection or the new one, never a mix. A view of the output name is refused.

A rejected document fails the write with the server's code and message; a duplicate key (`11000`)
is not retried. A failure creating the staging collection leaves nothing behind -- that step is one
atomic command, so a failed one created no collection to drop; a failure after it exists, copying
the output's indexes onto it, drops that collection before the error is reported, and so does a
failure during commit itself, since a session already marked committed can no longer abort. An
aborted (not yet committed) replace drops its staging collection the same way; an aborted append or
merge cannot unsend the requests it already delivered.

## Errors

Every failure is `mongodb: <what was being done>: <message> (code n Name)`. No connection at all,
a paused connection pool, a full wait queue, a dropped proxy connection, server-selection and
operation timeouts, a primary stepping down or a node recovering, a read cursor the server reaped
for sitting idle, and a write conflict are transient and retried by the engine -- each gets a fresh
connection, pool slot, or cursor on the next attempt; credentials, authorization, a missing
collection, and a malformed filter are not. A write error, including a write-concern failure, is
classified by its own server code rather than by exception type, so a duplicate key (`11000`) is
never retried even on a write-concern path, but an unsatisfied write concern with no other code, or
code `64`, is. `pz connector check` runs `ping` and `buildInfo` and reports the server version.

## Native AOT

The connector ships as a native binary. `MongoDB.Driver` publishes with trim/AOT analysis warnings
in paths this connector never takes (class-mapped POCO serialization, LINQ); everything here goes
through `BsonDocument`. One reflective lookup sits on the driver's own wire protocol -- the
dynamic-array serializer every reply decoder consults -- and is registered up front
(`MongoSerialization`), which is what keeps the binary native. The packaged binary is run against a
live server in CI on every change.

## Development

```bash
dotnet build Pz.Connector.MongoDb.slnx -c Release
dotnet test Pz.Connector.MongoDb.slnx -c Release --no-build      # server facts need docker; they SKIP without it
dotnet restore src/Pz.Connector.MongoDb -r linux-x64             # once, on a cold cache
dotnet publish src/Pz.Connector.MongoDb -c Release -r linux-x64 --no-restore
dotnet pack src/Pz.Connector.MongoDb -c Release -o packages      # nupkg with pz.connector.json
```

The docker facts start `mongo:8.0` through Testcontainers with a root user, so authentication is
exercised by every fact. `tests/e2e/` is the pz project CI runs against a packed nupkg. Releases
are tag-triggered (`v*`) and publish to nuget.org through trusted publishing.

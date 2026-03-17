# API reference

## `pgstream.PGStream`

The top-level entry point. One instance per consuming process.

```python
from pgstream import PGStream
```

### Constructor

```python
PGStream(
    dsn: str,
    slot_name: str = "pgstream",
    publication_name: str = "pgstream",
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dsn` | `str` | — | PostgreSQL connection string. The database must have `wal_level = logical`. |
| `slot_name` | `str` | `"pgstream"` | Replication slot name. Must be unique per consuming process. |
| `publication_name` | `str` | `"pgstream"` | Postgres publication name. |

### Configuration methods

#### `watch(*tables) -> PGStream`

Register one or more tables to watch. Must be called before `setup()`.

```python
stream.watch("documents")
stream.watch("documents", "chunks", "embeddings")
```

Returns `self` for method chaining.

---

#### `sink(sink: Sink) -> PGStream`

Attach a vector store sink. The sink is passed as the second argument to `on_change` handlers. Optional — if omitted, the handler receives `None` as the second argument.

```python
stream.sink(QdrantSink(...))
```

Returns `self` for method chaining.

---

#### `on_change(func) -> func`

Decorator to register an async event handler. The handler receives two arguments:

- `event`: a `ChangeEvent`
- `sink`: the attached `Sink`, or `None` if none was attached

```python
@stream.on_change
async def handle(event: ChangeEvent, sink):
    ...
```

Only one handler per instance is supported. Registering a second handler replaces the first (a warning is logged).

---

### Lifecycle methods

#### `async setup() -> None`

Create the replication slot and publication in Postgres. Idempotent — checks `pg_replication_slots` and `pg_publication` before creating anything. Safe to call on every startup.

Raises `ValueError` if no tables have been registered via `watch()`.

---

#### `async start() -> None`

Start the replication loop. Blocks until `stop()` is called or a fatal error occurs in the replication thread.

- Raises `RuntimeError` if no handler is registered or `setup()` was not called.
- Re-raises any exception from the replication thread on return.

---

#### `async stop() -> None`

Gracefully stop the streaming loop. Signals the replication thread to exit, waits up to 10 seconds for it to drain, then closes the sink. Safe to call multiple times.

---

#### `async teardown(drop_slot=True, drop_publication=True) -> None`

Drop the replication slot and/or publication from Postgres. Call this when permanently decommissioning this instance. After `teardown()`, call `setup()` again before the next `start()`.

---

## `pgstream.ChangeEvent`

A single committed row-level change decoded from the Postgres WAL.

```python
from pgstream import ChangeEvent
```

### Fields

| Field | Type | Description |
|---|---|---|
| `operation` | `Literal["insert", "update", "delete", "truncate"]` | The type of change. |
| `schema` | `str` | Postgres schema name (e.g. `"public"`). |
| `table` | `str` | Table name (e.g. `"documents"`). |
| `row` | `dict[str, str \| None]` | New row as `{column: value}`. Values are always strings or `None`. For `DELETE`, contains the deleted row. For `TRUNCATE`, is an empty dict. |
| `old_row` | `dict[str, str \| None] \| None` | Previous row on `UPDATE` or `DELETE` when `REPLICA IDENTITY FULL` is set. `None` otherwise. |
| `lsn` | `str` | WAL position at commit (e.g. `"0/1A3F28"`). |
| `commit_time` | `datetime` | UTC datetime of the transaction commit. |
| `xid` | `int` | Postgres transaction ID. |

### Notes

**Values are always strings.** Cast in your handler:

```python
int(event.row["id"])
float(event.row["price"])
```

---

## `pgstream.sinks.Sink`

Abstract base class for all vector store sinks.

```python
from pgstream.sinks import Sink
```

### Methods

#### `async upsert(id, vector, payload=None) -> None`

Insert or update a vector in the store.

| Parameter | Type | Description |
|---|---|---|
| `id` | `str` | Unique identifier (typically the stringified primary key). |
| `vector` | `list[float]` | Dense float embedding. |
| `payload` | `dict \| None` | Optional metadata stored alongside the vector. |

---

#### `async delete(id) -> None`

Remove a vector from the store by its ID. Must be idempotent — deleting a non-existent ID must not raise.

---

#### `async close() -> None`

Release resources held by the sink (connections, HTTP sessions, etc.). Called automatically by `PGStream.stop()`. Override if your sink holds a long-lived connection. Default implementation is a no-op.

---

## `pgstream.sinks.PgVectorSink`

Writes vectors to a pgvector-enabled Postgres table.

```python
from pgstream.sinks import PgVectorSink
```

### Constructor

```python
PgVectorSink(
    dsn: str,
    table: str = "embeddings",
    dimension: int | None = None,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dsn` | `str` | — | Postgres connection string. Can be the same database or a different one. |
| `table` | `str` | `"embeddings"` | Target table name. |
| `dimension` | `int \| None` | `None` | Informational only — not enforced. |

See [Sinks — pgvector](sinks.md#pgvector) for the required table schema.

---

## `pgstream.sinks.QdrantSink`

Writes vectors to a Qdrant collection.

```python
from pgstream.sinks import QdrantSink
```

### Constructor

```python
QdrantSink(
    url: str = "http://localhost:6333",
    collection_name: str = "pgstream",
    api_key: str | None = None,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `url` | `str` | `"http://localhost:6333"` | Qdrant server URL. |
| `collection_name` | `str` | `"pgstream"` | Target collection name. Must already exist. |
| `api_key` | `str \| None` | `None` | API key for Qdrant Cloud. |

**ID coercion:** Qdrant point IDs must be unsigned integers or UUIDs. pgstream coerces string IDs in this order:

1. Parse as integer (covers `SERIAL` / `BIGSERIAL` PKs).
2. Pass through as-is if it is a valid UUID.
3. SHA-256 hash to a stable `uint63` for all other strings.

See [Sinks — Qdrant](sinks.md#qdrant) for collection setup.

# Configuration

## `PGStream`

```python
PGStream(
    dsn: str,
    slot_name: str = "pgstream",
    publication_name: str = "pgstream",
)
```

| Option | Default | Description |
|---|---|---|
| `dsn` | — | PostgreSQL connection string. Must point to a database with `wal_level = logical`. |
| `slot_name` | `"pgstream"` | Replication slot name. If you run multiple pgstream instances against the same database, each must use a unique `slot_name`. |
| `publication_name` | `"pgstream"` | Postgres publication name. |

---

## Postgres requirements

### `wal_level`

Logical replication requires `wal_level = logical` in `postgresql.conf`. Check the current value:

```sql
SHOW wal_level;
```

To change it, edit `postgresql.conf` and restart Postgres:

```
wal_level = logical
```

This cannot be changed at runtime.

### `max_replication_slots`

Postgres has a limit on the number of replication slots (`max_replication_slots`, default 10). Each pgstream instance uses one slot. If you hit the limit, increase this value in `postgresql.conf` and restart.

### `max_wal_senders`

Each replication connection uses a WAL sender process (`max_wal_senders`, default 10). Increase if needed.

### `wal_sender_timeout`

Postgres closes idle replication connections after `wal_sender_timeout` (default `60s`). pgstream sends keepalive ACKs every 10 seconds to prevent this. If you set `wal_sender_timeout` below 10 seconds, increase it or set it to 0 (disabled).

---

## Handler timeout

The on_change handler has a hard timeout of **30 seconds**. If the handler does not return within 30 seconds, pgstream cancels the future, raises `RuntimeError`, and the replication thread crashes. The event will be replayed on the next startup.

This value is currently not configurable. If your use case requires longer timeouts, open an issue.

---

## Logging

pgstream uses Python's standard `logging` module. Logger names:

| Logger | What it covers |
|---|---|
| `pgstream` | `PGStream` lifecycle (setup, start, stop, thread events) |
| `pgstream.sinks.pgvector` | `PgVectorSink` operations |
| `pgstream.sinks.qdrant` | `QdrantSink` operations |

Configure via the standard `logging` API:

```python
import logging

logging.basicConfig(level=logging.DEBUG)

# Or target pgstream specifically
logging.getLogger("pgstream").setLevel(logging.DEBUG)
```

---

## Environment variables

pgstream does not read any environment variables directly. Pass configuration explicitly:

```python
import os

stream = PGStream(dsn=os.environ["PGSTREAM_DSN"])
```

---

## Multiple tables

```python
stream.watch("documents", "chunks", "images")
```

All watched tables are included in a single publication. Events from all tables go through the same `on_change` handler. Discriminate by `event.table`:

```python
@stream.on_change
async def handle(event: ChangeEvent, sink):
    if event.table == "documents":
        ...
    elif event.table == "chunks":
        ...
```

---

## Multiple processes

Each consumer process must use a unique `slot_name`:

```python
# Process A
PGStream(dsn=DSN, slot_name="pgstream_worker_a")

# Process B
PGStream(dsn=DSN, slot_name="pgstream_worker_b")
```

Both processes will receive all events independently (fan-out). There is no built-in partitioning across consumers.

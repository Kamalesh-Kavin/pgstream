# pgstream

A production-grade Python SDK that watches Postgres tables via **logical replication (CDC)** and syncs row changes to **vector stores** in real time.

```
pip install pgstream
```

---

## What it does

pgstream connects to your Postgres database using the **logical replication protocol** (the same protocol Debezium and pglogical use). Whenever you INSERT, UPDATE, or DELETE a row in a watched table, pgstream decodes the WAL change into a structured `ChangeEvent` Python object and calls your async handler with it.

Your handler is where the application logic lives — typically:
- Embed the new row content with your model of choice
- Upsert the resulting vector into a vector store (pgvector, Qdrant, etc.)
- Delete vectors for deleted rows

pgstream handles all the low-level plumbing: slot management, keepalive ACKs, at-least-once delivery, graceful shutdown.

---

## Quick start

```python
import asyncio
from pgstream import PGStream, ChangeEvent
from pgstream.sinks import QdrantSink

stream = PGStream(dsn="postgresql://user:pass@localhost/db")
stream.watch("documents")
stream.sink(QdrantSink(url="http://localhost:6333", collection_name="docs"))

@stream.on_change
async def handle(event: ChangeEvent, sink):
    if event.operation in ("insert", "update"):
        vector = await my_embed_function(event.row["content"])
        await sink.upsert(
            id=event.row["id"],
            vector=vector,
            payload={"content": event.row["content"]},
        )
    elif event.operation == "delete":
        await sink.delete(event.row["id"])

async def main():
    await stream.setup()   # idempotent — creates slot + publication once
    await stream.start()   # blocks; press Ctrl+C to stop

asyncio.run(main())
```

---

## Installation

```bash
# Core only (replication + event decoding)
pip install pgstream

# With pgvector sink
pip install "pgstream[pgvector]"

# With Qdrant sink
pip install "pgstream[qdrant]"

# Both
pip install "pgstream[all]"
```

**Requirements:**
- Python 3.13+
- Postgres 10+ with `wal_level = logical`
- The user in the DSN must have the `REPLICATION` privilege

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│  Postgres                                                          │
│                                                                    │
│  WAL  ──►  pgoutput plugin  ──►  replication slot  ──►  client    │
└────────────────────────────────────────────────────────────────────┘
                                                          │
                                        psycopg2 replication protocol
                                                          │
┌─────────────────────────────────────────────────────────┼──────────┐
│  pgstream                                               │          │
│                                                         ▼          │
│  ┌──────────────────────────┐    ┌────────────────────────────┐   │
│  │  ReplicationStream       │    │  PgOutputDecoder           │   │
│  │  (background thread)     │───►│  (binary wire format       │   │
│  │                          │    │   parser, pure Python)     │   │
│  │  · psycopg2 blocking     │    └────────────┬───────────────┘   │
│  │    replication loop      │                 │ ChangeEvent        │
│  │  · read_message()        │                 ▼                    │
│  │  · send_feedback (ACK)   │    ┌────────────────────────────┐   │
│  │  · keepalive every 10s   │    │  on_change handler         │   │
│  └──────────────────────────┘    │  (user's async function)   │   │
│            │                     │                            │   │
│  asyncio.run_coroutine_          │  runs in main event loop   │   │
│  threadsafe()  ─────────────────►│  via run_coroutine_        │   │
│                                  │  threadsafe()              │   │
│                                  └────────────┬───────────────┘   │
│                                               │                    │
│                                               ▼                    │
│                                  ┌────────────────────────────┐   │
│                                  │  Sink                      │   │
│                                  │  (PgVectorSink /           │   │
│                                  │   QdrantSink / custom)     │   │
└──────────────────────────────────────────────────────────────────┘
```

### Two connections, two purposes

| Connection | Library | Purpose |
|---|---|---|
| Replication connection | psycopg2 | Streams WAL bytes using the logical replication protocol |
| Normal query connection | asyncpg (in sinks) | Regular SQL: CREATE PUBLICATION, slot management, vector writes |

**Why two libraries?** asyncpg does not implement the logical replication protocol — it is a query client only. psycopg2 is the only Python library with full replication support (`LogicalReplicationConnection`, `start_replication()`, `read_message()`, `send_feedback()`).

### Threading model

The psycopg2 replication loop is blocking. Running it directly in the asyncio event loop would stall all other coroutines. pgstream runs it in a **background daemon thread**. When an event is decoded, `asyncio.run_coroutine_threadsafe()` submits the user's async handler to the main event loop and **blocks the thread** until it completes before ACKing the LSN. This preserves at-least-once delivery.

```
[background thread]             [main event loop]
        │                               │
  decode WAL bytes                      │
        │                               │
  run_coroutine_threadsafe ────────────►│  await handler(event, sink)
        │                               │        │
  future.result()  ◄───────────────────┤  return │
  (blocks thread)                       │
        │
  send_feedback (ACK)
```

---

## Module breakdown

| File | Responsibility |
|---|---|
| `events.py` | `ChangeEvent` dataclass — the single object flowing through the pipeline |
| `decoder.py` | `PgOutputDecoder` — pure Python binary parser for the pgoutput protocol |
| `replication.py` | `SlotManager` (setup/teardown) + `ReplicationStream` (streaming loop) |
| `stream.py` | `PGStream` — top-level user API, threading bridge, lifecycle management |
| `sinks/base.py` | `Sink` abstract base class |
| `sinks/pgvector.py` | `PgVectorSink` — asyncpg-based pgvector reference implementation |
| `sinks/qdrant.py` | `QdrantSink` — qdrant-client-based Qdrant reference implementation |

---

## The `ChangeEvent` object

```python
@dataclass
class ChangeEvent:
    operation:   Literal["insert", "update", "delete", "truncate"]
    schema:      str                          # e.g. "public"
    table:       str                          # e.g. "documents"
    row:         dict[str, str | None]        # new row (text-encoded values)
    old_row:     dict[str, str | None] | None # old row (only with REPLICA IDENTITY FULL)
    lsn:         str                          # WAL position, e.g. "0/1A3F28"
    commit_time: datetime                     # UTC datetime of the transaction
    xid:         int                          # Postgres transaction ID
```

**Column values are always strings.** pgoutput sends all column data text-encoded. pgstream does not coerce types — `event.row["price"]` is `"9.99"`, not `9.99`. Cast in your handler.

---

## Delivery guarantee

pgstream provides **at-least-once delivery**:
- Each LSN is ACKed (via `send_feedback()`) only **after** your handler returns successfully.
- If your handler raises, the LSN is not ACKed. On the next restart, Postgres replays from the last confirmed position.
- This means your handler may be called twice for the same event if it crashes mid-way. Design your sink writes to be **idempotent** (both `PgVectorSink` and `QdrantSink` use upsert semantics by default).

---

## Running the example

```bash
# Postgres must have wal_level = logical
export PGSTREAM_DSN=postgresql://user:pass@localhost:5432/db

uv run python examples/basic_watch.py
```

In a second terminal:
```sql
INSERT INTO documents (content) VALUES ('Hello, pgstream!');
UPDATE documents SET content = 'Updated' WHERE id = 1;
DELETE FROM documents WHERE id = 1;
```

---

## Running tests

```bash
# Unit tests only (no DB required)
uv run pytest tests/test_decoder.py -v

# Integration tests (requires Postgres with wal_level = logical)
export PGSTREAM_DSN=postgresql://user:pass@localhost:5432/db
uv run pytest tests/ -v

# Skip integration tests
uv run pytest tests/ -v -m "not integration"
```

---

## Implementing a custom Sink

```python
from pgstream.sinks import Sink

class MyPineconeSink(Sink):
    async def upsert(self, id: str, vector: list[float], payload: dict | None = None) -> None:
        # write to Pinecone, Weaviate, Milvus, etc.
        ...

    async def delete(self, id: str) -> None:
        ...

    async def close(self) -> None:
        # optional: release HTTP sessions, pools, etc.
        ...
```

---

## Postgres setup

```sql
-- 1. Enable logical replication (in postgresql.conf, then restart)
wal_level = logical

-- 2. Grant replication privilege to your user
ALTER USER myuser REPLICATION;

-- 3. pgstream handles the rest (CREATE PUBLICATION, slot creation) via setup()
```

---

## Key decisions and what I learned

### Why psycopg2 for replication?

asyncpg is widely used for async Postgres in Python, but it only implements the regular query protocol. The logical replication protocol requires a separate connection type (`LogicalReplicationConnection`) and special commands (`start_replication`, `read_message`, `send_feedback`). Only psycopg2 exposes these in Python. psycopg3 does not yet have a stable public API for replication. This was a surprising discovery that shaped the entire architecture.

### Why a background thread?

The psycopg2 replication loop is fundamentally blocking — `select()` and `read_message()` block the calling thread. Running this in the asyncio event loop would freeze all coroutines. The solution is a daemon thread for the replication loop, with `asyncio.run_coroutine_threadsafe()` to bridge back to the event loop for the user's handler. The thread blocks on `future.result()` until the handler completes, preserving the at-least-once delivery guarantee.

### Why text encoding?

pgoutput protocol v1 sends column values as text strings (e.g. `"42"` for an integer column). pgstream deliberately does not coerce types — doing so would require knowing the Postgres OID → Python type mapping, which is complex (timezone-aware datetimes, Decimals, custom enum types, arrays, JSONB...). Keeping values as strings keeps the library simple and lets the user decide on casting.

### The Relation message cache

Before any INSERT/UPDATE/DELETE, Postgres sends a Relation (R) message with the table's OID and column schema. This must be cached — DML messages only contain the OID, not column names. If the cache is missing an OID (edge case: consumer joined mid-stream), we emit a warning and skip the event rather than crashing. The cache is per-connection (OIDs are session-stable).

### ACK timing

`send_feedback(flush_lsn=...)` tells Postgres "I have processed everything up to this LSN, you can discard WAL". We ACK **after** the handler returns, never before. This is the crucial point for at-least-once delivery. We also send keepalive feedback every 10 seconds even when idle — Postgres will kill the replication connection after `wal_sender_timeout` (default 60s) if it hears nothing.

### Idempotent setup

`setup()` checks `pg_replication_slots` and `pg_publication` before creating anything. It is safe to call on every startup. This simplifies deployment — you don't need a migration step to create the slot; just call `setup()` at boot.

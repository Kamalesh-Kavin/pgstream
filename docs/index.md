# pgstream

pgstream is a production-grade Python SDK for Postgres **Change Data Capture (CDC)**. It watches tables via logical replication and syncs row changes to vector stores in real time.

## What it does

pgstream connects to your Postgres database using the logical replication protocol — the same protocol used by Debezium and pglogical. Whenever you `INSERT`, `UPDATE`, or `DELETE` a row in a watched table, pgstream decodes the WAL change into a structured `ChangeEvent` Python object and calls your async handler with it.

Your handler is where the application logic lives:

- Embed the new row content with your model of choice
- Upsert the resulting vector into a vector store (pgvector, Qdrant, etc.)
- Delete vectors for deleted rows

pgstream handles all the low-level plumbing: slot management, keepalive ACKs, at-least-once delivery, graceful shutdown.

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
    await stream.setup()   # idempotent — creates slot + publication
    await stream.start()   # blocks; Ctrl+C to stop

asyncio.run(main())
```

## Documentation

- [Getting started](getting-started.md) — installation, prerequisites, first watch
- [Concepts](concepts.md) — how CDC and logical replication work
- [API reference](api-reference.md) — full API docs
- [Sinks](sinks.md) — pgvector and Qdrant setup and usage
- [Building a custom sink](building-a-sink.md) — implement your own vector store backend
- [Delivery guarantees](delivery-guarantees.md) — at-least-once semantics, WAL ACK, replica identity
- [Configuration](configuration.md) — all configuration options

## Installation

```bash
pip install pgstream                  # core only
pip install "pgstream[pgvector]"      # + pgvector sink
pip install "pgstream[qdrant]"        # + Qdrant sink
pip install "pgstream[all]"           # all extras
```

## Requirements

- Python 3.13+
- Postgres 10+ with `wal_level = logical`
- The database user must have the `REPLICATION` privilege

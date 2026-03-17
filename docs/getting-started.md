# Getting started

## Prerequisites

### Python

pgstream requires Python 3.13+.

### Postgres

Your Postgres instance must have logical replication enabled. Check the current setting:

```sql
SHOW wal_level;
```

If the result is not `logical`, set it in `postgresql.conf` and restart Postgres:

```
wal_level = logical
```

### Replication privilege

The user in your DSN must have the `REPLICATION` privilege:

```sql
ALTER USER myuser REPLICATION;
```

pgstream also needs `CREATE` on the database (to create a publication) and `SELECT` on the watched tables.

---

## Installation

```bash
# Core only — replication loop + event decoding, no vector store dependencies
pip install pgstream

# With the pgvector sink (requires asyncpg)
pip install "pgstream[pgvector]"

# With the Qdrant sink (requires qdrant-client)
pip install "pgstream[qdrant]"

# All optional dependencies
pip install "pgstream[all]"
```

With `uv`:

```bash
uv add pgstream
uv add "pgstream[all]"
```

---

## Your first watch

### 1. Prepare the table

```sql
-- The table you want to watch must exist
CREATE TABLE documents (
    id      SERIAL PRIMARY KEY,
    content TEXT NOT NULL
);
```

If you want `old_row` to be populated on UPDATE and DELETE, enable full replica identity on the table:

```sql
ALTER TABLE documents REPLICA IDENTITY FULL;
```

### 2. Write the handler

```python
import asyncio
import logging
from pgstream import PGStream, ChangeEvent

logging.basicConfig(level=logging.INFO)

stream = PGStream(dsn="postgresql://user:pass@localhost/mydb")
stream.watch("documents")

@stream.on_change
async def handle(event: ChangeEvent, sink):
    print(f"{event.operation.upper()} {event.schema}.{event.table}")
    print(f"  row: {event.row}")

async def main():
    await stream.setup()
    await stream.start()

asyncio.run(main())
```

### 3. Run it

```bash
python watch.py
```

### 4. Trigger some changes

In a separate Postgres session:

```sql
INSERT INTO documents (content) VALUES ('Hello, pgstream!');
UPDATE documents SET content = 'Updated' WHERE id = 1;
DELETE FROM documents WHERE id = 1;
```

You will see output like:

```
INSERT public.documents
  row: {'id': '1', 'content': 'Hello, pgstream!'}
UPDATE public.documents
  row: {'id': '1', 'content': 'Updated'}
DELETE public.documents
  row: {'id': '1', 'content': 'Updated'}
```

---

## Connecting a sink

Attach a vector store sink so your handler can upsert vectors:

```python
from pgstream import PGStream, ChangeEvent
from pgstream.sinks import QdrantSink

stream = PGStream(dsn="postgresql://user:pass@localhost/mydb")
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
```

See [Sinks](sinks.md) for setup instructions and options for each built-in sink.

---

## Stopping cleanly

Press `Ctrl+C`. pgstream installs a `SIGINT` handler that calls `stop()` and waits for the replication thread to drain before closing the sink connection.

Alternatively, call `stop()` programmatically:

```python
asyncio.create_task(stream.stop())
```

---

## Teardown

To permanently remove the replication slot and publication from Postgres:

```python
await stream.teardown()
```

Call this when decommissioning the process entirely. After `teardown()`, call `setup()` again before the next `start()`.

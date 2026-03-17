# Sinks

pgstream ships with two built-in sinks. Both implement the `Sink` abstract base class and use upsert semantics, making them safe for at-least-once delivery.

---

## pgvector

`PgVectorSink` writes vectors into a pgvector-enabled Postgres table using asyncpg.

### Installation

```bash
pip install "pgstream[pgvector]"
```

### Postgres setup

```sql
-- Enable the pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create the target table
-- Adjust VECTOR(1536) to match your model's output dimension
CREATE TABLE embeddings (
    id      TEXT PRIMARY KEY,
    vector  VECTOR(1536),
    payload JSONB
);
```

### Usage

```python
from pgstream.sinks import PgVectorSink

sink = PgVectorSink(
    dsn="postgresql://user:pass@localhost/mydb",
    table="embeddings",
)
```

The sink can write to any Postgres database — it does not have to be the same one you are replicating from.

### Connection pooling

`PgVectorSink` uses an asyncpg connection pool (min 1, max 5 connections). The pool is created lazily on the first `upsert()` or `delete()` call. Call `close()` (or let `PGStream.stop()` call it) to drain the pool cleanly.

### SQL generated

`upsert()` issues:

```sql
INSERT INTO {table} (id, vector, payload)
VALUES ($1, $2::vector, $3::jsonb)
ON CONFLICT (id) DO UPDATE
    SET vector  = EXCLUDED.vector,
        payload = EXCLUDED.payload
```

`delete()` issues:

```sql
DELETE FROM {table} WHERE id = $1
```

Both operations are idempotent.

---

## Qdrant

`QdrantSink` writes vectors to a Qdrant collection using the official `qdrant-client` async client.

### Installation

```bash
pip install "pgstream[qdrant]"
```

### Qdrant setup

The collection must exist before using the sink. pgstream does not auto-create collections because the vector dimension and distance metric are model-specific decisions that belong to the caller.

```python
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

client = QdrantClient("http://localhost:6333")
client.create_collection(
    collection_name="documents",
    vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
)
```

Or with Docker:

```bash
docker run -p 6333:6333 qdrant/qdrant
```

### Usage

```python
from pgstream.sinks import QdrantSink

sink = QdrantSink(
    url="http://localhost:6333",
    collection_name="documents",
)
```

For Qdrant Cloud:

```python
sink = QdrantSink(
    url="https://your-cluster.qdrant.io",
    collection_name="documents",
    api_key="your-api-key",
)
```

### ID coercion

Qdrant point IDs must be unsigned integers or UUIDs. pgstream receives string IDs from Postgres and coerces them in this order:

| Input example | Result |
|---|---|
| `"42"` | `42` (integer) — covers `SERIAL` / `BIGSERIAL` PKs |
| `"550e8400-e29b-41d4-a716-446655440000"` | UUID string — passed through |
| `"some-arbitrary-string"` | `uint63` from SHA-256 hash — stable, collision-resistant |

The hash-based coercion is deterministic: the same string always maps to the same integer, so upserts remain idempotent.

---

## Choosing a sink

| | pgvector | Qdrant |
|---|---|---|
| Storage | Postgres | Dedicated vector DB |
| Setup | `CREATE EXTENSION vector` | Separate service |
| Joins | Yes — standard SQL | No |
| Filtering | SQL WHERE | Qdrant payload filters |
| Scalability | Limited by Postgres | Designed for large-scale ANN |
| Best for | Small-medium datasets, existing Postgres stack | Large-scale similarity search |

If you are already running Postgres and your dataset is under a few million vectors, pgvector is the simpler choice. For larger datasets or when you need purpose-built ANN indexing (HNSW with filtering), use Qdrant.

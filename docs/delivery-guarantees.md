# Delivery guarantees

pgstream provides **at-least-once delivery**.

---

## What this means

Every `ChangeEvent` your handler receives has been committed in Postgres. pgstream guarantees that each event is delivered to your handler **at least once**. Under normal operation, each event is delivered exactly once. Under failure, an event may be delivered more than once.

---

## How it works

### The ACK mechanism

After Postgres writes a transaction to the WAL, it holds those WAL segments until a consumer confirms it has processed them. This confirmation is called an **ACK** and is sent via `send_feedback(flush_lsn=...)`.

pgstream ACKs **after** your handler returns successfully:

```
1. Postgres streams WAL bytes
2. pgstream decodes event
3. pgstream calls your handler
4. your handler writes to vector store
5. handler returns
6. pgstream sends ACK to Postgres ← only now
```

If your handler raises an exception, the ACK is never sent. On the next connection, Postgres replays from the last confirmed LSN.

### Keepalive ACKs

Postgres closes the replication connection after `wal_sender_timeout` (default 60 seconds) if it hears nothing. pgstream sends a keepalive ACK every 10 seconds even when there are no events, preventing this timeout.

---

## Handler timeout

If your handler takes longer than 30 seconds, pgstream cancels the future and raises a `RuntimeError`. This causes the replication thread to crash and the slot to reset — the event will be replayed on the next startup.

Design your handler to complete well within 30 seconds. For expensive operations (large model inference, slow network calls), consider batching outside the handler or using a queue.

---

## Implications for your handler

Because events can be replayed, your handler must be **idempotent** — calling it twice with the same event must produce the same result.

Both `PgVectorSink` and `QdrantSink` use upsert semantics, so they are idempotent by default:

```sql
-- PgVectorSink
INSERT INTO embeddings (id, vector, payload)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET vector = EXCLUDED.vector, ...
```

```python
# QdrantSink
await client.upsert(collection_name=..., points=[PointStruct(id=..., vector=..., payload=...)])
# upsert = insert if not exists, replace if exists
```

If you are writing to a custom sink or doing other operations in your handler (e.g. sending a notification, writing to another database), ensure those operations are also idempotent or protected by a deduplication check.

---

## Replica identity and DELETE

By default, a `DELETE` event only includes the primary key columns in `event.row`. If your handler needs the full deleted row (e.g. to know the `content` field to delete from the vector store), you have two options:

**Option 1: Enable `REPLICA IDENTITY FULL`**

```sql
ALTER TABLE documents REPLICA IDENTITY FULL;
```

This makes Postgres include the full old row in `DELETE` events. `event.row` will contain all columns of the deleted row.

**Option 2: Use only the primary key**

If your vector store uses the row's primary key as the vector ID (which is the recommended pattern), you only need the ID for deletion:

```python
@stream.on_change
async def handle(event: ChangeEvent, sink):
    if event.operation == "delete":
        await sink.delete(event.row["id"])  # id is always present
```

---

## What pgstream does NOT guarantee

- **Exactly-once delivery.** Events may be delivered more than once if the process crashes after the handler completes but before the ACK is sent (a narrow window, but theoretically possible at the OS level).
- **Ordering across tables.** Events within a single transaction are delivered in commit order. Events across separate transactions are delivered in WAL order.
- **Gap-free delivery.** If `wal_level` is changed or the slot is dropped externally, the stream will not automatically recover.

# Concepts

## Change Data Capture (CDC)

Change Data Capture is the practice of detecting and capturing row-level changes in a database as they happen. Instead of periodically querying for new or updated rows, CDC gives you a real-time stream of `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` events.

Use cases:

- Keeping a vector store in sync with your primary database
- Maintaining a search index
- Replicating data to a secondary store
- Audit logging
- Event sourcing

---

## Postgres logical replication

Postgres exposes CDC through its **logical replication** protocol. This is the same mechanism used by tools like Debezium and pglogical.

### Write-Ahead Log (WAL)

Every write to Postgres is first recorded in the Write-Ahead Log (WAL) before the data is applied. This ensures durability. The WAL is an append-only sequence of records. Each record has a position called the **Log Sequence Number (LSN)** — a monotonically increasing 64-bit integer.

### Replication slots

A **replication slot** is a named cursor into the WAL. Postgres keeps WAL segments on disk as long as a slot has not confirmed consuming them. This prevents changes from being garbage-collected before your consumer has processed them.

```sql
-- pgstream creates this automatically via setup()
SELECT slot_name, plugin, confirmed_flush_lsn
FROM pg_replication_slots;
```

pgstream uses the `pgoutput` plugin, which is built into Postgres 10+ and requires no additional installation.

### Publications

A **publication** defines which tables are included in a logical replication stream. pgstream creates a publication that covers the exact set of tables you pass to `watch()`.

```sql
-- pgstream creates this automatically via setup()
SELECT pubname, pubtables FROM pg_publication_tables;
```

### The replication protocol

When pgstream calls `start()`, it opens a replication connection using psycopg2 and calls `start_replication()`. Postgres then streams WAL records encoded in the **pgoutput binary format**.

The stream looks like this:

```
Relation(OID=16388, schema="public", table="documents", columns=["id","content"])
Begin(xid=1234, commit_lsn=0/1A3F28, commit_time=...)
Insert(relation_oid=16388, new_row=["1","Hello"])
Commit(commit_lsn=0/1A3F28)
```

pgstream's decoder (`PgOutputDecoder`) parses these binary messages into `ChangeEvent` objects.

---

## The Relation message cache

Before any `INSERT`, `UPDATE`, or `DELETE`, Postgres sends a `Relation` message that contains the table's OID, schema, table name, and column names. DML messages only include the OID, not column names. pgstream caches the most recent `Relation` message per OID so it can attach column names when constructing `ChangeEvent.row`.

If the cache is missing an OID (for example, if the consumer joins mid-stream before the first DML on a table), pgstream logs a warning and skips the event rather than crashing.

---

## ACKs and WAL retention

After your handler returns successfully, pgstream calls `send_feedback(flush_lsn=...)`. This tells Postgres:

> I have processed everything up to this LSN. You may discard WAL up to this point.

If your process crashes before ACKing, Postgres will replay those events from the last confirmed LSN on the next connection. This is the basis for [at-least-once delivery](delivery-guarantees.md).

pgstream also sends a keepalive ACK every 10 seconds even when idle. Without this, Postgres will close the replication connection after `wal_sender_timeout` (default 60 seconds).

---

## Threading model

The psycopg2 replication loop is blocking — `read_message()` uses `select()` under the hood and blocks the calling thread. Running this directly in the asyncio event loop would stall all other coroutines.

pgstream runs the replication loop in a **background daemon thread**:

```
[background thread]              [main event loop]
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

`asyncio.run_coroutine_threadsafe()` submits the user's async handler to the main event loop and returns a `Future`. The replication thread blocks on `future.result()` until the handler completes. Only then does it ACK the LSN. This ensures the handler has a chance to write to the vector store before we tell Postgres we're done with that event.

---

## Column values are always strings

The pgoutput protocol v1 sends all column values as text strings. For example, a `BIGINT` column with value `42` arrives as the string `"42"`. pgstream does not coerce types. Cast in your handler:

```python
int(event.row["id"])
float(event.row["price"])
```

Type coercion would require maintaining an OID → Python type map for all Postgres built-in types plus extensions. Keeping values as strings keeps pgstream simple and lets you decide on casting strategy.

---

## REPLICA IDENTITY

By default, Postgres only includes the primary key columns in `old_row` for `UPDATE` and `DELETE` events. To get the full previous row:

```sql
ALTER TABLE documents REPLICA IDENTITY FULL;
```

With `REPLICA IDENTITY FULL`:
- `UPDATE`: `event.old_row` contains the row before the update, `event.row` contains the row after.
- `DELETE`: `event.row` contains the deleted row (same as `event.old_row`).

Without `REPLICA IDENTITY FULL`:
- `UPDATE`: `event.old_row` contains only the PK columns. `event.row` is the full new row.
- `DELETE`: `event.row` contains only the PK columns. `event.old_row` is `None`.

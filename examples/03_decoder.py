"""
examples/03_decoder.py — raw WAL decoder walkthrough
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shows how PgOutputDecoder turns raw pgoutput binary messages into
Python dicts — entirely in memory, no database connection needed.

This is useful for:
  - Understanding what pgstream receives from Postgres
  - Unit-testing your handler logic against crafted WAL payloads
  - Debugging CDC pipelines without spinning up Postgres

Run:
    pip install pgstream
    python examples/03_decoder.py

Reference:
    https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

from __future__ import annotations

import struct
from datetime import datetime, timezone, timedelta
from pprint import pprint

from pgstream.decoder import PgOutputDecoder

# ── Postgres epoch ────────────────────────────────────────────────────────────
# Postgres timestamps are microseconds since 2000-01-01 UTC, NOT Unix epoch.
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


# ── Binary message builders ───────────────────────────────────────────────────
# These replicate the exact binary format that psycopg2 hands to the decoder
# during logical replication.  In production you never write these — they come
# from Postgres automatically.  They're here so the example is self-contained.


def _cstr(s: str) -> bytes:
    """C-style null-terminated string."""
    return s.encode() + b"\x00"


def make_begin(lsn: int, ts_us: int, xid: int) -> bytes:
    """'B' message — marks the start of a transaction."""
    return b"B" + struct.pack(">qqi", lsn, ts_us, xid)


def make_commit(commit_lsn: int, end_lsn: int, ts_us: int) -> bytes:
    """'C' message — marks the end of a transaction."""
    return b"C" + struct.pack(">bqqq", 0, commit_lsn, end_lsn, ts_us)


def make_relation(
    oid: int, schema: str, table: str, columns: list[tuple[str, int, bool]]
) -> bytes:
    """'R' message — describes a table's schema.

    Sent before the first DML message for a given table, and whenever
    the table's schema changes.  The decoder caches it by OID.
    """
    body = struct.pack(">I", oid) + _cstr(schema) + _cstr(table)
    body += struct.pack(">B", ord("d"))  # replica identity = DEFAULT
    body += struct.pack(">H", len(columns))
    for name, type_oid, is_key in columns:
        body += struct.pack(">B", 1 if is_key else 0)
        body += _cstr(name) + struct.pack(">Ii", type_oid, -1)
    return b"R" + body


def make_tuple(values: list[str | None]) -> bytes:
    """Encode a row as a TupleData payload."""
    buf = struct.pack(">H", len(values))
    for v in values:
        if v is None:
            buf += b"n"  # null
        else:
            enc = v.encode()
            buf += b"t" + struct.pack(">I", len(enc)) + enc  # text
    return buf


def make_insert(oid: int, values: list[str | None]) -> bytes:
    """'I' message — a new row was inserted."""
    return b"I" + struct.pack(">I", oid) + b"N" + make_tuple(values)


def make_update(
    oid: int, new: list[str | None], old: list[str | None] | None = None
) -> bytes:
    """'U' message — a row was updated.

    If old is provided (REPLICA IDENTITY FULL), the 'O' tuple is included.
    """
    body = struct.pack(">I", oid)
    if old is not None:
        body += b"O" + make_tuple(old)
    body += b"N" + make_tuple(new)
    return b"U" + body


def make_delete(oid: int, key: list[str | None]) -> bytes:
    """'D' message — a row was deleted (key columns only by default)."""
    return b"D" + struct.pack(">I", oid) + b"K" + make_tuple(key)


def make_truncate(oids: list[int]) -> bytes:
    """'T' message — one or more tables were truncated."""
    body = struct.pack(">I", len(oids)) + b"\x00"
    for oid in oids:
        body += struct.pack(">I", oid)
    return b"T" + body


# ── Run the decoder ───────────────────────────────────────────────────────────

print("\n=== pgstream WAL decoder walkthrough ===\n")

decoder = PgOutputDecoder()

# Table: public.documents (id SERIAL PK, content TEXT, score FLOAT8)
OID = 42001
COLUMNS = [
    ("id", 23, True),  # int4,  primary key
    ("content", 25, False),  # text
    ("score", 701, False),  # float8
]

# Commit timestamp: 1 second after PG epoch
TS_US = 1_000_000
COMMIT_TIME = PG_EPOCH + timedelta(microseconds=TS_US)
XID = 99
LSN = 0x0A_BC_DE

# ── Step 1: Begin ─────────────────────────────────────────────────────────────
result = decoder.decode(make_begin(LSN, TS_US, XID))
print("1. BEGIN message")
print(f"   returns  : {result!r}  (None — no event yet)")
print(f"   lsn set  : {decoder._current_lsn}")
print(f"   xid set  : {decoder._current_xid}")
print(f"   time set : {decoder._current_commit_time}")

# ── Step 2: Relation ──────────────────────────────────────────────────────────
result = decoder.decode(make_relation(OID, "public", "documents", COLUMNS))
print(f"\n2. RELATION message (OID {OID})")
print(f"   returns  : {result!r}  (None — schema cached, not an event)")
rel = decoder._relations[OID]
print(
    f"   cached   : {rel.schema}.{rel.table}  columns={[c.name for c in rel.columns]}"
)

# ── Step 3: Insert ────────────────────────────────────────────────────────────
result = decoder.decode(make_insert(OID, ["1", "pgstream decoder walkthrough", "0.95"]))
print(f"\n3. INSERT message")
print(f"   decoded event:")
pprint(result, indent=6)

# ── Step 4: Update (with old row — REPLICA IDENTITY FULL) ────────────────────
result = decoder.decode(
    make_update(
        OID,
        new=["1", "pgstream decoder — updated content", "0.98"],
        old=["1", "pgstream decoder walkthrough", "0.95"],
    )
)
print(f"\n4. UPDATE message (old row present — REPLICA IDENTITY FULL)")
print(f"   decoded event:")
pprint(result, indent=6)

# ── Step 5: Delete ────────────────────────────────────────────────────────────
result = decoder.decode(make_delete(OID, ["1", None, None]))
print(f"\n5. DELETE message")
print(f"   decoded event:")
pprint(result, indent=6)

# ── Step 6: Truncate ──────────────────────────────────────────────────────────
result = decoder.decode(make_truncate([OID]))
print(f"\n6. TRUNCATE message")
print(f"   decoded event:")
pprint(result, indent=6)

# ── Step 7: Commit ────────────────────────────────────────────────────────────
result = decoder.decode(make_commit(LSN, LSN + 0x10, TS_US))
print(f"\n7. COMMIT message")
print(f"   returns  : {result!r}  (None — transaction boundary only)")

print("""
Key points:
  - BEGIN / COMMIT / RELATION messages return None — they update internal state only.
  - INSERT / UPDATE / DELETE / TRUNCATE return a dict with:
      operation, schema, table, row, old_row, lsn, commit_time, xid
  - lsn / xid / commit_time come from the enclosing BEGIN and are shared
    by all events in the same transaction.
  - old_row is only populated on UPDATE/DELETE with REPLICA IDENTITY FULL.
""")

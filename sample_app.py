"""
sample_app.py — pgstream smoke-test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run this to verify your pgstream installation and public API work correctly:

    pip install pgstream
    python sample_app.py

What it does:
  1. Verifies the public API imports correctly (PGStream, ChangeEvent, __version__)
  2. Tests the CLI entry-point (pgstream --help, --version, info, quickstart, config)
  3. Exercises ChangeEvent construction and repr
  4. Exercises the Sink ABC (subclassing + method dispatch)
  5. Exercises PGStreamguard-rails (errors raised before setup/watch)
  6. Verifies the decoder processes raw pgoutput bytes into ChangeEvents (no DB needed)

No live Postgres instance is required — all I/O is mocked or exercised in-memory.
"""

from __future__ import annotations

import asyncio
import struct
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

results: list[tuple[str, bool, str]] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    """Record and print a single pass/fail check."""
    results.append((label, condition, detail))
    status = "\033[32mPASS\033[0m" if condition else "\033[31mFAIL\033[0m"
    print(f"  [{status}]  {label}" + (f"  — {detail}" if detail else ""))


print()
print("\033[1mpgstream smoke-test\033[0m")
print("─" * 55)

# ──────────────────────────────────────────────────────────────────────────────
# 1. Public API imports
# ──────────────────────────────────────────────────────────────────────────────

print("\n[1] Public API imports")

try:
    from pgstream import PGStream, ChangeEvent, __version__

    check("pgstream imports without error", True)
    check(
        "__version__ is a non-empty string",
        isinstance(__version__, str) and bool(__version__),
    )
    check("PGStream class is importable", callable(PGStream))
    check("ChangeEvent class is importable", callable(ChangeEvent))
except ImportError as exc:
    check("pgstream imports without error", False, str(exc))

# ──────────────────────────────────────────────────────────────────────────────
# 2. ChangeEvent construction and repr
# ──────────────────────────────────────────────────────────────────────────────

print("\n[2] ChangeEvent construction and repr")

now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

event_insert = ChangeEvent(
    operation="insert",
    schema="public",
    table="documents",
    row={"id": "1", "content": "hello", "score": "0.9"},
    old_row=None,
    lsn="0/1A3F28",
    commit_time=now,
    xid=42,
)
check("ChangeEvent.operation == 'insert'", event_insert.operation == "insert")
check("ChangeEvent.table == 'documents'", event_insert.table == "documents")
check(
    "ChangeEvent.row keys correct",
    list(event_insert.row.keys()) == ["id", "content", "score"],
)
check("ChangeEvent.old_row is None on insert", event_insert.old_row is None)

event_update = ChangeEvent(
    operation="update",
    schema="public",
    table="documents",
    row={"id": "1", "content": "updated"},
    old_row={"id": "1", "content": "original"},
    lsn="0/1A3F30",
    commit_time=now,
    xid=43,
)
check("ChangeEvent.old_row present on update", event_update.old_row is not None)
check(
    "ChangeEvent old_row content correct", event_update.old_row["content"] == "original"
)  # type: ignore[index]

repr_str = repr(event_insert)
check("ChangeEvent repr contains operation", "insert" in repr_str)
check("ChangeEvent repr contains table", "documents" in repr_str)
check("ChangeEvent repr contains lsn", "0/1A3F28" in repr_str)

# ──────────────────────────────────────────────────────────────────────────────
# 3. Sink ABC
# ──────────────────────────────────────────────────────────────────────────────

print("\n[3] Sink ABC")

from pgstream.sinks.base import Sink


# Verify ABC enforcement — concrete subclass must implement upsert + delete
class InMemorySink(Sink):
    """Minimal in-memory sink for smoke-testing the ABC contract."""

    def __init__(self) -> None:
        self.upserted: list[tuple[str, list[float]]] = []
        self.deleted: list[str] = []

    async def upsert(
        self, id: str, vector: list[float], payload: dict | None = None
    ) -> None:
        self.upserted.append((id, vector))

    async def delete(self, id: str) -> None:
        self.deleted.append(id)


sink = InMemorySink()


async def _test_sink() -> None:
    await sink.upsert("doc-1", [0.1, 0.2, 0.3], payload={"text": "hello"})
    await sink.upsert("doc-2", [0.4, 0.5, 0.6])
    await sink.delete("doc-1")
    await sink.close()  # default no-op — must not raise


asyncio.run(_test_sink())

check("Sink.upsert records entries", len(sink.upserted) == 2)
check(
    "Sink.upsert stores correct vector", sink.upserted[0] == ("doc-1", [0.1, 0.2, 0.3])
)
check(
    "Sink.delete records entry", len(sink.deleted) == 1 and sink.deleted[0] == "doc-1"
)
check("Sink.close() is a no-op (no error)", True)  # reached means no exception

# ABC prevents instantiation without abstract methods
try:

    class BadSink(Sink):  # type: ignore[abstract]
        pass

    BadSink()  # type: ignore[abstract]
    check("Sink ABC blocks incomplete subclass", False, "expected TypeError")
except TypeError:
    check("Sink ABC blocks incomplete subclass", True)

# ──────────────────────────────────────────────────────────────────────────────
# 4. PGStream guard-rails (no Postgres needed)
# ──────────────────────────────────────────────────────────────────────────────

print("\n[4] PGStream guard-rails")

DSN = "postgresql://user:pass@localhost:5432/db"  # fake — never connected


async def _test_guardrails() -> None:
    # --- watch() required before setup() ---
    stream = PGStream(dsn=DSN)
    try:
        await stream.setup()
        check("ValueError raised if no tables watched", False, "expected ValueError")
    except ValueError as exc:
        check(
            "ValueError raised if no tables watched",
            "no tables registered" in str(exc),
            str(exc),
        )

    # --- setup() required before start() ---
    stream2 = PGStream(dsn=DSN)
    stream2.watch("documents")

    @stream2.on_change
    async def _handler(event: ChangeEvent, sink) -> None:
        pass

    try:
        await stream2.start()
        check("RuntimeError raised if setup() skipped", False, "expected RuntimeError")
    except RuntimeError as exc:
        check(
            "RuntimeError raised if setup() skipped",
            "setup() must be called" in str(exc),
            str(exc),
        )

    # --- handler required before start() ---
    stream3 = PGStream(dsn=DSN)
    stream3.watch("documents")
    # Manually inject a SlotManager stub so setup check passes
    from pgstream.replication import SlotManager

    stream3._slot_manager = MagicMock(spec=SlotManager)  # type: ignore[assignment]

    try:
        await stream3.start()
        check(
            "RuntimeError raised if no handler registered",
            False,
            "expected RuntimeError",
        )
    except RuntimeError as exc:
        check(
            "RuntimeError raised if no handler registered",
            "no handler registered" in str(exc),
            str(exc),
        )


asyncio.run(_test_guardrails())

# ──────────────────────────────────────────────────────────────────────────────
# 5. PGStream method chaining
# ──────────────────────────────────────────────────────────────────────────────

print("\n[5] PGStream method chaining")

stream_chain = PGStream(dsn=DSN)
sink_chain = InMemorySink()
result = stream_chain.watch("users", "orders").sink(sink_chain)

check("watch() + sink() chain returns self", result is stream_chain)
check("watch() registers tables", stream_chain._tables == ["users", "orders"])
check("sink() attaches sink", stream_chain._sink is sink_chain)

# ──────────────────────────────────────────────────────────────────────────────
# 6. Decoder (in-memory — no Postgres)
# ──────────────────────────────────────────────────────────────────────────────

print("\n[6] WAL decoder (in-memory)")

from pgstream.decoder import PgOutputDecoder

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)

# --- Helper builders (same format as the pgoutput protocol) ---


def _cstr(s: str) -> bytes:
    return s.encode("utf-8") + b"\x00"


def _make_relation(
    oid: int, schema: str, table: str, columns: list[tuple[str, int, bool]]
) -> bytes:
    body = struct.pack(">I", oid)
    body += _cstr(schema) + _cstr(table)
    body += struct.pack(">B", ord("d"))  # replica identity DEFAULT
    body += struct.pack(">H", len(columns))
    for name, type_oid, is_key in columns:
        body += struct.pack(">B", 1 if is_key else 0)
        body += _cstr(name) + struct.pack(">Ii", type_oid, -1)
    return b"R" + body


def _make_begin(lsn: int, ts_us: int, xid: int) -> bytes:
    return b"B" + struct.pack(">qqi", lsn, ts_us, xid)


def _make_tuple(values: list[str | None]) -> bytes:
    buf = struct.pack(">H", len(values))
    for v in values:
        if v is None:
            buf += b"n"
        else:
            enc = v.encode()
            buf += b"t" + struct.pack(">I", len(enc)) + enc
    return buf


def _make_insert(oid: int, values: list[str | None]) -> bytes:
    return b"I" + struct.pack(">I", oid) + b"N" + _make_tuple(values)


def _make_delete(oid: int, key_values: list[str | None]) -> bytes:
    return b"D" + struct.pack(">I", oid) + b"K" + _make_tuple(key_values)


OID = 9999
COLUMNS = [("id", 23, True), ("content", 25, False)]
REL_MSG = _make_relation(OID, "public", "documents", COLUMNS)
BEGIN_MSG = _make_begin(0x1A3F28, 1_000_000, 77)

dec = PgOutputDecoder()

# Begin sets internal state but yields None
dec.decode(BEGIN_MSG)
dec.decode(REL_MSG)

# Insert
res = dec.decode(_make_insert(OID, ["42", "pgstream rocks"]))
check(
    "Decoder produces insert ChangeEvent dict",
    res is not None and res["operation"] == "insert",
)
check(
    "Decoder inserts row values correctly",
    res is not None and res["row"] == {"id": "42", "content": "pgstream rocks"},
)
check("Decoder attaches LSN to event", res is not None and res["lsn"] == "0/1A3F28")
check("Decoder attaches xid to event", res is not None and res["xid"] == 77)

# Delete
res2 = dec.decode(_make_delete(OID, ["42", None]))
check(
    "Decoder produces delete ChangeEvent dict",
    res2 is not None and res2["operation"] == "delete",
)
check("Decoder captures delete key row", res2 is not None and res2["row"]["id"] == "42")

# ──────────────────────────────────────────────────────────────────────────────
# 7. CLI entry-point
# ──────────────────────────────────────────────────────────────────────────────

print("\n[7] CLI entry-point")


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    """Run `pgstream <args>` in a subprocess and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "pgstream._cli", *args],
        capture_output=True,
        text=True,
    )


r = _run_cli("--help")
check("pgstream --help exits 0", r.returncode == 0)
check("pgstream --help mentions 'info'", "info" in r.stdout)
check("pgstream --help mentions 'quickstart'", "quickstart" in r.stdout)
check("pgstream --help mentions 'config'", "config" in r.stdout)

r = _run_cli("--version")
check("pgstream --version exits 0", r.returncode == 0)
check("pgstream --version shows version string", __version__ in r.stdout)

r = _run_cli("info")
check("pgstream info exits 0", r.returncode == 0)
check("pgstream info mentions PyPI", "pypi.org" in r.stdout.lower())

r = _run_cli("quickstart")
check("pgstream quickstart exits 0", r.returncode == 0)
check("pgstream quickstart shows PGStream", "PGStream" in r.stdout)

r = _run_cli("config")
check("pgstream config exits 0", r.returncode == 0)
check("pgstream config shows dsn field", "dsn" in r.stdout)

# ──────────────────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────────────────

print()
print("─" * 55)
passed = sum(1 for _, ok, _ in results if ok)
total = len(results)
print(f"\n  {passed}/{total} checks passed\n")

sys.exit(0 if passed == total else 1)

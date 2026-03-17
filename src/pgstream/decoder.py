"""
decoder.py — pgoutput binary wire format parser.

When Postgres streams WAL via logical replication, it uses the `pgoutput`
plugin to encode changes as binary messages. This module decodes those raw
bytes into structured Python objects.

Protocol overview (pgoutput v1):
  Each message starts with a 1-byte type tag, followed by type-specific fields.
  All multi-byte integers are big-endian.

  Message types we handle:
    B  — Begin        (start of transaction)
    C  — Commit       (end of transaction, carries final LSN + timestamp)
    R  — Relation     (schema descriptor for a table — sent before first DML)
    I  — Insert       (new row)
    U  — Update       (old row optional + new row)
    D  — Delete       (old row, key-only or full depending on REPLICA IDENTITY)
    T  — Truncate     (table emptied)

  Types we intentionally skip (not needed for our use case):
    O  — Origin       (change originated from another replication node)
    Y  — Type         (custom type OID mapping — we only use text encoding)
    M  — Message      (pg_logical_emit_message — application-level messages)

Key concept — Relation cache:
  Before the first DML on a table, Postgres sends an R message with that
  table's OID, schema, name, and column list. We cache this as a
  RelationInfo so that when we see an Insert/Update/Delete with a bare OID,
  we can look up column names. The cache is keyed by OID (an integer assigned
  by Postgres, stable within a session but not across restarts — that's fine
  because the replication connection is a single long-lived session).

TupleData format (shared by Insert/Update/Delete):
  Int16  — number of columns
  For each column, one of:
    b'n'        — NULL value
    b'u'        — unchanged TOASTed value (we cannot decode this; we emit None)
    b't' + Int32 len + bytes  — text-encoded value
    b'b' + Int32 len + bytes  — binary-encoded value (we raise; not expected in v1)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import NamedTuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Postgres epoch is 2000-01-01 00:00:00 UTC.
# Timestamps in pgoutput are microseconds since this epoch.
_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _pg_ts_to_datetime(microseconds: int) -> datetime:
    """Convert a Postgres microsecond timestamp to a UTC datetime."""
    return _PG_EPOCH + timedelta(microseconds=microseconds)


def _lsn_to_str(lsn: int) -> str:
    """Convert a 64-bit integer LSN to the human-readable '0/1A3F28' format."""
    high = lsn >> 32
    low = lsn & 0xFFFFFFFF
    return f"{high:X}/{low:X}"


# ---------------------------------------------------------------------------
# Relation cache entry
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """Metadata for a single column in a relation."""
    name: str       # column name (e.g. "id", "content")
    type_oid: int   # Postgres type OID — we don't use this for decoding in v1
    is_key: bool    # True if this column is part of the replica identity key


@dataclass
class RelationInfo:
    """
    Cached schema descriptor for one table.

    Populated from R (Relation) messages. We re-cache whenever a new R
    message arrives for the same OID (which Postgres sends if the schema
    changes mid-stream).
    """
    oid: int
    schema: str
    table: str
    columns: list[ColumnInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

class PgOutputDecoder:
    """
    Stateful parser for the pgoutput logical replication protocol.

    Stateful because:
      1. We must cache Relation messages to decode subsequent DML.
      2. We track the current transaction's metadata (LSN, timestamp, XID)
         which is emitted with every Begin and applied to all DML messages
         in that transaction.

    Usage:
        decoder = PgOutputDecoder()
        for raw_msg in replication_cursor:
            event = decoder.decode(raw_msg.payload)
            if event is not None:
                # event is a dict ready to be turned into a ChangeEvent
                yield event
    """

    def __init__(self) -> None:
        # OID → RelationInfo for all tables seen so far.
        self._relations: dict[int, RelationInfo] = {}

        # Current transaction context — set on Begin, cleared on Commit.
        self._current_lsn: str = "0/0"
        self._current_commit_time: datetime = _PG_EPOCH
        self._current_xid: int = 0

    def decode(self, payload: bytes) -> dict | None:
        """
        Decode one raw pgoutput message.

        Returns a dict with decoded change data if the message is an
        Insert/Update/Delete/Truncate, or None for Begin/Commit/Relation/
        other metadata messages (they update internal state but don't
        produce user-visible events).

        The returned dict has the shape:
          {
            "operation": "insert" | "update" | "delete" | "truncate",
            "schema": str,
            "table": str,
            "row": dict[str, str | None],
            "old_row": dict[str, str | None] | None,
            "lsn": str,
            "commit_time": datetime,
            "xid": int,
          }
        """
        if not payload:
            return None

        # First byte is the message type tag.
        msg_type = chr(payload[0])
        data = payload[1:]  # remaining bytes after the type tag

        if msg_type == "B":
            self._handle_begin(data)
        elif msg_type == "C":
            self._handle_commit(data)
        elif msg_type == "R":
            self._handle_relation(data)
        elif msg_type == "I":
            return self._handle_insert(data)
        elif msg_type == "U":
            return self._handle_update(data)
        elif msg_type == "D":
            return self._handle_delete(data)
        elif msg_type == "T":
            return self._handle_truncate(data)
        # O, Y, M, S, E, etc. — silently ignored

        return None

    # ------------------------------------------------------------------
    # Transaction boundary handlers
    # ------------------------------------------------------------------

    def _handle_begin(self, data: bytes) -> None:
        """
        Begin message layout:
          Int64  — final LSN of this transaction
          Int64  — commit timestamp (microseconds since PG epoch)
          Int32  — XID
        """
        # > = big-endian, q = signed 64-bit, I = unsigned 32-bit
        final_lsn, commit_ts, xid = struct.unpack_from(">qqi", data, 0)
        self._current_lsn = _lsn_to_str(final_lsn)
        self._current_commit_time = _pg_ts_to_datetime(commit_ts)
        self._current_xid = xid

    def _handle_commit(self, data: bytes) -> None:
        """
        Commit message layout:
          Int8   — flags (unused, always 0)
          Int64  — commit LSN
          Int64  — end LSN of the transaction
          Int64  — commit timestamp
        We use the timestamp here to refresh, though it should match Begin.
        """
        # flags(1 byte) + commit_lsn(8) + end_lsn(8) + ts(8) = 25 bytes
        flags = data[0]
        commit_lsn, end_lsn, commit_ts = struct.unpack_from(">qqq", data, 1)
        self._current_lsn = _lsn_to_str(commit_lsn)
        self._current_commit_time = _pg_ts_to_datetime(commit_ts)

    # ------------------------------------------------------------------
    # Schema descriptor
    # ------------------------------------------------------------------

    def _handle_relation(self, data: bytes) -> None:
        """
        Relation message layout:
          Int32  — relation OID
          String — namespace (schema name), null-terminated
          String — relation name (table name), null-terminated
          Int8   — replica identity setting ('d'=default, 'f'=full, 'i'=index, 'n'=nothing)
          Int16  — number of columns
          For each column:
            Int8   — flags (1 = part of key)
            String — column name, null-terminated
            Int32  — type OID
            Int32  — type modifier (atttypmod)
        """
        offset = 0

        # Relation OID
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4

        # Schema name (null-terminated C string)
        schema, offset = _read_cstring(data, offset)

        # Table name (null-terminated C string)
        table, offset = _read_cstring(data, offset)

        # Replica identity setting (1 byte) — we record it but don't act on it
        _replica_identity = data[offset]
        offset += 1

        # Number of columns
        num_cols, = struct.unpack_from(">H", data, offset)
        offset += 2

        columns: list[ColumnInfo] = []
        for _ in range(num_cols):
            col_flags = data[offset]
            offset += 1
            col_name, offset = _read_cstring(data, offset)
            type_oid, type_mod = struct.unpack_from(">Ii", data, offset)
            offset += 8
            columns.append(ColumnInfo(
                name=col_name,
                type_oid=type_oid,
                is_key=bool(col_flags & 0x01),
            ))

        self._relations[oid] = RelationInfo(
            oid=oid,
            schema=schema,
            table=table,
            columns=columns,
        )

    # ------------------------------------------------------------------
    # DML handlers
    # ------------------------------------------------------------------

    def _handle_insert(self, data: bytes) -> dict | None:
        """
        Insert message layout:
          Int32  — relation OID
          Byte1  — 'N' (new tuple marker)
          TupleData — the new row
        """
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4

        relation = self._get_relation(oid)
        if relation is None:
            return None

        # Skip the 'N' byte
        offset += 1

        row, offset = self._decode_tuple(data, offset, relation)

        return self._make_event("insert", relation, row, None)

    def _handle_update(self, data: bytes) -> dict | None:
        """
        Update message layout:
          Int32  — relation OID
          Optional:
            Byte1 'K' + TupleData — old key (if replica identity = index and key changed)
            Byte1 'O' + TupleData — old row (if replica identity = full)
          Byte1 'N' + TupleData   — new row (always present)
        """
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4

        relation = self._get_relation(oid)
        if relation is None:
            return None

        old_row: dict | None = None

        # Check for optional 'K' or 'O' prefix before the 'N'
        marker = chr(data[offset])
        if marker in ("K", "O"):
            offset += 1  # skip the K/O byte
            old_row, offset = self._decode_tuple(data, offset, relation)
            marker = chr(data[offset])  # should now be 'N'

        # Skip the 'N' byte
        assert marker == "N", f"Expected 'N' in UPDATE, got {marker!r}"
        offset += 1

        new_row, offset = self._decode_tuple(data, offset, relation)

        return self._make_event("update", relation, new_row, old_row)

    def _handle_delete(self, data: bytes) -> dict | None:
        """
        Delete message layout:
          Int32  — relation OID
          Byte1 'K' or 'O' — indicates key-only or full old row
          TupleData — the old row
        """
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4

        relation = self._get_relation(oid)
        if relation is None:
            return None

        # Skip the 'K' or 'O' byte — either way we decode the tuple
        offset += 1

        old_row, offset = self._decode_tuple(data, offset, relation)

        # For DELETE, `row` = the old/key row; `old_row` = None (no "new" state)
        return self._make_event("delete", relation, old_row, None)

    def _handle_truncate(self, data: bytes) -> dict | None:
        """
        Truncate message layout:
          Int32  — number of relations truncated
          Int8   — option flags (CASCADE=1, RESTART IDENTITY=2)
          Int32* — relation OIDs (one per relation)

        We emit one event per truncated table (makes handler logic uniform).
        This function returns only the first; the loop in replication.py must
        handle multi-table truncates by calling decode_truncate directly.
        For simplicity in v1 we return a single synthetic event for the first
        relation (full multi-table support is a future enhancement).
        """
        offset = 0
        num_relations, = struct.unpack_from(">I", data, offset)
        offset += 4
        _flags = data[offset]
        offset += 1

        if num_relations == 0:
            return None

        # Read the first OID only (v1 limitation — documented)
        oid, = struct.unpack_from(">I", data, offset)
        relation = self._get_relation(oid)
        if relation is None:
            return None

        return self._make_event("truncate", relation, {}, None)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _get_relation(self, oid: int) -> RelationInfo | None:
        """
        Look up a cached RelationInfo. Returns None (and logs a warning) if
        the OID is unknown — this should not happen in normal operation since
        Postgres always sends R before the first DML, but defensive handling
        prevents a crash on edge cases (e.g. the consumer joining mid-stream).
        """
        rel = self._relations.get(oid)
        if rel is None:
            # Not crashing — just skip this message. The next R message for
            # this OID will re-populate the cache.
            import warnings
            warnings.warn(
                f"pgstream: received DML for unknown relation OID {oid}. "
                "The Relation message may have been missed. Skipping event."
            )
        return rel

    def _decode_tuple(
        self, data: bytes, offset: int, relation: RelationInfo
    ) -> tuple[dict[str, str | None], int]:
        """
        Decode a TupleData block into a dict of column_name → value.

        TupleData layout:
          Int16  — number of columns
          For each column, one of:
            b'n'              — NULL
            b'u'              — unchanged TOAST (we emit None and warn)
            b't' + Int32 len + bytes  — text value
            b'b' + Int32 len + bytes  — binary value (rare in v1; we decode as raw bytes)
        """
        num_cols, = struct.unpack_from(">H", data, offset)
        offset += 2

        row: dict[str, str | None] = {}

        for i in range(num_cols):
            # Get the column name from the cached relation schema.
            # If the number of columns in the message doesn't match the
            # cached schema, we fall back to a positional key.
            col_name = relation.columns[i].name if i < len(relation.columns) else f"col_{i}"

            col_type = chr(data[offset])
            offset += 1

            if col_type == "n":
                # NULL
                row[col_name] = None

            elif col_type == "u":
                # Unchanged TOASTed value — actual data not sent by Postgres.
                # This happens when a large column (>2KB) wasn't modified in
                # the UPDATE. We emit None; the caller can re-fetch if needed.
                row[col_name] = None

            elif col_type == "t":
                # Text-encoded value
                val_len, = struct.unpack_from(">I", data, offset)
                offset += 4
                row[col_name] = data[offset : offset + val_len].decode("utf-8")
                offset += val_len

            elif col_type == "b":
                # Binary-encoded value (only with binary=true option, which we
                # don't enable — included for completeness / future-proofing)
                val_len, = struct.unpack_from(">I", data, offset)
                offset += 4
                # Return raw bytes as a hex string so it's always serialisable
                row[col_name] = data[offset : offset + val_len].hex()
                offset += val_len

            else:
                raise ValueError(
                    f"pgstream decoder: unknown column type byte {col_type!r} "
                    f"for column {col_name!r} in {relation.schema}.{relation.table}"
                )

        return row, offset

    def _make_event(
        self,
        operation: str,
        relation: RelationInfo,
        row: dict,
        old_row: dict | None,
    ) -> dict:
        """Build the raw event dict from decoded parts + current transaction context."""
        return {
            "operation": operation,
            "schema": relation.schema,
            "table": relation.table,
            "row": row,
            "old_row": old_row,
            "lsn": self._current_lsn,
            "commit_time": self._current_commit_time,
            "xid": self._current_xid,
        }


# ---------------------------------------------------------------------------
# Low-level byte utilities
# ---------------------------------------------------------------------------

def _read_cstring(data: bytes, offset: int) -> tuple[str, int]:
    """
    Read a null-terminated UTF-8 string from `data` starting at `offset`.
    Returns (string, new_offset) where new_offset points past the null byte.
    """
    end = data.index(b"\x00", offset)
    value = data[offset:end].decode("utf-8")
    return value, end + 1  # +1 to skip the null terminator

"""
tests/test_decoder.py — Unit tests for the pgoutput binary decoder.

These tests work entirely in memory: we hand-craft raw pgoutput bytes and
assert that PgOutputDecoder produces the correct Python structures.

No database connection is required. This makes these tests fast and runnable
in CI without a Postgres container.

Why hand-craft bytes?
  The decoder is the most critical low-level component. Testing it directly
  with known byte sequences gives us precise confidence that our struct.unpack
  calls and offset arithmetic are correct, independent of a live Postgres stream.

The pgoutput binary format is documented in:
  https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

All integers are big-endian. The Postgres epoch is 2000-01-01 00:00:00 UTC.
"""

import struct
from datetime import datetime, timezone, timedelta

import pytest

from pgstream.decoder import PgOutputDecoder, RelationInfo, ColumnInfo


# ---------------------------------------------------------------------------
# Helpers to build raw pgoutput messages
# ---------------------------------------------------------------------------

PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _make_cstring(s: str) -> bytes:
    """Encode a null-terminated C string (as pgoutput uses)."""
    return s.encode("utf-8") + b"\x00"


def _make_begin(lsn: int, commit_ts_us: int, xid: int) -> bytes:
    """
    Build a Begin (B) message.

    Layout: type(1) + final_lsn(8, int64) + commit_ts(8, int64) + xid(4, int32)
    Note: xid in Begin is signed 32-bit (XID is actually uint32, but pg uses int32 here).
    """
    return b"B" + struct.pack(">qqi", lsn, commit_ts_us, xid)


def _make_commit(commit_lsn: int, end_lsn: int, commit_ts_us: int) -> bytes:
    """
    Build a Commit (C) message.

    Layout: type(1) + flags(1) + commit_lsn(8) + end_lsn(8) + ts(8)
    """
    return b"C" + struct.pack(">bqqq", 0, commit_lsn, end_lsn, commit_ts_us)


def _make_relation(
    oid: int,
    schema: str,
    table: str,
    columns: list[tuple[str, int, bool]],  # (name, type_oid, is_key)
    replica_identity: int = ord("d"),
) -> bytes:
    """
    Build a Relation (R) message.

    Layout: type(1) + oid(4) + schema(cstr) + table(cstr) + replica_identity(1)
            + num_cols(2) + for each col: flags(1) + name(cstr) + type_oid(4) + type_mod(4)
    """
    body = struct.pack(">I", oid)
    body += _make_cstring(schema)
    body += _make_cstring(table)
    body += struct.pack(">B", replica_identity)
    body += struct.pack(">H", len(columns))
    for name, type_oid, is_key in columns:
        flags = 1 if is_key else 0
        body += struct.pack(">B", flags)
        body += _make_cstring(name)
        body += struct.pack(">Ii", type_oid, -1)  # type_mod -1 = no modifier
    return b"R" + body


def _make_tuple_data(values: list[str | None]) -> bytes:
    """
    Build a TupleData block.

    values: list where None → 'n' (NULL), str → 't' + length + bytes
    """
    buf = struct.pack(">H", len(values))
    for v in values:
        if v is None:
            buf += b"n"
        else:
            encoded = v.encode("utf-8")
            buf += b"t" + struct.pack(">I", len(encoded)) + encoded
    return buf


def _make_insert(oid: int, values: list[str | None]) -> bytes:
    """
    Build an Insert (I) message.

    Layout: type(1) + oid(4) + 'N'(1) + TupleData
    """
    body = struct.pack(">I", oid) + b"N" + _make_tuple_data(values)
    return b"I" + body


def _make_update_new_only(oid: int, new_values: list[str | None]) -> bytes:
    """
    Build an Update (U) message with only a new tuple (no old row).

    Layout: type(1) + oid(4) + 'N'(1) + TupleData(new)
    """
    body = struct.pack(">I", oid) + b"N" + _make_tuple_data(new_values)
    return b"U" + body


def _make_update_with_old(
    oid: int,
    old_values: list[str | None],
    new_values: list[str | None],
) -> bytes:
    """
    Build an Update (U) message with REPLICA IDENTITY FULL old row ('O' prefix).
    """
    body = (
        struct.pack(">I", oid)
        + b"O"
        + _make_tuple_data(old_values)
        + b"N"
        + _make_tuple_data(new_values)
    )
    return b"U" + body


def _make_delete(oid: int, key_values: list[str | None]) -> bytes:
    """
    Build a Delete (D) message (key-only row, 'K' prefix).
    """
    body = struct.pack(">I", oid) + b"K" + _make_tuple_data(key_values)
    return b"D" + body


def _make_truncate(oids: list[int]) -> bytes:
    """
    Build a Truncate (T) message.

    Layout: type(1) + num_relations(4) + flags(1) + oid* (4 each)
    """
    body = struct.pack(">I", len(oids)) + b"\x00"
    for oid in oids:
        body += struct.pack(">I", oid)
    return b"T" + body


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def decoder():
    """Fresh PgOutputDecoder for each test."""
    return PgOutputDecoder()


# OID we'll use across tests for the "documents" table.
DOCUMENTS_OID = 12345

# Column definitions for the documents table: id (int4, key), content (text), score (float8)
DOCUMENTS_COLUMNS = [
    ("id",      23,    True),   # int4 OID = 23
    ("content", 25,    False),  # text OID = 25
    ("score",   701,   False),  # float8 OID = 701
]

# Pre-built Relation message for documents table
RELATION_MSG = _make_relation(
    oid=DOCUMENTS_OID,
    schema="public",
    table="documents",
    columns=DOCUMENTS_COLUMNS,
)

# A Begin message: LSN 0x1A3F28, timestamp = PG epoch + 1 second, xid = 42
_BEGIN_LSN = 0x1A3F28
_BEGIN_TS_US = 1_000_000   # 1 second in microseconds
_BEGIN_XID = 42
BEGIN_MSG = _make_begin(_BEGIN_LSN, _BEGIN_TS_US, _BEGIN_XID)


# ---------------------------------------------------------------------------
# Tests: Begin / Commit
# ---------------------------------------------------------------------------

class TestBeginCommit:
    def test_begin_returns_none_but_sets_state(self, decoder):
        """Begin messages update internal state but return None (no user event)."""
        result = decoder.decode(BEGIN_MSG)
        assert result is None

        # Internal state should now reflect the Begin message.
        # LSN 0x1A3F28 < 2^32, so high=0, low=0x1A3F28 → "0/1A3F28"
        assert decoder._current_lsn == "0/1A3F28"
        assert decoder._current_xid == _BEGIN_XID
        expected_ts = PG_EPOCH + timedelta(microseconds=_BEGIN_TS_US)
        assert decoder._current_commit_time == expected_ts

    def test_commit_returns_none(self, decoder):
        """Commit messages update LSN/timestamp but return None."""
        decoder.decode(BEGIN_MSG)  # set initial state
        commit_msg = _make_commit(
            commit_lsn=0x1A3F30,
            end_lsn=0x1A3F40,
            commit_ts_us=2_000_000,
        )
        result = decoder.decode(commit_msg)
        assert result is None
        # LSN should be updated from Commit
        # 0x1A3F30 < 2^32 → high=0, low=0x1A3F30 → "0/1A3F30"
        assert decoder._current_lsn == "0/1A3F30"

    def test_empty_payload_returns_none(self, decoder):
        assert decoder.decode(b"") is None


# ---------------------------------------------------------------------------
# Tests: Relation
# ---------------------------------------------------------------------------

class TestRelation:
    def test_relation_is_cached(self, decoder):
        """Relation messages should populate the internal _relations cache."""
        decoder.decode(RELATION_MSG)
        assert DOCUMENTS_OID in decoder._relations

        rel = decoder._relations[DOCUMENTS_OID]
        assert isinstance(rel, RelationInfo)
        assert rel.schema == "public"
        assert rel.table == "documents"
        assert len(rel.columns) == 3

    def test_relation_columns(self, decoder):
        decoder.decode(RELATION_MSG)
        rel = decoder._relations[DOCUMENTS_OID]

        id_col = rel.columns[0]
        assert id_col.name == "id"
        assert id_col.is_key is True

        content_col = rel.columns[1]
        assert content_col.name == "content"
        assert content_col.is_key is False

    def test_relation_is_updated_on_re_send(self, decoder):
        """If Postgres re-sends a Relation message (schema change), we update the cache."""
        decoder.decode(RELATION_MSG)

        # New relation with an extra column
        new_rel_msg = _make_relation(
            oid=DOCUMENTS_OID,
            schema="public",
            table="documents",
            columns=DOCUMENTS_COLUMNS + [("tags", 1009, False)],
        )
        decoder.decode(new_rel_msg)

        rel = decoder._relations[DOCUMENTS_OID]
        assert len(rel.columns) == 4
        assert rel.columns[3].name == "tags"

    def test_relation_returns_none(self, decoder):
        """Relation messages never return a user event."""
        result = decoder.decode(RELATION_MSG)
        assert result is None


# ---------------------------------------------------------------------------
# Tests: Insert
# ---------------------------------------------------------------------------

class TestInsert:
    def _setup(self, decoder):
        """Seed the decoder with Begin + Relation so it can decode DML."""
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

    def test_insert_basic(self, decoder):
        self._setup(decoder)
        insert_msg = _make_insert(DOCUMENTS_OID, ["1", "hello world", "0.95"])
        result = decoder.decode(insert_msg)

        assert result is not None
        assert result["operation"] == "insert"
        assert result["schema"] == "public"
        assert result["table"] == "documents"
        assert result["row"] == {"id": "1", "content": "hello world", "score": "0.95"}
        assert result["old_row"] is None

    def test_insert_carries_transaction_context(self, decoder):
        self._setup(decoder)
        insert_msg = _make_insert(DOCUMENTS_OID, ["1", "x", "1.0"])
        result = decoder.decode(insert_msg)

        # 0x1A3F28 < 2^32 → "0/1A3F28"
        assert result["lsn"] == "0/1A3F28"
        assert result["xid"] == _BEGIN_XID
        assert result["commit_time"] == PG_EPOCH + timedelta(microseconds=_BEGIN_TS_US)

    def test_insert_with_null_column(self, decoder):
        self._setup(decoder)
        insert_msg = _make_insert(DOCUMENTS_OID, ["2", None, None])
        result = decoder.decode(insert_msg)

        assert result["row"]["id"] == "2"
        assert result["row"]["content"] is None
        assert result["row"]["score"] is None

    def test_insert_returns_none_for_unknown_oid(self, decoder):
        """If we receive DML for an unknown OID (no Relation message), skip it."""
        decoder.decode(BEGIN_MSG)
        # Do NOT send RELATION_MSG — OID will be unknown
        insert_msg = _make_insert(DOCUMENTS_OID, ["1", "x", "1.0"])
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = decoder.decode(insert_msg)
        assert result is None
        assert len(w) == 1
        assert "unknown relation OID" in str(w[0].message)


# ---------------------------------------------------------------------------
# Tests: Update
# ---------------------------------------------------------------------------

class TestUpdate:
    def _setup(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

    def test_update_new_only(self, decoder):
        """Update without old row (default replica identity)."""
        self._setup(decoder)
        update_msg = _make_update_new_only(DOCUMENTS_OID, ["1", "new content", "0.88"])
        result = decoder.decode(update_msg)

        assert result is not None
        assert result["operation"] == "update"
        assert result["row"] == {"id": "1", "content": "new content", "score": "0.88"}
        assert result["old_row"] is None

    def test_update_with_old_row(self, decoder):
        """Update with REPLICA IDENTITY FULL — old row is populated."""
        self._setup(decoder)
        update_msg = _make_update_with_old(
            DOCUMENTS_OID,
            old_values=["1", "old content", "0.5"],
            new_values=["1", "new content", "0.9"],
        )
        result = decoder.decode(update_msg)

        assert result is not None
        assert result["operation"] == "update"
        assert result["row"] == {"id": "1", "content": "new content", "score": "0.9"}
        assert result["old_row"] == {"id": "1", "content": "old content", "score": "0.5"}


# ---------------------------------------------------------------------------
# Tests: Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def _setup(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

    def test_delete_key_only(self, decoder):
        """Delete with default replica identity — only the PK is present in row."""
        self._setup(decoder)
        # Key-only: only the `id` column is sent (NULL for non-key cols in practice,
        # but here we send all cols as the helper does, which is fine for the test).
        delete_msg = _make_delete(DOCUMENTS_OID, ["7", None, None])
        result = decoder.decode(delete_msg)

        assert result is not None
        assert result["operation"] == "delete"
        # For delete: `row` = the old/key row, `old_row` = None
        assert result["row"]["id"] == "7"
        assert result["old_row"] is None

    def test_delete_carries_correct_lsn(self, decoder):
        self._setup(decoder)
        delete_msg = _make_delete(DOCUMENTS_OID, ["3", None, None])
        result = decoder.decode(delete_msg)
        assert result["lsn"] == "0/1A3F28"


# ---------------------------------------------------------------------------
# Tests: Truncate
# ---------------------------------------------------------------------------

class TestTruncate:
    def test_truncate_single_table(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)
        trunc_msg = _make_truncate([DOCUMENTS_OID])
        result = decoder.decode(trunc_msg)

        assert result is not None
        assert result["operation"] == "truncate"
        assert result["table"] == "documents"
        assert result["row"] == {}

    def test_truncate_no_relations_returns_none(self, decoder):
        """Edge case: truncate with 0 relations listed."""
        decoder.decode(BEGIN_MSG)
        trunc_msg = _make_truncate([])
        result = decoder.decode(trunc_msg)
        assert result is None


# ---------------------------------------------------------------------------
# Tests: Ignored message types
# ---------------------------------------------------------------------------

class TestIgnoredMessages:
    def test_origin_message_ignored(self, decoder):
        """'O' (Origin) messages are silently skipped."""
        origin_msg = b"O" + b"\x00" * 8  # 8 bytes of junk
        result = decoder.decode(origin_msg)
        assert result is None

    def test_type_message_ignored(self, decoder):
        """'Y' (Type) messages are silently skipped."""
        type_msg = b"Y" + b"\x00" * 8
        result = decoder.decode(type_msg)
        assert result is None


# ---------------------------------------------------------------------------
# Tests: Utilities
# ---------------------------------------------------------------------------

class TestUtilities:
    def test_lsn_zero(self, decoder):
        """LSN 0 should render as '0/0'."""
        msg = _make_begin(0, 0, 0)
        decoder.decode(msg)
        assert decoder._current_lsn == "0/0"

    def test_lsn_large(self, decoder):
        """
        LSN with both high and low parts populated.
        0x0001_0000_0000 = 4294967296
        high = 0x0001_0000_0000 >> 32 = 1
        low  = 0x0001_0000_0000 & 0xFFFFFFFF = 0
        Expected: "1/0"
        """
        lsn = 0x0001_0000_0000   # = 4294967296 = 2^32
        msg = _make_begin(lsn, 0, 1)
        decoder.decode(msg)
        assert decoder._current_lsn == "1/0"

    def test_postgres_epoch_timestamp(self, decoder):
        """0 microseconds should decode to the Postgres epoch (2000-01-01 UTC)."""
        msg = _make_begin(0, 0, 1)
        decoder.decode(msg)
        assert decoder._current_commit_time == PG_EPOCH

    def test_unchanged_toast_value(self, decoder):
        """
        'u' (unchanged TOAST) column type should produce None in the row dict.

        This happens in UPDATE when a large column wasn't modified. The raw
        data is not sent by Postgres — we can't decode it, so we emit None.
        """
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

        # Hand-craft an Insert with one 'u' (unchanged TOAST) column
        # We craft raw TupleData directly for this edge case.
        tuple_data = struct.pack(">H", 3)  # 3 columns
        tuple_data += b"t" + struct.pack(">I", 1) + b"5"   # id = "5" (text)
        tuple_data += b"u"                                   # content = unchanged TOAST
        tuple_data += b"t" + struct.pack(">I", 3) + b"0.1" # score = "0.1" (text)

        msg = b"I" + struct.pack(">I", DOCUMENTS_OID) + b"N" + tuple_data
        result = decoder.decode(msg)

        assert result is not None
        assert result["row"]["id"] == "5"
        assert result["row"]["content"] is None   # unchanged TOAST → None
        assert result["row"]["score"] == "0.1"

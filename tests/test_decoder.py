"""Unit tests for the pgoutput binary decoder.

These tests work entirely in memory — no database connection required.
Each test crafts raw pgoutput byte payloads and asserts that
``PgOutputDecoder`` produces the correct Python structures.

Reference: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

import struct
from datetime import datetime, timezone, timedelta

import pytest

from pgstream.decoder import PgOutputDecoder, RelationInfo, ColumnInfo


PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------

def _cstr(s: str) -> bytes:
    return s.encode("utf-8") + b"\x00"


def _make_begin(lsn: int, commit_ts_us: int, xid: int) -> bytes:
    return b"B" + struct.pack(">qqi", lsn, commit_ts_us, xid)


def _make_commit(commit_lsn: int, end_lsn: int, commit_ts_us: int) -> bytes:
    return b"C" + struct.pack(">bqqq", 0, commit_lsn, end_lsn, commit_ts_us)


def _make_relation(
    oid: int,
    schema: str,
    table: str,
    columns: list[tuple[str, int, bool]],
    replica_identity: int = ord("d"),
) -> bytes:
    body = struct.pack(">I", oid)
    body += _cstr(schema)
    body += _cstr(table)
    body += struct.pack(">B", replica_identity)
    body += struct.pack(">H", len(columns))
    for name, type_oid, is_key in columns:
        body += struct.pack(">B", 1 if is_key else 0)
        body += _cstr(name)
        body += struct.pack(">Ii", type_oid, -1)
    return b"R" + body


def _make_tuple_data(values: list[str | None]) -> bytes:
    buf = struct.pack(">H", len(values))
    for v in values:
        if v is None:
            buf += b"n"
        else:
            encoded = v.encode("utf-8")
            buf += b"t" + struct.pack(">I", len(encoded)) + encoded
    return buf


def _make_insert(oid: int, values: list[str | None]) -> bytes:
    return b"I" + struct.pack(">I", oid) + b"N" + _make_tuple_data(values)


def _make_update_new_only(oid: int, new_values: list[str | None]) -> bytes:
    return b"U" + struct.pack(">I", oid) + b"N" + _make_tuple_data(new_values)


def _make_update_with_old(
    oid: int,
    old_values: list[str | None],
    new_values: list[str | None],
) -> bytes:
    body = (
        struct.pack(">I", oid)
        + b"O"
        + _make_tuple_data(old_values)
        + b"N"
        + _make_tuple_data(new_values)
    )
    return b"U" + body


def _make_delete(oid: int, key_values: list[str | None]) -> bytes:
    return b"D" + struct.pack(">I", oid) + b"K" + _make_tuple_data(key_values)


def _make_truncate(oids: list[int]) -> bytes:
    body = struct.pack(">I", len(oids)) + b"\x00"
    for oid in oids:
        body += struct.pack(">I", oid)
    return b"T" + body


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def decoder():
    return PgOutputDecoder()


DOCUMENTS_OID = 12345
DOCUMENTS_COLUMNS = [
    ("id",      23,    True),
    ("content", 25,    False),
    ("score",   701,   False),
]
RELATION_MSG = _make_relation(
    oid=DOCUMENTS_OID,
    schema="public",
    table="documents",
    columns=DOCUMENTS_COLUMNS,
)

_BEGIN_LSN = 0x1A3F28
_BEGIN_TS_US = 1_000_000
_BEGIN_XID = 42
BEGIN_MSG = _make_begin(_BEGIN_LSN, _BEGIN_TS_US, _BEGIN_XID)


# ---------------------------------------------------------------------------
# Begin / Commit
# ---------------------------------------------------------------------------

class TestBeginCommit:
    def test_begin_returns_none_but_sets_state(self, decoder):
        result = decoder.decode(BEGIN_MSG)
        assert result is None
        assert decoder._current_lsn == "0/1A3F28"
        assert decoder._current_xid == _BEGIN_XID
        assert decoder._current_commit_time == PG_EPOCH + timedelta(microseconds=_BEGIN_TS_US)

    def test_commit_returns_none(self, decoder):
        decoder.decode(BEGIN_MSG)
        commit_msg = _make_commit(0x1A3F30, 0x1A3F40, 2_000_000)
        result = decoder.decode(commit_msg)
        assert result is None
        assert decoder._current_lsn == "0/1A3F30"

    def test_empty_payload_returns_none(self, decoder):
        assert decoder.decode(b"") is None


# ---------------------------------------------------------------------------
# Relation
# ---------------------------------------------------------------------------

class TestRelation:
    def test_relation_is_cached(self, decoder):
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
        assert rel.columns[0].name == "id"
        assert rel.columns[0].is_key is True
        assert rel.columns[1].name == "content"
        assert rel.columns[1].is_key is False

    def test_relation_is_updated_on_re_send(self, decoder):
        decoder.decode(RELATION_MSG)
        new_rel_msg = _make_relation(
            oid=DOCUMENTS_OID,
            schema="public",
            table="documents",
            columns=DOCUMENTS_COLUMNS + [("tags", 1009, False)],
        )
        decoder.decode(new_rel_msg)
        assert len(decoder._relations[DOCUMENTS_OID].columns) == 4

    def test_relation_returns_none(self, decoder):
        assert decoder.decode(RELATION_MSG) is None


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

class TestInsert:
    def _setup(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

    def test_insert_basic(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_insert(DOCUMENTS_OID, ["1", "hello world", "0.95"]))
        assert result is not None
        assert result["operation"] == "insert"
        assert result["schema"] == "public"
        assert result["table"] == "documents"
        assert result["row"] == {"id": "1", "content": "hello world", "score": "0.95"}
        assert result["old_row"] is None

    def test_insert_carries_transaction_context(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_insert(DOCUMENTS_OID, ["1", "x", "1.0"]))
        assert result["lsn"] == "0/1A3F28"
        assert result["xid"] == _BEGIN_XID
        assert result["commit_time"] == PG_EPOCH + timedelta(microseconds=_BEGIN_TS_US)

    def test_insert_with_null_column(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_insert(DOCUMENTS_OID, ["2", None, None]))
        assert result["row"]["id"] == "2"
        assert result["row"]["content"] is None
        assert result["row"]["score"] is None

    def test_insert_returns_none_for_unknown_oid(self, decoder):
        decoder.decode(BEGIN_MSG)
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = decoder.decode(_make_insert(DOCUMENTS_OID, ["1", "x", "1.0"]))
        assert result is None
        assert len(w) == 1
        assert "unknown relation OID" in str(w[0].message)


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

class TestUpdate:
    def _setup(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

    def test_update_new_only(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_update_new_only(DOCUMENTS_OID, ["1", "new content", "0.88"]))
        assert result is not None
        assert result["operation"] == "update"
        assert result["row"] == {"id": "1", "content": "new content", "score": "0.88"}
        assert result["old_row"] is None

    def test_update_with_old_row(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_update_with_old(
            DOCUMENTS_OID,
            old_values=["1", "old content", "0.5"],
            new_values=["1", "new content", "0.9"],
        ))
        assert result is not None
        assert result["operation"] == "update"
        assert result["row"] == {"id": "1", "content": "new content", "score": "0.9"}
        assert result["old_row"] == {"id": "1", "content": "old content", "score": "0.5"}


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

class TestDelete:
    def _setup(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

    def test_delete_key_only(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_delete(DOCUMENTS_OID, ["7", None, None]))
        assert result is not None
        assert result["operation"] == "delete"
        assert result["row"]["id"] == "7"
        assert result["old_row"] is None

    def test_delete_carries_correct_lsn(self, decoder):
        self._setup(decoder)
        result = decoder.decode(_make_delete(DOCUMENTS_OID, ["3", None, None]))
        assert result["lsn"] == "0/1A3F28"


# ---------------------------------------------------------------------------
# Truncate
# ---------------------------------------------------------------------------

class TestTruncate:
    def test_truncate_single_table(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)
        result = decoder.decode(_make_truncate([DOCUMENTS_OID]))
        assert result is not None
        assert result["operation"] == "truncate"
        assert result["table"] == "documents"
        assert result["row"] == {}

    def test_truncate_no_relations_returns_none(self, decoder):
        decoder.decode(BEGIN_MSG)
        assert decoder.decode(_make_truncate([])) is None


# ---------------------------------------------------------------------------
# Ignored message types
# ---------------------------------------------------------------------------

class TestIgnoredMessages:
    def test_origin_message_ignored(self, decoder):
        assert decoder.decode(b"O" + b"\x00" * 8) is None

    def test_type_message_ignored(self, decoder):
        assert decoder.decode(b"Y" + b"\x00" * 8) is None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

class TestUtilities:
    def test_lsn_zero(self, decoder):
        decoder.decode(_make_begin(0, 0, 0))
        assert decoder._current_lsn == "0/0"

    def test_lsn_large(self, decoder):
        decoder.decode(_make_begin(0x0001_0000_0000, 0, 1))
        assert decoder._current_lsn == "1/0"

    def test_postgres_epoch_timestamp(self, decoder):
        decoder.decode(_make_begin(0, 0, 1))
        assert decoder._current_commit_time == PG_EPOCH

    def test_unchanged_toast_value(self, decoder):
        decoder.decode(BEGIN_MSG)
        decoder.decode(RELATION_MSG)

        tuple_data = struct.pack(">H", 3)
        tuple_data += b"t" + struct.pack(">I", 1) + b"5"
        tuple_data += b"u"
        tuple_data += b"t" + struct.pack(">I", 3) + b"0.1"

        msg = b"I" + struct.pack(">I", DOCUMENTS_OID) + b"N" + tuple_data
        result = decoder.decode(msg)

        assert result is not None
        assert result["row"]["id"] == "5"
        assert result["row"]["content"] is None
        assert result["row"]["score"] == "0.1"

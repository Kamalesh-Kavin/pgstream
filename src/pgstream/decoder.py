from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import NamedTuple


_PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _pg_ts_to_datetime(microseconds: int) -> datetime:
    return _PG_EPOCH + timedelta(microseconds=microseconds)


def _lsn_to_str(lsn: int) -> str:
    high = lsn >> 32
    low = lsn & 0xFFFFFFFF
    return f"{high:X}/{low:X}"


@dataclass
class ColumnInfo:
    name: str
    type_oid: int
    is_key: bool


@dataclass
class RelationInfo:
    oid: int
    schema: str
    table: str
    columns: list[ColumnInfo] = field(default_factory=list)


class PgOutputDecoder:
    """Stateful parser for the pgoutput logical replication protocol (v1).

    Caches ``Relation`` messages keyed by OID so that subsequent DML messages
    can be decoded into named columns. Transaction context (LSN, timestamp,
    XID) is tracked from ``Begin`` messages and attached to every emitted event.

    Usage::

        decoder = PgOutputDecoder()
        for raw_msg in replication_cursor:
            event = decoder.decode(raw_msg.payload)
            if event is not None:
                yield event
    """

    def __init__(self) -> None:
        self._relations: dict[int, RelationInfo] = {}
        self._current_lsn: str = "0/0"
        self._current_commit_time: datetime = _PG_EPOCH
        self._current_xid: int = 0

    def decode(self, payload: bytes) -> dict | None:
        """Decode one raw pgoutput message.

        Returns a dict for Insert/Update/Delete/Truncate messages, or ``None``
        for Begin/Commit/Relation and other metadata messages.
        """
        if not payload:
            return None

        msg_type = chr(payload[0])
        data = payload[1:]

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

        return None

    def _handle_begin(self, data: bytes) -> None:
        final_lsn, commit_ts, xid = struct.unpack_from(">qqi", data, 0)
        self._current_lsn = _lsn_to_str(final_lsn)
        self._current_commit_time = _pg_ts_to_datetime(commit_ts)
        self._current_xid = xid

    def _handle_commit(self, data: bytes) -> None:
        commit_lsn, end_lsn, commit_ts = struct.unpack_from(">qqq", data, 1)
        self._current_lsn = _lsn_to_str(commit_lsn)
        self._current_commit_time = _pg_ts_to_datetime(commit_ts)

    def _handle_relation(self, data: bytes) -> None:
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4
        schema, offset = _read_cstring(data, offset)
        table, offset = _read_cstring(data, offset)
        offset += 1  # replica identity byte
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
            oid=oid, schema=schema, table=table, columns=columns
        )

    def _handle_insert(self, data: bytes) -> dict | None:
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4
        relation = self._get_relation(oid)
        if relation is None:
            return None
        offset += 1  # skip 'N' byte
        row, offset = self._decode_tuple(data, offset, relation)
        return self._make_event("insert", relation, row, None)

    def _handle_update(self, data: bytes) -> dict | None:
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4
        relation = self._get_relation(oid)
        if relation is None:
            return None

        old_row: dict | None = None
        marker = chr(data[offset])
        if marker in ("K", "O"):
            offset += 1
            old_row, offset = self._decode_tuple(data, offset, relation)
            marker = chr(data[offset])

        assert marker == "N", f"Expected 'N' in UPDATE, got {marker!r}"
        offset += 1
        new_row, offset = self._decode_tuple(data, offset, relation)
        return self._make_event("update", relation, new_row, old_row)

    def _handle_delete(self, data: bytes) -> dict | None:
        offset = 0
        oid, = struct.unpack_from(">I", data, offset)
        offset += 4
        relation = self._get_relation(oid)
        if relation is None:
            return None
        offset += 1  # skip 'K' or 'O' byte
        old_row, offset = self._decode_tuple(data, offset, relation)
        return self._make_event("delete", relation, old_row, None)

    def _handle_truncate(self, data: bytes) -> dict | None:
        offset = 0
        num_relations, = struct.unpack_from(">I", data, offset)
        offset += 4
        offset += 1  # flags byte
        if num_relations == 0:
            return None
        oid, = struct.unpack_from(">I", data, offset)
        relation = self._get_relation(oid)
        if relation is None:
            return None
        return self._make_event("truncate", relation, {}, None)

    def _get_relation(self, oid: int) -> RelationInfo | None:
        rel = self._relations.get(oid)
        if rel is None:
            import warnings
            warnings.warn(
                f"pgstream: received DML for unknown relation OID {oid}. "
                "The Relation message may have been missed. Skipping event."
            )
        return rel

    def _decode_tuple(
        self, data: bytes, offset: int, relation: RelationInfo
    ) -> tuple[dict[str, str | None], int]:
        num_cols, = struct.unpack_from(">H", data, offset)
        offset += 2
        row: dict[str, str | None] = {}

        for i in range(num_cols):
            col_name = relation.columns[i].name if i < len(relation.columns) else f"col_{i}"
            col_type = chr(data[offset])
            offset += 1

            if col_type == "n":
                row[col_name] = None
            elif col_type == "u":
                # Unchanged TOASTed value — not sent by Postgres; emit None.
                row[col_name] = None
            elif col_type == "t":
                val_len, = struct.unpack_from(">I", data, offset)
                offset += 4
                row[col_name] = data[offset : offset + val_len].decode("utf-8")
                offset += val_len
            elif col_type == "b":
                val_len, = struct.unpack_from(">I", data, offset)
                offset += 4
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


def _read_cstring(data: bytes, offset: int) -> tuple[str, int]:
    end = data.index(b"\x00", offset)
    value = data[offset:end].decode("utf-8")
    return value, end + 1

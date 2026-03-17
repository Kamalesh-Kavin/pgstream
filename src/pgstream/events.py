from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


@dataclass
class ChangeEvent:
    """A single committed row-level change decoded from the Postgres WAL.

    Attributes:
        operation:   One of ``"insert"``, ``"update"``, ``"delete"``, ``"truncate"``.
        schema:      Postgres schema name (e.g. ``"public"``).
        table:       Table name (e.g. ``"documents"``).
        row:         New row as ``{column: value}``. Values are always strings or
                     ``None``; cast them yourself (e.g. ``int(event.row["id"])``).
                     For DELETE this contains the old/key row. For TRUNCATE it is
                     an empty dict.
        old_row:     Previous row on UPDATE or DELETE when ``REPLICA IDENTITY FULL``
                     is set. ``None`` otherwise.
        lsn:         WAL Log Sequence Number at commit (e.g. ``"0/1A3F28"``).
        commit_time: UTC datetime of the transaction commit.
        xid:         Postgres transaction ID.
    """

    operation: Literal["insert", "update", "delete", "truncate"]
    schema: str
    table: str
    row: dict[str, str | None]
    old_row: dict[str, str | None] | None
    lsn: str
    commit_time: datetime
    xid: int

    def __repr__(self) -> str:
        row_preview = {k: v for k, v in list(self.row.items())[:3]}
        suffix = "..." if len(self.row) > 3 else ""
        return (
            f"ChangeEvent({self.operation} {self.schema}.{self.table} "
            f"lsn={self.lsn} row={row_preview}{suffix})"
        )

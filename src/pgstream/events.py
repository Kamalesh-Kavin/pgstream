"""
events.py — The ChangeEvent dataclass.

This is the central object that flows through the entire pgstream pipeline.
Every handler the user writes receives a ChangeEvent. It represents a single
committed row-level change decoded from the Postgres WAL.

Design notes:
- We use a plain dataclass (not Pydantic) to keep the core library dependency-free.
- `row` is always a dict[str, str | None] — values are text-encoded by Postgres
  (pgoutput sends column data as text strings by default at protocol_version=1).
  The SDK deliberately does NOT coerce types — it's the user's job to cast
  `event.row["price"]` to float, etc. This keeps pgstream simple and avoids
  surprises with timezone-aware datetimes, Decimals, custom types, etc.
- `old_row` is only populated when the table has REPLICA IDENTITY FULL set,
  or when an UPDATE touches a column in the REPLICA IDENTITY INDEX.
  For most tables it will be None on UPDATE and contain only the PK on DELETE.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


@dataclass
class ChangeEvent:
    """
    A single committed row-level change from the Postgres WAL.

    Attributes:
        operation:   What happened — "insert", "update", "delete", or "truncate".
        schema:      Postgres schema name, e.g. "public".
        table:       Table name, e.g. "documents".
        row:         The new row as a dict of column_name → text value (or None).
                     For DELETE this is the old row (whatever REPLICA IDENTITY provides).
                     For TRUNCATE this is an empty dict.
        old_row:     The old row on UPDATE/DELETE when REPLICA IDENTITY FULL is set.
                     None if not available.
        lsn:         WAL Log Sequence Number at the commit, e.g. "0/1A3F28".
                     Used internally for ACK; exposed so users can checkpoint manually.
        commit_time: UTC datetime when this transaction committed.
        xid:         Postgres transaction ID (XID). Useful for grouping related changes.
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
        # Compact repr — avoids printing huge row dicts in log lines.
        row_preview = {k: v for k, v in list(self.row.items())[:3]}
        suffix = "..." if len(self.row) > 3 else ""
        return (
            f"ChangeEvent({self.operation} {self.schema}.{self.table} "
            f"lsn={self.lsn} row={row_preview}{suffix})"
        )

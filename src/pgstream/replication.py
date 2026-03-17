from __future__ import annotations

import logging
import select
import threading
import time
from typing import Callable

import psycopg2
import psycopg2.extras
from psycopg2.extras import (
    LogicalReplicationConnection,
    ReplicationCursor,
    ReplicationMessage,
)

from .decoder import PgOutputDecoder
from .events import ChangeEvent

logger = logging.getLogger("pgstream.replication")


class SlotManager:
    """Creates and drops the replication slot and publication.

    Uses a normal (non-replication) psycopg2 connection for all DDL.
    All methods are safe to call from a thread executor.
    """

    def __init__(self, dsn: str, slot_name: str, publication_name: str) -> None:
        self._dsn = dsn
        self.slot_name = slot_name
        self.publication_name = publication_name

    def setup(self, tables: list[str]) -> None:
        """Create the publication and replication slot if they don't exist.

        Idempotent — safe to call on every startup.

        Args:
            tables: Unqualified table names to watch, e.g. ``["documents"]``.
        """
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = True
        try:
            cur = conn.cursor()

            cur.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s",
                (self.publication_name,),
            )
            if cur.fetchone() is None:
                table_list = ", ".join(f'"{t}"' for t in tables)
                cur.execute(
                    f"CREATE PUBLICATION {self.publication_name} FOR TABLE {table_list}"
                )
                logger.info("Created publication %r for tables: %s", self.publication_name, tables)
            else:
                logger.info("Publication %r already exists — skipping.", self.publication_name)

            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (self.slot_name,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (self.slot_name,),
                )
                logger.info("Created replication slot %r.", self.slot_name)
            else:
                logger.info("Replication slot %r already exists — skipping.", self.slot_name)
        finally:
            conn.close()

    def teardown(self, drop_slot: bool = True, drop_publication: bool = True) -> None:
        """Drop the replication slot and/or publication.

        Args:
            drop_slot:        Drop the replication slot (default ``True``).
            drop_publication: Drop the publication (default ``True``).

        Warning:
            Dropping the slot causes Postgres to stop retaining WAL. Any events
            that occur while the slot is absent will be permanently lost.
        """
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = True
        try:
            cur = conn.cursor()
            if drop_slot:
                cur.execute(
                    "SELECT pg_drop_replication_slot(%s) "
                    "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                    (self.slot_name, self.slot_name),
                )
                logger.info("Dropped replication slot %r.", self.slot_name)
            if drop_publication:
                cur.execute(f"DROP PUBLICATION IF EXISTS {self.publication_name}")
                logger.info("Dropped publication %r.", self.publication_name)
        finally:
            conn.close()


class ReplicationStream:
    """Opens a replication connection and streams :class:`ChangeEvent` objects.

    Blocking — runs the Postgres replication protocol loop in the calling
    thread. :class:`~pgstream.stream.PGStream` wraps this in a background
    thread so the asyncio event loop is not blocked.

    Call :meth:`stop` from any thread to exit the loop cleanly.
    """

    KEEPALIVE_INTERVAL = 10.0

    def __init__(self, dsn: str, slot_name: str, publication_name: str) -> None:
        self._dsn = dsn
        self._slot_name = slot_name
        self._publication_name = publication_name
        self._stop_event = threading.Event()
        self._decoder = PgOutputDecoder()
        self._conn: psycopg2.extensions.connection | None = None

    def stop(self) -> None:
        """Signal the streaming loop to stop after the current message."""
        self._stop_event.set()
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass

    def stream(
        self,
        tables: list[str],
        on_event: Callable[[ChangeEvent], None],
    ) -> None:
        """Stream WAL events from Postgres, calling *on_event* for each.

        The LSN is ACKed to Postgres only after *on_event* returns without
        raising, guaranteeing at-least-once delivery.

        Blocks until :meth:`stop` is called or a fatal error occurs.

        Args:
            tables:   Table names being watched (used for filtering truncates).
            on_event: Synchronous callback invoked for each :class:`ChangeEvent`.
        """
        self._stop_event.clear()

        self._conn = psycopg2.connect(
            self._dsn,
            connection_factory=LogicalReplicationConnection,
        )

        try:
            cur: ReplicationCursor = self._conn.cursor()
            cur.start_replication(
                slot_name=self._slot_name,
                decode=False,
                options={
                    "proto_version": "1",
                    "publication_names": self._publication_name,
                },
            )
            logger.info(
                "Started replication from slot %r / publication %r",
                self._slot_name,
                self._publication_name,
            )

            last_keepalive = time.monotonic()

            while not self._stop_event.is_set():
                try:
                    msg: ReplicationMessage | None = cur.read_message()
                except psycopg2.OperationalError:
                    break

                if msg is not None:
                    raw = self._decoder.decode(bytes(msg.payload))

                    if raw is not None:
                        event = ChangeEvent(
                            operation=raw["operation"],
                            schema=raw["schema"],
                            table=raw["table"],
                            row=raw["row"],
                            old_row=raw["old_row"],
                            lsn=raw["lsn"],
                            commit_time=raw["commit_time"],
                            xid=raw["xid"],
                        )
                        on_event(event)

                    try:
                        cur.send_feedback(flush_lsn=msg.data_start)
                    except (psycopg2.InterfaceError, psycopg2.OperationalError):
                        break
                    last_keepalive = time.monotonic()

                else:
                    try:
                        fd = self._conn.fileno()
                        select.select([fd], [], [], 1.0)
                    except OSError:
                        break

                    now = time.monotonic()
                    if now - last_keepalive > self.KEEPALIVE_INTERVAL:
                        try:
                            cur.send_feedback()
                        except (psycopg2.InterfaceError, psycopg2.OperationalError):
                            break
                        last_keepalive = now

        finally:
            try:
                if self._conn and not self._conn.closed:
                    self._conn.close()
            except Exception:
                pass
            self._conn = None
            logger.info("Replication stream stopped.")

"""
replication.py — Replication slot lifecycle and WAL streaming loop.

This module owns everything that talks directly to Postgres via the
replication protocol. It is the only place in pgstream that uses psycopg2.

Responsibilities:
  1. Open a replication connection (separate from the normal query connection).
  2. Create/verify the replication slot and the publication (idempotent).
  3. Stream WAL messages, decode them via PgOutputDecoder, and yield ChangeEvents.
  4. Send feedback (ACK the LSN) after each event is successfully processed.
  5. Drop the slot/publication on teardown.

Key concepts:

  Replication connection vs. normal connection:
    Logical replication uses a special Postgres protocol mode. You cannot
    run normal SQL (SELECT, INSERT, etc.) on a replication connection. psycopg2
    exposes this via `connection_factory=LogicalReplicationConnection`. We need
    TWO connections: one for replication streaming, one for normal SQL (to
    CREATE PUBLICATION, query pg_replication_slots, etc.).

  Replication slot:
    A named, persistent cursor into the WAL. Postgres holds WAL segments until
    the slot's LSN is advanced. Creating a slot is idempotent (we check first).
    IMPORTANT: slots that are never consumed will cause WAL to accumulate and
    eventually fill disk. pgstream always drops slots on teardown.

  Publication:
    A Postgres publication declares which tables' changes flow through a slot.
    We create one publication per pgstream instance. Like the slot, we create
    it idempotently. The publication is what honours the `watch("table1", ...)`
    call — only listed tables are published.

  Feedback / ACK:
    Postgres tracks the "confirmed flush LSN" for each slot. We advance it by
    calling send_feedback(flush_lsn=msg.data_start) AFTER the user's handler
    returns successfully. If the handler raises, we do NOT ACK — guaranteeing
    at-least-once delivery on restart.

  consume_stream vs. read_message:
    psycopg2 offers two consumption patterns:
      - consume_stream(callback): blocking; calls callback for each message.
      - read_message(): non-blocking; returns None if no message available.
    We use read_message() in a loop so we can:
      a) respect a stop signal (asyncio-compatible)
      b) send keepalive feedback even when idle (Postgres requires periodic
         status updates or it will consider the consumer dead after
         wal_sender_timeout, default 60s)
"""

from __future__ import annotations

import logging
import select
import threading
import time
from typing import Callable, Iterator

import psycopg2
import psycopg2.extras
from psycopg2.extras import (
    LogicalReplicationConnection,
    ReplicationCursor,
    ReplicationMessage,
    StopReplication,
)

from .decoder import PgOutputDecoder
from .events import ChangeEvent

logger = logging.getLogger("pgstream.replication")


# ---------------------------------------------------------------------------
# SlotManager — handles setup and teardown via normal SQL connection
# ---------------------------------------------------------------------------

class SlotManager:
    """
    Manages the replication slot and publication lifecycle.

    Uses a normal (non-replication) psycopg2 connection for all DDL,
    because replication connections cannot run regular SQL.
    """

    def __init__(self, dsn: str, slot_name: str, publication_name: str) -> None:
        self._dsn = dsn
        self.slot_name = slot_name
        self.publication_name = publication_name

    def setup(self, tables: list[str]) -> None:
        """
        Create the publication and replication slot if they don't already exist.

        `tables` is a list of unqualified table names, e.g. ["documents", "chunks"].
        They must exist in the database before setup() is called.

        This method is idempotent — safe to call on every startup.
        """
        conn = psycopg2.connect(self._dsn)
        conn.autocommit = True  # DDL cannot run inside a transaction in Postgres
        try:
            cur = conn.cursor()

            # -- Publication --------------------------------------------------
            # Check if the publication already exists.
            cur.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s",
                (self.publication_name,),
            )
            if cur.fetchone() is None:
                # Build: CREATE PUBLICATION <name> FOR TABLE t1, t2, ...
                # We quote identifiers to handle reserved words / mixed case.
                table_list = ", ".join(
                    f'"{t}"' for t in tables
                )
                cur.execute(
                    f"CREATE PUBLICATION {self.publication_name} FOR TABLE {table_list}"
                )
                logger.info("Created publication %r for tables: %s", self.publication_name, tables)
            else:
                logger.info("Publication %r already exists — skipping creation.", self.publication_name)

            # -- Replication slot ---------------------------------------------
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (self.slot_name,),
            )
            if cur.fetchone() is None:
                # pg_create_logical_replication_slot returns (slot_name, lsn)
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (self.slot_name,),
                )
                logger.info("Created replication slot %r.", self.slot_name)
            else:
                logger.info("Replication slot %r already exists — skipping creation.", self.slot_name)

        finally:
            conn.close()

    def teardown(self, drop_slot: bool = True, drop_publication: bool = True) -> None:
        """
        Drop the replication slot and/or publication.

        By default both are dropped. Pass drop_slot=False to keep the slot
        (e.g. if you want to resume exactly where you left off after a redeploy
        without rebuilding a new snapshot).

        WARNING: If you never call teardown(), the slot will hold WAL forever.
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
                cur.execute(
                    f"DROP PUBLICATION IF EXISTS {self.publication_name}"
                )
                logger.info("Dropped publication %r.", self.publication_name)

        finally:
            conn.close()


# ---------------------------------------------------------------------------
# ReplicationStream — the streaming loop
# ---------------------------------------------------------------------------

class ReplicationStream:
    """
    Opens a replication connection and streams ChangeEvents indefinitely.

    Usage:
        stream = ReplicationStream(dsn, slot_name="pgstream", publication_name="pgstream")
        for event in stream.stream(tables=["documents"]):
            process(event)
            # stream automatically ACKs each event after this loop iteration

    The iteration is blocking — it runs the Postgres replication protocol loop
    in the calling thread. PGStream wraps this in a background thread so the
    main asyncio event loop is not blocked.

    Stop the loop by calling stream.stop() from any thread.
    """

    # How often to send keepalive feedback to Postgres (seconds).
    # Must be less than wal_sender_timeout (default 60s on most installs).
    KEEPALIVE_INTERVAL = 10.0

    def __init__(self, dsn: str, slot_name: str, publication_name: str) -> None:
        self._dsn = dsn
        self._slot_name = slot_name
        self._publication_name = publication_name
        self._stop_event = threading.Event()
        self._decoder = PgOutputDecoder()
        self._conn: psycopg2.connection | None = None

    def stop(self) -> None:
        """Signal the streaming loop to stop after the current message."""
        self._stop_event.set()
        # If the loop is blocked in select(), wake it up by closing the conn.
        # The select() will return readable, read_message() will raise, and
        # we catch that to break cleanly.
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
        """
        Stream WAL events from Postgres and call `on_event` for each.

        `on_event` is called synchronously in this thread. After it returns
        without raising, the WAL position (LSN) is ACKed to Postgres.
        If `on_event` raises, the exception propagates out of this method
        and the LSN is NOT ACKed — so on restart Postgres replays from the
        last confirmed LSN.

        This method blocks until stop() is called or a fatal error occurs.
        """
        self._stop_event.clear()

        # Open a dedicated replication connection.
        # NOTE: replication=database is required in the DSN / connect args.
        # psycopg2 handles this via the connection_factory kwarg.
        self._conn = psycopg2.connect(
            self._dsn,
            connection_factory=LogicalReplicationConnection,
        )

        try:
            cur: ReplicationCursor = self._conn.cursor()

            # Start streaming from the slot.
            # proto_version=1 and publication_names are required pgoutput options.
            cur.start_replication(
                slot_name=self._slot_name,
                decode=False,        # we get raw bytes; we decode ourselves
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
                # read_message() is non-blocking — returns None immediately
                # if there are no buffered messages.
                try:
                    msg: ReplicationMessage | None = cur.read_message()
                except psycopg2.OperationalError:
                    # Connection closed (e.g. by stop()) — exit cleanly.
                    break

                if msg is not None:
                    # Decode the raw pgoutput bytes into a structured dict.
                    raw = self._decoder.decode(bytes(msg.payload))

                    if raw is not None:
                        # Build a ChangeEvent from the decoded dict.
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

                        # Call the user's handler. If it raises, we propagate
                        # without ACKing — at-least-once delivery guarantee.
                        on_event(event)

                    # ACK this LSN regardless of whether we emitted an event.
                    # This advances the slot past metadata-only messages
                    # (Begin, Commit, Relation) so WAL doesn't accumulate.
                    try:
                        cur.send_feedback(flush_lsn=msg.data_start)
                    except (psycopg2.InterfaceError, psycopg2.OperationalError):
                        # Cursor/connection was closed (e.g. by stop()) between
                        # reading the message and ACKing it — exit cleanly.
                        break
                    last_keepalive = time.monotonic()

                else:
                    # No message available. Wait briefly for activity.
                    # select() on the connection fd gives us an efficient wait
                    # without spinning at 100% CPU.
                    try:
                        fd = self._conn.fileno()
                        r, _, _ = select.select([fd], [], [], 1.0)
                    except OSError:
                        # fd is invalid — connection was closed by stop(). Exit.
                        break

                    # Send a keepalive (status update) if we've been idle.
                    # This tells Postgres we're alive and prevents wal_sender_timeout.
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

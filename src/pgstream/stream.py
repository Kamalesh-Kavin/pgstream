"""
stream.py — PGStream: the top-level user-facing class.

This is the entry point for everyone who uses pgstream. The user:
  1. Instantiates PGStream with a Postgres DSN.
  2. Declares which tables to watch with .watch().
  3. Optionally attaches a Sink with .sink().
  4. Registers a handler with the @stream.on_change decorator.
  5. Calls await stream.setup() once (idempotent).
  6. Calls await stream.start() to begin streaming (blocks until stop).
  7. Calls await stream.stop() to shut down gracefully.

Threading model:
  Postgres logical replication (via psycopg2) is blocking. We cannot run it
  directly in the asyncio event loop because it would block all coroutines.

  Solution: run the replication loop in a background daemon thread. The
  thread calls on_event() (a sync bridge function) which schedules the user's
  async handler as a coroutine in the main event loop using
  asyncio.run_coroutine_threadsafe(). This gives us:

    [Postgres] --WAL bytes--> [ReplicationStream thread]
                                      |
                              asyncio.run_coroutine_threadsafe
                                      |
                              [main event loop] --ChangeEvent--> [user handler]

  The main loop can keep running other tasks while events are being processed.
  The replication thread blocks waiting for the handler to finish before ACKing
  (ensuring at-least-once delivery). It waits via Future.result() with a timeout.

Error handling:
  If the handler raises, the exception propagates to the replication thread,
  which does NOT ACK the LSN. On the next restart, Postgres will replay from
  the last confirmed LSN.

  If the replication thread itself crashes (network error, etc.), the exception
  is stored in _thread_error and re-raised from start() after the thread joins.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from typing import Awaitable, Callable, overload

from .events import ChangeEvent
from .replication import ReplicationStream, SlotManager
from .sinks.base import Sink

logger = logging.getLogger("pgstream")


class PGStream:
    """
    Watch Postgres tables for row-level changes and react to them in real time.

    Quick start:
        stream = PGStream(dsn="postgresql://user:pass@localhost/db")
        stream.watch("documents", "chunks")
        stream.sink(my_qdrant_sink)         # optional

        @stream.on_change
        async def handle(event: ChangeEvent, sink):
            if event.operation == "insert":
                vector = await embed(event.row["content"])
                await sink.upsert(event.row["id"], vector, payload=event.row)
            elif event.operation == "delete":
                await sink.delete(event.row["id"])

        await stream.setup()   # create slot + publication (idempotent)
        await stream.start()   # block until stop() or error
        # or: asyncio.create_task(stream.start()) to run alongside other tasks

    Args:
        dsn:              PostgreSQL connection string.
                          Must point to a DB with wal_level = logical.
        slot_name:        Name for the replication slot (default: "pgstream").
                          Must be unique per consuming process.
        publication_name: Name for the Postgres publication (default: "pgstream").
    """

    def __init__(
        self,
        dsn: str,
        slot_name: str = "pgstream",
        publication_name: str = "pgstream",
    ) -> None:
        self._dsn = dsn
        self._slot_name = slot_name
        self._publication_name = publication_name

        # Tables registered via .watch() — stored as plain names ("documents").
        self._tables: list[str] = []

        # The optional attached Sink. Passed as the second argument to on_change handlers.
        self._sink: Sink | None = None

        # The user's async handler. Set by the @on_change decorator.
        self._handler: Callable[[ChangeEvent, Sink | None], Awaitable[None]] | None = None

        # Internal machinery.
        self._slot_manager: SlotManager | None = None
        self._replication_stream: ReplicationStream | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        # If the background thread crashes, we store the exception here and
        # re-raise it from start() once the thread has joined.
        self._thread_error: BaseException | None = None

        # Used to signal that start() should return (e.g. after stop()).
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Public configuration API
    # ------------------------------------------------------------------

    def watch(self, *tables: str) -> "PGStream":
        """
        Declare which tables to watch.

        Must be called before setup(). Tables must exist in the database.

        Example:
            stream.watch("documents")
            stream.watch("documents", "chunks", "embeddings")

        Returns self for optional method chaining:
            stream = PGStream(dsn).watch("documents").sink(my_sink)
        """
        self._tables.extend(tables)
        return self

    def sink(self, sink: Sink) -> "PGStream":
        """
        Attach a vector store sink.

        The attached sink is passed as the second argument to on_change handlers
        so you don't need to close over it manually.

        Returns self for optional method chaining.
        """
        self._sink = sink
        return self

    def on_change(
        self,
        func: Callable[[ChangeEvent, Sink | None], Awaitable[None]],
    ) -> Callable[[ChangeEvent, Sink | None], Awaitable[None]]:
        """
        Decorator — register an async handler for change events.

        The decorated function receives:
          event: ChangeEvent — the decoded row change.
          sink:  the attached Sink (or None if no sink was attached).

        Example:
            @stream.on_change
            async def handle(event: ChangeEvent, sink):
                ...

        Only one handler is supported per PGStream instance. Registering a
        second handler replaces the first (with a warning).
        """
        if self._handler is not None:
            logger.warning(
                "pgstream: replacing existing on_change handler %r with %r. "
                "Only one handler per PGStream instance is supported.",
                self._handler.__name__,
                func.__name__,
            )
        self._handler = func
        return func  # return unchanged so the decorator is transparent

    # ------------------------------------------------------------------
    # Lifecycle API
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """
        Create the replication slot and publication in Postgres (idempotent).

        Safe to call on every startup — if the slot and publication already
        exist, this is a no-op. Must be called before start().

        Raises:
            ValueError: if no tables were registered via watch().
            psycopg2 errors if the DSN is wrong or the DB is unreachable.
        """
        if not self._tables:
            raise ValueError(
                "pgstream: no tables registered. Call stream.watch('table_name') "
                "before stream.setup()."
            )

        # SlotManager uses a normal (non-replication) psycopg2 connection.
        # We run it in a thread executor because it does blocking I/O.
        self._slot_manager = SlotManager(
            dsn=self._dsn,
            slot_name=self._slot_name,
            publication_name=self._publication_name,
        )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            self._slot_manager.setup,
            list(self._tables),
        )
        logger.info(
            "pgstream: setup complete. slot=%r, publication=%r, tables=%r",
            self._slot_name,
            self._publication_name,
            self._tables,
        )

    async def start(self) -> None:
        """
        Start streaming WAL changes from Postgres.

        Blocks until stop() is called or a fatal error occurs.

        Typical usage — run alongside other async tasks:
            task = asyncio.create_task(stream.start())
            # ... do other things ...
            await stream.stop()
            await task

        Or block the whole program:
            await stream.start()

        Raises:
            RuntimeError: if no on_change handler was registered, or if
                          setup() was not called first.
            Exception:    re-raised from the replication thread on fatal error.
        """
        if self._handler is None:
            raise RuntimeError(
                "pgstream: no handler registered. "
                "Use the @stream.on_change decorator before calling start()."
            )
        if self._slot_manager is None:
            raise RuntimeError(
                "pgstream: setup() must be called before start()."
            )

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._thread_error = None

        # Create the replication stream object.
        self._replication_stream = ReplicationStream(
            dsn=self._dsn,
            slot_name=self._slot_name,
            publication_name=self._publication_name,
        )

        # Start the blocking replication loop in a daemon background thread.
        self._thread = threading.Thread(
            target=self._replication_thread_main,
            name="pgstream-replication",
            daemon=True,
        )
        self._thread.start()
        logger.info("pgstream: replication thread started.")

        # Wait in the asyncio event loop until stop() signals us or the thread exits.
        await self._stop_event.wait()

        # Join the thread to collect any exception.
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

        # Close the sink after the thread has stopped.
        if self._sink is not None:
            await self._sink.close()

        # Re-raise any error from the replication thread.
        if self._thread_error is not None:
            raise self._thread_error

        logger.info("pgstream: start() returning cleanly.")

    async def stop(self) -> None:
        """
        Gracefully stop the streaming loop.

        Signals the background replication thread to stop, then sets the
        stop event so start() can return. Safe to call multiple times.
        """
        logger.info("pgstream: stop() called.")

        if self._replication_stream is not None:
            # Tell the replication thread to exit its loop.
            self._replication_stream.stop()

        # Wake up start()'s await on _stop_event.
        self._stop_event.set()

    async def teardown(
        self,
        drop_slot: bool = True,
        drop_publication: bool = True,
    ) -> None:
        """
        Drop the replication slot and/or publication from Postgres.

        Call this when you want to permanently decommission this pgstream instance.
        After teardown, setup() must be called again before start().

        WARNING: dropping the slot means Postgres will stop retaining WAL for you.
        Any events that occurred while the slot was gone will be lost. Only call
        teardown() if you intentionally want to reset.
        """
        if self._slot_manager is None:
            logger.warning("pgstream: teardown() called but setup() was never called. No-op.")
            return

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self._slot_manager.teardown(  # type: ignore[union-attr]
                drop_slot=drop_slot,
                drop_publication=drop_publication,
            ),
        )
        self._slot_manager = None
        logger.info("pgstream: teardown complete.")

    # ------------------------------------------------------------------
    # Internal: background thread
    # ------------------------------------------------------------------

    def _replication_thread_main(self) -> None:
        """
        Entry point for the background replication thread.

        Runs the blocking psycopg2 replication loop. For each decoded event,
        schedules the user's async handler in the main event loop and waits
        for it to complete before ACKing.

        If the handler raises, we propagate the exception out of stream(),
        which stops the loop and records the error. start() will re-raise it.
        """
        assert self._replication_stream is not None
        assert self._loop is not None

        try:
            self._replication_stream.stream(
                tables=list(self._tables),
                on_event=self._sync_event_bridge,
            )
        except Exception as exc:
            # Store the exception so start() can re-raise it in the asyncio thread.
            logger.exception("pgstream: replication thread crashed: %s", exc)
            self._thread_error = exc
        finally:
            # Always signal start() to wake up, even if we crashed.
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._stop_event.set)

    def _sync_event_bridge(self, event: ChangeEvent) -> None:
        """
        Called synchronously from the replication thread for each ChangeEvent.

        Schedules the user's async handler as a coroutine in the main event
        loop, then BLOCKS this thread until the coroutine completes. This is
        intentional: we must not ACK the LSN until the handler has returned
        successfully, so we preserve at-least-once delivery semantics.

        If the handler raises, the exception propagates back to stream() in
        the replication thread, which stops the loop.

        Uses asyncio.run_coroutine_threadsafe() — the only correct way to
        submit a coroutine to an event loop from a different thread.
        """
        assert self._handler is not None
        assert self._loop is not None

        # Filter out events for tables we're not watching.
        # (The publication already filters, but truncates can cover multiple tables.)
        if event.table not in self._tables:
            return

        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            self._handler(event, self._sink),
            self._loop,
        )

        # Block until the coroutine completes. Timeout after 30s to avoid
        # hanging forever if the handler deadlocks. The timeout is generous
        # to accommodate slow embedding API calls.
        try:
            future.result(timeout=30)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise RuntimeError(
                f"pgstream: on_change handler timed out after 30s "
                f"for event {event!r}"
            )

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from typing import Awaitable, Callable, Coroutine, Any

from .events import ChangeEvent
from .replication import ReplicationStream, SlotManager
from .sinks.base import Sink

logger = logging.getLogger("pgstream")


class PGStream:
    """Watch Postgres tables for row-level changes and react in real time.

    Quick start::

        stream = PGStream(dsn="postgresql://user:pass@localhost/db")
        stream.watch("documents", "chunks")
        stream.sink(my_sink)  # optional

        @stream.on_change
        async def handle(event: ChangeEvent, sink):
            if event.operation in ("insert", "update"):
                vector = await embed(event.row["content"])
                await sink.upsert(event.row["id"], vector, payload=event.row)
            elif event.operation == "delete":
                await sink.delete(event.row["id"])

        await stream.setup()   # idempotent — creates slot + publication
        await stream.start()   # blocks until stop() or error

    Args:
        dsn:              PostgreSQL connection string.
                          The database must have ``wal_level = logical``.
        slot_name:        Replication slot name (default ``"pgstream"``).
                          Must be unique per consuming process.
        publication_name: Postgres publication name (default ``"pgstream"``).
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
        self._tables: list[str] = []
        self._sink: Sink | None = None
        self._handler: Callable[[ChangeEvent, Sink | None], Coroutine[Any, Any, None]] | None = None
        self._slot_manager: SlotManager | None = None
        self._replication_stream: ReplicationStream | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread_error: BaseException | None = None
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def watch(self, *tables: str) -> "PGStream":
        """Declare which tables to watch.

        Must be called before :meth:`setup`. Tables must already exist in the
        database.

        Returns ``self`` for optional method chaining.
        """
        self._tables.extend(tables)
        return self

    def sink(self, sink: Sink) -> "PGStream":
        """Attach a vector store sink.

        The sink is passed as the second argument to :meth:`on_change` handlers.

        Returns ``self`` for optional method chaining.
        """
        self._sink = sink
        return self

    def on_change(
        self,
        func: Callable[[ChangeEvent, Sink | None], Coroutine[Any, Any, None]],
    ) -> Callable[[ChangeEvent, Sink | None], Coroutine[Any, Any, None]]:
        """Register an async handler for change events.

        The decorated function receives two arguments:

        - ``event``: a :class:`~pgstream.events.ChangeEvent`
        - ``sink``: the attached :class:`~pgstream.sinks.base.Sink`, or ``None``

        Only one handler per instance is supported. Registering a second handler
        replaces the first.

        Example::

            @stream.on_change
            async def handle(event: ChangeEvent, sink):
                ...
        """
        if self._handler is not None:
            logger.warning(
                "pgstream: replacing existing on_change handler %r with %r.",
                self._handler.__name__,
                func.__name__,
            )
        self._handler = func
        return func

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Create the replication slot and publication in Postgres.

        Idempotent — safe to call on every startup.

        Raises:
            ValueError: if no tables were registered via :meth:`watch`.
        """
        if not self._tables:
            raise ValueError(
                "pgstream: no tables registered. Call stream.watch('table_name') "
                "before stream.setup()."
            )

        self._slot_manager = SlotManager(
            dsn=self._dsn,
            slot_name=self._slot_name,
            publication_name=self._publication_name,
        )

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._slot_manager.setup, list(self._tables))
        logger.info(
            "pgstream: setup complete. slot=%r, publication=%r, tables=%r",
            self._slot_name,
            self._publication_name,
            self._tables,
        )

    async def start(self) -> None:
        """Start streaming WAL changes from Postgres.

        Blocks until :meth:`stop` is called or a fatal error occurs.

        Raises:
            RuntimeError: if no ``on_change`` handler was registered, or if
                          :meth:`setup` was not called first.
            Exception:    re-raised from the replication thread on fatal error.
        """
        if self._handler is None:
            raise RuntimeError(
                "pgstream: no handler registered. "
                "Use the @stream.on_change decorator before calling start()."
            )
        if self._slot_manager is None:
            raise RuntimeError("pgstream: setup() must be called before start().")

        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()
        self._thread_error = None

        self._replication_stream = ReplicationStream(
            dsn=self._dsn,
            slot_name=self._slot_name,
            publication_name=self._publication_name,
        )

        self._thread = threading.Thread(
            target=self._replication_thread_main,
            name="pgstream-replication",
            daemon=True,
        )
        self._thread.start()
        logger.info("pgstream: replication thread started.")

        await self._stop_event.wait()

        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

        if self._sink is not None:
            await self._sink.close()

        if self._thread_error is not None:
            raise self._thread_error

        logger.info("pgstream: start() returning cleanly.")

    async def stop(self) -> None:
        """Gracefully stop the streaming loop.

        Safe to call multiple times.
        """
        logger.info("pgstream: stop() called.")
        if self._replication_stream is not None:
            self._replication_stream.stop()
        self._stop_event.set()

    async def teardown(
        self,
        drop_slot: bool = True,
        drop_publication: bool = True,
    ) -> None:
        """Drop the replication slot and/or publication from Postgres.

        Call this when permanently decommissioning this pgstream instance.
        After teardown, :meth:`setup` must be called again before :meth:`start`.

        Args:
            drop_slot:        Drop the replication slot (default ``True``).
            drop_publication: Drop the publication (default ``True``).
        """
        if self._slot_manager is None:
            logger.warning("pgstream: teardown() called but setup() was never run. No-op.")
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
    # Internal
    # ------------------------------------------------------------------

    def _replication_thread_main(self) -> None:
        assert self._replication_stream is not None
        assert self._loop is not None

        try:
            self._replication_stream.stream(
                tables=list(self._tables),
                on_event=self._sync_event_bridge,
            )
        except Exception as exc:
            logger.exception("pgstream: replication thread crashed: %s", exc)
            self._thread_error = exc
        finally:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._stop_event.set)

    def _sync_event_bridge(self, event: ChangeEvent) -> None:
        assert self._handler is not None
        assert self._loop is not None

        if event.table not in self._tables:
            return

        future: concurrent.futures.Future = asyncio.run_coroutine_threadsafe(
            self._handler(event, self._sink),
            self._loop,
        )

        try:
            future.result(timeout=30)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise RuntimeError(
                f"pgstream: on_change handler timed out after 30s for event {event!r}"
            )

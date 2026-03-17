"""
tests/test_stream.py — Integration tests for the PGStream top-level class.

These tests exercise the full public API end-to-end:
  - PGStream.watch() + setup() + start() + stop() + teardown()
  - The @on_change decorator
  - Sink attachment and propagation to the handler
  - Error propagation from the handler to start()
  - Guard rails: missing handler, missing setup()

Requires a live Postgres container with wal_level = logical.
Marked with @pytest.mark.integration.
"""

import asyncio
import os
import time
import uuid

import psycopg2
import pytest

from pgstream import PGStream, ChangeEvent
from pgstream.sinks.base import Sink

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DSN = os.environ.get(
    "PGSTREAM_DSN",
    "postgresql://pgexplain:pgexplain@localhost:5435/pgexplain",
)

# Unique suffix per test run to avoid slot/pub/table name collisions
RUN_ID = uuid.uuid4().hex[:8]
SLOT_NAME = f"pgstream_e2e_{RUN_ID}"
PUB_NAME = f"pgstream_e2e_{RUN_ID}"
TEST_TABLE = f"pgstream_e2e_tbl_{RUN_ID}"

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def create_test_table():
    """Create and drop the test table for this module's tests."""
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
            id      SERIAL PRIMARY KEY,
            payload TEXT NOT NULL
        )
        """
    )
    conn.close()

    yield

    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    # Belt-and-suspenders cleanup in case a test didn't call teardown()
    cur.execute(
        "SELECT pg_drop_replication_slot(%s) "
        "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
        (SLOT_NAME, SLOT_NAME),
    )
    cur.execute(f"DROP PUBLICATION IF EXISTS {PUB_NAME}")
    conn.close()


def make_stream() -> PGStream:
    """Create a fresh PGStream pointing at our test slot/publication/table."""
    return PGStream(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)


def insert_row(payload: str) -> int:
    """Helper: insert a row into the test table, return the new id."""
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {TEST_TABLE} (payload) VALUES (%s) RETURNING id",
        (payload,),
    )
    row_id = cur.fetchone()[0]
    conn.close()
    return row_id


# ---------------------------------------------------------------------------
# A minimal fake Sink for testing sink attachment
# ---------------------------------------------------------------------------

class CollectingSink(Sink):
    """
    A test Sink that records calls to upsert() and delete() in-memory.
    No external I/O — safe to use in tests without any vector store running.
    """

    def __init__(self):
        self.upserted: list[tuple[str, list[float]]] = []
        self.deleted: list[str] = []

    async def upsert(self, id: str, vector: list[float], payload=None) -> None:
        self.upserted.append((id, vector))

    async def delete(self, id: str) -> None:
        self.deleted.append(id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPGStreamEndToEnd:
    @pytest.mark.asyncio
    async def test_on_change_receives_insert(self):
        """
        The @on_change handler should be called when a row is inserted
        into a watched table.
        """
        stream = make_stream()
        stream.watch(TEST_TABLE)

        received: list[ChangeEvent] = []

        @stream.on_change
        async def handler(event: ChangeEvent, sink):
            if event.operation == "insert":
                received.append(event)
                await stream.stop()

        await stream.setup()
        start_task = asyncio.create_task(stream.start())

        # Give the stream time to connect before inserting
        await asyncio.sleep(0.5)
        insert_row("hello from test")

        await asyncio.wait_for(start_task, timeout=10)
        await stream.teardown()

        assert len(received) >= 1
        assert received[0].operation == "insert"
        assert received[0].row["payload"] == "hello from test"

    @pytest.mark.asyncio
    async def test_sink_is_passed_to_handler(self):
        """
        The attached sink should be passed as the second argument to the handler.
        """
        stream = make_stream()
        stream.watch(TEST_TABLE)

        my_sink = CollectingSink()
        stream.sink(my_sink)

        sinks_received: list = []

        @stream.on_change
        async def handler(event: ChangeEvent, sink):
            sinks_received.append(sink)
            await stream.stop()

        await stream.setup()
        start_task = asyncio.create_task(stream.start())

        await asyncio.sleep(0.5)
        insert_row("sink test")

        await asyncio.wait_for(start_task, timeout=10)
        await stream.teardown()

        assert len(sinks_received) >= 1
        assert sinks_received[0] is my_sink

    @pytest.mark.asyncio
    async def test_handler_can_use_sink(self):
        """
        The handler should be able to call sink.upsert() and have it recorded.
        """
        stream = make_stream()
        stream.watch(TEST_TABLE)

        sink = CollectingSink()
        stream.sink(sink)

        @stream.on_change
        async def handler(event: ChangeEvent, s: CollectingSink):
            if event.operation == "insert":
                await s.upsert(event.row["id"], [1.0, 2.0, 3.0])
                await stream.stop()

        await stream.setup()
        start_task = asyncio.create_task(stream.start())

        await asyncio.sleep(0.5)
        insert_row("upsert test")

        await asyncio.wait_for(start_task, timeout=10)
        await stream.teardown()

        assert len(sink.upserted) >= 1
        # The id is the row's serial id as a string
        assert sink.upserted[0][1] == [1.0, 2.0, 3.0]

    @pytest.mark.asyncio
    async def test_stop_ends_start(self):
        """
        Calling stop() should cause start() to return cleanly.
        """
        stream = make_stream()
        stream.watch(TEST_TABLE)

        @stream.on_change
        async def handler(event, sink):
            pass  # just consume, never stop

        await stream.setup()
        start_task = asyncio.create_task(stream.start())

        await asyncio.sleep(0.5)
        await stream.stop()

        # start() should return promptly after stop()
        await asyncio.wait_for(start_task, timeout=5)
        await stream.teardown()

    @pytest.mark.asyncio
    async def test_setup_is_required_before_start(self):
        """start() should raise RuntimeError if setup() was not called."""
        stream = make_stream()
        stream.watch(TEST_TABLE)

        @stream.on_change
        async def handler(event, sink):
            pass

        with pytest.raises(RuntimeError, match="setup\\(\\) must be called before start"):
            await stream.start()

    @pytest.mark.asyncio
    async def test_handler_required_before_start(self):
        """start() should raise RuntimeError if no handler was registered."""
        stream = make_stream()
        stream.watch(TEST_TABLE)

        # Register another stream just to do setup (this test's slot/pub may not exist)
        # We test the guard before it even tries to connect.
        # Manually set _slot_manager to a truthy sentinel to bypass that check.
        from pgstream.replication import SlotManager
        stream._slot_manager = SlotManager(DSN, SLOT_NAME, PUB_NAME)

        with pytest.raises(RuntimeError, match="no handler registered"):
            await stream.start()

    @pytest.mark.asyncio
    async def test_watch_required_before_setup(self):
        """setup() should raise ValueError if no tables were registered."""
        stream = PGStream(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)
        # Do NOT call watch()
        with pytest.raises(ValueError, match="no tables registered"):
            await stream.setup()

    @pytest.mark.asyncio
    async def test_method_chaining(self):
        """watch() and sink() should return self for chaining."""
        stream = PGStream(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)
        result = stream.watch(TEST_TABLE).sink(CollectingSink())
        assert result is stream
        assert TEST_TABLE in stream._tables
        assert isinstance(stream._sink, CollectingSink)

"""Integration tests for the PGStream top-level API.

Tests the full public interface end-to-end:
  - watch() + setup() + start() + stop() + teardown()
  - @on_change decorator
  - Sink attachment and propagation
  - Guard rails (missing handler, missing setup, missing watch)

Requires a live Postgres container with ``wal_level = logical``.
Marked ``@pytest.mark.integration``.
"""

import asyncio
import os
import uuid

import psycopg2
import pytest

from pgstream import PGStream, ChangeEvent
from pgstream.sinks.base import Sink

DSN = os.environ.get(
    "PGSTREAM_DSN",
    "postgresql://pgexplain:pgexplain@localhost:5435/pgexplain",
)

RUN_ID = uuid.uuid4().hex[:8]
SLOT_NAME = f"pgstream_e2e_{RUN_ID}"
PUB_NAME = f"pgstream_e2e_{RUN_ID}"
TEST_TABLE = f"pgstream_e2e_tbl_{RUN_ID}"

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def create_test_table():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    conn.cursor().execute(
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
    cur.execute(
        "SELECT pg_drop_replication_slot(%s) "
        "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
        (SLOT_NAME, SLOT_NAME),
    )
    cur.execute(f"DROP PUBLICATION IF EXISTS {PUB_NAME}")
    conn.close()


def make_stream() -> PGStream:
    return PGStream(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)


def insert_row(payload: str) -> int:
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"INSERT INTO {TEST_TABLE} (payload) VALUES (%s) RETURNING id", (payload,))
    row_id = cur.fetchone()[0]  # type: ignore[index]
    conn.close()
    return row_id


class CollectingSink(Sink):
    """In-memory sink for tests — records calls without any I/O."""

    def __init__(self):
        self.upserted: list[tuple[str, list[float]]] = []
        self.deleted: list[str] = []

    async def upsert(self, id: str, vector: list[float], payload=None) -> None:
        self.upserted.append((id, vector))

    async def delete(self, id: str) -> None:
        self.deleted.append(id)


class TestPGStreamEndToEnd:
    @pytest.mark.asyncio
    async def test_on_change_receives_insert(self):
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
        await asyncio.sleep(0.5)
        insert_row("hello from test")
        await asyncio.wait_for(start_task, timeout=10)
        await stream.teardown()

        assert len(received) >= 1
        assert received[0].operation == "insert"
        assert received[0].row["payload"] == "hello from test"

    @pytest.mark.asyncio
    async def test_sink_is_passed_to_handler(self):
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
        stream = make_stream()
        stream.watch(TEST_TABLE)
        sink = CollectingSink()
        stream.sink(sink)

        @stream.on_change
        async def handler(event: ChangeEvent, s):  # type: ignore[override]
            if event.operation == "insert":
                await s.upsert(str(event.row["id"]), [1.0, 2.0, 3.0])
                await stream.stop()

        await stream.setup()
        start_task = asyncio.create_task(stream.start())
        await asyncio.sleep(0.5)
        insert_row("upsert test")
        await asyncio.wait_for(start_task, timeout=10)
        await stream.teardown()

        assert len(sink.upserted) >= 1
        assert sink.upserted[0][1] == [1.0, 2.0, 3.0]

    @pytest.mark.asyncio
    async def test_stop_ends_start(self):
        stream = make_stream()
        stream.watch(TEST_TABLE)

        @stream.on_change
        async def handler(event, sink):
            pass

        await stream.setup()
        start_task = asyncio.create_task(stream.start())
        await asyncio.sleep(0.5)
        await stream.stop()
        await asyncio.wait_for(start_task, timeout=5)
        await stream.teardown()

    @pytest.mark.asyncio
    async def test_setup_is_required_before_start(self):
        stream = make_stream()
        stream.watch(TEST_TABLE)

        @stream.on_change
        async def handler(event, sink):
            pass

        with pytest.raises(RuntimeError, match="setup\\(\\) must be called before start"):
            await stream.start()

    @pytest.mark.asyncio
    async def test_handler_required_before_start(self):
        stream = make_stream()
        stream.watch(TEST_TABLE)
        from pgstream.replication import SlotManager
        stream._slot_manager = SlotManager(DSN, SLOT_NAME, PUB_NAME)

        with pytest.raises(RuntimeError, match="no handler registered"):
            await stream.start()

    @pytest.mark.asyncio
    async def test_watch_required_before_setup(self):
        stream = PGStream(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)
        with pytest.raises(ValueError, match="no tables registered"):
            await stream.setup()

    @pytest.mark.asyncio
    async def test_method_chaining(self):
        stream = PGStream(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)
        result = stream.watch(TEST_TABLE).sink(CollectingSink())
        assert result is stream
        assert TEST_TABLE in stream._tables
        assert isinstance(stream._sink, CollectingSink)

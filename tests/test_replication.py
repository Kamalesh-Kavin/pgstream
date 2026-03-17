"""Integration tests for SlotManager and ReplicationStream.

Requires a Postgres instance with ``wal_level = logical``.
Set ``PGSTREAM_DSN`` or rely on the default dev container (port 5435).

Run integration tests:
    pytest -m integration
Skip integration tests:
    pytest -m "not integration"
"""

import os
import threading
import time
import uuid

import psycopg2
import pytest

from pgstream.replication import SlotManager, ReplicationStream
from pgstream.events import ChangeEvent

DSN = os.environ.get(
    "PGSTREAM_DSN",
    "postgresql://pgexplain:pgexplain@localhost:5435/pgexplain",
)

RUN_ID = uuid.uuid4().hex[:8]
SLOT_NAME = f"pgstream_test_{RUN_ID}"
PUB_NAME = f"pgstream_test_{RUN_ID}"
TEST_TABLE = f"pgstream_test_tbl_{RUN_ID}"

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def test_table():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    conn.cursor().execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
            id      SERIAL PRIMARY KEY,
            content TEXT NOT NULL
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


@pytest.fixture
def slot_manager():
    mgr = SlotManager(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)
    yield mgr
    try:
        mgr.teardown(drop_slot=True, drop_publication=True)
    except Exception:
        pass


class TestSlotManager:
    def test_setup_creates_slot(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT_NAME,))
        assert cur.fetchone() is not None
        conn.close()

    def test_setup_creates_publication(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (PUB_NAME,))
        assert cur.fetchone() is not None
        conn.close()

    def test_setup_is_idempotent(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        slot_manager.setup(tables=[TEST_TABLE])

    def test_teardown_drops_slot(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        slot_manager.teardown(drop_slot=True, drop_publication=False)
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT_NAME,))
        assert cur.fetchone() is None
        conn.close()

    def test_teardown_drops_publication(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        slot_manager.teardown(drop_slot=False, drop_publication=True)
        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (PUB_NAME,))
        assert cur.fetchone() is None
        conn.close()


class TestReplicationStream:
    def test_stream_receives_insert(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        received: list[ChangeEvent] = []
        stream = ReplicationStream(DSN, SLOT_NAME, PUB_NAME)

        def on_event(event: ChangeEvent):
            received.append(event)
            if len(received) >= 1:
                stream.stop()

        t = threading.Thread(
            target=stream.stream,
            kwargs={"tables": [TEST_TABLE], "on_event": on_event},
            daemon=True,
        )
        t.start()
        time.sleep(0.5)

        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        conn.cursor().execute(f"INSERT INTO {TEST_TABLE} (content) VALUES (%s)", ("test content",))
        conn.close()

        t.join(timeout=10)
        assert len(received) >= 1
        assert received[0].operation == "insert"
        assert received[0].table == TEST_TABLE
        assert received[0].row["content"] == "test content"

    def test_stream_receives_update(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {TEST_TABLE} (content) VALUES (%s) RETURNING id", ("original",))
        row_id = cur.fetchone()[0]  # type: ignore[index]

        received: list[ChangeEvent] = []
        stream = ReplicationStream(DSN, SLOT_NAME, PUB_NAME)

        def on_event(event: ChangeEvent):
            if event.operation == "update":
                received.append(event)
                stream.stop()

        t = threading.Thread(
            target=stream.stream,
            kwargs={"tables": [TEST_TABLE], "on_event": on_event},
            daemon=True,
        )
        t.start()
        time.sleep(0.5)

        cur.execute(f"UPDATE {TEST_TABLE} SET content = %s WHERE id = %s", ("updated", row_id))
        conn.close()

        t.join(timeout=10)
        assert len(received) >= 1
        assert received[0].row["content"] == "updated"

    def test_stream_receives_delete(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {TEST_TABLE} (content) VALUES (%s) RETURNING id", ("to delete",))
        row_id = cur.fetchone()[0]  # type: ignore[index]

        received: list[ChangeEvent] = []
        stream = ReplicationStream(DSN, SLOT_NAME, PUB_NAME)

        def on_event(event: ChangeEvent):
            if event.operation == "delete":
                received.append(event)
                stream.stop()

        t = threading.Thread(
            target=stream.stream,
            kwargs={"tables": [TEST_TABLE], "on_event": on_event},
            daemon=True,
        )
        t.start()
        time.sleep(0.5)

        cur.execute(f"DELETE FROM {TEST_TABLE} WHERE id = %s", (row_id,))
        conn.close()

        t.join(timeout=10)
        assert len(received) >= 1
        assert received[0].operation == "delete"

    def test_stop_is_graceful(self, slot_manager):
        slot_manager.setup(tables=[TEST_TABLE])
        stream = ReplicationStream(DSN, SLOT_NAME, PUB_NAME)
        errors: list[Exception] = []

        def run():
            try:
                stream.stream(tables=[TEST_TABLE], on_event=lambda e: None)
            except Exception as exc:
                errors.append(exc)

        t = threading.Thread(target=run, daemon=True)
        t.start()
        time.sleep(0.3)
        stream.stop()
        t.join(timeout=5)

        assert not t.is_alive()
        assert errors == []

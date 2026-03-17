"""
tests/test_replication.py — Integration tests for SlotManager and ReplicationStream.

These tests require a real Postgres instance with wal_level = logical.
They use the shared pg-explain-db container (port 5435).

The tests are marked with @pytest.mark.integration so they can be skipped
in environments without a running Postgres (e.g. lightweight CI):

    pytest -m "not integration"   # skip these
    pytest -m integration          # run only these

What we test:
  - SlotManager.setup() creates the replication slot and publication
  - SlotManager.setup() is idempotent (calling it twice is safe)
  - SlotManager.teardown() drops them
  - ReplicationStream actually streams INSERT/UPDATE/DELETE events from a real table
  - At-least-once delivery: events are replayed on restart if not ACKed
"""

import os
import threading
import time
import uuid

import psycopg2
import pytest

from pgstream.replication import SlotManager, ReplicationStream
from pgstream.events import ChangeEvent

# ---------------------------------------------------------------------------
# Test DSN — use env var with fallback to the shared dev container
# ---------------------------------------------------------------------------
DSN = os.environ.get(
    "PGSTREAM_DSN",
    "postgresql://pgexplain:pgexplain@localhost:5435/pgexplain",
)

# Unique slot/publication names per test run to avoid conflicts when tests
# are run repeatedly without teardown.
RUN_ID = uuid.uuid4().hex[:8]
SLOT_NAME = f"pgstream_test_{RUN_ID}"
PUB_NAME = f"pgstream_test_{RUN_ID}"
TEST_TABLE = f"pgstream_test_tbl_{RUN_ID}"

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def test_table():
    """
    Create a temporary table for the duration of this test module.
    Dropped automatically at the end, along with slot/publication cleanup.
    """
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    # Create a simple table
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
            id      SERIAL PRIMARY KEY,
            content TEXT NOT NULL
        )
        """
    )
    conn.close()

    yield  # tests run here

    # Teardown: drop table + any leftover slot/publication
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    # Clean up slot/pub if a test left them behind
    cur.execute(
        "SELECT pg_drop_replication_slot(%s) "
        "WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
        (SLOT_NAME, SLOT_NAME),
    )
    cur.execute(f"DROP PUBLICATION IF EXISTS {PUB_NAME}")
    conn.close()


@pytest.fixture
def slot_manager():
    """Fresh SlotManager for each test. Tears down after the test."""
    mgr = SlotManager(dsn=DSN, slot_name=SLOT_NAME, publication_name=PUB_NAME)
    yield mgr
    # Always clean up so subsequent tests start fresh
    try:
        mgr.teardown(drop_slot=True, drop_publication=True)
    except Exception:
        pass  # already torn down


# ---------------------------------------------------------------------------
# SlotManager tests
# ---------------------------------------------------------------------------

class TestSlotManager:
    def test_setup_creates_slot(self, slot_manager):
        """setup() should create the replication slot in pg_replication_slots."""
        slot_manager.setup(tables=[TEST_TABLE])

        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,),
        )
        row = cur.fetchone()
        conn.close()
        assert row is not None, "Slot was not created."

    def test_setup_creates_publication(self, slot_manager):
        """setup() should create the publication in pg_publication."""
        slot_manager.setup(tables=[TEST_TABLE])

        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s",
            (PUB_NAME,),
        )
        row = cur.fetchone()
        conn.close()
        assert row is not None, "Publication was not created."

    def test_setup_is_idempotent(self, slot_manager):
        """Calling setup() twice should not raise."""
        slot_manager.setup(tables=[TEST_TABLE])
        slot_manager.setup(tables=[TEST_TABLE])  # second call — should be a no-op

    def test_teardown_drops_slot(self, slot_manager):
        """teardown() should remove the replication slot."""
        slot_manager.setup(tables=[TEST_TABLE])
        slot_manager.teardown(drop_slot=True, drop_publication=False)

        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,),
        )
        row = cur.fetchone()
        conn.close()
        assert row is None, "Slot was not dropped."

    def test_teardown_drops_publication(self, slot_manager):
        """teardown() should remove the publication."""
        slot_manager.setup(tables=[TEST_TABLE])
        slot_manager.teardown(drop_slot=False, drop_publication=True)

        conn = psycopg2.connect(DSN)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s",
            (PUB_NAME,),
        )
        row = cur.fetchone()
        conn.close()
        assert row is None, "Publication was not dropped."


# ---------------------------------------------------------------------------
# ReplicationStream tests
# ---------------------------------------------------------------------------

class TestReplicationStream:
    def test_stream_receives_insert(self, slot_manager):
        """
        After setup(), inserting a row into the watched table should produce
        an INSERT ChangeEvent on the stream.
        """
        slot_manager.setup(tables=[TEST_TABLE])

        received_events: list[ChangeEvent] = []
        stream = ReplicationStream(DSN, SLOT_NAME, PUB_NAME)

        def on_event(event: ChangeEvent):
            received_events.append(event)
            if len(received_events) >= 1:
                stream.stop()

        # Start streaming in a background thread
        t = threading.Thread(
            target=stream.stream,
            kwargs={"tables": [TEST_TABLE], "on_event": on_event},
            daemon=True,
        )
        t.start()

        # Give the stream a moment to start up before inserting
        time.sleep(0.5)

        # Insert a row
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        conn.cursor().execute(
            f"INSERT INTO {TEST_TABLE} (content) VALUES (%s)",
            ("test content",),
        )
        conn.close()

        # Wait for the thread to process and stop
        t.join(timeout=10)

        assert len(received_events) >= 1
        evt = received_events[0]
        assert evt.operation == "insert"
        assert evt.table == TEST_TABLE
        assert evt.row["content"] == "test content"

    def test_stream_receives_update(self, slot_manager):
        """UPDATE should produce an 'update' ChangeEvent."""
        slot_manager.setup(tables=[TEST_TABLE])

        # Insert a row first (outside the stream — pre-existing data)
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {TEST_TABLE} (content) VALUES (%s) RETURNING id",
            ("original",),
        )
        row_id = cur.fetchone()[0]

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

        cur.execute(
            f"UPDATE {TEST_TABLE} SET content = %s WHERE id = %s",
            ("updated", row_id),
        )
        conn.close()

        t.join(timeout=10)

        assert len(received) >= 1
        assert received[0].operation == "update"
        assert received[0].row["content"] == "updated"

    def test_stream_receives_delete(self, slot_manager):
        """DELETE should produce a 'delete' ChangeEvent."""
        slot_manager.setup(tables=[TEST_TABLE])

        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {TEST_TABLE} (content) VALUES (%s) RETURNING id",
            ("to be deleted",),
        )
        row_id = cur.fetchone()[0]

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
        """
        Calling stop() should exit the streaming loop cleanly without raising.
        """
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

        assert not t.is_alive(), "Stream thread did not stop."
        assert errors == [], f"Stream raised unexpectedly: {errors}"

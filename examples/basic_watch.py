"""
examples/basic_watch.py — Minimal working example of pgstream.

Demonstrates the full pgstream workflow:
  1. Create a table to watch (idempotent DDL)
  2. Configure PGStream with a DSN
  3. Register an async on_change handler
  4. Call setup() once, then start() to stream events

This script prints every INSERT/UPDATE/DELETE that happens on the
`documents` table while it is running. Open a second terminal and
run SQL to see events appear:

    psql postgresql://pgexplain:pgexplain@localhost:5435/pgexplain
    INSERT INTO documents (content) VALUES ('Hello, pgstream!');
    UPDATE documents SET content = 'Updated!' WHERE id = 1;
    DELETE FROM documents WHERE id = 1;

Press Ctrl+C to stop.

Run this example:
    export PGSTREAM_DSN=postgresql://pgexplain:pgexplain@localhost:5435/pgexplain
    uv run python examples/basic_watch.py
"""

import asyncio
import logging
import os
import signal

import psycopg2

from pgstream import PGStream, ChangeEvent

# ---------------------------------------------------------------------------
# Logging — show INFO messages so you can see what pgstream is doing internally
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Config — read DSN from environment variable
# ---------------------------------------------------------------------------
DSN = os.environ.get(
    "PGSTREAM_DSN",
    "postgresql://pgexplain:pgexplain@localhost:5435/pgexplain",
)


# ---------------------------------------------------------------------------
# Ensure the watched table exists
# ---------------------------------------------------------------------------

def create_table_if_not_exists() -> None:
    """
    Create the `documents` table if it doesn't exist.

    In real usage you'd manage this with migrations (Alembic, Flyway, etc.).
    We create it here so the example is self-contained.
    """
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            id      SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    conn.close()
    print("Table 'documents' ready.")


# ---------------------------------------------------------------------------
# Main async entrypoint
# ---------------------------------------------------------------------------

async def main() -> None:
    create_table_if_not_exists()

    # 1. Create the stream
    stream = PGStream(
        dsn=DSN,
        slot_name="pgstream_example",        # replication slot name
        publication_name="pgstream_example", # publication name
    )

    # 2. Declare which tables to watch
    stream.watch("documents")

    # 3. Register the handler — this is your application logic.
    #    In a real app you'd embed `event.row["content"]` and upsert to a vector store.
    @stream.on_change
    async def handle(event: ChangeEvent, sink) -> None:
        """
        Called for every committed INSERT, UPDATE, DELETE, or TRUNCATE on `documents`.

        event.row      — the new row (dict of column_name → string value)
        event.old_row  — the old row (only with REPLICA IDENTITY FULL or on DELETE)
        event.lsn      — WAL position string, e.g. "0/1A3F28"
        event.xid      — Postgres transaction ID
        """
        print()
        print(f"  operation : {event.operation.upper()}")
        print(f"  table     : {event.schema}.{event.table}")
        print(f"  row       : {event.row}")
        print(f"  old_row   : {event.old_row}")
        print(f"  lsn       : {event.lsn}")
        print(f"  xid       : {event.xid}")
        print(f"  committed : {event.commit_time}")

        # In a real pipeline you'd do something like:
        #
        # if event.operation in ("insert", "update"):
        #     vector = await your_embedding_function(event.row["content"])
        #     await sink.upsert(
        #         id=event.row["id"],
        #         vector=vector,
        #         payload={"content": event.row["content"]},
        #     )
        # elif event.operation == "delete":
        #     await sink.delete(event.row["id"])

    # 4. Setup — idempotent. Creates the replication slot + publication.
    print(f"\nConnecting to {DSN}")
    print("Creating replication slot and publication (idempotent)...")
    await stream.setup()
    print("Setup complete. Watching 'documents' for changes...")
    print("Press Ctrl+C to stop.\n")

    # 5. Handle Ctrl+C gracefully
    loop = asyncio.get_running_loop()

    def handle_signal():
        print("\nShutting down...")
        asyncio.create_task(stream.stop())

    loop.add_signal_handler(signal.SIGINT, handle_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_signal)

    # 6. Start streaming — blocks until stop() is called
    try:
        await stream.start()
    finally:
        # 7. Optional teardown — drops the slot so WAL doesn't accumulate.
        #    Comment this out if you want to resume from where you left off.
        print("Tearing down slot and publication...")
        await stream.teardown()
        print("Done.")


if __name__ == "__main__":
    asyncio.run(main())

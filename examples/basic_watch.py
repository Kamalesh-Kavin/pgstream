"""Minimal working example of pgstream.

Watches the `documents` table and prints every change.
To see events, open a second terminal and run:

    psql $PGSTREAM_DSN
    INSERT INTO documents (content) VALUES ('Hello, pgstream!');
    UPDATE documents SET content = 'Updated!' WHERE id = 1;
    DELETE FROM documents WHERE id = 1;

Press Ctrl+C to stop.

Run:
    export PGSTREAM_DSN=postgresql://user:pass@localhost/db
    uv run python examples/basic_watch.py
"""

import asyncio
import logging
import os
import signal

import psycopg2

from pgstream import PGStream, ChangeEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DSN = os.environ.get(
    "PGSTREAM_DSN",
    "postgresql://pgexplain:pgexplain@localhost:5435/pgexplain",
)


def create_table_if_not_exists() -> None:
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    conn.cursor().execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            id         SERIAL PRIMARY KEY,
            content    TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    conn.close()
    print("Table 'documents' ready.")


async def main() -> None:
    create_table_if_not_exists()

    stream = PGStream(
        dsn=DSN,
        slot_name="pgstream_example",
        publication_name="pgstream_example",
    )
    stream.watch("documents")

    @stream.on_change
    async def handle(event: ChangeEvent, sink) -> None:
        print()
        print(f"  operation : {event.operation.upper()}")
        print(f"  table     : {event.schema}.{event.table}")
        print(f"  row       : {event.row}")
        print(f"  old_row   : {event.old_row}")
        print(f"  lsn       : {event.lsn}")
        print(f"  xid       : {event.xid}")
        print(f"  committed : {event.commit_time}")

        # In a real pipeline:
        #
        # if event.operation in ("insert", "update"):
        #     vector = await your_embed_fn(event.row["content"])
        #     await sink.upsert(event.row["id"], vector, payload=event.row)
        # elif event.operation == "delete":
        #     await sink.delete(event.row["id"])

    print(f"\nConnecting to Postgres...")
    await stream.setup()
    print("Watching 'documents' for changes. Press Ctrl+C to stop.\n")

    loop = asyncio.get_running_loop()

    def handle_signal():
        print("\nShutting down...")
        asyncio.create_task(stream.stop())

    loop.add_signal_handler(signal.SIGINT, handle_signal)
    loop.add_signal_handler(signal.SIGTERM, handle_signal)

    try:
        await stream.start()
    finally:
        await stream.teardown()
        print("Done.")


if __name__ == "__main__":
    asyncio.run(main())

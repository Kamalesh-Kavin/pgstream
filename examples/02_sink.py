"""
examples/02_sink.py — custom Sink + PGStream (mock-based)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shows how to implement the Sink ABC and wire it to a PGStream instance.

The Sink ABC has two required methods:
  - upsert(id, vector, payload)  — insert or update a vector
  - delete(id)                   — remove a vector

This example uses a simple in-memory sink and simulated ChangeEvents
so you can run it without a live Postgres database.

Run:
    pip install pgstream
    python examples/02_sink.py

What you see:
    - Three simulated CDC events: INSERT, UPDATE, DELETE
    - The sink records each call
    - The final state of the in-memory store is printed
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from pgstream import ChangeEvent
from pgstream.sinks.base import Sink

# ── 1. Implement the Sink ABC ─────────────────────────────────────────────────
#
# In production you would replace this with:
#   - PGVectorSink  (pip install pgstream[pgvector])
#   - QdrantSink    (pip install pgstream[qdrant])
#   - Your own sink for any other vector store


class InMemoryVectorStore(Sink):
    """Toy in-memory vector store — demonstrates the Sink contract.

    A real sink would hold a connection to pgvector, Qdrant, Weaviate, etc.
    """

    def __init__(self) -> None:
        # Maps document ID → (vector, payload)
        self._store: dict[str, tuple[list[float], dict | None]] = {}

    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """Insert or overwrite a document in the store."""
        self._store[id] = (vector, payload)
        print(f"  [upsert] id={id!r}  vector_dim={len(vector)}  payload={payload}")

    async def delete(self, id: str) -> None:
        """Remove a document from the store (idempotent)."""
        removed = self._store.pop(id, None)
        status = "removed" if removed else "not found (idempotent)"
        print(f"  [delete] id={id!r}  → {status}")

    async def close(self) -> None:
        """Release resources (no-op for the in-memory store)."""
        print("  [close]  store released")

    def __repr__(self) -> str:
        return f"InMemoryVectorStore(size={len(self._store)})"


# ── 2. A fake embed function ──────────────────────────────────────────────────
#
# In production this would call an embedding model:
#   from openai import AsyncOpenAI
#   client = AsyncOpenAI()
#   resp = await client.embeddings.create(model="text-embedding-3-small", input=text)
#   return resp.data[0].embedding


async def embed(text: str) -> list[float]:
    """Return a fake 4-dimensional embedding vector for demo purposes."""
    # Real embeddings are 768-3072 floats.  We use 4 for readability.
    return [float(ord(c)) / 1000.0 for c in text[:4].ljust(4)]


# ── 3. The on_change handler ──────────────────────────────────────────────────
#
# This is exactly the handler you would register with @stream.on_change.
# The only difference here is that we call it directly with fake events
# instead of wiring it to a live PGStream instance.


async def handle(event: ChangeEvent, sink: Sink) -> None:
    """Process a single CDC event and sync it to the vector store.

    This is the canonical pgstream handler pattern:
      - INSERT / UPDATE → embed the content and upsert into the vector store
      - DELETE          → remove the document from the vector store
    """
    if event.operation in ("insert", "update"):
        content = event.row.get("content") or ""
        vector = await embed(content)
        await sink.upsert(
            id=str(event.row["id"]),
            vector=vector,
            payload={"table": event.table, "content": content, "lsn": event.lsn},
        )
    elif event.operation == "delete":
        await sink.delete(id=str(event.row["id"]))
    # truncate is not handled here — add elif event.operation == "truncate" as needed


# ── 4. Simulate CDC events ────────────────────────────────────────────────────

_NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _event(op: str, row: dict, old_row: dict | None = None) -> ChangeEvent:
    return ChangeEvent(
        operation=op,  # type: ignore[arg-type]
        schema="public",
        table="documents",
        row=row,
        old_row=old_row,
        lsn="0/1A3F28",
        commit_time=_NOW,
        xid=42,
    )


EVENTS = [
    _event("insert", {"id": "1", "content": "pgstream makes CDC easy"}),
    _event("insert", {"id": "2", "content": "vector search with pgvector"}),
    _event(
        "update",
        row={"id": "1", "content": "pgstream makes CDC effortless"},
        old_row={"id": "1", "content": "pgstream makes CDC easy"},
    ),
    _event("delete", {"id": "2", "content": None}),
]


async def main() -> None:
    sink = InMemoryVectorStore()

    print("\n=== pgstream Sink example (mock CDC events) ===\n")
    print("Processing events:\n")

    for event in EVENTS:
        print(f"  {event!r}")
        await handle(event, sink)
        print()

    await sink.close()

    print(f"\nFinal store state: {sink}")
    for doc_id, (vector, payload) in sink._store.items():
        print(f"  id={doc_id!r}  vector={vector}  payload={payload}")


asyncio.run(main())

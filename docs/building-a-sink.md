# Building a custom sink

Any vector store that supports upsert and delete can be used with pgstream. Implement the `Sink` abstract base class to add support for Pinecone, Weaviate, Milvus, OpenSearch, or any other backend.

---

## Interface

```python
from pgstream.sinks import Sink

class MySink(Sink):
    async def upsert(self, id: str, vector: list[float], payload: dict | None = None) -> None:
        ...

    async def delete(self, id: str) -> None:
        ...

    async def close(self) -> None:  # optional
        ...
```

Three methods. Only `upsert` and `delete` are required — `close` has a default no-op implementation.

### Rules

- All three methods must be `async`.
- `upsert` must be **idempotent** — calling it twice with the same `id` must not create duplicates.
- `delete` must be **idempotent** — deleting a non-existent ID must not raise an exception.
- `close` is called automatically by `PGStream.stop()`. Release any connections or HTTP sessions here.

---

## Example: Pinecone

```python
import asyncio
from pgstream.sinks import Sink

class PineconeSink(Sink):
    def __init__(self, api_key: str, index_name: str) -> None:
        from pinecone import Pinecone
        self._pc = Pinecone(api_key=api_key)
        self._index = self._pc.Index(index_name)

    async def upsert(self, id: str, vector: list[float], payload: dict | None = None) -> None:
        # Pinecone's client is synchronous — run in executor to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self._index.upsert(vectors=[{"id": id, "values": vector, "metadata": payload or {}}]),
        )

    async def delete(self, id: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self._index.delete(ids=[id]))
```

Note: if the client library is synchronous (like Pinecone's), wrap calls in `asyncio.get_running_loop().run_in_executor(None, ...)` to avoid blocking the event loop.

---

## Example: Weaviate

```python
import weaviate
from pgstream.sinks import Sink

class WeaviateSink(Sink):
    def __init__(self, url: str, class_name: str) -> None:
        self._client = weaviate.connect_to_local(url)
        self._collection = self._client.collections.get(class_name)

    async def upsert(self, id: str, vector: list[float], payload: dict | None = None) -> None:
        import asyncio
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            lambda: self._collection.data.insert(
                properties=payload or {},
                vector=vector,
                uuid=id,
            ),
        )

    async def delete(self, id: str) -> None:
        import asyncio
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: self._collection.data.delete_by_id(id))

    async def close(self) -> None:
        self._client.close()
```

---

## Using your sink

```python
from pgstream import PGStream, ChangeEvent

stream = PGStream(dsn="postgresql://user:pass@localhost/mydb")
stream.watch("documents")
stream.sink(PineconeSink(api_key="...", index_name="docs"))

@stream.on_change
async def handle(event: ChangeEvent, sink):
    if event.operation in ("insert", "update"):
        vector = await embed(event.row["content"])
        await sink.upsert(id=event.row["id"], vector=vector, payload=event.row)
    elif event.operation == "delete":
        await sink.delete(event.row["id"])

async def main():
    await stream.setup()
    await stream.start()
```

---

## Typing

If you want strict typing, annotate your handler with your concrete sink type:

```python
@stream.on_change
async def handle(event: ChangeEvent, sink: PineconeSink):  # type: ignore[override]
    ...
```

The `# type: ignore[override]` suppresses the type checker's complaint about the narrowed type — the decorator signature uses `Sink | None`, and narrowing to a concrete type is intentional.

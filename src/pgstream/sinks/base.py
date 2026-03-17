from __future__ import annotations

from abc import ABC, abstractmethod


class Sink(ABC):
    """Abstract base class for all pgstream vector store sinks.

    Implement this interface to add support for a new vector store.
    All I/O methods are async.

    Example::

        class MyVectorDB(Sink):
            async def upsert(self, id: str, vector: list[float], payload: dict | None = None) -> None:
                await self._client.upsert(id, vector, metadata=payload)

            async def delete(self, id: str) -> None:
                await self._client.delete(id)
    """

    @abstractmethod
    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """Insert or update a vector in the store.

        Args:
            id:      Unique identifier for this document (stringified PK).
            vector:  Dense float embedding, e.g. ``[0.12, -0.45, ...]``.
            payload: Optional metadata stored alongside the vector.
        """
        ...

    @abstractmethod
    async def delete(self, id: str) -> None:
        """Remove a vector from the store by its ID.

        Implementations should be idempotent — deleting a non-existent ID
        must not raise.
        """
        ...

    async def close(self) -> None:
        """Release resources held by this sink (connections, HTTP sessions, etc.).

        Called automatically by :meth:`~pgstream.stream.PGStream.stop`.
        Override if your sink holds a long-lived connection.
        """
        pass

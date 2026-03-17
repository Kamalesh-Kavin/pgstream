"""
sinks/base.py — Abstract Sink interface.

Every vector store backend must implement this interface. The design is
intentionally minimal: pgstream does not prescribe how you store vectors,
only that you can upsert and delete by a string ID.

Why a string ID?
  Postgres primary keys are often integers or UUIDs. We stringify them so
  the Sink interface doesn't need to know the source column type. The user
  controls the mapping (e.g. str(row["id"]) or row["uuid_col"]).

Why no batch API at the interface level?
  Batching is a performance concern that belongs to each Sink implementation.
  Some backends (Qdrant) have a native batch upsert; others don't. Exposing
  a single-item API here keeps the interface clean. Implementations can
  buffer internally and flush on each call or on a timer — that's an
  implementation detail hidden behind this interface.

Extension pattern for community sinks:
  class MyCustomSink(Sink):
      async def upsert(self, id: str, vector: list[float], payload: dict | None = None) -> None:
          ...
      async def delete(self, id: str) -> None:
          ...
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class Sink(ABC):
    """
    Abstract base class for all pgstream vector store sinks.

    A Sink receives decoded row data from pgstream and is responsible for
    writing it to an external vector store. All methods are async — sinks
    are expected to do I/O (HTTP calls, DB writes, etc.).
    """

    @abstractmethod
    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """
        Insert or update a vector in the store.

        Args:
            id:      Unique identifier for this document (stringified PK).
            vector:  Dense float embedding, e.g. [0.12, -0.45, ...].
            payload: Optional metadata dict stored alongside the vector.
                     Useful for filtered search (e.g. payload["tenant_id"]).
        """
        ...

    @abstractmethod
    async def delete(self, id: str) -> None:
        """
        Remove a vector from the store by its ID.

        Called when a DELETE event is received for a watched table.
        Implementations should be idempotent (deleting a non-existent ID
        should not raise).
        """
        ...

    async def close(self) -> None:
        """
        Optional: release resources held by this sink (HTTP sessions, pool, etc.).

        Called by PGStream.stop(). Default implementation does nothing.
        Override if your sink holds a long-lived connection.
        """
        pass

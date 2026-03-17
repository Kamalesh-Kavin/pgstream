"""
sinks/qdrant.py — Qdrant reference sink implementation.

Qdrant (https://qdrant.tech) is a self-hostable, high-performance vector
database written in Rust. It supports HNSW indexing, rich payload filtering,
and an HTTP/gRPC API.

Run Qdrant locally with Docker:
    docker run -p 6333:6333 qdrant/qdrant

This sink uses the official `qdrant-client` Python SDK, which is async-native.

Setup required:
    The Qdrant collection must be created before using this sink. You can
    create it via the Qdrant dashboard, REST API, or programmatically:

        from qdrant_client import QdrantClient
        from qdrant_client.models import VectorParams, Distance

        client = QdrantClient(url="http://localhost:6333")
        client.create_collection(
            collection_name="documents",
            vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
        )

    pgstream does NOT auto-create collections — this is intentional. Collection
    creation involves choosing vector dimension and distance metric, which are
    model-specific decisions that belong to the user.

Why not auto-create?
  If we auto-created with wrong parameters (e.g. wrong dimension), every upsert
  would fail silently or with confusing errors. Explicit is better here.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .base import Sink

logger = logging.getLogger("pgstream.sinks.qdrant")

# Lazy import — qdrant-client is an optional dependency.
# Users who don't use Qdrant should not need to install it.
try:
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.models import PointStruct
    _QDRANT_AVAILABLE = True
except ImportError:
    _QDRANT_AVAILABLE = False


class QdrantSink(Sink):
    """
    Writes vectors to a Qdrant collection.

    Args:
        url:             Qdrant server URL, e.g. "http://localhost:6333".
        collection_name: Name of the Qdrant collection to write to.
        api_key:         Optional API key for Qdrant Cloud.

    The `id` field passed to upsert() becomes the Qdrant point ID.
    Qdrant point IDs must be either unsigned integers or UUIDs.
    pgstream passes string IDs; we attempt to parse them as integers first
    (for numeric PKs), then as UUIDs, and finally hash them to a uint64
    as a last resort. This is documented in the upsert() docstring.
    """

    def __init__(
        self,
        url: str = "http://localhost:6333",
        collection_name: str = "pgstream",
        api_key: str | None = None,
    ) -> None:
        if not _QDRANT_AVAILABLE:
            raise ImportError(
                "qdrant-client is not installed. "
                "Run: uv add qdrant-client  (or pip install qdrant-client)"
            )
        self._url = url
        self._collection_name = collection_name
        self._api_key = api_key
        self._client: AsyncQdrantClient | None = None

    def _get_client(self) -> "AsyncQdrantClient":
        """Lazily initialise the Qdrant async client."""
        if self._client is None:
            self._client = AsyncQdrantClient(
                url=self._url,
                api_key=self._api_key,
            )
        return self._client

    def _coerce_id(self, id: str) -> int | str:
        """
        Convert a string ID to a Qdrant-compatible point ID.

        Qdrant only accepts unsigned integers or UUID strings as point IDs.
        Strategy:
          1. Try to parse as an integer (handles bigint/serial PKs).
          2. If it looks like a UUID (8-4-4-4-12 hex), pass through as-is.
          3. Otherwise, hash to a uint64 using Python's built-in hash
             (stable within a process; documented limitation for non-int/UUID PKs).
        """
        # Try integer
        try:
            return int(id)
        except ValueError:
            pass

        # Try UUID (Qdrant accepts UUID strings directly)
        import re
        UUID_RE = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )
        if UUID_RE.match(id):
            return id

        # Fallback: deterministic hash to uint64
        # We use hashlib for cross-process stability (Python's hash() is randomised).
        import hashlib
        digest = hashlib.sha256(id.encode()).digest()
        # Take first 8 bytes as unsigned int, mask to positive 64-bit
        return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF

    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """
        Upsert a point into the Qdrant collection.

        If a point with this id already exists, it is overwritten (upsert
        semantics). Qdrant's upsert is idempotent and safe to retry.
        """
        client = self._get_client()
        point_id = self._coerce_id(id)

        await client.upsert(
            collection_name=self._collection_name,
            points=[
                PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload or {},
                )
            ],
        )
        logger.debug(
            "Upserted id=%s (qdrant_id=%s) into collection %s",
            id, point_id, self._collection_name,
        )

    async def delete(self, id: str) -> None:
        """
        Delete a point from the Qdrant collection by its ID.

        Uses Qdrant's PointIdsList selector. Deleting a non-existent point
        is a no-op (Qdrant does not raise an error).
        """
        from qdrant_client.models import PointIdsList

        client = self._get_client()
        point_id = self._coerce_id(id)

        await client.delete(
            collection_name=self._collection_name,
            points_selector=PointIdsList(points=[point_id]),
        )
        logger.debug(
            "Deleted id=%s (qdrant_id=%s) from collection %s",
            id, point_id, self._collection_name,
        )

    async def close(self) -> None:
        """Close the Qdrant async client."""
        if self._client is not None:
            await self._client.close()
            self._client = None
            logger.info("QdrantSink client closed.")

from __future__ import annotations

import hashlib
import logging
import re

from .base import Sink

logger = logging.getLogger("pgstream.sinks.qdrant")

try:
    from qdrant_client import AsyncQdrantClient
    from qdrant_client.models import PointStruct
    _QDRANT_AVAILABLE = True
except ImportError:
    _QDRANT_AVAILABLE = False

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


class QdrantSink(Sink):
    """Writes vectors to a `Qdrant <https://qdrant.tech>`_ collection.

    The Qdrant collection must exist before using this sink. Create it once::

        from qdrant_client import QdrantClient
        from qdrant_client.models import VectorParams, Distance

        QdrantClient("http://localhost:6333").create_collection(
            collection_name="documents",
            vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
        )

    pgstream does not auto-create collections because the vector dimension and
    distance metric are model-specific decisions that belong to the caller.

    Args:
        url:             Qdrant server URL (default ``"http://localhost:6333"``).
        collection_name: Target collection name (default ``"pgstream"``).
        api_key:         API key for Qdrant Cloud (optional).

    **ID coercion:** Qdrant point IDs must be unsigned integers or UUIDs.
    pgstream passes string IDs and coerces them in order:

    1. Parse as integer (covers ``SERIAL`` / ``BIGSERIAL`` PKs).
    2. Pass through as-is if it matches the UUID format.
    3. SHA-256 hash to a stable ``uint63`` for all other strings.
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
                "Run: pip install pgstream[qdrant]"
            )
        self._url = url
        self._collection_name = collection_name
        self._api_key = api_key
        self._client: AsyncQdrantClient | None = None

    def _get_client(self) -> "AsyncQdrantClient":
        if self._client is None:
            self._client = AsyncQdrantClient(url=self._url, api_key=self._api_key)
        return self._client

    def _coerce_id(self, id: str) -> int | str:
        try:
            return int(id)
        except ValueError:
            pass
        if _UUID_RE.match(id):
            return id
        digest = hashlib.sha256(id.encode()).digest()
        return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF

    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """Upsert a point into the Qdrant collection. Idempotent."""
        client = self._get_client()
        point_id = self._coerce_id(id)
        await client.upsert(
            collection_name=self._collection_name,
            points=[PointStruct(id=point_id, vector=vector, payload=payload or {})],
        )
        logger.debug("Upserted id=%s (qdrant_id=%s) into %s", id, point_id, self._collection_name)

    async def delete(self, id: str) -> None:
        """Delete a point by ``id``. No-op if the point does not exist."""
        from qdrant_client.models import PointIdsList
        client = self._get_client()
        point_id = self._coerce_id(id)
        await client.delete(
            collection_name=self._collection_name,
            points_selector=PointIdsList(points=[point_id]),
        )
        logger.debug("Deleted id=%s (qdrant_id=%s) from %s", id, point_id, self._collection_name)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
            logger.info("QdrantSink client closed.")

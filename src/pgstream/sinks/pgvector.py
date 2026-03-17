from __future__ import annotations

import json
import logging

import asyncpg

from .base import Sink

logger = logging.getLogger("pgstream.sinks.pgvector")


class PgVectorSink(Sink):
    """Writes vectors to a `pgvector <https://github.com/pgvector/pgvector>`_-enabled Postgres table.

    The target table must have this schema::

        CREATE EXTENSION IF NOT EXISTS vector;
        CREATE TABLE embeddings (
            id      TEXT PRIMARY KEY,
            vector  VECTOR(1536),   -- match your model's output dimension
            payload JSONB
        );

    Args:
        dsn:       Postgres connection string.
        table:     Target table name (default ``"embeddings"``).
        dimension: Embedding dimension — informational only.
    """

    def __init__(
        self,
        dsn: str,
        table: str = "embeddings",
        dimension: int | None = None,
    ) -> None:
        self._dsn = dsn
        self._table = table
        self._dimension = dimension
        self._pool: asyncpg.Pool | None = None

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=1,
                max_size=5,
                init=self._init_connection,
            )
        return self._pool

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection) -> None:
        await conn.execute("SET search_path TO public")

    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """Insert or update a vector row. Uses ``INSERT ... ON CONFLICT DO UPDATE``."""
        pool = await self._get_pool()
        vector_str = "[" + ",".join(str(v) for v in vector) + "]"
        payload_json = json.dumps(payload) if payload is not None else "{}"

        await pool.execute(
            f"""
            INSERT INTO {self._table} (id, vector, payload)
            VALUES ($1, $2::vector, $3::jsonb)
            ON CONFLICT (id) DO UPDATE
                SET vector  = EXCLUDED.vector,
                    payload = EXCLUDED.payload
            """,
            id,
            vector_str,
            payload_json,
        )
        logger.debug("Upserted id=%s into %s", id, self._table)

    async def delete(self, id: str) -> None:
        """Delete a row by ``id``. No-op if the row does not exist."""
        pool = await self._get_pool()
        await pool.execute(f"DELETE FROM {self._table} WHERE id = $1", id)
        logger.debug("Deleted id=%s from %s", id, self._table)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.info("PgVectorSink pool closed.")

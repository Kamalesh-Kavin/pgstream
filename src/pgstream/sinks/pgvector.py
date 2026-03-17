"""
sinks/pgvector.py — pgvector reference sink implementation.

pgvector (https://github.com/pgvector/pgvector) is a Postgres extension that
adds a `vector` column type and approximate nearest-neighbour indexes (HNSW,
IVFFlat). It's the simplest vector store to run alongside pgstream because
you likely already have Postgres running.

Setup required in your database:
    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE TABLE embeddings (
        id      TEXT PRIMARY KEY,
        vector  VECTOR(1536),        -- match your embedding model's dimension
        payload JSONB
    );
    -- Optional: approximate nearest-neighbour index for fast similarity search
    CREATE INDEX ON embeddings USING hnsw (vector vector_cosine_ops);

This sink uses asyncpg for async writes (non-blocking, compatible with asyncio).
psycopg2 is only used for the replication connection in replication.py; all
normal query connections use asyncpg.

Why asyncpg here instead of psycopg2?
  The user's handler runs in an asyncio event loop. Blocking psycopg2 calls
  in the handler would stall the loop. asyncpg is async-native and correct here.
  (The replication loop runs in a background thread, so psycopg2 is fine there.)
"""

from __future__ import annotations

import json
import logging

import asyncpg

from .base import Sink

logger = logging.getLogger("pgstream.sinks.pgvector")


class PgVectorSink(Sink):
    """
    Writes vectors to a pgvector-enabled Postgres table.

    Args:
        dsn:       Postgres connection string, e.g.
                   "postgresql://user:pass@localhost:5432/db"
        table:     Target table name (default "embeddings").
                   Must have columns: id TEXT, vector VECTOR(n), payload JSONB.
        dimension: Embedding dimension. Used only for validation/logging.
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
        """Lazily initialise the asyncpg connection pool."""
        if self._pool is None:
            # Register the pgvector codec so asyncpg can handle VECTOR columns.
            # We store vectors as text (list literal) and let Postgres cast them.
            # This avoids needing a custom codec — Postgres accepts '[1,2,3]'::vector.
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=1,
                max_size=5,
                init=self._init_connection,
            )
        return self._pool

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection) -> None:
        """
        Per-connection initialisation.
        Register a text codec for the 'vector' type so asyncpg accepts Python
        list → pgvector column without a custom binary codec.
        """
        await conn.execute("SET search_path TO public")

    async def upsert(
        self,
        id: str,
        vector: list[float],
        payload: dict | None = None,
    ) -> None:
        """
        Insert or update a row in the pgvector table.

        Uses INSERT ... ON CONFLICT (id) DO UPDATE so it's safe to call
        repeatedly for the same id (idempotent).

        The vector is passed as a Postgres array literal string, e.g.
        '[0.1, 0.2, ...]', which Postgres casts to the VECTOR type.
        """
        pool = await self._get_pool()

        # Serialise vector as a Postgres array literal that pgvector understands.
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
        """Remove a row by id. No-op if the id doesn't exist."""
        pool = await self._get_pool()
        await pool.execute(
            f"DELETE FROM {self._table} WHERE id = $1",
            id,
        )
        logger.debug("Deleted id=%s from %s", id, self._table)

    async def close(self) -> None:
        """Close the asyncpg pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.info("PgVectorSink pool closed.")

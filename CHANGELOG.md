# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] - 2025-01-01

### Added

- `PGStream` — top-level API for watching tables and managing the replication lifecycle (`watch`, `sink`, `on_change`, `setup`, `start`, `stop`, `teardown`).
- `ChangeEvent` — typed dataclass representing a single committed row-level change from the WAL (`operation`, `schema`, `table`, `row`, `old_row`, `lsn`, `commit_time`, `xid`).
- `PgOutputDecoder` — pure Python binary parser for the pgoutput logical replication protocol (Begin, Commit, Relation, Insert, Update, Delete, Truncate messages).
- `SlotManager` — idempotent setup and teardown of Postgres replication slots and publications.
- `ReplicationStream` — background thread loop for streaming WAL events via psycopg2.
- `Sink` — abstract base class for vector store backends.
- `PgVectorSink` — reference sink implementation using asyncpg and pgvector.
- `QdrantSink` — reference sink implementation using qdrant-client with automatic ID coercion.
- At-least-once delivery guarantee: LSNs are ACKed only after the handler returns successfully.
- Threading bridge: psycopg2 blocking replication loop runs in a daemon thread; handlers execute in the main asyncio event loop via `asyncio.run_coroutine_threadsafe`.
- Full documentation in `docs/`.
- Unit and integration test suite.

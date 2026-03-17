"""
pgstream — Postgres CDC → vector store sync SDK.

Public API surface:

    from pgstream import PGStream, ChangeEvent
    from pgstream.sinks import PgVectorSink, QdrantSink

The top-level package intentionally exports only the essentials. Sink
implementations live under pgstream.sinks so users can import just what
they need without pulling in all optional dependencies.

Version policy: semantic versioning. Breaking changes bump the major version.
"""

from .events import ChangeEvent
from .stream import PGStream

__all__ = ["PGStream", "ChangeEvent"]
__version__ = "0.1.0"

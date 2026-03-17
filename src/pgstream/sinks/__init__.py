"""
sinks/__init__.py — Public exports for the pgstream sinks package.

Import the abstract base class from here, and conditionally expose the
reference implementations. This keeps optional dependencies truly optional:
if a user hasn't installed qdrant-client, importing pgstream.sinks will
still work — QdrantSink simply won't be importable until they install it.
"""

from .base import Sink
from .pgvector import PgVectorSink

# QdrantSink is always importable from this package; the ImportError for
# qdrant-client is raised at instantiation time (not import time) so that
# users who only want pgvector can import from pgstream.sinks freely.
from .qdrant import QdrantSink

__all__ = ["Sink", "PgVectorSink", "QdrantSink"]

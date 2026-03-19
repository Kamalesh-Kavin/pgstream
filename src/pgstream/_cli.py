"""
pgstream._cli
~~~~~~~~~~~~~

Command-line interface for pgstream.

Provides the ``pgstream`` command installed as a script entry-point when you
``pip install pgstream``.  The CLI is a quick-reference tool — pgstream itself
is a library, not a long-running daemon.

Usage::

    pgstream --help
    pgstream --version
    pgstream info
    pgstream quickstart
    pgstream config
"""

from __future__ import annotations

import argparse
import sys
import textwrap

from pgstream import __version__

# ──────────────────────────────────────────────────────────────────────────────
# Colour helpers (degrades gracefully on non-TTY / Windows)
# ──────────────────────────────────────────────────────────────────────────────

_RESET = "\033[0m"
_BOLD = "\033[1m"
_CYAN = "\033[36m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_DIM = "\033[2m"


def _c(text: str, *codes: str) -> str:
    """Wrap *text* in ANSI escape codes only when stdout is a real TTY."""
    if not sys.stdout.isatty():
        return text
    return "".join(codes) + text + _RESET


# ──────────────────────────────────────────────────────────────────────────────
# Sub-command handlers
# ──────────────────────────────────────────────────────────────────────────────


def _cmd_info(_args: argparse.Namespace) -> None:
    """Print a short summary of what pgstream is and where to find docs."""
    print(
        textwrap.dedent(
            f"""\

            {_c("pgstream", _BOLD, _CYAN)}  {_c(f"v{__version__}", _DIM)}

            Python SDK for Postgres CDC (Change Data Capture).
            Watch tables via logical replication and sync changes to vector stores:
              {_c("→", _GREEN)} pgvector  (pip install pgstream[pgvector])
              {_c("→", _GREEN)} Qdrant    (pip install pgstream[qdrant])

            {_c("Docs:", _BOLD)}      https://github.com/Kamalesh-Kavin/pgstream/tree/main/docs
            {_c("PyPI:", _BOLD)}      https://pypi.org/project/pgstream/
            {_c("Issues:", _BOLD)}    https://github.com/Kamalesh-Kavin/pgstream/issues

            Run  {_c("pgstream quickstart", _YELLOW)}  to see a copy-pasteable example.
            Run  {_c("pgstream config", _YELLOW)}      to see all configuration options.
            """
        )
    )


def _cmd_quickstart(_args: argparse.Namespace) -> None:
    """Print a minimal working pgstream example."""
    snippet = textwrap.dedent(
        """\
        # ── install ───────────────────────────────────────────────────────────
        # pip install pgstream

        # ── prerequisites ─────────────────────────────────────────────────────
        # 1. Postgres must have wal_level = logical
        #    ALTER SYSTEM SET wal_level = logical;
        #    SELECT pg_reload_conf();
        #
        # 2. Create a replication slot:
        #    SELECT pg_create_logical_replication_slot(\'pgstream_slot\', \'pgoutput\');
        #
        # 3. Create a publication:
        #    CREATE PUBLICATION pgstream_pub FOR TABLE users, orders;

        # ── main.py ───────────────────────────────────────────────────────────
        import asyncio
        from pgstream import PGStream, ChangeEvent

        DSN = "postgresql://user:password@localhost:5432/mydb"

        async def main() -> None:
            stream = PGStream(
                dsn=DSN,
                publication="pgstream_pub",
                slot_name="pgstream_slot",
            )

            async for event in stream:
                print(event)          # ChangeEvent(table, op, old, new)
                await process(event)


        async def process(event: ChangeEvent) -> None:
            if event.table == "users" and event.op == "INSERT":
                print(f"New user: {event.new}")


        asyncio.run(main())

        # ── with pgvector sink ────────────────────────────────────────────────
        # pip install pgstream[pgvector]

        from pgstream.sinks.pgvector import PGVectorSink

        sink = PGVectorSink(
            dsn=DSN,
            table="embeddings",
            embed=lambda row: my_embed_fn(row["text"]),
        )

        async def main_with_sink() -> None:
            stream = PGStream(dsn=DSN, publication="pgstream_pub", slot_name="pgstream_slot")
            async for event in stream:
                await sink.handle(event)
        """
    )
    print(
        _c("── Quickstart ──────────────────────────────────────────────────────", _DIM)
    )
    print()
    try:
        from pygments import highlight  # type: ignore
        from pygments.formatters import TerminalFormatter  # type: ignore
        from pygments.lexers import PythonLexer  # type: ignore

        print(highlight(snippet, PythonLexer(), TerminalFormatter()))
    except ImportError:
        print(snippet)
    print(
        _c("────────────────────────────────────────────────────────────────────", _DIM)
    )


def _cmd_config(_args: argparse.Namespace) -> None:
    """Print PGStream constructor arguments."""
    print(
        textwrap.dedent(
            f"""\

            {_c("PGStream — constructor arguments", _BOLD, _CYAN)}

              from pgstream import PGStream

              stream = PGStream(
                  dsn         = "postgresql://user:pass@localhost:5432/mydb",
                  publication = "pgstream_pub",
                  slot_name   = "pgstream_slot",
              )

            {_c("Field reference:", _BOLD)}

              {_c("dsn", _YELLOW)}          str   required
                  PostgreSQL connection string (libpq format).

              {_c("publication", _YELLOW)}  str   required
                  Name of the Postgres publication created with
                  CREATE PUBLICATION ... FOR TABLE ...

              {_c("slot_name", _YELLOW)}    str   required
                  Name of the logical replication slot created with
                  pg_create_logical_replication_slot(..., 'pgoutput').

            {_c("ChangeEvent fields:", _BOLD)}

              {_c("table", _YELLOW)}   str             Table name (schema-qualified if not public).
              {_c("op", _YELLOW)}      str             "INSERT" | "UPDATE" | "DELETE"
              {_c("old", _YELLOW)}     dict | None     Old row values (UPDATE / DELETE only).
              {_c("new", _YELLOW)}     dict | None     New row values (INSERT / UPDATE only).

            {_c("Optional extras:", _BOLD)}

              pip install pgstream[pgvector]   # PGVectorSink — write embeddings to pgvector
              pip install pgstream[qdrant]     # QdrantSink   — write embeddings to Qdrant
              pip install pgstream[all]        # all sinks
            """
        )
    )


# ──────────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pgstream",
        description=(
            "pgstream — Python SDK for Postgres CDC.\n"
            "Stream table changes via logical replication to vector stores."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            commands:
              info        Show version, links, and a short description
              quickstart  Print a copy-pasteable streaming example
              config      Print PGStream arguments and ChangeEvent fields

            examples:
              pgstream info
              pgstream quickstart
              pgstream config
            """
        ),
    )
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"pgstream {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="command")
    subparsers.add_parser("info", help="Show version, links, and a short description")
    subparsers.add_parser("quickstart", help="Print a copy-pasteable streaming example")
    subparsers.add_parser(
        "config", help="Print PGStream arguments and ChangeEvent fields"
    )

    return parser


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────


def main() -> None:
    """Entry point registered as ``pgstream = pgstream._cli:main``."""
    parser = _build_parser()
    args = parser.parse_args()

    dispatch = {
        "info": _cmd_info,
        "quickstart": _cmd_quickstart,
        "config": _cmd_config,
    }

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    dispatch[args.command](args)


if __name__ == "__main__":
    main()

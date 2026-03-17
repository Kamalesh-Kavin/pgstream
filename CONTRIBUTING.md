# Contributing to pgstream

Thank you for your interest in contributing. This document explains how to set up the development environment, run tests, and submit changes.

---

## Development setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (`pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Docker (for integration tests)

### Clone and install

```bash
git clone https://github.com/Kamalesh-Kavin/pgstream.git
cd pgstream
uv sync --all-extras
```

This installs the package in editable mode along with all optional dependencies and dev tools (`pytest`, `pytest-asyncio`, `asyncpg`).

---

## Running tests

### Unit tests (no database required)

```bash
uv run pytest tests/test_decoder.py -v
```

### Integration tests

Integration tests require a running Postgres instance with `wal_level = logical`. The easiest way is Docker:

```bash
docker run -d \
  --name pgstream-test-db \
  -e POSTGRES_USER=pgstream \
  -e POSTGRES_PASSWORD=pgstream \
  -e POSTGRES_DB=pgstream \
  -p 5432:5432 \
  postgres:16 \
  -c wal_level=logical

export PGSTREAM_DSN=postgresql://pgstream:pgstream@localhost:5432/pgstream
uv run pytest tests/ -v
```

### Skip integration tests

```bash
uv run pytest tests/ -v -m "not integration"
```

---

## Code style

There is no enforced linter or formatter configured yet. Keep the existing style:

- 4-space indentation
- Type annotations on all public functions and methods
- Docstrings on all public classes and methods (Google/NumPy style is fine)
- No unnecessary comments — code should be self-explanatory

---

## Submitting a pull request

1. Fork the repository and create a branch from `main`:

   ```bash
   git checkout -b feat/my-feature
   ```

2. Make your changes.

3. Run the full test suite and ensure it passes.

4. Push your branch and open a pull request against `main`.

5. Fill in the pull request template.

---

## Reporting bugs

Open an issue using the [bug report template](https://github.com/Kamalesh-Kavin/pgstream/issues/new?template=bug_report.md).

Include:
- pgstream version (`pip show pgstream`)
- Python version
- Postgres version
- Minimal reproduction case
- Full traceback

---

## Requesting features

Open an issue using the [feature request template](https://github.com/Kamalesh-Kavin/pgstream/issues/new?template=feature_request.md).

---

## Adding a new sink

See [docs/building-a-sink.md](docs/building-a-sink.md) for the `Sink` interface. If you want to contribute a built-in sink:

1. Create `src/pgstream/sinks/yourstore.py`.
2. Add the optional dependency to `pyproject.toml` under `[project.optional-dependencies]`.
3. Export the class from `src/pgstream/sinks/__init__.py`.
4. Write tests in `tests/test_yourstore_sink.py`.
5. Add a section to `docs/sinks.md`.

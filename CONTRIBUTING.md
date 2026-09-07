# Contributing to Portiere

Thank you for your interest in contributing to Portiere!

## Development Setup

```bash
# Use Python 3.12 for the maintainer baseline
git clone https://github.com/Cuspal/portiere.git
cd portiere
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

On Windows, create the environment with `py -3.12 -m venv .venv` and activate it with `.venv\Scripts\Activate.ps1` in PowerShell. Use the same `python -m pip` commands afterward. Keep optional model engines in a separate environment so ordinary checks do not depend on downloaded models or native ML libraries.

`requirements/dev.txt` pins the contributor dependencies, including Polars, Pandas, quality checks and build tools, with platform/Python markers. It does not constrain library consumers. CI tests Python 3.10–3.12; the bundled integration workflow exercises Python 3.12 on Linux, macOS and Windows. A configured CI job is a coverage target, not evidence that an unrun platform passes.

Regenerate the lock deliberately after editing dependencies, review the diff and rerun checks. Add `--upgrade` when intentionally refreshing pinned versions:

```bash
uv pip compile pyproject.toml requirements/dev-tools.in --extra dev --extra polars --extra quality --python-version 3.10 --universal --no-annotate -o requirements/dev.txt
```

See the [uv locking documentation](https://docs.astral.sh/uv/pip/compile/) for resolution and synchronization behavior. Start with a new virtual environment when reproducing a failure; installing requirements does not remove unrelated packages already present.

## Running Tests

```bash
# Run the default suite (excludes model-download tests marked slow)
python -m pytest

# Run with coverage report
python -m pytest --cov=portiere --cov-report=term-missing

# Run a specific test file
python -m pytest tests/test_quickstart.py -v

# Reproduce the first-use pipeline with bundled data
portiere quickstart --output-dir ./demo-output
```

Model benchmarks require a separate environment with the appropriate extras and vocabulary/model assets. Select them explicitly with `python -m pytest -m slow`; they are not part of the offline contributor baseline. Spark adapter tests require PySpark and Java and otherwise skip.

Before a pull request, run `ruff check src/ tests/`, `ruff format --check src/ tests/`, `mypy src/portiere/`, and the default test suite. Build with `python -m build` and test the installed wheel from outside the checkout when changing packaging or bundled data. The publication checklist is in [docs/releasing.md](docs/releasing.md).

## Code Style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.

```bash
# Check for issues
ruff check src/ tests/

# Auto-fix issues
ruff check --fix src/ tests/

# Format code
ruff format src/ tests/
```

## Project Structure

```
src/portiere/
├── __init__.py          # Public API: portiere.init()
├── config.py            # Configuration models
├── project.py           # Project orchestration
├── runner/              # ETL pipeline runner
├── stages/              # Pipeline stages (ingest → validate)
├── models/              # Data models (SchemaMapping, ConceptMapping)
├── engines/             # Compute engines (Polars, Spark, Pandas)
├── knowledge/           # Knowledge layer backends (BM25s, FAISS, etc.)
├── local/               # Local AI components (schema mapper, concept mapper)
├── llm/                 # LLM provider integrations
├── embedding/           # Embedding provider integrations
├── standards/           # YAML-driven clinical standard definitions
├── quality/             # Data quality validation (Great Expectations)
├── artifacts/           # ETL artifact generation (Jinja2 templates)
├── storage/             # Storage backends (local filesystem)
└── cli/                 # CLI commands (portiere models)
```

## Adding a New Standard

Place a YAML definition file in `src/portiere/standards/`. See `omop_cdm_v5.4.yaml` for the expected schema format. The standard is automatically discovered by `portiere.standards.list_standards()`.

## Adding a New Knowledge Backend

1. Create `src/portiere/knowledge/<name>_backend.py` implementing `AbstractKnowledgeBackend`.
2. Register it in `src/portiere/knowledge/factory.py`.
3. Add the optional dependency to `pyproject.toml` under `[project.optional-dependencies]`.
4. Add tests in `tests/test_knowledge_backends.py`.

## Submitting a Pull Request

1. Fork the repository and create a branch from `main`.
2. Make your changes with tests.
3. Ensure `ruff check` passes and all tests pass.
4. Open a pull request against `main` with a clear description of the change.

## Reporting Issues

Open an issue at [https://github.com/Cuspal/portiere/issues](https://github.com/Cuspal/portiere/issues).

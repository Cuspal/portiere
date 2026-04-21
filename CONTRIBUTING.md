# Contributing to Portiere

Thank you for your interest in contributing to Portiere!

## Development Setup

```bash
# Clone and install in editable mode with dev dependencies
git clone https://github.com/Cuspal/portiere.git
cd portiere
pip install -e ".[dev,polars,quality]"
```

## Running Tests

```bash
# Run all tests
python -m pytest

# Run with coverage report
python -m pytest --cov=portiere --cov-report=term-missing

# Run a specific test file
python -m pytest tests/test_models.py -v
```

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

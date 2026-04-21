# Changelog

All notable changes to the Portiere SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-21

### Changed

- **Breaking**: Removed `OMOPModel` and `FHIRModel` legacy classes — all standards now use YAML-driven `YAMLTargetModel` via `get_target_model()`
- Standards-aware validation — `GXValidator` derives conformance checks (code/vocabulary, temporal) from YAML field type metadata for all standards (OMOP, FHIR, HL7 v2, OpenEHR, custom)
- Renamed `_build_omop_suite` → `_build_expectation_suite` (backward-compatible alias retained)
- Fixed `SchemaMapping.finalize()` and `ConceptMapping.finalize()` raising `NotImplementedError` in open-source SDK
- Fixed `portiere.init()` `engine` parameter now optional (defaults to `PolarsEngine()`)
- Fixed `KnowledgeLayerConfig` missing `fusion_weights` field
- Fixed `SchemaMappingItem` NaN coercion for `source_table` and `source_column` from CSV round-trips
- Added `bm25s` and `numpy` to required dependencies

## [0.1.0-alpha] - 2026-03-25

### Added

- **Unified API** — `portiere.init()` entry point for creating and loading mapping projects
- **5-stage pipeline** — Ingest & Profile, Schema Mapping, Concept Mapping, ETL Generation, Validation
- **4 compute engines** — Polars, PySpark, Pandas, DuckDB with abstract `AbstractEngine` interface
- **9 knowledge layer backends** — BM25s (default), FAISS, Elasticsearch, ChromaDB, PGVector, MongoDB, Qdrant, Milvus, Hybrid (RRF fusion)
- **4 LLM providers** — OpenAI, Anthropic Claude, AWS Bedrock, Ollama (local) with BYO-LLM pattern
- **4 clinical standards** — OMOP CDM v5.4, FHIR R4, HL7 v2.5.1, OpenEHR 1.0.4 (YAML-driven, extensible)
- **5 cross-standard mappings** — OMOP↔FHIR, HL7v2→FHIR, OMOP→OpenEHR, FHIR→OpenEHR
- **AI-powered mapping** — SapBERT clinical embeddings, cross-encoder reranking, LLM verification
- **Confidence routing** — Three-tier auto-accept/needs-review/manual with human-in-the-loop approval
- **ETL artifact generation** — Standalone Spark/Polars/Pandas scripts via Jinja2 templates
- **Data quality validation** — Great Expectations integration for completeness, conformance, plausibility
- **Configuration auto-discovery** — YAML files, Python objects, environment variables with Pydantic Settings
- **Local storage backend** — YAML/CSV/JSON artifact persistence with project directory structure
- **CLI tool** — `portiere models` command for downloading, listing, and inspecting embedding models
- **PEP 561 compliance** — `py.typed` marker for typed package support
- **CI/CD** — GitHub Actions for lint, typecheck, test (Python 3.10/3.11/3.12), build, and PyPI publishing
- **22 documentation guides** — Quickstart, API reference, configuration, deployment, migration, and more
- **19 example notebooks** — Progressive walkthroughs from quickstart to end-to-end transformations

### Notes

- Cloud features (`Client`, `SyncManager`) are importable but raise `NotImplementedError` — Portiere Cloud is on the development roadmap
- This is an alpha release (`Development Status :: 3 - Alpha`)

[0.1.0]: https://github.com/Cuspal/portiere/releases/tag/v0.1.0

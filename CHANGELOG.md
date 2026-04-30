# Changelog

All notable changes to the Portiere SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-04-30

The "benchmark integrity hotfix." Re-publishes the ICD-10-CM → SNOMED top-10 number after fixing a silent candidate-list truncation that made the published top-5 and top-10 figures structurally identical.

### Fixed

- **`LocalConceptMapper.map_code` returned only the first 5 candidates** instead of the full ranked list the retrieval pipeline produced (up to 10). The hardcoded `candidates[:5]` was sized for review-UI display and incorrectly applied as the universal cap, so any consumer reading `ConceptMappingItem.candidates` for top-k retrieval analysis lost ranks 6–10. The mapper now returns the full ranked list; review-UI consumers re-slice as needed.

### Changed

- **`expected_results.json` re-published** against the same Athena 2026-04-30 vocabulary release. `top_1` (0.288) and `top_5` (0.528) are unchanged — the truncation didn't drop ranks 0–4. `top_10` and `mrr` are corrected: the v0.2.0 published `top_10 == top_5 == 0.528` was an artifact of the truncation, not a property of the retrieval. The corrected numbers are the new BM25-only baseline that v0.3.0's hybrid embedding work must beat.

### Tests

- New `TestLocalConceptMapperCandidatesNotTruncated::test_returns_all_candidates_from_retrieval_pipeline` — regression test asserting the mapper returns all 10 candidates when the backend produces them.
- New `TestComputeMetrics::test_top10_differentiates_from_top5` — defense-in-depth metric assertion that catches future regressions of the same class.

## [0.2.0] - 2026-04-30

The "credibility release." Closes the gap between v0.1.0's claims and reality across standards coverage, plausibility validation, reproducibility, demo activation, benchmarks, and repo hygiene.

### Added

- **Standards coverage**:
  - OMOP CDM v5.4: 8 → 19 entities. New clinical-data tables: `provider`, `care_site`, `death`, `observation_period`, `device_exposure`, `note`. New vocabulary tables: `concept`, `vocabulary`, `concept_relationship`, `domain`, `concept_class` (needed for FK validation).
  - FHIR R4: 8 → 18 resources. New: `Practitioner`, `Organization`, `Location`, `Specimen`, `Immunization`, `ServiceRequest`, `MedicationAdministration`, `MedicationDispense`, `Bundle`, `DocumentReference`.
  - `condition_end_date` added to OMOP `condition_occurrence` (was omitted in v0.1.0; needed for the `condition_start_before_end` temporal plausibility rule).
- **Real plausibility validation** ([docs/plausibility.md](docs/plausibility.md)):
  - New hybrid grammar: 5-rule YAML DSL (`range`, `regex`, `enum`, `temporal_order`, `fk_exists`) + Python rules for cross-table / ValueSet / aggregate checks.
  - 5 implemented OMOP Python rules (`birth_before_death`, `condition_dates_consistent`, `concept_id_fk`, `domain_match`, `age_in_range`).
  - 3 implemented FHIR Python rules (`patient_birthdate_not_future`, `observation_status_in_valueset`, `medication_request_intent_in_valueset`).
  - DuckDB-backed FK validation scales to full-Athena-sized vocabulary tables.
  - YAML `plausibility:` blocks added to OMOP `person` + `condition_occurrence` and FHIR `Observation` + `MedicationRequest`.
  - `severity` model: `error` (fails validation) | `warn` (reported, doesn't fail).
- **Reproducibility manifest** ([docs/reproducibility.md](docs/reproducibility.md)):
  - Every pipeline run emits a versioned `manifest.lock.json` capturing env (portiere/python/os), git state, project + target + task + source_standard, embedding identity tuple, knowledge-backend identity, vocab fingerprints, prompt template hashes, threshold snapshot, source-data fingerprint (with credentials redacted at the recorder boundary), per-stage entries.
  - `Project` is now a context manager (`with portiere.init(...) as project:`); auto-finalizes on `__exit__`, including on exceptions. Explicit `finalize_run()` also exposed.
  - New `portiere replay <manifest>` CLI command verifies referenced artifacts and reconstructs the project; raises `ManifestReplayError` on missing/mismatched artifact.
  - `ValidationReport` gains `overall_success_score` and `plausibility_rule_results` fields.
- **Bundled demo + `portiere quickstart`**:
  - ~14 KB of curated Synthea-style source CSVs (20 patients, 30 conditions, 37 observations, 25 medications) with deliberately mid-spice messy column names.
  - 27-concept Athena-format vocabulary subset (ICD-10-CM + LOINC + RxNorm).
  - `portiere quickstart` runs the full 5-stage pipeline end-to-end **fully offline** in ~10s. Emits a manifest covering all stages.
  - New `EmbeddingConfig.provider="none"` (`NoOpEmbeddingProvider`) for fully-offline operation without `sentence-transformers`.
- **ICD-10-CM → SNOMED benchmark** ([docs/benchmarks/athena-icd-snomed.md](docs/benchmarks/athena-icd-snomed.md)):
  - New `benchmarks/athena_icd_snomed/` with runner, test-set generator, and `expected_results.json`.
  - New `portiere benchmark athena-icd-snomed --athena-dir <path>` CLI command.
  - Metrics: top-1, top-5, top-10, MRR.
  - Held-out test set is committed as integer concept_ids only (license-clean).
- **GitHub hygiene**: `SECURITY.md` (private vulnerability disclosure), `CODE_OF_CONDUCT.md` (Contributor Covenant 2.1), `CITATION.cff`, `SUPPORT.md`, issue & PR templates.
- **Documentation**: 3 new docs pages (`docs/reproducibility.md`, `docs/plausibility.md`, `docs/benchmarks/athena-icd-snomed.md`) + 2 new notebooks (`16_quickstart_walkthrough.ipynb`, `17_reproducibility_manifest.ipynb`).
- **Coverage**: repo-wide test coverage raised from 66% to ~75%.

### Changed

- **CLI restructure** *(breaking for direct entry-point users)*: `portiere = "portiere.cli.models:models"` → `portiere = "portiere.cli:cli"`. The `portiere` console script is now a top-level group with subcommands: `models`, `replay`, `quickstart`, `benchmark`. User-facing usage goes from `portiere download X` to `portiere models download X` (which is what the README has been documenting all along).
- **Plausibility score** in `GXValidator` is no longer the tautological overall expectation success rate. Old `_compute_plausibility` renamed to `_compute_overall_success` (preserves the v0.1.0 metric under an accurate name); new `_compute_plausibility` operates on plausibility-tagged rule results only. Existing pipelines will see their `plausibility_score` change — typically rising or falling sharply depending on whether the underlying rules pass.
- `pyproject.toml` `quality` extra now includes `duckdb` (needed for FK validation backend).
- README's standards-coverage claims now match `list_standards()` reality: 19 OMOP entities (was implied "complete"), 18 FHIR resources (was implied "complete").
- README "Mapping Review Workflow" wording corrected — UI is library-only in v0.2.0 (no web app); planned for v0.3.0.
- Citation BibTeX year/key fixed (was `portiere2024` despite v0.2.0 release; now `portiere2026`).

### Fixed

- `condition_occurrence` now includes `condition_end_date` (was omitted; needed for plausibility temporal rules).
- Plausibility rules with missing optional columns now skip with `passed=True (skipped)` rather than spuriously failing — matches healthcare reality where columns like `death_date` may legitimately be absent.

### Migration notes

- **CLI users:** if you scripted `portiere download <model>`, update to `portiere models download <model>`. The `portiere models X` form has been the documented path since v0.1.0; this change makes it the actually-working path.
- **Pipeline-aware code:** `Project` is now a context manager. Existing `project = portiere.init(...); ...; (no finalize)` code keeps working — runs just don't auto-finalize their manifest. Switch to `with portiere.init(...) as project:` to opt in, or call `project.finalize_run()` explicitly.
- **Validation report consumers:** `plausibility_score` semantics changed (now real plausibility, not GX success rate). The old metric is preserved as `overall_success_score`. If you were comparing against v0.1.0 thresholds, recalibrate.
- **Quality extra:** if you have `pip install portiere-health[quality]` in a lock file, refresh it to pull in `duckdb`.

[0.2.0]: https://github.com/Cuspal/portiere/releases/tag/v0.2.0

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

# Review Orientation — Portiere full-library review

Run started 2026-08-13 (local). Branch `main` @ `beabb7e`, tree clean at Phase 0.

## Pipeline map (public entry → internals)

```
portiere.init(config) ──► Project
   Project.add_source() ──► stages.stage1_ingest.ingest_source ──► engines/*.profile
                                └─ _detect_code_columns / _detect_phi_columns (name heuristic)
                                └─ (opt) _scrub_profile_values ──► deid.PHIScrubber
   Project.map_schema()  ──► stages.stage2_schema ──► local.schema_mapper (patterns + embed + rerank)
   Project.map_concepts()──► stages.stage3_concepts ──► local.concept_mapper
                                └─ knowledge/* backend  └─ local.reranker  └─ (opt) local.llm_verifier ──► llm.gateway ──► provider  ← ONLY egress
   Project.run_etl()     ──► runner.ETLRunner ──► artifacts/code_generator
   Project.validate()    ──► quality.validator (+ plausibility, fhir_profile)
   every op ──► repro.recorder ──► manifest.lock.json
```

**The single egress boundary** is `llm.gateway → provider` (remote LLM) and any
remote knowledge/embedding backend. `PortiereConfig(offline=True)` +
`egress.egress_violations` are the gate. Everything else is local file I/O.

**Reachable from public API** (`portiere.__all__`, 22 symbols): config models,
`ETLRunner`, `Client` (cloud stub), exceptions, `init`, `__version__`. `Project`
itself is returned by `init` but not exported by name.

## Baseline (recorded in state.json)

- **Tests (default):** 1284 passed, 2 skipped, 5 deselected (154s).
- **Tests (`-m slow`):** measuring (background).
- **ruff:** clean. **ruff format --check:** clean (223 files).
- **mypy `src/portiere/`:** 1 error — `knowledge/local_faiss_backend.py:74`
  (SentenceTransformer→None assignment). **Local-env only**: CI does not install
  faiss so CI typecheck is clean. Treated as known/env, not a new finding.
- **PHI ratchet (regex backend):** recall **1.0**, precision **1.0** over the
  fixed corpus (18 structural positives / 14 negatives). Corpus at
  `tests/fixtures/phi_corpus/corpus.jsonl`; scorer `score.py`.
- **PHI ratchet (presidio backend):** **NOT MEASURABLE** — presidio/spacy not
  installed in this env. The `[phi]` extra path cannot be exercised here; any
  presidio-specific finding is code-read only, not runtime-verified.
- **Public API:** 22 exports snapshotted to `api-baseline.json`, sha256[:16]
  `2243a4b4a63e5415`.

## Known-debt (not new findings)

- 8 deferred ruff rules in `pyproject.toml`.
- mypy runs with `implicit_optional=true`, `disallow_untyped_defs=false` — the
  `Typing :: Typed` classifier is stronger than the enforcement (L7 note).
- faiss mypy error above.

## Corpus responsibility split

Regex backend is responsible for structural PHI (EMAIL, NATIONAL_ID [US SSN +
13-digit], MRN, DATE, PHONE). It is NOT responsible for person names / addresses
(presidio-only) or Malaysian NRIC (`######-##-####`, 12 digits — matches no
current recogniser). Those are marked `presidio` / `regex_gap` in the corpus and
excluded from the regex recall denominator; they surface as PV-01 findings, not
baseline drops.

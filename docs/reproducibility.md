# Reproducibility Manifest

Every Portiere pipeline run emits a versioned `manifest.lock.json` that captures exactly what produced the output — model identity, vocabulary fingerprints, source-data hash, threshold snapshot, per-stage entries — so a run can be reproduced by command months later.

Reproducibility in Portiere means **the same inputs and configuration produce the same pipeline state**. It does **not** promise byte-identical outputs: clinical pipelines have legitimate nondeterminism (LLM sampling, BM25 tie-breaks, FAISS thread effects). Outputs reproduce within a small tolerance.

## What's recorded

The manifest is a strict Pydantic-validated JSON document with a versioned schema (`manifest_version: "1"`). Every field is either present or explicitly `null`.

### Always recorded

| Field | Example | Notes |
|---|---|---|
| `manifest_version` | `"1"` | Bumped on breaking schema changes |
| `run.run_id` | `"abc123def456"` | 12-hex-char identifier per run |
| `run.started_at` / `finished_at` / `duration_seconds` | ISO-8601 / float | UTC timestamps |
| `portiere_version` / `python_version` / `os_string` | `"0.2.0"` / `"3.12.1"` / `"Darwin-25.3.0"` | Environment fingerprint |
| `git_sha` / `git_dirty` | hex / bool, or `null` | `null` when not a git repo |
| `project_name` / `target_model` / `task` | strings | `task` is `"standardize"` or `"cross_map"` |
| `source_standard` | `"omop_cdm_v5.4"` or `null` | Set when `task == "cross_map"` |
| `vocabularies_requested` | `["SNOMED", "LOINC"]` | The vocab list passed at `init()` |
| `embedding` | `{name, hf_revision, dimension, sha256_of_config}` | Identity tuple — see below |
| `knowledge_backend` | `{type, index_hash}` | `"bm25s" \| "faiss" \| ...` |
| `vocabularies` | `[{name, version_date, sha256_of_source_file, path}]` | Per-vocab fingerprint |
| `prompt_templates` | `[{name, sha256}]` | Hash only — template text never stored |
| `thresholds` | full ThresholdsConfig snapshot | |
| `source_data` | file: `{path, sha256}` / DB: `{connection_string_redacted, table_or_query}` | Credentials never enter the schema |
| `stages` | per-stage `{stage, started_at, finished_at, inputs, outputs, metrics}` | One entry per pipeline op |

### Never recorded

API keys (OpenAI / Anthropic / AWS credentials), unredacted database connection strings, prompt template text, embedding model weight bytes (only the identity tuple — see below).

A regression test (`tests/test_recorder.py::TestRecorderNoSecretLeak`) sets `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` / `AWS_SECRET_ACCESS_KEY` in the environment and asserts none of those values appear in the emitted manifest. Belt-and-suspenders.

### Why the embedding is an identity tuple, not a hash of weights

SapBERT is ~440 MB. Hashing it on every run would add seconds and convey no more identity than the HF revision SHA already does. The manifest records `{name, hf_revision, dimension}` — three small strings — and trusts Hugging Face to deliver the same weights for the same revision.

When a model is local-only (no HF metadata), `sha256_of_config` is an optional fallback that hashes `config.json` (a few KB).

## Where it lives

```
<config.local_project_dir>/<project_name>/runs/<run_id>/manifest.lock.json
```

A run begins on the first pipeline op (`profile`, `map_schema`, `map_concepts`, `run_etl`, or `validate`) and is finalized when:

- `Project` is used as a context manager and `__exit__` fires (recommended), or
- `project.finalize_run()` is called explicitly.

```python
import portiere
from portiere.engines import PolarsEngine

with portiere.init(name="hospital-migration", engine=PolarsEngine()) as project:
    source = project.add_source("patients.csv")
    schema_map = project.map_schema(source)
    concept_map = project.map_concepts(source=source)
    project.run_etl(source, output_dir="out/")
    project.validate(output_path="out/")
# manifest finalized here, written to runs/<run_id>/manifest.lock.json
```

## Replay

```bash
portiere replay <project>/runs/<run_id>/manifest.lock.json
```

Replay does three things:

1. **Validate referenced artifacts.** Every recorded vocab path and the source data path must exist; sha256 hashes must match. If anything's missing or different, replay raises `ManifestReplayError` with a clear message:
   ```
   Replay failed: source data sha256 mismatch: /path/to/patients.csv
   (manifest says 'a4f3...'; current file differs)
   ```

2. **Reconstruct the project.** A new `portiere.init()` is called with the manifest's recorded `target_model`, `task`, `source_standard`, and `vocabularies_requested`. The new project gets a `replay-` prefixed name to avoid clobbering the original's storage.

3. **Re-attach the source.** If `source_data.path` points to an existing file, it's added to the new project. From there, the caller can re-invoke pipeline ops as needed; v0.2.0 stops at reconstruction. Auto-replaying every recorded stage is a v0.3.0 feature.

### Common replay errors

| Error | Cause | Fix |
|---|---|---|
| `source data missing` | Source file moved or deleted | Restore the file or update the manifest path |
| `vocabulary missing` | Athena vocab moved or deleted | Restore the vocab directory |
| `vocabulary sha256 mismatch` | Vocab file content changed | Re-export from Athena to the same release date |
| `source data sha256 mismatch` | Source CSV was overwritten | Restore from backup |

## What replay does NOT promise

- **Bit-identical outputs.** LLM sampling, BM25 tie-breaks, and FAISS thread effects all introduce small nondeterminism. The same input, same model revision, same vocab will produce *very similar* mappings, but the per-row decisions can flicker at the edge of confidence thresholds.
- **Perfect environment recreation.** Replay validates artifact identity but doesn't reinstall Python packages or pin transitive deps. Pin your environment with `pip freeze > requirements.lock.txt` for that level of fidelity.
- **Full stage replay.** v0.2.0 reconstructs the project and re-attaches the source; the caller re-invokes pipeline ops. Auto-replay of recorded stages comes in v0.3.0.

## Audit / compliance use

The manifest is the right artifact to attach to an IRB submission, a publication's data-availability statement, or an internal change-management ticket. It answers:

- Which model was used?
- Which vocabulary version?
- Which source data (by hash, not just path)?
- Which thresholds?
- When was each stage run, with what inputs and outputs?

It does **not** answer "what were the per-row mapping decisions" — those live in the `concept_mapping.csv` / `schema_mapping.csv` artifacts, which the manifest's stage entries can point to via `outputs`.

## See also

- [Spec §4.2](../specs/2026-04-29-v0.2.0-release-design.md) — full design rationale
- [`portiere replay` CLI](documentations/02-unified-api-reference.md) — invocation reference
- [Quickstart manifest example](notebooks_examples/17_reproducibility_manifest.ipynb) — interactive walkthrough

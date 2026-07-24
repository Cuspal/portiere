# Benchmark: ICD-10-CM → SNOMED Concept Mapping

Portiere's accuracy on the canonical OMOP concept-mapping task: given an ICD-10-CM source code, predict the standard SNOMED CT concept it should map to. Evaluated against the OHDSI Athena `CONCEPT_RELATIONSHIP` gold standard.

## Headline numbers (v0.4.0 — measured after the blending fix)

| Backend                                        | top-1 | top-5 | top-10 |   MRR | N |
|------------------------------------------------|------:|------:|-------:|------:|---:|
| **BM25 + cross-encoder reranker**              | **0.303** | **0.533** | **0.588** | **0.402** | 1,000 |
| Hybrid (BM25+FAISS via RRF) + reranker         | 0.286 | 0.505 |  0.587 | 0.377 | 1,000 |
| SapBERT + FAISS + reranker                     | 0.285 | 0.462 |  0.546 | 0.363 | 1,000 |
| USAGI 1.4.3 (OHDSI baseline) †                 | 0.262 | 0.262 |  0.262 | 0.262 | 1,000 |

**Every Portiere configuration beats the OHDSI USAGI baseline** — best top-1
0.303 vs 0.262 (+16% relative). † USAGI was scored via its UI
`source_to_concept_map` export, which carries a single best match per code, so
its top-5/top-10 equal its top-1; batch mode (with ranked candidates) could
only raise those two cells, not top-1.

Auto-accept share (BM25+reranker, default thresholds): **51.8%** of codes
routed `auto` (518 auto / 317 review / 165 manual / 0 unmapped) — the
human-throughput proxy: half the workload never needs a reviewer, and the
review tier arrives with ranked candidates.

Athena release: `2026-04-30`. Source of truth: `src/portiere/benchmarks/athena_icd_snomed/expected_results.json`. The pre-fix v0.3.x rows are archived in `expected_results_v03x.json`.

> **The v0.4.0 blending fix — how these rows changed and why.**
>
> The published pipeline blends cross-encoder scores with retrieval scores
> (60/40). Pre-v0.4.0, the blend used **raw** retrieval scores, whose scales
> differ wildly per backend: BM25 is unbounded (5–30+), FAISS cosine is 0–1,
> and hybrid RRF maxes out near 2/(k+1) ≈ 0.033. In effect the three rows were
> measured under **three different blending regimes**:
>
> | Backend | Effective regime |
> |---|---|
> | bm25s | retrieval-only (raw BM25 dwarfs the ≤0.6 CE term) |
> | faiss | roughly the advertised 60/40 blend |
> | hybrid | CE-only (RRF term numerically negligible) |
>
> This mechanism explained the v0.3.x anomaly of hybrid (0.251) scoring below
> **both** of its own inputs — its ranking was fully controlled by
> `cross-encoder/ms-marco-MiniLM-L-6-v2`. It also meant BM25's win was partly
> an accident of scale: it was *protected* from the misapplied reranker rather
> than blended with it.
>
> v0.4.0 fixes the blend (per-list min-max normalization of the retrieval
> component; regression-tested in `tests/test_reranker_blending.py`) and adds
> `--reranker/--no-reranker` so the reranker's contribution is measurable in
> isolation. The ablation below **confirms the diagnosis with data**: the
> reranker-off bm25s row reproduces the old published 0.288 exactly (the old
> "BM25" row really was retrieval-only), and the old hybrid anomaly is gone.

### Reranker ablation (v0.4.0 — measured)

| Backend | Reranker | top-1 | top-5 | top-10 | MRR | N |
|---|---|------:|------:|-------:|----:|---:|
| bm25s  | on  | **0.303** | 0.533 | 0.588 | 0.402 | 1,000 |
| bm25s  | off | 0.288 | 0.528 | 0.588 | 0.390 | 1,000 |
| faiss  | on  | 0.285 | 0.462 | 0.546 | 0.363 | 1,000 |
| faiss  | off | 0.261 | 0.425 | 0.503 | 0.335 | 1,000 |
| hybrid | on  | 0.286 | 0.505 | 0.587 | 0.377 | 1,000 |
| hybrid | off | 0.287 | 0.504 | 0.585 | 0.374 | 1,000 |

**Interpretation (per the pre-registered branches in the v0.4.0 design):** with
the scale bug fixed, the cross-encoder **helps** — +1.5pt top-1 on bm25s, +2.4pt
on faiss, neutral on hybrid. The v0.3.x conclusion "the reranker hurts" was an
artifact of the blending defect, not a property of the model. Retrieval quality
is now the ceiling; the domain-reranker swap (MedCPT/BioLORD) remains a
worthwhile v0.4.x experiment (`--reranker-model`) but is no longer a blocker.

This shape is consistent with the published medical-IR literature: dense retrieval shines on noisy free-text queries (clinical notes, patient-described symptoms) where lexical overlap is low. On structured code-to-code tasks like this one, lexical retrieval is the right default.

We publish all rows so users can pick the right backend for their actual data, not just the one that wins this specific benchmark.

Reproduce any row:

```bash
portiere benchmark athena-icd-snomed --backend bm25s  --athena-dir /path/to/athena
portiere benchmark athena-icd-snomed --backend faiss  --athena-dir /path/to/athena
portiere benchmark athena-icd-snomed --backend hybrid --athena-dir /path/to/athena
# add --no-reranker to any of the above for the ablation rows

# USAGI baseline: no published JAR supports headless batch mode — use the
# UI-export bridge (next section).
```

Differences within ±1% are expected (LLM sampling, BM25 ties, FAISS index re-build).

### USAGI baseline (UI-export bridge)

USAGI is OHDSI's official Java mapping tool — TF-IDF over Athena concept names + manual review UI. It is the canonical baseline any ML-driven mapper must clear.

**No published USAGI release (≤ v1.4.3) has a headless batch mode** — the JAR launches the review UI (`--backend usagi` therefore reports the backend as unavailable). The published USAGI row was produced with the reproducible **UI-export bridge**:

```bash
# 1. Deterministic gold-set input for USAGI's "Import codes" (seed=42, n=1,000)
python3 scripts/usagi_ui_bridge.py gen-input --athena-dir /path/to/athena --out usagi_input.csv

# 2. In the USAGI UI (java -jar Usagi_v1.4.3.jar):
#    build index from the same Athena dir → File > Import codes (source_code /
#    source_name; filter standard concepts + SNOMED) → auto-map, NO hand
#    corrections → File > Export source_to_concept_map → usagi_export.csv

# 3. Score + append the row
python3 scripts/usagi_ui_bridge.py score --athena-dir /path/to/athena \
    --export usagi_export.csv \
    --out src/portiere/benchmarks/athena_icd_snomed/expected_results.json
```

The SHA-pinned JAR download (`scripts/download_usagi.sh`, asset `Usagi_v1.4.3.jar`, sha256 `dbcfe9e2…`) remains in CI as a supply-chain check.

## Methodology

### Test set

- **Source pool:** ICD-10-CM concepts in `CONCEPT.csv` that have at least one `Maps to` row in `CONCEPT_RELATIONSHIP.csv`. (Some ICD-10-CM codes have no Maps-to in Athena and are excluded — there's nothing to score against.)
- **Sampling:** random sample of N=1,000 with `seed=42`. v0.3.0 adds opt-in proportional stratification by Athena domain — pass `--stratify-by domain` to the CLI. Default behavior (uniform random) is unchanged from v0.2.1 so the published rows above remain reproducible.
- **Persistence:** the held-out concept_ids are committed at [`benchmarks/athena_icd_snomed/gold_test_set.csv`](https://github.com/Cuspal/portiere/blob/main/src/portiere/benchmarks/athena_icd_snomed/gold_test_set.csv) — **integer IDs only, no Athena content**, so the file is license-clean to ship.
- **Generation:** [`scripts/build_benchmark_test_set.py`](https://github.com/Cuspal/portiere/blob/main/scripts/build_benchmark_test_set.py) regenerates the test set deterministically from any Athena export. Run once at release-prep time and commit the output.

### Knowledge layer

The runner builds an index from the rest of Athena (excluding the test concepts) over **SNOMED standard concepts only**. v0.3.0 supports three retrieval backends, selected via `--backend`:

- `bm25s` — pure-Python BM25s, no embedding model, no GPU. Fastest to build.
- `faiss` — SapBERT (`cambridgeltl/SapBERT-from-PubMedBERT-fulltext`) embeddings indexed in FAISS.
- `hybrid` — both indexes, results combined via Reciprocal Rank Fusion (RRF) with `k=60`.

Embedding indexing requires `sentence-transformers` and ~600 MB of model weights; the BM25 baseline has no such dependency.

### Inference

For each held-out ICD-10-CM concept:

1. Submit `{code: <icd_code>, description: <icd_concept_name>}` to `project.map_concepts()`.
2. Take the top-k SNOMED candidates from the returned `ConceptCandidate` list.
3. Compare against the gold set (every SNOMED concept that ICD code maps to in `CONCEPT_RELATIONSHIP`).

ICD codes can map to multiple SNOMED concepts (one ICD code can be more specific than the available SNOMED, or vice versa). Any of the gold targets at rank ≤ k counts as a hit.

### Metrics

| Metric | Definition |
|---|---|
| **top-1** | Fraction of test cases where the rank-0 prediction is in the gold set |
| **top-5** | Fraction where any of the rank-0..4 predictions is in gold |
| **top-10** | Fraction where any of the rank-0..9 predictions is in gold |
| **MRR** | Mean reciprocal rank: average of `1 / (1 + first_hit_rank)`; 0 if no prediction in top-k matches |

## Reproducing

### 1. Get an Athena export

Free with registration at https://athena.ohdsi.org/. Tick at minimum **SNOMED**, **ICD10CM**, and any others you want indexed. Download the bundle, extract it.

### 2. Install Portiere with the necessary extras

```bash
pip install portiere-health[polars,quality]
```

### 3. Run the benchmark

```bash
portiere benchmark athena-icd-snomed \
    --athena-dir /path/to/extracted/athena \
    --out my_bench_run.json \
    --athena-release-date 2024-09-01
```

Default test set is the committed `gold_test_set.csv`; override with `--test-set` if you've regenerated it from a different Athena release.

### 4. Compare

Diff your `my_bench_run.json` against `benchmarks/athena_icd_snomed/expected_results.json`. Differences within ±1% are expected; anything beyond should be investigated (vocabulary release skew, version drift, etc.).

## Known limitations

- **No baseline comparison.** USAGI (the OHDSI community's official mapping tool) is the obvious comparator; integrating it requires a Java environment and a fresh harness — deferred to v0.3.x.
- **One vocabulary pair only.** ICD-9-CM → SNOMED, RxNorm → ATC, and LOINC-cross-vocab benchmarks are all good additions — planned for v0.3.x / v0.4.0.
- **Dense / hybrid did not lift this benchmark.** As discussed above, the lexical-overlap structure of ICD→SNOMED favors sparse retrieval. The three rows are still useful: they let users see the floor for their backend choice and avoid the trap of assuming "hybrid is always better."
- **Test-set construction defaults to uniformly random.** Stratified sampling is available via `--stratify-by domain` but does not change the published rows (which use uniform sampling for v0.2.1 comparability).

## Why this benchmark

ICD-10-CM → SNOMED is the canonical OMOP concept-mapping task. Every observational research study that ingests US claims data faces it. Choosing this benchmark for v0.2.0 means the published number lands directly on the question every adopter asks first: "how accurate is this on the task I'm going to use it for?"

Synthea round-trip and BC5CDR (the alternatives we considered) are appealing for self-contained reproducibility, but Synthea invites the "synthetic data isn't real" critique forever, and BC5CDR is a different shape (mention extraction → MeSH) that doesn't exercise Portiere's actual code path. ICD→SNOMED is the right shape.

## See also

- Spec §4.4 (v0.2.0 release design, maintainer archive) — full benchmark design rationale + alternatives considered
- [`portiere benchmark` CLI reference](../documentations/02-unified-api-reference.md)
- [`benchmarks/athena_icd_snomed/runner.py`](https://github.com/Cuspal/portiere/blob/main/src/portiere/benchmarks/athena_icd_snomed/runner.py) — the harness implementation

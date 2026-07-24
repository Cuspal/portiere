# Benchmark: ICD-10-CM → SNOMED Concept Mapping

Portiere's accuracy on the canonical OMOP concept-mapping task: given an ICD-10-CM source code, predict the standard SNOMED CT concept it should map to. Evaluated against the OHDSI Athena `CONCEPT_RELATIONSHIP` gold standard.

## Headline numbers (v0.3.1 — 4-backend ablation)

| Backend                                       | top-1 | top-5 | top-10 |   MRR | N |
|-----------------------------------------------|------:|------:|-------:|------:|---:|
| **BM25 (sparse only)**                        | **0.288** | **0.528** | **0.588** | **0.390** | 1,000 |
| SapBERT + FAISS (dense only)                  | 0.278 | 0.473 |  0.551 | 0.361 | 1,000 |
| SapBERT + BM25 + FAISS via RRF (hybrid)       | 0.251 | 0.473 |  0.558 | 0.343 | 1,000 |
| USAGI 1.4.4 (OHDSI baseline)                  |  TBD  |  TBD  |   TBD  |  TBD  | 1,000 |

The USAGI row is populated by a manual one-time run against the user's Athena export (see "Reproducing → USAGI baseline" below). v0.3.1 wires the `--backend usagi` plumbing and the dedicated `.github/workflows/usagi-baseline.yml` workflow; the published numbers will land alongside the v0.3.1 release in a follow-up commit.

Athena release: `2026-04-30`. Numbers are the source-of-truth values from `src/portiere/benchmarks/athena_icd_snomed/expected_results.json`.

**Honest result:** BM25 wins. On ICD-10-CM → SNOMED, the gold mapping shares vocabulary with the source — there's strong lexical overlap between an ICD description and its target SNOMED description — so a tuned sparse retriever beats both SapBERT-FAISS and the hybrid RRF combiner.

> **⚠️ v0.4.0 finding — the v0.3.x rows above were measured under a score-scale defect and must be re-measured.**
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
> This mechanism explains the anomaly of hybrid (0.251) scoring below **both**
> of its own inputs — its ranking was fully controlled by
> `cross-encoder/ms-marco-MiniLM-L-6-v2`, a general-purpose *web-search*
> reranker applied to clinical terminology. It also means BM25's win was
> partly an accident of scale: it was *protected* from the mismatched
> reranker rather than blended with it.
>
> v0.4.0 fixes the blend (per-list min-max normalization of the retrieval
> component; regression-tested in `tests/test_reranker_blending.py`) and adds
> `--reranker/--no-reranker` so the reranker's contribution is measurable in
> isolation. All rows below the fix are «RUN-AND-FILL» pending re-measurement —
> see the v0.4.0 measurement runbook (`docs/benchmarks/v0.4.0-measurement-runbook.md`).

### Reranker ablation (v0.4.0 — «RUN-AND-FILL»)

| Backend | Reranker | top-1 | top-5 | top-10 | MRR | N |
|---|---|------:|------:|-------:|----:|---:|
| bm25s  | on  | «RUN-AND-FILL» | | | | 1,000 |
| bm25s  | off | «RUN-AND-FILL» | | | | 1,000 |
| faiss  | on  | «RUN-AND-FILL» | | | | 1,000 |
| faiss  | off | «RUN-AND-FILL» | | | | 1,000 |
| hybrid | on  | «RUN-AND-FILL» | | | | 1,000 |
| hybrid | off | «RUN-AND-FILL» | | | | 1,000 |

The on/off delta per backend attributes the cross-encoder's contribution. The
interpretation branches are pre-registered in
`specs/2026-07-24-v0.4.0-trustworthy-release-design.md` §4 (A2): reranker hurts →
swap to a biomedical cross-encoder; neutral → consider `provider="none"` default;
helps → retrieval is the ceiling and the human-throughput framing leads.

This shape is consistent with the published medical-IR literature: dense retrieval shines on noisy free-text queries (clinical notes, patient-described symptoms) where lexical overlap is low. On structured code-to-code tasks like this one, lexical retrieval is the right default.

We publish all rows so users can pick the right backend for their actual data, not just the one that wins this specific benchmark.

Reproduce any row:

```bash
portiere benchmark athena-icd-snomed --backend bm25s  --athena-dir /path/to/athena
portiere benchmark athena-icd-snomed --backend faiss  --athena-dir /path/to/athena
portiere benchmark athena-icd-snomed --backend hybrid --athena-dir /path/to/athena

# USAGI baseline (Java 17 + USAGI_JAR env var required)
USAGI_JAR=./vendor/usagi.jar portiere benchmark athena-icd-snomed \
    --backend usagi --athena-dir /path/to/athena
```

Differences within ±1% are expected (LLM sampling, BM25 ties, FAISS index re-build).

### USAGI baseline (v0.3.1)

USAGI is OHDSI's official Java mapping tool — TF-IDF over Athena concept names + manual review UI. It is the canonical baseline any ML-driven mapper must clear.

Setup:

```bash
# Pin a JAR SHA-256 from https://github.com/OHDSI/Usagi/releases
export USAGI_JAR_SHA256=<sha256-of-the-jar>
bash scripts/download_usagi.sh   # writes ./vendor/usagi.jar
export USAGI_JAR=$PWD/vendor/usagi.jar
```

Then run the baseline backend just like the others — it slots into the same `expected_results.json` `runs[]` array via `append_run_to_expected_results(run, backend="usagi", ...)`.

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

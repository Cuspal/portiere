# Benchmark: ICD-10-CM → SNOMED Concept Mapping

Portiere's accuracy on the canonical OMOP concept-mapping task: given an ICD-10-CM source code, predict the standard SNOMED CT concept it should map to. Evaluated against the OHDSI Athena `CONCEPT_RELATIONSHIP` gold standard.

## Headline numbers

| Metric | Score | N |
|---|---:|---:|
| Top-1 accuracy | **0.288** | 1,000 |
| Top-5 accuracy | **0.528** | 1,000 |
| Top-10 accuracy | **0.528** | 1,000 |
| MRR | **0.38151666666666617** | 1,000 |

Athena release: `2026-04-30`. Numbers above are from `benchmarks/athena_icd_snomed/expected_results.json` — that JSON is the source of truth; this table cites it.

Reproduce on your own Athena export:

```bash
portiere benchmark athena-icd-snomed --athena-dir /path/to/athena
```

Differences within ±1% are expected (LLM sampling, BM25 ties).

## Methodology

### Test set

- **Source pool:** ICD-10-CM concepts in `CONCEPT.csv` that have at least one `Maps to` row in `CONCEPT_RELATIONSHIP.csv`. (Some ICD-10-CM codes have no Maps-to in Athena and are excluded — there's nothing to score against.)
- **Sampling:** random sample of N=1,000 with `seed=42`. Stratified sampling by domain or specificity is a v0.3.0 enhancement.
- **Persistence:** the held-out concept_ids are committed at [`benchmarks/athena_icd_snomed/gold_test_set.csv`](../../benchmarks/athena_icd_snomed/gold_test_set.csv) — **integer IDs only, no Athena content**, so the file is license-clean to ship.
- **Generation:** [`scripts/build_benchmark_test_set.py`](../../scripts/build_benchmark_test_set.py) regenerates the test set deterministically from any Athena export. Run once at release-prep time and commit the output.

### Knowledge layer

The runner builds a BM25s index from the rest of Athena (excluding the test concepts) over **SNOMED standard concepts only**. v0.2.0 uses pure-Python BM25s — no embedding model, no GPU. Future versions can repeat the benchmark with FAISS / hybrid retrieval and publish comparison numbers.

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

- **Test-set construction is uniformly random**, not stratified by domain or specificity. ICD-10-CM has uneven coverage by clinical area (a lot of injury codes, fewer rare disease codes); a stratified sample would give a more domain-balanced view of accuracy. Planned for v0.3.0.
- **No baseline comparison.** USAGI (the OHDSI community's official mapping tool) is the obvious comparator; integrating it requires a Java environment and a fresh harness, deferred to v0.3.0.
- **One vocabulary pair only.** ICD-9-CM → SNOMED, RxNorm → ATC, and LOINC-cross-vocab benchmarks are all good additions for v0.3.0.
- **BM25-only retrieval.** v0.2.0 ships the demo with a pure-lexical baseline. The published numbers are not the maximum Portiere achieves — embedding-based retrieval (SapBERT + FAISS) typically lifts top-1 by 5–15 points. Re-running with `--config-embedding huggingface --config-knowledge faiss` is straightforward; we just don't ship those numbers in v0.2.0.

## Why this benchmark

ICD-10-CM → SNOMED is the canonical OMOP concept-mapping task. Every observational research study that ingests US claims data faces it. Choosing this benchmark for v0.2.0 means the published number lands directly on the question every adopter asks first: "how accurate is this on the task I'm going to use it for?"

Synthea round-trip and BC5CDR (the alternatives we considered) are appealing for self-contained reproducibility, but Synthea invites the "synthetic data isn't real" critique forever, and BC5CDR is a different shape (mention extraction → MeSH) that doesn't exercise Portiere's actual code path. ICD→SNOMED is the right shape.

## See also

- [Spec §4.4](../../specs/2026-04-29-v0.2.0-release-design.md) — full benchmark design rationale + alternatives considered
- [`portiere benchmark` CLI reference](../documentations/02-unified-api-reference.md)
- [`benchmarks/athena_icd_snomed/runner.py`](../../benchmarks/athena_icd_snomed/runner.py) — the harness implementation

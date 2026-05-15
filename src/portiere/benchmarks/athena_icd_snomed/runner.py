"""ICD-10-CM → SNOMED concept-mapping benchmark runner.

Held-out evaluation: a sample of ICD-10-CM concept_ids is removed from
the user's Athena export, Portiere is asked to map each one back to a
standard concept, and the predictions are compared against Athena's
``CONCEPT_RELATIONSHIP`` ``Maps to`` rows.

Reproducibility caveat: outputs are reproducible to within ±1% — LLM
sampling, BM25 tie-breaks, and FAISS thread effects make exact
byte-replay impractical (and the spec doesn't promise it).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

import portiere
from portiere.benchmarks.athena_icd_snomed.sampling import (
    generate_test_ids as _sampling_generate_test_ids,
)
from portiere.benchmarks.athena_icd_snomed.usagi_baseline import (
    run_usagi as _run_usagi_or_raise,
)
from portiere.config import EmbeddingConfig, KnowledgeLayerConfig, PortiereConfig
from portiere.knowledge import build_knowledge_layer


@dataclass
class BenchmarkResult:
    """Aggregate metrics for one benchmark run."""

    n: int
    top_1: float
    top_5: float
    top_10: float
    mrr: float


def compute_metrics(
    predictions: dict[int, list[int]],
    gold: dict[int, set[int]],
) -> BenchmarkResult:
    """Compute top-k accuracy and MRR.

    Args:
        predictions: ``source_concept_id -> ranked list of predicted target IDs``
        gold: ``source_concept_id -> set of acceptable target IDs``
            (ICD codes can map to multiple SNOMED concepts; any match counts).

    A test concept with no entry in ``gold`` is excluded from the
    denominator (it has nothing to evaluate against).
    """
    scored: list[tuple[int, list[int], set[int]]] = []
    for src, preds in predictions.items():
        if gold.get(src):
            scored.append((src, preds, gold[src]))

    n = len(scored)
    if n == 0:
        return BenchmarkResult(n=0, top_1=0.0, top_5=0.0, top_10=0.0, mrr=0.0)

    top_1 = top_5 = top_10 = 0
    rr_sum = 0.0
    for _src, preds, golds in scored:
        first_hit = next(
            (rank for rank, p in enumerate(preds) if p in golds),
            None,
        )
        if first_hit is None:
            continue
        if first_hit < 1:
            top_1 += 1
        if first_hit < 5:
            top_5 += 1
        if first_hit < 10:
            top_10 += 1
        rr_sum += 1.0 / (first_hit + 1)

    return BenchmarkResult(
        n=n,
        top_1=top_1 / n,
        top_5=top_5 / n,
        top_10=top_10 / n,
        mrr=rr_sum / n,
    )


def _load_athena_concept(athena_dir: Path) -> pd.DataFrame:
    return pd.read_csv(athena_dir / "CONCEPT.csv", sep="\t", low_memory=False)


def _load_athena_relationships(athena_dir: Path) -> pd.DataFrame:
    return pd.read_csv(athena_dir / "CONCEPT_RELATIONSHIP.csv", sep="\t", low_memory=False)


def _generate_test_ids(
    concept: pd.DataFrame,
    cr: pd.DataFrame,
    *,
    n: int = 1000,
    seed: int = 42,
    stratify_by: str | None = None,
) -> set[int]:
    return _sampling_generate_test_ids(
        concept, cr, n=n, seed=seed, stratify_by=cast(Literal[None, "domain"], stratify_by)
    )


def run_benchmark(
    athena_dir: str | Path,
    *,
    test_set_path: str | Path | None = None,
    test_set_size: int = 1000,
    k: int = 10,
    backend: str = "hybrid",
    stratify_by: str | None = None,
) -> BenchmarkResult:
    """Run the ICD-10-CM → SNOMED benchmark against a real Athena export.

    Args:
        athena_dir: Path to the user's Athena vocabulary download
            (extracted directory containing ``CONCEPT.csv`` etc.).
        test_set_path: Path to ``gold_test_set.csv`` — a single column
            ``icd10cm_concept_id`` of held-out test concept IDs.
            If ``None``, a deterministic sample is generated in-memory
            from the Athena export (seed=42).
        test_set_size: Size of in-memory test set (only used when
            ``test_set_path`` is None). Default 1000.
        k: Top-k cutoff for retrieval (default 10).

    Returns:
        :class:`BenchmarkResult` with n, top_1, top_5, top_10, mrr.
    """
    athena = Path(athena_dir)
    concept = _load_athena_concept(athena)
    cr = _load_athena_relationships(athena)

    if test_set_path is not None:
        test_ids = set(pd.read_csv(test_set_path)["icd10cm_concept_id"].astype(int))
    else:
        test_ids = _generate_test_ids(concept, cr, n=test_set_size, stratify_by=stratify_by)

    # Gold mappings: ICD source → set of SNOMED standard concepts
    maps_to = cr[(cr["relationship_id"] == "Maps to") & cr["concept_id_1"].isin(test_ids)]
    gold: dict[int, set[int]] = maps_to.groupby("concept_id_1")["concept_id_2"].apply(set).to_dict()

    # For each test concept, ask Portiere to predict the top-k SNOMED
    # standard concepts. We submit each ICD code's source string and
    # description as the input. The map_concepts() flow returns its
    # candidates list, which we treat as the ranked prediction.
    test_concept_rows = concept[concept["concept_id"].isin(test_ids)]

    # USAGI baseline: bypass Portiere's pipeline, route through OHDSI's
    # mapping tool via subprocess. Returns predictions in the same shape.
    if backend == "usagi":
        import os

        input_rows = [
            {
                "concept_id": int(row["concept_id"]),
                "concept_code": str(row["concept_code"]),
                "concept_name": str(row["concept_name"]),
            }
            for _idx, row in test_concept_rows.iterrows()
        ]
        if not input_rows:
            return BenchmarkResult(n=0, top_1=0.0, top_5=0.0, top_10=0.0, mrr=0.0)
        usagi_jar = Path(os.environ.get("USAGI_JAR", "vendor/usagi.jar"))
        usagi_predictions = _run_usagi_or_raise(
            input_rows=input_rows,
            athena_concept_csv=athena / "CONCEPT.csv",
            usagi_jar=usagi_jar,
        )
        return compute_metrics(usagi_predictions, gold)

    # Build a knowledge index from the rest of Athena (excluding test concepts)
    # — for v0.2.0 we use a simple BM25s index over all standard concepts.
    # Use the user's cache dir, never inside the repo (see issue: a
    # 155 MB BM25 index file got committed by accident in v0.2.0
    # release-prep when work_dir was Path(athena).parent / "_bench_index").
    import tempfile

    work_dir = Path(tempfile.mkdtemp(prefix="portiere_bench_athena_icd_snomed_"))
    knowledge_paths = build_knowledge_layer(
        athena_path=str(athena),
        output_path=str(work_dir),
        backend=backend,
        vocabularies=["SNOMED"],
    )

    embedding_cfg = EmbeddingConfig(provider="none") if backend == "bm25s" else EmbeddingConfig()
    config = PortiereConfig(
        local_project_dir=work_dir,
        knowledge_layer=KnowledgeLayerConfig(backend=cast(Any, backend), **knowledge_paths),
        embedding=embedding_cfg,
    )
    project = portiere.init(
        name="bench-icd-snomed",
        target_model="omop_cdm_v5.4",
        vocabularies=["SNOMED"],
        config=config,
    )

    codes_input: list[dict | str] = []
    for _idx, row in test_concept_rows.iterrows():
        codes_input.append(
            {
                "code": str(row["concept_code"]),
                "description": str(row["concept_name"]),
                "count": 1,
            }
        )
    if not codes_input:
        return BenchmarkResult(n=0, top_1=0.0, top_5=0.0, top_10=0.0, mrr=0.0)

    concept_map = project.map_concepts(codes=codes_input)

    # Build predictions: source ICD concept_id → ordered list of predicted SNOMED IDs.
    code_to_id = dict(
        zip(
            test_concept_rows["concept_code"].astype(str),
            test_concept_rows["concept_id"].astype(int),
        )
    )

    predictions: dict[int, list[int]] = {}
    for item in concept_map.items:
        src_id = code_to_id.get(str(item.source_code))
        if src_id is None:
            continue
        ranked = [c.concept_id for c in (item.candidates or [])][:k]
        # Always include the top-1 chosen concept first if not already present
        if item.target_concept_id and item.target_concept_id not in ranked:
            ranked.insert(0, item.target_concept_id)
        predictions[src_id] = ranked

    return compute_metrics(predictions, gold)


def append_run_to_expected_results(
    result: BenchmarkResult,
    *,
    backend: str,
    athena_release_date: str,
    out: str | Path,
) -> None:
    """Append (or replace) a single backend's run row in the multi-run JSON.

    The top-level shape is::

        {"athena_release_date": "...", "runs": [{"backend": "...", ...}, ...]}

    Re-running with the same ``backend`` overwrites that row so that
    per-backend CLI invocations accumulate into one file.
    """
    out = Path(out)
    if out.exists():
        payload = json.loads(out.read_text())
    else:
        payload = {"athena_release_date": athena_release_date, "runs": []}

    payload["athena_release_date"] = athena_release_date
    runs = [r for r in payload.get("runs", []) if r.get("backend") != backend]
    runs.append(
        {
            "backend": backend,
            "n": result.n,
            "top_1": result.top_1,
            "top_5": result.top_5,
            "top_10": result.top_10,
            "mrr": result.mrr,
        }
    )
    payload["runs"] = runs
    out.write_text(json.dumps(payload, indent=2, sort_keys=True))


def write_expected_results(
    result: BenchmarkResult,
    *,
    athena_release_date: str,
    out: str | Path,
) -> None:
    """Write a benchmark snapshot to JSON.

    The JSON is the source of truth for the markdown narrative in
    ``docs/benchmarks/athena-icd-snomed.md`` — the doc cites these
    numbers, not the other way around.
    """
    payload = {"athena_release_date": athena_release_date, **asdict(result)}
    Path(out).write_text(json.dumps(payload, indent=2, sort_keys=True))

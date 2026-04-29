"""ICD-10-CM → SNOMED concept-mapping benchmark (Slice 6).

The OHDSI Athena vocabulary export contains ``CONCEPT_RELATIONSHIP``
rows with ``relationship_id == 'Maps to'`` linking each ICD-10-CM code
to the standard SNOMED concept it represents. We hold out N codes,
ask Portiere to map them, and compare against Athena's gold answer.

Reproducibility notes:

* Held-out test set is committed as ``gold_test_set.csv`` (integer IDs
  only — license-clean, no Athena content).
* Runner takes ``athena_dir`` so the user supplies their own export
  (free with registration at https://athena.ohdsi.org/).
* Numbers we publish in ``expected_results.json`` are reproducible
  within ±1% (LLM nondeterminism).
"""

from benchmarks.athena_icd_snomed.runner import (
    BenchmarkResult,
    compute_metrics,
    run_benchmark,
    write_expected_results,
)

__all__ = [
    "BenchmarkResult",
    "compute_metrics",
    "run_benchmark",
    "write_expected_results",
]

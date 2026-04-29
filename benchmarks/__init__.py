"""Portiere benchmarks — published accuracy numbers on standard tasks.

Each subdirectory implements one benchmark with a runner, a held-out
test set (license-clean: integer concept_ids only), and an
``expected_results.json`` snapshot of published numbers.

Available benchmarks:

* ``athena_icd_snomed`` — ICD-10-CM → SNOMED concept mapping against the
  OHDSI Athena ``CONCEPT_RELATIONSHIP`` gold standard.
"""

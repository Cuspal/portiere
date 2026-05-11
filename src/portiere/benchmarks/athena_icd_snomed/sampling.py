"""Held-out-set generators for the ICD-10-CM → SNOMED benchmark.

Two modes:

* ``stratify_by=None`` — uniform random sample (v0.2.1 behavior, preserved).
* ``stratify_by="domain"`` — proportional per-domain sample, so a benchmark
  with n=1,000 against an Athena release where Conditions are 60% of the
  pool has ~600 Condition codes in the test set.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd


def generate_test_ids(
    concept: pd.DataFrame,
    cr: pd.DataFrame,
    *,
    n: int = 1000,
    seed: int = 42,
    stratify_by: Literal[None, "domain"] = None,
) -> set[int]:
    """Generate a deterministic held-out test set from an Athena export.

    Returns a set of ICD-10-CM ``concept_id`` values that have at least
    one ``Maps to`` SNOMED relationship (so the benchmark has gold to
    score against).
    """
    icd = concept[concept["vocabulary_id"] == "ICD10CM"]
    has_gold = cr[
        (cr["relationship_id"] == "Maps to") & cr["concept_id_1"].isin(icd["concept_id"])
    ]["concept_id_1"].drop_duplicates()
    pool = icd[icd["concept_id"].isin(has_gold)]
    if len(pool) == 0:
        return set()
    if len(pool) <= n:
        return set(pool["concept_id"].astype(int))

    if stratify_by is None:
        sampled = pool["concept_id"].sample(n=n, random_state=seed)
        return set(sampled.astype(int))

    if stratify_by == "domain":
        return _stratified_by_domain(pool, n=n, seed=seed)

    raise ValueError(f"Unsupported stratify_by={stratify_by!r}; expected None or 'domain'.")


def _stratified_by_domain(pool: pd.DataFrame, *, n: int, seed: int) -> set[int]:
    """Proportional stratified sample by ``domain_id``."""
    domain_counts = pool["domain_id"].value_counts()
    total = int(domain_counts.sum())
    sampled_ids: set[int] = set()
    for domain, count in domain_counts.items():
        share = max(1, round(n * (count / total)))
        domain_pool = pool[pool["domain_id"] == domain]
        share = min(share, len(domain_pool))
        sampled = domain_pool["concept_id"].sample(n=share, random_state=seed)
        sampled_ids.update(int(x) for x in sampled)
    # Trim to exactly n (rounding may give +1)
    if len(sampled_ids) > n:
        sampled_ids = set(sorted(sampled_ids)[:n])
    return sampled_ids

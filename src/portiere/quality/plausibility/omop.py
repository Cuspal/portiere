"""OMOP CDM v5.4 plausibility rules implemented in Python.

These checks don't fit the YAML DSL because they require multi-table
joins, aggregates, or derived values (e.g., age from year_of_birth).
"""

from __future__ import annotations

import pandas as pd

from portiere.quality.plausibility.runner import RuleResult


def birth_before_death(person_df: pd.DataFrame) -> RuleResult:
    """Per-row: ``birth_datetime`` ≤ ``death_datetime`` for any person whose
    death record is present.
    """
    rule_id = "birth_before_death"
    if "birth_datetime" not in person_df.columns:
        return RuleResult(
            rule_id,
            "error",
            True,
            0,
            0,
            detail="birth_datetime column not present",
        )
    if "death_datetime" not in person_df.columns:
        return RuleResult(
            rule_id,
            "error",
            True,
            0,
            0,
            detail="death_datetime column not present (acceptable — no deaths to check)",
        )
    pair = person_df[["birth_datetime", "death_datetime"]].dropna()
    if pair.empty:
        return RuleResult(rule_id, "error", True, 0, 0)
    before = pd.to_datetime(pair["birth_datetime"], errors="coerce")
    after = pd.to_datetime(pair["death_datetime"], errors="coerce")
    bad = (before > after) & before.notna() & after.notna()
    return RuleResult(
        rule_id,
        "error",
        passed=int(bad.sum()) == 0,
        total_rows=len(pair),
        failed_count=int(bad.sum()),
    )


def condition_dates_consistent(condition_df: pd.DataFrame) -> RuleResult:
    """Per-row: ``condition_start_date`` ≤ ``condition_end_date`` for any
    condition with both dates set.
    """
    rule_id = "condition_dates_consistent"
    cols = {"condition_start_date", "condition_end_date"}
    if not cols.issubset(condition_df.columns):
        return RuleResult(
            rule_id,
            "warn",
            True,
            0,
            0,
            detail="condition_start_date or condition_end_date not present",
        )
    pair = condition_df[list(cols)].dropna()
    if pair.empty:
        return RuleResult(rule_id, "error", True, 0, 0)
    start = pd.to_datetime(pair["condition_start_date"], errors="coerce")
    end = pd.to_datetime(pair["condition_end_date"], errors="coerce")
    bad = (start > end) & start.notna() & end.notna()
    return RuleResult(
        rule_id,
        "error",
        passed=int(bad.sum()) == 0,
        total_rows=len(pair),
        failed_count=int(bad.sum()),
    )


def concept_id_fk(
    table_df: pd.DataFrame,
    concept_df: pd.DataFrame,
    *,
    columns: list[str],
) -> list[RuleResult]:
    """For each ``*_concept_id`` in ``columns``, every non-null value must
    point to a row in the ``concept`` table (joined on ``concept_id``).

    Returns one :class:`RuleResult` per column.
    """
    import duckdb

    con = duckdb.connect(":memory:")
    con.register("t", table_df)
    con.register("c", concept_df)
    out: list[RuleResult] = []
    for col in columns:
        rule_id = f"concept_id_fk:{col}"
        if col not in table_df.columns:
            out.append(RuleResult(rule_id, "error", True, 0, 0, detail=f"column {col} not present"))
            continue
        miss_sql = (
            f"SELECT COUNT(*) FROM t "
            f"WHERE {col} IS NOT NULL "
            f"AND {col} NOT IN (SELECT concept_id FROM c)"
        )
        total_sql = f"SELECT COUNT(*) FROM t WHERE {col} IS NOT NULL"
        failed = int(con.execute(miss_sql).fetchone()[0])
        total = int(con.execute(total_sql).fetchone()[0])
        out.append(RuleResult(rule_id, "error", failed == 0, total, failed))
    return out


def domain_match(
    table_df: pd.DataFrame,
    concept_df: pd.DataFrame,
    *,
    expected_domain_per_column: dict[str, str],
) -> list[RuleResult]:
    """Every non-null value in column X must reference a concept whose
    ``domain_id`` equals the expected domain.

    Example: ``condition_concept_id`` must reference a concept in the
    ``Condition`` domain.
    """
    import duckdb

    con = duckdb.connect(":memory:")
    con.register("t", table_df)
    con.register("c", concept_df)
    out: list[RuleResult] = []
    for col, expected in expected_domain_per_column.items():
        rule_id = f"domain_match:{col}"
        if col not in table_df.columns:
            out.append(RuleResult(rule_id, "error", True, 0, 0, detail=f"column {col} not present"))
            continue
        bad_sql = (
            f"SELECT COUNT(*) FROM t LEFT JOIN c ON t.{col} = c.concept_id "
            f"WHERE t.{col} IS NOT NULL "
            f"AND (c.domain_id IS NULL OR c.domain_id <> ?)"
        )
        total_sql = f"SELECT COUNT(*) FROM t WHERE {col} IS NOT NULL"
        failed = int(con.execute(bad_sql, [expected]).fetchone()[0])
        total = int(con.execute(total_sql).fetchone()[0])
        out.append(RuleResult(rule_id, "error", failed == 0, total, failed))
    return out


def age_in_range(
    person_df: pd.DataFrame,
    *,
    min_age: int = 0,
    max_age: int = 125,
    reference_year: int = 2026,
) -> RuleResult:
    """Derived age = ``reference_year - year_of_birth`` must be in
    ``[min_age, max_age]``. Implausible ages (e.g., 1850 birth year) flag.
    """
    rule_id = "age_in_range"
    if "year_of_birth" not in person_df.columns:
        return RuleResult(
            rule_id,
            "warn",
            True,
            0,
            0,
            detail="year_of_birth missing",
        )
    s = person_df["year_of_birth"].dropna()
    age = reference_year - s
    bad = (age < min_age) | (age > max_age)
    return RuleResult(
        rule_id,
        "warn",
        passed=int(bad.sum()) == 0,
        total_rows=len(s),
        failed_count=int(bad.sum()),
    )

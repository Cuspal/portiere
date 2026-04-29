"""Plausibility rule runner.

Translates :mod:`portiere.quality.plausibility.dsl` rules into concrete
checks against a pandas ``DataFrame``. Column-level rules
(``range``/``regex``/``enum``/``temporal_order``) run via
:func:`run_column_rule`; cross-table ``fk_exists`` rules run via
:func:`run_fk_rule` and require the referenced table.

For ``fk_exists`` validation, the runner uses DuckDB. DuckDB registers
pandas DataFrames as views with no copy, so this scales cleanly to full
Athena-sized vocabulary tables (~10M concepts). DuckDB is part of the
``portiere-health[quality]`` extra.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from portiere.quality.plausibility.dsl import (
    EnumRule,
    FkExistsRule,
    PlausibilityRule,
    RangeRule,
    RegexRule,
    TemporalOrderRule,
)

if TYPE_CHECKING:
    pass


@dataclass
class RuleResult:
    """Outcome of running a single plausibility rule."""

    rule_id: str
    severity: str
    passed: bool
    total_rows: int
    failed_count: int
    detail: str = ""


def run_column_rule(df: pd.DataFrame, rule: PlausibilityRule) -> RuleResult:
    """Run a column-level rule against a DataFrame.

    Handles ``range``, ``regex``, ``enum``, and ``temporal_order``. Routes
    callers to :func:`run_fk_rule` for ``fk_exists`` (which needs a
    reference table not available here).
    """
    if isinstance(rule, RangeRule):
        return _run_range(df, rule)
    if isinstance(rule, RegexRule):
        return _run_regex(df, rule)
    if isinstance(rule, EnumRule):
        return _run_enum(df, rule)
    if isinstance(rule, TemporalOrderRule):
        return _run_temporal(df, rule)
    if isinstance(rule, FkExistsRule):
        raise TypeError(
            f"run_column_rule does not handle fk_exists rules; use run_fk_rule for rule {rule.id!r}"
        )
    raise TypeError(f"unknown rule type: {type(rule).__name__}")


def run_fk_rule(
    df: pd.DataFrame,
    rule: FkExistsRule,
    *,
    ref_tables: dict[str, pd.DataFrame],
) -> RuleResult:
    """Verify every non-null value in ``df[rule.column]`` exists in
    ``ref_tables[rule.ref_table][rule.ref_column]``.

    Uses DuckDB for the join — handles full-Athena-scale reference tables
    without copying. ``duckdb`` is part of the ``[quality]`` extra.
    """
    if rule.column not in df.columns:
        return RuleResult(
            rule.id,
            rule.severity,
            passed=True,
            total_rows=0,
            failed_count=0,
            detail=f"column {rule.column!r} not in DataFrame (skipped)",
        )
    if rule.ref_table not in ref_tables:
        return RuleResult(
            rule.id,
            rule.severity,
            passed=True,
            total_rows=0,
            failed_count=0,
            detail=f"ref_table {rule.ref_table!r} not provided (skipped)",
        )

    import duckdb

    con = duckdb.connect(":memory:")
    con.register("under_test", df)
    con.register("ref", ref_tables[rule.ref_table])
    miss_sql = (
        f"SELECT COUNT(*) FROM under_test "
        f"WHERE {rule.column} IS NOT NULL "
        f"AND {rule.column} NOT IN (SELECT {rule.ref_column} FROM ref)"
    )
    total_sql = f"SELECT COUNT(*) FROM under_test WHERE {rule.column} IS NOT NULL"
    failed = int(con.execute(miss_sql).fetchone()[0])
    total = int(con.execute(total_sql).fetchone()[0])
    return RuleResult(rule.id, rule.severity, failed == 0, total, failed)


# ── Internal helpers ───────────────────────────────────────────────


def _column_missing_result(rule: PlausibilityRule, column: str, n_rows: int) -> RuleResult:
    """Skip the rule (passed=True) when the referenced column is absent.

    Optional columns are common in healthcare (e.g., a hospital that doesn't
    collect ``death_date``). A missing column is not a violation; completeness
    checks already cover required-column presence.
    """
    return RuleResult(
        rule.id,
        rule.severity,
        passed=True,
        total_rows=0,
        failed_count=0,
        detail=f"column {column!r} not in DataFrame (skipped)",
    )


def _run_range(df: pd.DataFrame, rule: RangeRule) -> RuleResult:
    if rule.column not in df.columns:
        return _column_missing_result(rule, rule.column, len(df))
    s = df[rule.column].dropna()
    mask = pd.Series([True] * len(s), index=s.index)
    if rule.min is not None:
        mask &= s >= rule.min
    if rule.max is not None:
        mask &= s <= rule.max
    failed = int((~mask).sum())
    return RuleResult(rule.id, rule.severity, failed == 0, len(s), failed)


def _run_regex(df: pd.DataFrame, rule: RegexRule) -> RuleResult:
    if rule.column not in df.columns:
        return _column_missing_result(rule, rule.column, len(df))
    s = df[rule.column].dropna().astype(str)
    mask = s.str.match(rule.pattern, na=False)
    failed = int((~mask).sum())
    return RuleResult(rule.id, rule.severity, failed == 0, len(s), failed)


def _run_enum(df: pd.DataFrame, rule: EnumRule) -> RuleResult:
    if rule.column not in df.columns:
        return _column_missing_result(rule, rule.column, len(df))
    s = df[rule.column].dropna()
    mask = s.isin(rule.values)
    failed = int((~mask).sum())
    return RuleResult(rule.id, rule.severity, failed == 0, len(s), failed)


def _run_temporal(df: pd.DataFrame, rule: TemporalOrderRule) -> RuleResult:
    for col in (rule.before, rule.after):
        if col not in df.columns:
            return _column_missing_result(rule, col, len(df))
    pair = df[[rule.before, rule.after]].dropna()
    before = pd.to_datetime(pair[rule.before], errors="coerce")
    after = pd.to_datetime(pair[rule.after], errors="coerce")
    valid_pair = before.notna() & after.notna()
    bad = valid_pair & (before > after)
    return RuleResult(
        rule.id,
        rule.severity,
        passed=int(bad.sum()) == 0,
        total_rows=int(valid_pair.sum()),
        failed_count=int(bad.sum()),
    )

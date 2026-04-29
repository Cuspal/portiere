"""Tests for the plausibility rule runner (Slice 3 Tasks 3.3 + 3.4)."""

from __future__ import annotations

import pandas as pd
import pytest


def _rule(d):
    from portiere.quality.plausibility.dsl import parse_rule

    return parse_rule(d)


class TestRangeRule:
    def test_passes_when_all_in_range(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"age": [10, 50, 100, 124]})
        rule = _rule({"id": "age", "type": "range", "column": "age", "min": 0, "max": 125})
        result = run_column_rule(df, rule)
        assert result.passed is True
        assert result.failed_count == 0
        assert result.total_rows == 4

    def test_fails_when_value_out_of_range(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"age": [10, 50, 200]})
        rule = _rule({"id": "age", "type": "range", "column": "age", "min": 0, "max": 125})
        result = run_column_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 1

    def test_min_only_constraint(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"qty": [-1, 0, 5]})
        rule = _rule({"id": "qty_nonneg", "type": "range", "column": "qty", "min": 0})
        result = run_column_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 1

    def test_missing_column_is_skipped(self):
        """Missing column → skipped (passed=True with detail).

        Optional columns are common in healthcare; treating them as
        violations would generate noise. Completeness checks cover
        required-column presence.
        """
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"other": [1, 2, 3]})
        rule = _rule({"id": "age", "type": "range", "column": "age", "min": 0, "max": 125})
        result = run_column_rule(df, rule)
        assert result.passed is True
        assert "skipped" in result.detail
        assert result.total_rows == 0
        assert result.failed_count == 0

    def test_nulls_are_excluded(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"age": [10, None, 50]})
        rule = _rule({"id": "age", "type": "range", "column": "age", "min": 0, "max": 125})
        result = run_column_rule(df, rule)
        assert result.passed is True
        assert result.total_rows == 2  # null skipped


class TestRegexRule:
    def test_passes_when_all_match(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"icd": ["E11.9", "I10", "R73.03"]})
        rule = _rule(
            {
                "id": "icd_format",
                "type": "regex",
                "column": "icd",
                "pattern": r"^[A-Z]\d{1,3}(\.\d+)?$",
            }
        )
        result = run_column_rule(df, rule)
        assert result.passed is True

    def test_fails_when_value_does_not_match(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"icd": ["E11.9", "BAD-CODE", "R73.03"]})
        rule = _rule(
            {
                "id": "icd_format",
                "type": "regex",
                "column": "icd",
                "pattern": r"^[A-Z]\d{1,3}(\.\d+)?$",
            }
        )
        result = run_column_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 1


class TestEnumRule:
    def test_passes(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"gender": ["M", "F", "U"]})
        rule = _rule(
            {"id": "gender", "type": "enum", "column": "gender", "values": ["M", "F", "U"]}
        )
        result = run_column_rule(df, rule)
        assert result.passed is True

    def test_fails_on_unknown_value(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"gender": ["M", "F", "X"]})
        rule = _rule(
            {"id": "gender", "type": "enum", "column": "gender", "values": ["M", "F", "U"]}
        )
        result = run_column_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 1


class TestTemporalOrderRule:
    def test_passes_when_all_ordered(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame(
            {
                "birth_datetime": ["1980-01-01", "1990-05-10"],
                "death_datetime": ["2020-01-01", "2050-05-10"],
            }
        )
        rule = _rule(
            {
                "id": "bbd",
                "type": "temporal_order",
                "before": "birth_datetime",
                "after": "death_datetime",
            }
        )
        result = run_column_rule(df, rule)
        assert result.passed is True

    def test_fails_when_after_precedes_before(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame(
            {
                "birth_datetime": ["1990-01-01"],
                "death_datetime": ["1985-01-01"],  # death before birth
            }
        )
        rule = _rule(
            {
                "id": "bbd",
                "type": "temporal_order",
                "before": "birth_datetime",
                "after": "death_datetime",
            }
        )
        result = run_column_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 1

    def test_skips_rows_with_null_after(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame(
            {
                "birth_datetime": ["1980-01-01", "1990-05-10"],
                "death_datetime": ["2020-01-01", None],  # alive
            }
        )
        rule = _rule(
            {
                "id": "bbd",
                "type": "temporal_order",
                "before": "birth_datetime",
                "after": "death_datetime",
            }
        )
        result = run_column_rule(df, rule)
        assert result.passed is True
        assert result.total_rows == 1  # only the row with both present


class TestRunColumnRuleRejectsFkRule:
    def test_fk_rule_routes_to_run_fk_rule(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"x": [1]})
        rule = _rule(
            {
                "id": "fk",
                "type": "fk_exists",
                "column": "x",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        with pytest.raises(TypeError, match="run_fk_rule"):
            run_column_rule(df, rule)


class TestFkExistsRule:
    def test_passes_when_all_values_in_ref(self):
        from portiere.quality.plausibility.runner import run_fk_rule

        ref = pd.DataFrame({"concept_id": [1, 2, 3, 4, 5]})
        df = pd.DataFrame({"condition_concept_id": [1, 3, 5]})
        rule = _rule(
            {
                "id": "fk",
                "type": "fk_exists",
                "column": "condition_concept_id",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        result = run_fk_rule(df, rule, ref_tables={"concept": ref})
        assert result.passed is True
        assert result.failed_count == 0

    def test_fails_when_value_not_in_ref(self):
        from portiere.quality.plausibility.runner import run_fk_rule

        ref = pd.DataFrame({"concept_id": [1, 2, 3]})
        df = pd.DataFrame({"condition_concept_id": [1, 99, 100]})
        rule = _rule(
            {
                "id": "fk",
                "type": "fk_exists",
                "column": "condition_concept_id",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        result = run_fk_rule(df, rule, ref_tables={"concept": ref})
        assert result.passed is False
        assert result.failed_count == 2

    def test_nulls_are_excluded(self):
        from portiere.quality.plausibility.runner import run_fk_rule

        ref = pd.DataFrame({"concept_id": [1, 2, 3]})
        df = pd.DataFrame({"x": [1, None, 3]})
        rule = _rule(
            {
                "id": "fk",
                "type": "fk_exists",
                "column": "x",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        result = run_fk_rule(df, rule, ref_tables={"concept": ref})
        assert result.passed is True
        assert result.total_rows == 2  # null skipped

    def test_missing_ref_table_is_skipped(self):
        """No reference table provided → skipped (passed=True)."""
        from portiere.quality.plausibility.runner import run_fk_rule

        df = pd.DataFrame({"x": [1, 2, 3]})
        rule = _rule(
            {
                "id": "fk",
                "type": "fk_exists",
                "column": "x",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        result = run_fk_rule(df, rule, ref_tables={})
        assert result.passed is True
        assert "skipped" in result.detail

    def test_missing_column_is_skipped(self):
        from portiere.quality.plausibility.runner import run_fk_rule

        ref = pd.DataFrame({"concept_id": [1]})
        df = pd.DataFrame({"other": [1]})
        rule = _rule(
            {
                "id": "fk",
                "type": "fk_exists",
                "column": "x",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        result = run_fk_rule(df, rule, ref_tables={"concept": ref})
        assert result.passed is True
        assert "skipped" in result.detail


class TestRuleResultShape:
    def test_severity_reflected_from_rule(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"age": [50]})
        rule = _rule(
            {
                "id": "age",
                "type": "range",
                "column": "age",
                "min": 0,
                "max": 125,
                "severity": "warn",
            }
        )
        result = run_column_rule(df, rule)
        assert result.severity == "warn"

    def test_rule_id_propagated(self):
        from portiere.quality.plausibility.runner import run_column_rule

        df = pd.DataFrame({"age": [50]})
        rule = _rule({"id": "my_rule_id", "type": "range", "column": "age", "min": 0})
        result = run_column_rule(df, rule)
        assert result.rule_id == "my_rule_id"

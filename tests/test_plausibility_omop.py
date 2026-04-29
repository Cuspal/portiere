"""Tests for OMOP-specific plausibility Python rules (Slice 3 Task 3.5).

These rules are cross-table or aggregate checks that don't fit the
YAML DSL grammar.
"""

from __future__ import annotations

import pandas as pd


class TestBirthBeforeDeath:
    def test_passes_when_birth_before_death(self):
        from portiere.quality.plausibility.omop import birth_before_death

        df = pd.DataFrame(
            {
                "birth_datetime": ["1980-01-01", "1990-05-10"],
                "death_datetime": ["2020-01-01", "2050-05-10"],
            }
        )
        r = birth_before_death(df)
        assert r.passed is True

    def test_fails_when_death_precedes_birth(self):
        from portiere.quality.plausibility.omop import birth_before_death

        df = pd.DataFrame({"birth_datetime": ["1990-01-01"], "death_datetime": ["1985-01-01"]})
        r = birth_before_death(df)
        assert r.passed is False
        assert r.failed_count == 1

    def test_skips_rows_with_no_death(self):
        from portiere.quality.plausibility.omop import birth_before_death

        df = pd.DataFrame(
            {
                "birth_datetime": ["1980-01-01", "1990-05-10"],
                "death_datetime": ["2020-01-01", None],
            }
        )
        r = birth_before_death(df)
        assert r.passed is True
        assert r.total_rows == 1

    def test_passes_trivially_when_death_column_absent(self):
        """No death column means no death records to validate."""
        from portiere.quality.plausibility.omop import birth_before_death

        df = pd.DataFrame({"birth_datetime": ["1980-01-01"]})
        r = birth_before_death(df)
        assert r.passed is True
        assert "not present" in r.detail


class TestConditionDatesConsistent:
    def test_passes(self):
        from portiere.quality.plausibility.omop import condition_dates_consistent

        df = pd.DataFrame(
            {
                "condition_start_date": ["2020-01-01", "2021-06-01"],
                "condition_end_date": ["2020-02-01", "2021-07-01"],
            }
        )
        r = condition_dates_consistent(df)
        assert r.passed is True

    def test_fails_when_end_precedes_start(self):
        from portiere.quality.plausibility.omop import condition_dates_consistent

        df = pd.DataFrame(
            {
                "condition_start_date": ["2020-06-01"],
                "condition_end_date": ["2020-01-01"],
            }
        )
        r = condition_dates_consistent(df)
        assert r.passed is False
        assert r.failed_count == 1


class TestConceptIdFk:
    def test_all_present(self):
        from portiere.quality.plausibility.omop import concept_id_fk

        concept = pd.DataFrame({"concept_id": [1, 2, 3, 4, 5]})
        cond = pd.DataFrame({"condition_concept_id": [1, 3, 5]})
        results = concept_id_fk(cond, concept, columns=["condition_concept_id"])
        assert len(results) == 1
        assert results[0].passed is True

    def test_some_missing(self):
        from portiere.quality.plausibility.omop import concept_id_fk

        concept = pd.DataFrame({"concept_id": [1, 2, 3]})
        cond = pd.DataFrame({"condition_concept_id": [1, 99, 100]})
        results = concept_id_fk(cond, concept, columns=["condition_concept_id"])
        assert results[0].passed is False
        assert results[0].failed_count == 2

    def test_multiple_columns(self):
        from portiere.quality.plausibility.omop import concept_id_fk

        concept = pd.DataFrame({"concept_id": [1, 2, 3]})
        cond = pd.DataFrame(
            {
                "condition_concept_id": [1, 2],
                "condition_type_concept_id": [3, 99],
            }
        )
        results = concept_id_fk(
            cond,
            concept,
            columns=["condition_concept_id", "condition_type_concept_id"],
        )
        assert len(results) == 2
        assert results[0].passed is True
        assert results[1].passed is False


class TestDomainMatch:
    def test_matches_domain(self):
        from portiere.quality.plausibility.omop import domain_match

        concept = pd.DataFrame(
            {
                "concept_id": [1, 2, 3],
                "domain_id": ["Condition", "Condition", "Drug"],
            }
        )
        cond = pd.DataFrame({"condition_concept_id": [1, 2]})  # both Condition
        r = domain_match(
            cond,
            concept,
            expected_domain_per_column={"condition_concept_id": "Condition"},
        )
        assert r[0].passed is True

    def test_wrong_domain_fails(self):
        from portiere.quality.plausibility.omop import domain_match

        concept = pd.DataFrame(
            {"concept_id": [1, 2, 3], "domain_id": ["Condition", "Drug", "Condition"]}
        )
        cond = pd.DataFrame({"condition_concept_id": [1, 2]})  # 2 is in Drug, wrong
        r = domain_match(
            cond,
            concept,
            expected_domain_per_column={"condition_concept_id": "Condition"},
        )
        assert r[0].passed is False
        assert r[0].failed_count == 1


class TestAgeInRange:
    def test_passes(self):
        from portiere.quality.plausibility.omop import age_in_range

        df = pd.DataFrame({"year_of_birth": [1950, 1980, 2010]})
        r = age_in_range(df, reference_year=2026)
        assert r.passed is True

    def test_fails_for_implausibly_old(self):
        from portiere.quality.plausibility.omop import age_in_range

        df = pd.DataFrame({"year_of_birth": [1850, 1980]})  # 1850 → 176 years
        r = age_in_range(df, reference_year=2026)
        assert r.passed is False
        assert r.failed_count == 1

    def test_passes_when_year_missing(self):
        from portiere.quality.plausibility.omop import age_in_range

        df = pd.DataFrame({"other": [1, 2]})
        r = age_in_range(df, reference_year=2026)
        assert r.passed is True
        assert "year_of_birth missing" in r.detail

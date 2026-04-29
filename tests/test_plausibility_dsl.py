"""Tests for the plausibility YAML DSL parser (Slice 3 Task 3.1)."""

from __future__ import annotations

import pytest


class TestParseRule:
    """parse_rule() converts a single YAML rule dict to a typed Pydantic model."""

    def test_parse_range_rule(self):
        from portiere.quality.plausibility.dsl import RangeRule, parse_rule

        rule = parse_rule(
            {"id": "age_in_range", "type": "range", "column": "age", "min": 0, "max": 125}
        )
        assert isinstance(rule, RangeRule)
        assert rule.id == "age_in_range"
        assert rule.column == "age"
        assert rule.min == 0
        assert rule.max == 125
        # default severity
        assert rule.severity == "error"

    def test_parse_range_rule_min_only(self):
        from portiere.quality.plausibility.dsl import parse_rule

        rule = parse_rule({"id": "non_negative", "type": "range", "column": "qty", "min": 0})
        assert rule.min == 0
        assert rule.max is None

    def test_parse_regex_rule(self):
        from portiere.quality.plausibility.dsl import RegexRule, parse_rule

        rule = parse_rule(
            {
                "id": "icd10_format",
                "type": "regex",
                "column": "icd_code",
                "pattern": r"^[A-Z]\d{2}(\.\d+)?$",
            }
        )
        assert isinstance(rule, RegexRule)
        assert rule.column == "icd_code"
        assert "[A-Z]" in rule.pattern

    def test_parse_enum_rule(self):
        from portiere.quality.plausibility.dsl import EnumRule, parse_rule

        rule = parse_rule(
            {"id": "gender_enum", "type": "enum", "column": "gender", "values": ["M", "F", "U"]}
        )
        assert isinstance(rule, EnumRule)
        assert rule.values == ["M", "F", "U"]

    def test_parse_temporal_order_rule(self):
        from portiere.quality.plausibility.dsl import TemporalOrderRule, parse_rule

        rule = parse_rule(
            {
                "id": "birth_before_death",
                "type": "temporal_order",
                "before": "birth_datetime",
                "after": "death_datetime",
            }
        )
        assert isinstance(rule, TemporalOrderRule)
        assert rule.before == "birth_datetime"
        assert rule.after == "death_datetime"

    def test_parse_fk_exists_rule(self):
        from portiere.quality.plausibility.dsl import FkExistsRule, parse_rule

        rule = parse_rule(
            {
                "id": "concept_fk",
                "type": "fk_exists",
                "column": "condition_concept_id",
                "ref_table": "concept",
                "ref_column": "concept_id",
            }
        )
        assert isinstance(rule, FkExistsRule)
        assert rule.column == "condition_concept_id"
        assert rule.ref_table == "concept"
        assert rule.ref_column == "concept_id"

    def test_parse_unknown_type_raises(self):
        from portiere.quality.plausibility.dsl import parse_rule

        with pytest.raises(ValueError, match="unknown plausibility rule type"):
            parse_rule({"id": "x", "type": "unknown_type", "column": "y"})

    def test_severity_warn_explicit(self):
        from portiere.quality.plausibility.dsl import parse_rule

        rule = parse_rule(
            {
                "id": "loose_age",
                "type": "range",
                "column": "age",
                "min": 0,
                "max": 125,
                "severity": "warn",
            }
        )
        assert rule.severity == "warn"

    def test_severity_invalid_raises(self):
        from portiere.quality.plausibility.dsl import parse_rule

        with pytest.raises(ValueError):
            parse_rule(
                {
                    "id": "x",
                    "type": "range",
                    "column": "y",
                    "min": 0,
                    "max": 1,
                    "severity": "info",  # only error|warn allowed
                }
            )

    def test_extra_fields_forbidden(self):
        from portiere.quality.plausibility.dsl import parse_rule

        with pytest.raises(ValueError):
            parse_rule(
                {
                    "id": "x",
                    "type": "range",
                    "column": "y",
                    "min": 0,
                    "max": 1,
                    "spurious_field": "should not be allowed",
                }
            )


class TestParseRules:
    """parse_rules() handles a list of rule dicts."""

    def test_parse_rules_empty(self):
        from portiere.quality.plausibility.dsl import parse_rules

        assert parse_rules([]) == []

    def test_parse_rules_mixed_types(self):
        from portiere.quality.plausibility.dsl import (
            EnumRule,
            FkExistsRule,
            RangeRule,
            parse_rules,
        )

        rules = parse_rules(
            [
                {"id": "r1", "type": "range", "column": "age", "min": 0, "max": 125},
                {"id": "r2", "type": "enum", "column": "gender", "values": ["M", "F"]},
                {
                    "id": "r3",
                    "type": "fk_exists",
                    "column": "concept_id",
                    "ref_table": "concept",
                    "ref_column": "concept_id",
                },
            ]
        )
        assert len(rules) == 3
        assert isinstance(rules[0], RangeRule)
        assert isinstance(rules[1], EnumRule)
        assert isinstance(rules[2], FkExistsRule)


class TestYAMLTargetModelPlausibility:
    """YAMLTargetModel.get_plausibility_rules(entity) (Slice 3 Task 3.2)."""

    def test_returns_empty_for_entity_without_block(self, tmp_path):
        from portiere.standards import YAMLTargetModel

        yaml_text = """
name: test_std_no_plausibility
version: "1.0"
standard_type: relational
entities:
  thing:
    fields:
      id: {type: integer, required: true, description: id}
"""
        p = tmp_path / "std.yaml"
        p.write_text(yaml_text)
        model = YAMLTargetModel(p)
        assert model.get_plausibility_rules("thing") == []

    def test_returns_parsed_rules(self, tmp_path):
        from portiere.quality.plausibility.dsl import TemporalOrderRule
        from portiere.standards import YAMLTargetModel

        yaml_text = """
name: test_std_with_plausibility
version: "1.0"
standard_type: relational
entities:
  person:
    fields:
      birth_datetime: {type: datetime, required: true, description: bd}
      death_datetime: {type: datetime, required: false, description: dd}
    plausibility:
      - id: birth_before_death
        type: temporal_order
        before: birth_datetime
        after: death_datetime
        severity: error
"""
        p = tmp_path / "std.yaml"
        p.write_text(yaml_text)
        model = YAMLTargetModel(p)
        rules = model.get_plausibility_rules("person")
        assert len(rules) == 1
        assert isinstance(rules[0], TemporalOrderRule)
        assert rules[0].id == "birth_before_death"

    def test_returns_empty_for_missing_entity(self, tmp_path):
        from portiere.standards import YAMLTargetModel

        yaml_text = """
name: test_std
version: "1.0"
standard_type: relational
entities:
  thing:
    fields:
      id: {type: integer, required: true, description: id}
"""
        p = tmp_path / "std.yaml"
        p.write_text(yaml_text)
        model = YAMLTargetModel(p)
        assert model.get_plausibility_rules("nonexistent") == []


class TestBuiltInStandardsPlausibility:
    """Slice 3 Task 3.7 — built-in standards declare plausibility rules."""

    def test_omop_person_has_rules(self):
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        rules = m.get_plausibility_rules("person")
        assert len(rules) >= 1, "OMOP person should declare at least 1 plausibility rule"

    def test_omop_condition_occurrence_has_temporal_rule(self):
        from portiere.quality.plausibility.dsl import TemporalOrderRule
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        rules = m.get_plausibility_rules("condition_occurrence")
        assert any(isinstance(r, TemporalOrderRule) for r in rules), (
            "condition_occurrence should declare a temporal_order rule (start_date <= end_date)"
        )

    def test_omop_condition_occurrence_has_fk_rule(self):
        from portiere.quality.plausibility.dsl import FkExistsRule
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("omop_cdm_v5.4")
        rules = m.get_plausibility_rules("condition_occurrence")
        assert any(isinstance(r, FkExistsRule) for r in rules), (
            "condition_occurrence should declare an fk_exists rule "
            "(condition_concept_id -> concept)"
        )

    def test_fhir_observation_has_status_enum(self):
        from portiere.quality.plausibility.dsl import EnumRule
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("fhir_r4")
        rules = m.get_plausibility_rules("Observation")
        enum_rules = [r for r in rules if isinstance(r, EnumRule) and r.column == "status"]
        assert enum_rules, "Observation should declare a status enum rule"

    def test_fhir_medication_request_has_intent_enum(self):
        from portiere.quality.plausibility.dsl import EnumRule
        from portiere.standards import YAMLTargetModel

        m = YAMLTargetModel.from_name("fhir_r4")
        rules = m.get_plausibility_rules("MedicationRequest")
        enum_rules = [r for r in rules if isinstance(r, EnumRule) and r.column == "intent"]
        assert enum_rules, "MedicationRequest should declare an intent enum rule"

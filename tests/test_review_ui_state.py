"""Tests for the Streamlit review UI state helpers (Slice 4, v0.3.1).

Tests cover only the pure load/save/decision helpers — not the Streamlit
UI itself. The UI is exercised by manual smoke test (see
docs/mapping-review-ui.md once Slice 6 lands).
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml


def _make_project_with_schema_mapping(project_dir: Path) -> Path:
    """Materialize the directory layout the LocalBackend writes."""
    schema_dir = project_dir / "schema_mappings"
    schema_dir.mkdir(parents=True, exist_ok=True)
    items = [
        {
            "source_table": "patients",
            "source_column": "patient_id",
            "target_table": "person",
            "target_column": "person_id",
            "confidence": 0.99,
            "status": "auto_accepted",
            "candidates": [],
        },
        {
            "source_table": "patients",
            "source_column": "dob",
            "target_table": "person",
            "target_column": "birth_datetime",
            "confidence": 0.72,
            "status": "needs_review",
            "candidates": [
                {"target_table": "person", "target_column": "year_of_birth", "score": 0.65}
            ],
        },
    ]
    (schema_dir / "schema_mapping.yaml").write_text(yaml.dump(items))
    return schema_dir


class TestLoadSchemaMapping:
    def test_load_returns_mapping_with_items(self, tmp_path):
        from portiere.review_ui.state import load_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = load_schema_mapping(tmp_path)
        assert len(mapping.items) == 2
        assert mapping.items[0].source_column == "patient_id"
        assert mapping.items[0].target_column == "person_id"

    def test_load_returns_empty_when_no_mapping_file(self, tmp_path):
        from portiere.review_ui.state import load_schema_mapping

        # Empty project dir — no schema_mappings/ subdir
        mapping = load_schema_mapping(tmp_path)
        assert mapping.items == []

    def test_load_prefers_reviewed_when_present(self, tmp_path):
        """If schema_mapping_reviewed.json exists, it takes precedence."""
        from portiere.review_ui.state import load_schema_mapping

        schema_dir = _make_project_with_schema_mapping(tmp_path)
        reviewed = {
            "items": [
                {
                    "source_table": "patients",
                    "source_column": "patient_id",
                    "target_table": "person",
                    "target_column": "person_id",
                    "confidence": 0.99,
                    "status": "approved",  # human-edited
                    "candidates": [],
                }
            ]
        }
        (schema_dir / "schema_mapping_reviewed.json").write_text(json.dumps(reviewed))

        mapping = load_schema_mapping(tmp_path)
        assert len(mapping.items) == 1
        assert mapping.items[0].status.value == "approved"


class TestSaveReviewedMapping:
    def test_save_writes_to_reviewed_json(self, tmp_path):
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem
        from portiere.review_ui.state import save_reviewed_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_table="patients",
                    source_column="patient_id",
                    target_table="person",
                    target_column="person_id",
                    confidence=0.99,
                    status="approved",
                )
            ]
        )
        save_reviewed_schema_mapping(mapping, tmp_path)

        reviewed_path = tmp_path / "schema_mappings" / "schema_mapping_reviewed.json"
        assert reviewed_path.exists()
        data = json.loads(reviewed_path.read_text())
        assert "items" in data
        assert data["items"][0]["status"] == "approved"

    def test_save_does_not_modify_original_yaml(self, tmp_path):
        """The original schema_mapping.yaml is never touched."""
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem
        from portiere.review_ui.state import save_reviewed_schema_mapping

        schema_dir = _make_project_with_schema_mapping(tmp_path)
        original_yaml = (schema_dir / "schema_mapping.yaml").read_text()

        mapping = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_table="patients",
                    source_column="patient_id",
                    target_table="person",
                    target_column="person_id",
                    confidence=0.99,
                    status="rejected",
                )
            ]
        )
        save_reviewed_schema_mapping(mapping, tmp_path)
        assert (schema_dir / "schema_mapping.yaml").read_text() == original_yaml


class TestApplyUserDecision:
    def test_apply_approve_sets_status_to_approved(self, tmp_path):
        from portiere.review_ui.state import apply_user_decision, load_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = load_schema_mapping(tmp_path)

        updated = apply_user_decision(mapping, index=1, decision="approve")
        assert updated.items[1].status.value == "approved"
        # Item 0 untouched
        assert updated.items[0].status.value == "auto_accepted"

    def test_apply_reject_sets_status_to_rejected(self, tmp_path):
        from portiere.review_ui.state import apply_user_decision, load_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = load_schema_mapping(tmp_path)

        updated = apply_user_decision(mapping, index=1, decision="reject")
        assert updated.items[1].status.value == "rejected"

    def test_apply_override_sets_target_and_status(self, tmp_path):
        from portiere.review_ui.state import apply_user_decision, load_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = load_schema_mapping(tmp_path)

        updated = apply_user_decision(
            mapping,
            index=1,
            decision="override",
            target_table="person",
            target_column="year_of_birth",
        )
        assert updated.items[1].status.value == "overridden"
        assert updated.items[1].override_target_column == "year_of_birth"
        assert updated.items[1].effective_target_column == "year_of_birth"

    def test_apply_invalid_decision_raises(self, tmp_path):
        import pytest

        from portiere.review_ui.state import apply_user_decision, load_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = load_schema_mapping(tmp_path)
        with pytest.raises(ValueError, match=r"[Uu]nknown decision"):
            apply_user_decision(mapping, index=0, decision="zoinks")

    def test_apply_out_of_range_index_raises(self, tmp_path):
        import pytest

        from portiere.review_ui.state import apply_user_decision, load_schema_mapping

        _make_project_with_schema_mapping(tmp_path)
        mapping = load_schema_mapping(tmp_path)
        with pytest.raises(IndexError):
            apply_user_decision(mapping, index=99, decision="approve")


# ── Concept mapping (Slice 5) ─────────────────────────────────────


def _make_project_with_concept_mapping(project_dir: Path) -> Path:
    """Materialize the directory layout for concept mappings."""
    cm_dir = project_dir / "concept_mappings"
    cm_dir.mkdir(parents=True, exist_ok=True)
    items = [
        {
            "source_code": "E11.9",
            "source_description": "Type 2 diabetes mellitus",
            "source_count": 42,
            "target_concept_id": 201826,
            "target_concept_name": "Type 2 diabetes mellitus",
            "target_vocabulary_id": "SNOMED",
            "target_domain_id": "Condition",
            "confidence": 0.99,
            "method": "auto",
            "candidates": [
                {
                    "concept_id": 201826,
                    "concept_name": "Type 2 diabetes mellitus",
                    "vocabulary_id": "SNOMED",
                    "domain_id": "Condition",
                    "concept_class_id": "Clinical Finding",
                    "standard_concept": "S",
                    "score": 0.99,
                },
            ],
        },
        {
            "source_code": "R73.03",
            "source_description": "Prediabetes",
            "source_count": 7,
            "target_concept_id": 4080556,
            "target_concept_name": "Prediabetic state",
            "target_vocabulary_id": "SNOMED",
            "target_domain_id": "Condition",
            "confidence": 0.62,
            "method": "review",
            "candidates": [
                {
                    "concept_id": 4080556,
                    "concept_name": "Prediabetic state",
                    "vocabulary_id": "SNOMED",
                    "domain_id": "Condition",
                    "concept_class_id": "Clinical Finding",
                    "standard_concept": "S",
                    "score": 0.62,
                },
                {
                    "concept_id": 9999,
                    "concept_name": "Impaired glucose tolerance",
                    "vocabulary_id": "SNOMED",
                    "domain_id": "Condition",
                    "concept_class_id": "Clinical Finding",
                    "standard_concept": "S",
                    "score": 0.55,
                },
            ],
        },
    ]
    (cm_dir / "concept_mapping.yaml").write_text(yaml.dump(items))
    return cm_dir


class TestLoadConceptMapping:
    def test_load_returns_mapping_with_items(self, tmp_path):
        from portiere.review_ui.state import load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)
        assert len(mapping.items) == 2
        assert mapping.items[0].source_code == "E11.9"
        assert mapping.items[1].target_concept_id == 4080556
        assert len(mapping.items[1].candidates) == 2

    def test_load_returns_empty_when_no_file(self, tmp_path):
        from portiere.review_ui.state import load_concept_mapping

        mapping = load_concept_mapping(tmp_path)
        assert mapping.items == []

    def test_load_prefers_reviewed_json_when_present(self, tmp_path):
        from portiere.review_ui.state import load_concept_mapping

        cm_dir = _make_project_with_concept_mapping(tmp_path)
        reviewed = {
            "items": [
                {
                    "source_code": "E11.9",
                    "target_concept_id": 201826,
                    "target_concept_name": "Type 2 diabetes mellitus",
                    "confidence": 0.99,
                    "method": "override",  # human-edited
                    "candidates": [],
                    "provenance": {"reviewer_note": "Confirmed manually"},
                }
            ]
        }
        (cm_dir / "concept_mapping_reviewed.json").write_text(json.dumps(reviewed))

        mapping = load_concept_mapping(tmp_path)
        assert len(mapping.items) == 1
        assert mapping.items[0].method.value == "override"
        assert mapping.items[0].provenance.get("reviewer_note") == "Confirmed manually"


class TestSaveReviewedConceptMapping:
    def test_save_writes_to_reviewed_json(self, tmp_path):
        from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
        from portiere.review_ui.state import save_reviewed_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="E11.9",
                    target_concept_id=201826,
                    target_concept_name="Type 2 diabetes mellitus",
                    confidence=0.99,
                    method="override",
                )
            ]
        )
        save_reviewed_concept_mapping(mapping, tmp_path)

        out = tmp_path / "concept_mappings" / "concept_mapping_reviewed.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert data["items"][0]["method"] == "override"

    def test_save_does_not_modify_original_yaml(self, tmp_path):
        from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
        from portiere.review_ui.state import save_reviewed_concept_mapping

        cm_dir = _make_project_with_concept_mapping(tmp_path)
        original = (cm_dir / "concept_mapping.yaml").read_text()

        mapping = ConceptMapping(
            items=[ConceptMappingItem(source_code="E11.9", method="unmapped", confidence=0.0)]
        )
        save_reviewed_concept_mapping(mapping, tmp_path)
        assert (cm_dir / "concept_mapping.yaml").read_text() == original


class TestApplyConceptDecision:
    def test_apply_approve_uses_top_candidate(self, tmp_path):
        from portiere.review_ui.state import apply_concept_decision, load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)

        updated = apply_concept_decision(mapping, index=1, decision="approve")
        # Top candidate (rank 0) → method becomes AUTO; target stays the same.
        assert updated.items[1].method.value in ("auto", "override")
        assert updated.items[1].target_concept_id == 4080556

    def test_apply_reject_marks_unmapped(self, tmp_path):
        from portiere.review_ui.state import apply_concept_decision, load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)

        updated = apply_concept_decision(mapping, index=1, decision="reject")
        assert updated.items[1].method.value == "unmapped"

    def test_apply_override_with_candidate_index(self, tmp_path):
        from portiere.review_ui.state import apply_concept_decision, load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)

        updated = apply_concept_decision(
            mapping,
            index=1,
            decision="override",
            candidate_index=1,  # second candidate (9999, "Impaired glucose tolerance")
            reviewer_note="Better fits clinical context",
        )
        assert updated.items[1].method.value == "override"
        assert updated.items[1].target_concept_id == 9999
        assert updated.items[1].target_concept_name == "Impaired glucose tolerance"
        assert updated.items[1].provenance.get("reviewer_note") == "Better fits clinical context"

    def test_apply_override_with_free_form_concept_id(self, tmp_path):
        from portiere.review_ui.state import apply_concept_decision, load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)

        updated = apply_concept_decision(
            mapping,
            index=1,
            decision="override",
            target_concept_id=4170143,
            target_concept_name="Glucose intolerance",
            reviewer_note="Off-candidate-list pick",
        )
        assert updated.items[1].target_concept_id == 4170143
        assert updated.items[1].target_concept_name == "Glucose intolerance"
        assert updated.items[1].method.value == "override"

    def test_apply_invalid_decision_raises(self, tmp_path):
        import pytest

        from portiere.review_ui.state import apply_concept_decision, load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)
        with pytest.raises(ValueError, match=r"[Uu]nknown decision"):
            apply_concept_decision(mapping, index=0, decision="boop")

    def test_apply_out_of_range_raises(self, tmp_path):
        import pytest

        from portiere.review_ui.state import apply_concept_decision, load_concept_mapping

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)
        with pytest.raises(IndexError):
            apply_concept_decision(mapping, index=99, decision="approve")


# ── Sort + filter helpers (Slice 5) ───────────────────────────────


class TestConceptMappingSort:
    def test_sort_by_confidence_ascending_surfaces_low_confidence_first(self, tmp_path):
        from portiere.review_ui.state import load_concept_mapping, sort_by_confidence_ascending

        _make_project_with_concept_mapping(tmp_path)
        mapping = load_concept_mapping(tmp_path)
        indices = sort_by_confidence_ascending(mapping)
        # 0.62 comes before 0.99
        assert mapping.items[indices[0]].confidence < mapping.items[indices[1]].confidence

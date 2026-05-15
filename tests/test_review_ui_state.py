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

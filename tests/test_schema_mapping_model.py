"""Tests for SchemaMapping and SchemaMappingItem models."""

import pytest

from portiere.models.schema_mapping import (
    MappingStatus,
    SchemaMapping,
    SchemaMappingItem,
)

# ── SchemaMappingItem ──────────────────────────────────────────────────────


class TestSchemaMappingItem:
    def test_default_status_is_needs_review(self):
        item = SchemaMappingItem(source_column="patient_id")
        assert item.status == MappingStatus.NEEDS_REVIEW

    def test_effective_target_uses_override_when_set(self):
        item = SchemaMappingItem(
            source_column="pid",
            target_table="person",
            target_column="person_id",
            override_target_table="visit_occurrence",
            override_target_column="visit_id",
        )
        assert item.effective_target_table == "visit_occurrence"
        assert item.effective_target_column == "visit_id"

    def test_effective_target_falls_back_to_ai_suggestion(self):
        item = SchemaMappingItem(
            source_column="pid",
            target_table="person",
            target_column="person_id",
        )
        assert item.effective_target_table == "person"
        assert item.effective_target_column == "person_id"

    def test_approve_sets_approved_status(self):
        item = SchemaMappingItem(source_column="age", status=MappingStatus.NEEDS_REVIEW)
        item.approve()
        assert item.status == MappingStatus.APPROVED

    def test_approve_with_override(self):
        item = SchemaMappingItem(source_column="dob")
        item.approve(target_table="person", target_column="birth_datetime")
        assert item.status == MappingStatus.OVERRIDDEN
        assert item.override_target_table == "person"
        assert item.override_target_column == "birth_datetime"

    def test_reject(self):
        item = SchemaMappingItem(source_column="col")
        item.reject()
        assert item.status == MappingStatus.REJECTED

    def test_nan_coercion_for_source_table(self):
        item = SchemaMappingItem(source_column="col", source_table=float("nan"))
        assert item.source_table == ""

    def test_nan_coercion_for_source_column(self):
        item = SchemaMappingItem(source_column=float("nan"))
        assert item.source_column == ""

    def test_none_coercion_for_source_table(self):
        item = SchemaMappingItem(source_column="col", source_table=None)
        assert item.source_table == ""


# ── SchemaMapping ──────────────────────────────────────────────────────────


def _make_mapping(n_auto=2, n_review=3, n_rejected=1) -> SchemaMapping:
    items = []
    for i in range(n_auto):
        items.append(
            SchemaMappingItem(
                source_column=f"auto_{i}",
                target_table="person",
                target_column=f"col_{i}",
                confidence=0.97,
                status=MappingStatus.AUTO_ACCEPTED,
            )
        )
    for i in range(n_review):
        items.append(
            SchemaMappingItem(
                source_column=f"review_{i}",
                confidence=0.75,
                status=MappingStatus.NEEDS_REVIEW,
            )
        )
    for i in range(n_rejected):
        items.append(
            SchemaMappingItem(
                source_column=f"rejected_{i}",
                confidence=0.3,
                status=MappingStatus.REJECTED,
            )
        )
    return SchemaMapping(items=items)


class TestSchemaMappingFilters:
    def test_needs_review_returns_correct_items(self):
        sm = _make_mapping(n_auto=2, n_review=3, n_rejected=1)
        assert len(sm.needs_review()) == 3

    def test_auto_accepted_returns_correct_items(self):
        sm = _make_mapping(n_auto=2, n_review=3)
        assert len(sm.auto_accepted()) == 2

    def test_rejected_returns_correct_items(self):
        sm = _make_mapping(n_auto=2, n_review=1, n_rejected=2)
        assert len(sm.rejected()) == 2

    def test_overridden_returns_overridden_items(self):
        sm = _make_mapping()
        sm.override("review_0", "visit_occurrence", "visit_id")
        assert len(sm.overridden()) == 1


class TestSchemaMappingActions:
    def test_get_item_found(self):
        sm = _make_mapping(n_review=2)
        item = sm.get_item("review_0")
        assert item.source_column == "review_0"

    def test_get_item_not_found_raises(self):
        sm = _make_mapping()
        with pytest.raises(KeyError, match="no_such_col"):
            sm.get_item("no_such_col")

    def test_approve_changes_status(self):
        sm = _make_mapping(n_review=2)
        sm.approve("review_0")
        assert sm.get_item("review_0").status == MappingStatus.APPROVED

    def test_reject_changes_status(self):
        sm = _make_mapping(n_review=2)
        sm.reject("review_0")
        assert sm.get_item("review_0").status == MappingStatus.REJECTED

    def test_override_changes_target(self):
        sm = _make_mapping(n_review=2)
        sm.override("review_0", "condition_occurrence", "condition_concept_id")
        item = sm.get_item("review_0")
        assert item.effective_target_table == "condition_occurrence"
        assert item.effective_target_column == "condition_concept_id"
        assert item.status == MappingStatus.OVERRIDDEN

    def test_approve_all_clears_review_queue(self):
        sm = _make_mapping(n_review=3)
        assert len(sm.needs_review()) == 3
        sm.approve_all()
        assert len(sm.needs_review()) == 0

    def test_finalize_sets_finalized_flag(self):
        sm = _make_mapping(n_review=0)
        sm.finalize()
        assert sm.finalized is True

    def test_finalize_warns_if_items_pending(self, caplog):
        sm = _make_mapping(n_review=2)
        import logging

        with caplog.at_level(logging.WARNING):
            sm.finalize()
        assert sm.finalized is True


class TestSchemaMappingSummary:
    def test_summary_keys(self):
        sm = _make_mapping()
        s = sm.summary()
        assert "total" in s
        assert "auto_accepted" in s
        assert "needs_review" in s
        assert "approved" in s
        assert "auto_rate" in s

    def test_summary_total(self):
        sm = _make_mapping(n_auto=2, n_review=3, n_rejected=1)
        assert sm.summary()["total"] == 6

    def test_repr_contains_stats(self):
        sm = _make_mapping(n_auto=2, n_review=1)
        r = repr(sm)
        assert "SchemaMapping" in r
        assert "total=" in r


class TestSchemaMappingFromApiResponse:
    def test_high_confidence_auto_accepted(self):
        response = {
            "mappings": [
                {
                    "source_column": "pid",
                    "target_table": "person",
                    "target_column": "person_id",
                    "confidence": 0.97,
                }
            ]
        }
        sm = SchemaMapping.from_api_response(response, None)
        assert sm.items[0].status == MappingStatus.AUTO_ACCEPTED

    def test_medium_confidence_needs_review(self):
        response = {
            "mappings": [
                {
                    "source_column": "dob",
                    "confidence": 0.80,
                }
            ]
        }
        sm = SchemaMapping.from_api_response(response, None)
        assert sm.items[0].status == MappingStatus.NEEDS_REVIEW

    def test_items_key_also_accepted(self):
        response = {"items": [{"source_column": "age", "confidence": 0.95}]}
        sm = SchemaMapping.from_api_response(response, None)
        assert len(sm.items) == 1


class TestSchemaMappingCSVRoundtrip:
    def test_to_csv_and_from_csv(self, tmp_path):
        sm = _make_mapping(n_auto=1, n_review=1, n_rejected=0)
        csv_path = str(tmp_path / "mappings.csv")
        sm.to_csv(csv_path)
        sm2 = SchemaMapping.from_csv(csv_path)
        assert len(sm2.items) == len(sm.items)

    def test_to_dataframe_has_correct_columns(self):
        sm = _make_mapping(n_auto=1, n_review=1)
        df = sm.to_dataframe()
        assert "source_column" in df.columns
        assert "target_table" in df.columns
        assert "confidence" in df.columns
        assert "status" in df.columns

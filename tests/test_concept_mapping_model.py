"""Tests for ConceptMapping and ConceptMappingItem models."""

import pytest

from portiere.models.concept_mapping import (
    ConceptCandidate,
    ConceptMapping,
    ConceptMappingItem,
    ConceptMappingMethod,
)


def _make_candidate(
    concept_id=44054006, name="Type 2 diabetes", vocab="SNOMED"
) -> ConceptCandidate:
    return ConceptCandidate(
        concept_id=concept_id,
        concept_name=name,
        vocabulary_id=vocab,
        domain_id="Condition",
        concept_class_id="Clinical Finding",
        standard_concept="S",
        score=0.95,
    )


def _make_item(
    code="E11.9",
    method=ConceptMappingMethod.REVIEW,
    mapped=True,
) -> ConceptMappingItem:
    item = ConceptMappingItem(
        source_code=code,
        source_description="Type 2 diabetes",
        confidence=0.90,
        method=method,
        candidates=[_make_candidate()],
    )
    if mapped:
        item.target_concept_id = 44054006
        item.target_concept_name = "Type 2 diabetes mellitus"
        item.target_vocabulary_id = "SNOMED"
    return item


# ── ConceptMappingItem ──────────────────────────────────────────────────────


class TestConceptMappingItem:
    def test_is_mapped_true_when_target_set(self):
        item = _make_item(mapped=True)
        assert item.is_mapped is True

    def test_is_mapped_false_when_no_target(self):
        item = _make_item(mapped=False)
        assert item.is_mapped is False

    def test_approved_for_auto_method(self):
        item = _make_item(method=ConceptMappingMethod.AUTO)
        assert item.approved is True

    def test_approved_for_override_method(self):
        item = _make_item(method=ConceptMappingMethod.OVERRIDE)
        assert item.approved is True

    def test_not_approved_for_review(self):
        item = _make_item(method=ConceptMappingMethod.REVIEW)
        assert item.approved is False

    def test_rejected_for_unmapped(self):
        item = _make_item(method=ConceptMappingMethod.UNMAPPED)
        assert item.rejected is True

    def test_not_rejected_for_auto(self):
        item = _make_item(method=ConceptMappingMethod.AUTO)
        assert item.rejected is False

    def test_approve_first_candidate(self):
        item = _make_item(mapped=False, method=ConceptMappingMethod.REVIEW)
        item.approve(candidate_index=0)
        assert item.target_concept_id == 44054006
        assert item.method == ConceptMappingMethod.AUTO

    def test_approve_second_candidate_sets_override(self):
        item = ConceptMappingItem(
            source_code="E11.9",
            candidates=[
                _make_candidate(concept_id=111, name="First"),
                _make_candidate(concept_id=222, name="Second"),
            ],
        )
        item.approve(candidate_index=1)
        assert item.target_concept_id == 222
        assert item.method == ConceptMappingMethod.OVERRIDE

    def test_approve_no_candidates_sets_auto(self):
        item = ConceptMappingItem(source_code="E11.9", candidates=[])
        item.approve()
        assert item.method == ConceptMappingMethod.AUTO

    def test_reject_sets_unmapped(self):
        item = _make_item()
        item.reject()
        assert item.method == ConceptMappingMethod.UNMAPPED

    def test_override_sets_concept(self):
        item = _make_item(mapped=False)
        item.override(concept_id=9999, concept_name="Custom", vocabulary_id="Custom")
        assert item.target_concept_id == 9999
        assert item.method == ConceptMappingMethod.OVERRIDE

    def test_mark_unmapped(self):
        item = _make_item(method=ConceptMappingMethod.AUTO)
        item.mark_unmapped()
        assert item.method == ConceptMappingMethod.UNMAPPED


# ── ConceptMapping ──────────────────────────────────────────────────────────


def _make_mapping(n_auto=2, n_review=2, n_unmapped=1) -> ConceptMapping:
    items = []
    for i in range(n_auto):
        items.append(
            ConceptMappingItem(
                source_code=f"AUTO{i:03d}",
                source_description=f"Auto mapped {i}",
                target_concept_id=i + 1000,
                target_concept_name=f"Concept {i}",
                target_vocabulary_id="SNOMED",
                confidence=0.97,
                method=ConceptMappingMethod.AUTO,
            )
        )
    for i in range(n_review):
        items.append(
            ConceptMappingItem(
                source_code=f"REVIEW{i:03d}",
                confidence=0.75,
                method=ConceptMappingMethod.REVIEW,
                candidates=[_make_candidate(concept_id=2000 + i)],
            )
        )
    for i in range(n_unmapped):
        items.append(
            ConceptMappingItem(
                source_code=f"UNMAPPED{i:03d}",
                confidence=0.2,
                method=ConceptMappingMethod.UNMAPPED,
            )
        )
    return ConceptMapping(items=items)


class TestConceptMappingFilters:
    def test_needs_review(self):
        cm = _make_mapping(n_review=3)
        assert len(cm.needs_review()) == 3

    def test_auto_mapped(self):
        cm = _make_mapping(n_auto=4)
        assert len(cm.auto_mapped()) == 4

    def test_unmapped_includes_manual_and_unmapped(self):
        items = [
            ConceptMappingItem(source_code="A", method=ConceptMappingMethod.UNMAPPED),
            ConceptMappingItem(source_code="B", method=ConceptMappingMethod.MANUAL),
        ]
        cm = ConceptMapping(items=items)
        assert len(cm.unmapped()) == 2


class TestConceptMappingActions:
    def test_get_item_found(self):
        cm = _make_mapping(n_review=2)
        item = cm.get_item("REVIEW000")
        assert item.source_code == "REVIEW000"

    def test_get_item_not_found_raises(self):
        cm = _make_mapping()
        with pytest.raises(KeyError):
            cm.get_item("NOSUCHCODE")

    def test_approve(self):
        cm = _make_mapping(n_review=2)
        cm.approve("REVIEW000", candidate_index=0)
        item = cm.get_item("REVIEW000")
        assert item.method == ConceptMappingMethod.AUTO

    def test_reject(self):
        cm = _make_mapping(n_review=2)
        cm.reject("REVIEW000")
        assert cm.get_item("REVIEW000").method == ConceptMappingMethod.UNMAPPED

    def test_override(self):
        cm = _make_mapping(n_review=2)
        cm.override("REVIEW000", concept_id=99999, concept_name="Override", vocabulary_id="ICD10CM")
        item = cm.get_item("REVIEW000")
        assert item.target_concept_id == 99999
        assert item.method == ConceptMappingMethod.OVERRIDE

    def test_approve_all(self):
        cm = _make_mapping(n_review=3)
        assert len(cm.needs_review()) == 3
        cm.approve_all()
        assert len(cm.needs_review()) == 0

    def test_finalize(self):
        cm = _make_mapping(n_review=0)
        cm.finalize()
        assert cm.finalized is True


class TestConceptMappingSummary:
    def test_summary_keys(self):
        cm = _make_mapping()
        s = cm.summary()
        assert "total" in s
        assert "auto_mapped" in s
        assert "needs_review" in s
        assert "manual_required" in s
        assert "auto_rate" in s
        assert "coverage" in s

    def test_repr(self):
        cm = _make_mapping(n_auto=2, n_review=1)
        r = repr(cm)
        assert "ConceptMapping" in r
        assert "auto=" in r


class TestConceptMappingFromApiResponse:
    def test_from_api_response_parses_items(self):
        response = {
            "items": [
                {
                    "source_code": "E11.9",
                    "source_description": "Type 2 diabetes",
                    "target_concept_id": 44054006,
                    "target_concept_name": "Type 2 diabetes mellitus",
                    "target_vocabulary_id": "SNOMED",
                    "confidence": 0.97,
                    "method": "auto",
                    "candidates": [],
                    "provenance": {},
                }
            ]
        }
        cm = ConceptMapping.from_api_response(response, None)
        assert len(cm.items) == 1
        assert cm.items[0].source_code == "E11.9"
        assert cm.items[0].method == ConceptMappingMethod.AUTO


class TestConceptMappingExport:
    def test_to_source_to_concept_map_only_mapped(self):
        cm = _make_mapping(n_auto=2, n_unmapped=1)
        stcm = cm.to_source_to_concept_map()
        assert len(stcm) == 2
        for row in stcm:
            assert "source_code" in row
            assert "target_concept_id" in row

    def test_to_dataframe(self):
        cm = _make_mapping(n_auto=2, n_review=1, n_unmapped=0)
        df = cm.to_dataframe()
        assert "source_code" in df.columns
        assert "target_concept_id" in df.columns
        assert len(df) == 3

    def test_to_json_and_from_json(self, tmp_path):
        cm = _make_mapping(n_auto=1, n_review=0, n_unmapped=0)
        path = str(tmp_path / "mappings.json")
        cm.to_json(path)
        cm2 = ConceptMapping.from_json(path)
        assert len(cm2.items) == 1
        assert cm2.items[0].source_code == cm.items[0].source_code

    def test_to_csv_and_from_csv(self, tmp_path):
        cm = _make_mapping(n_auto=2, n_review=0, n_unmapped=0)
        path = str(tmp_path / "mappings.csv")
        cm.to_csv(path)
        cm2 = ConceptMapping.from_csv(path)
        assert len(cm2.items) == 2


class TestConceptMappingFromRecords:
    def test_from_records_basic(self):
        records = [
            {
                "source_code": "E11.9",
                "source_description": "Type 2 diabetes",
                "target_concept_id": 44054006,
                "target_vocabulary_id": "SNOMED",
                "confidence": 0.95,
                "method": "auto",
            }
        ]
        cm = ConceptMapping.from_records(records)
        assert len(cm.items) == 1
        assert cm.items[0].source_code == "E11.9"

    def test_from_records_handles_nan_values(self):
        records = [
            {
                "source_code": "I10",
                "source_description": float("nan"),
                "target_concept_id": float("nan"),
                "confidence": float("nan"),
            }
        ]
        cm = ConceptMapping.from_records(records)
        assert len(cm.items) == 1
        assert cm.items[0].source_description is None
        assert cm.items[0].target_concept_id is None

    def test_from_records_missing_source_code_raises(self):
        with pytest.raises(ValueError, match="source_code"):
            ConceptMapping.from_records([{"concept_id": 123}])

    def test_from_records_vocabulary_id_alias(self):
        records = [
            {
                "source_code": "E11.9",
                "vocabulary_id": "SNOMED",  # alias for target_vocabulary_id
                "target_concept_id": 44054006,
            }
        ]
        cm = ConceptMapping.from_records(records)
        assert cm.items[0].target_vocabulary_id == "SNOMED"

    def test_from_dataframe(self):
        import pandas as pd

        df = pd.DataFrame(
            [
                {"source_code": "E11.9", "target_concept_id": 44054006, "method": "auto"},
                {"source_code": "I10", "target_concept_id": 319826, "method": "auto"},
            ]
        )
        cm = ConceptMapping.from_dataframe(df)
        assert len(cm.items) == 2

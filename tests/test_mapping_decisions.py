"""Reviewed decisions have the same meaning in execution and interchange."""

import pytest

from portiere.engines import PolarsEngine
from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
from portiere.models.schema_mapping import SchemaMapping
from portiere.runner import ETLRunner


@pytest.mark.parametrize("method", ["review", "manual", "unmapped"])
def test_pending_and_rejected_suggestions_are_not_executable(method):
    mapping = ConceptMapping(
        items=[
            ConceptMappingItem(
                source_code="00123", source_column="code", target_concept_id=100, method=method
            )
        ]
    )
    runner = ETLRunner.from_mappings(PolarsEngine(), SchemaMapping(), mapping)

    assert runner.concept_items == []
    assert mapping.to_source_to_concept_map() == []


def test_human_review_preserves_inference_and_survives_csv(tmp_path):
    item = ConceptMappingItem(source_code="00123", target_concept_id=100, method="review")
    item.approve()
    mapping = ConceptMapping(items=[item])
    path = str(tmp_path / "reviewed.csv")
    mapping.to_csv(path)
    restored = ConceptMapping.from_csv(path).items[0]

    assert restored.inference_method == "review"
    assert restored.review_decision == "approved"
    assert restored.approved is True


def test_rejection_keeps_suggestion_but_excludes_omop_export():
    item = ConceptMappingItem(source_code="00123", target_concept_id=100, method="auto")
    item.reject()

    assert item.target_concept_id == 100
    assert ConceptMapping(items=[item]).to_source_to_concept_map() == []


def test_explicit_rejection_wins_over_legacy_auto_method():
    item = ConceptMappingItem(
        source_code="00123", target_concept_id=100, method="auto", review_decision="rejected"
    )

    assert item.approved is False
    assert ConceptMapping(items=[item]).to_source_to_concept_map() == []


def test_api_import_does_not_erase_rejection():
    mapping = ConceptMapping.from_api_response(
        {
            "items": [
                {
                    "source_code": "00123",
                    "target_concept_id": 100,
                    "method": "auto",
                    "inference_method": "review",
                    "review_decision": "rejected",
                }
            ]
        },
        None,
    )

    assert mapping.items[0].inference_method == "review"
    assert mapping.to_source_to_concept_map() == []


def test_approved_export_roundtrip_keeps_review_decision():
    original = ConceptMapping(
        items=[
            ConceptMappingItem(
                source_code="00123",
                target_concept_id=100,
                method="review",
                review_decision="approved",
            )
        ]
    )
    restored = ConceptMapping.from_records(original.to_source_to_concept_map())

    assert restored.items[0].approved
    assert restored.items[0].inference_method == "review"


def test_bulk_approval_leaves_explicit_rejections_alone():
    item = ConceptMappingItem(
        source_code="00123",
        target_concept_id=100,
        method="review",
        review_decision="rejected",
        candidates=[
            {
                "concept_id": 100,
                "concept_name": "Test",
                "vocabulary_id": "Test",
                "domain_id": "Condition",
                "concept_class_id": "Test",
                "standard_concept": "S",
            }
        ],
    )
    mapping = ConceptMapping(items=[item])
    mapping.approve_all()

    assert mapping.to_source_to_concept_map() == []


@pytest.mark.parametrize("index", [-1, 2])
def test_invalid_candidate_index_does_not_record_approval(index):
    item = ConceptMappingItem(source_code="00123", target_concept_id=100)
    with pytest.raises(ValueError, match="candidate"):
        item.approve(index)
    assert item.approved is False

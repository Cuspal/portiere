"""Concept extraction processes complete code sets; retrieval respects metadata."""

import pytest

from portiere.config import EmbeddingConfig, PortiereConfig, RerankerConfig
from portiere.engines import get_engine
from portiere.local.concept_mapper import LocalConceptMapper
from portiere.stages.stage3_concepts import map_concepts


@pytest.mark.parametrize("engine_name", ["pandas", "polars"])
def test_concept_extraction_does_not_stop_at_5000(tmp_path, monkeypatch, engine_name):
    path = tmp_path / "codes.csv"
    path.write_text("diagnosis_code\n" + "\n".join(f"C{i:05}" for i in range(5001)))

    async def echo_codes(self, codes, vocabularies, domain=None):
        return [
            {"source_code": c["code"], "source_count": c["count"], "method": "manual"}
            for c in codes
        ]

    monkeypatch.setattr(LocalConceptMapper, "map_batch", echo_codes)
    result = map_concepts(
        client=None,
        engine=get_engine(engine_name),
        source_path=str(path),
        code_columns=["diagnosis_code"],
        vocabularies=["SNOMED"],
        config=PortiereConfig(),
    )
    items = result["mappings"]["diagnosis_code"]["items"]
    assert {item["source_code"] for item in items} == {f"C{i:05}" for i in range(5001)}


@pytest.mark.parametrize("engine_name", ["pandas", "polars"])
@pytest.mark.parametrize("codes", [["00123", "00456"], ["NA", "NULL", "00123"]])
def test_concept_extraction_preserves_literal_codes(tmp_path, monkeypatch, engine_name, codes):
    path = tmp_path / "codes.csv"
    path.write_text("diagnosis_code\n" + "\n".join(codes))

    async def echo_codes(self, codes, vocabularies, domain=None):
        return [{"source_code": c["code"], "method": "manual"} for c in codes]

    monkeypatch.setattr(LocalConceptMapper, "map_batch", echo_codes)
    result = map_concepts(
        client=None,
        engine=get_engine(engine_name),
        source_path=str(path),
        code_columns=["diagnosis_code"],
        vocabularies=["SNOMED"],
        config=PortiereConfig(),
    )
    assert {item["source_code"] for item in result["mappings"]["diagnosis_code"]["items"]} == set(
        codes
    )


def mapper_with_index():
    mapper = LocalConceptMapper(
        PortiereConfig(
            embedding=EmbeddingConfig(provider="none"), reranker=RerankerConfig(provider="none")
        )
    )
    mapper._initialize()
    mapper._code_index = {
        "A12": {
            "concept_id": 100,
            "concept_name": "Synthetic source concept",
            "vocabulary_id": "ICD10CM",
            "domain_id": "Condition",
            "standard_concept": "",
            "concept_class_id": "Test",
        }
    }
    return mapper


def test_direct_code_lookup_keeps_nonstandard_metadata():
    result = mapper_with_index().search("A12", vocabularies=["ICD10CM"], domain="Condition")
    assert result[0]["standard_concept"] == ""


@pytest.mark.parametrize("vocabularies,domain", [(["SNOMED"], None), (None, "Drug")])
def test_direct_code_lookup_respects_filters(vocabularies, domain):
    assert mapper_with_index().search("A12", vocabularies=vocabularies, domain=domain) == []


@pytest.mark.asyncio
async def test_nonstandard_source_concept_is_not_accepted_as_standard_target():
    result = await mapper_with_index().map_code("A12", vocabularies=["ICD10CM"])
    assert result.get("target_concept_id") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("direct_hit", [True, False])
async def test_standard_target_search_filters_before_early_return(direct_hit):
    from unittest.mock import MagicMock

    mapper = mapper_with_index()
    mapper._router = None
    backend = MagicMock()
    nonstandard = mapper._code_index["A12"]
    backend.search.return_value = [dict(nonstandard, concept_id=i) for i in range(10)] + [
        {
            **nonstandard,
            "concept_id": 999,
            "standard_concept": "S",
            "score": 0.96,
        }
    ]
    mapper._knowledge_backend = backend
    if not direct_hit:
        mapper._code_index = {}
    result = await mapper.map_code("A12")
    assert result["target_concept_id"] == 999


def test_local_bm25_retains_invalid_metadata_for_filtering(tmp_path):
    import json

    from portiere.knowledge.bm25s_backend import BM25sBackend

    concept = {
        "concept_id": 123,
        "concept_name": "Synthetic expired condition",
        "standard_concept": "S",
        "vocabulary_id": "SNOMED",
        "invalid_reason": "D",
    }
    path = tmp_path / "corpus.json"
    path.write_text(json.dumps([concept]))
    mapper = mapper_with_index()
    mapper._knowledge_backend = BM25sBackend(path, use_stemming=False)
    assert mapper.search("Synthetic expired condition") == []


def test_athena_loader_excludes_invalid_standard_concepts(tmp_path):
    from portiere.knowledge.athena import load_athena_concepts

    (tmp_path / "CONCEPT.csv").write_text(
        "concept_id\tconcept_name\tvocabulary_id\tstandard_concept\tinvalid_reason\n"
        "123\tExpired synthetic condition\tSNOMED\tS\tD\n"
        "456\tCurrent synthetic condition\tSNOMED\tS\t\n"
    )
    assert [c["concept_id"] for c in load_athena_concepts(tmp_path)] == [456]

"""Tests for Athena vocabulary loader and knowledge layer builder."""

import json
from pathlib import Path

import pytest

from portiere.knowledge.athena import build_knowledge_layer, load_athena_concepts


@pytest.fixture
def athena_dir(tmp_path):
    """Create a mock Athena download directory with sample data."""
    # CONCEPT.csv (tab-delimited, matching Athena format)
    concept_rows = [
        "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\tstandard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason",
        "201826\tType 2 diabetes mellitus\tCondition\tSNOMED\tClinical Finding\tS\t44054006\t19700101\t20991231\t",
        "320128\tEssential hypertension\tCondition\tSNOMED\tClinical Finding\tS\t59621000\t19700101\t20991231\t",
        "3004249\tHemoglobin A1c/Hemoglobin.total in Blood\tMeasurement\tLOINC\tLab Test\tS\t4548-4\t19700101\t20991231\t",
        "1503297\tMetformin\tDrug\tRxNorm\tIngredient\tS\t6809\t19700101\t20991231\t",
        "35207070\tType 2 diabetes mellitus, unspecified\tCondition\tICD10CM\t4-char billing code\t\tE11.9\t19700101\t20991231\t",
        "0\tDomain not specified\tMetadata\tNone\tMetadata\t\t0\t19700101\t20991231\t",
    ]
    concept_file = tmp_path / "CONCEPT.csv"
    concept_file.write_text("\n".join(concept_rows))

    # CONCEPT_SYNONYM.csv (tab-delimited)
    synonym_rows = [
        "concept_id\tconcept_synonym_name\tlanguage_concept_id",
        "201826\tdiabetes type 2\t4180186",
        "201826\tDM2\t4180186",
        "201826\tT2DM\t4180186",
        "320128\thigh blood pressure\t4180186",
        "320128\tHTN\t4180186",
    ]
    synonym_file = tmp_path / "CONCEPT_SYNONYM.csv"
    synonym_file.write_text("\n".join(synonym_rows))

    return tmp_path


class TestLoadAthenaConcepts:
    def test_loads_standard_concepts_only(self, athena_dir):
        concepts = load_athena_concepts(athena_dir)
        # Should only include rows with standard_concept = "S" (4 concepts)
        assert len(concepts) == 4
        concept_ids = {c["concept_id"] for c in concepts}
        assert concept_ids == {201826, 320128, 3004249, 1503297}

    def test_filters_by_vocabulary(self, athena_dir):
        concepts = load_athena_concepts(athena_dir, vocabularies=["SNOMED"])
        assert len(concepts) == 2
        for c in concepts:
            assert c["vocabulary_id"] == "SNOMED"

    def test_filters_multiple_vocabularies(self, athena_dir):
        concepts = load_athena_concepts(athena_dir, vocabularies=["SNOMED", "LOINC"])
        assert len(concepts) == 3
        vocab_ids = {c["vocabulary_id"] for c in concepts}
        assert vocab_ids == {"SNOMED", "LOINC"}

    def test_attaches_synonyms(self, athena_dir):
        concepts = load_athena_concepts(athena_dir)
        diabetes = next(c for c in concepts if c["concept_id"] == 201826)
        assert "synonyms" in diabetes
        assert "DM2" in diabetes["synonyms"]
        assert "T2DM" in diabetes["synonyms"]

    def test_excludes_concept_name_from_synonyms(self, athena_dir):
        concepts = load_athena_concepts(athena_dir)
        diabetes = next(c for c in concepts if c["concept_id"] == 201826)
        # The concept name itself should not appear in synonyms
        for syn in diabetes.get("synonyms", []):
            assert syn.lower() != "type 2 diabetes mellitus"

    def test_concept_fields(self, athena_dir):
        concepts = load_athena_concepts(athena_dir)
        diabetes = next(c for c in concepts if c["concept_id"] == 201826)
        assert diabetes["concept_name"] == "Type 2 diabetes mellitus"
        assert diabetes["vocabulary_id"] == "SNOMED"
        assert diabetes["domain_id"] == "Condition"
        assert diabetes["concept_class_id"] == "Clinical Finding"
        assert diabetes["standard_concept"] == "S"

    def test_missing_concept_csv_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match=r"CONCEPT\.csv not found"):
            load_athena_concepts(tmp_path)

    def test_no_synonyms_file_ok(self, athena_dir):
        # Remove synonym file
        (athena_dir / "CONCEPT_SYNONYM.csv").unlink()
        concepts = load_athena_concepts(athena_dir)
        assert len(concepts) == 4
        # Concepts without synonyms should not have the key
        metformin = next(c for c in concepts if c["concept_id"] == 1503297)
        assert "synonyms" not in metformin


class TestBuildKnowledgeLayer:
    def test_build_bm25s(self, athena_dir, tmp_path):
        output = tmp_path / "output"
        paths = build_knowledge_layer(
            athena_path=athena_dir,
            output_path=output,
            backend="bm25s",
        )
        assert "bm25s_corpus_path" in paths
        corpus_path = Path(paths["bm25s_corpus_path"])
        assert corpus_path.exists()

        with open(corpus_path) as f:
            data = json.load(f)
        assert len(data) == 4

    def test_build_bm25s_with_vocab_filter(self, athena_dir, tmp_path):
        output = tmp_path / "output"
        paths = build_knowledge_layer(
            athena_path=athena_dir,
            output_path=output,
            backend="bm25s",
            vocabularies=["SNOMED"],
        )
        with open(paths["bm25s_corpus_path"]) as f:
            data = json.load(f)
        assert len(data) == 2

    def test_invalid_backend_raises(self, athena_dir, tmp_path):
        with pytest.raises(ValueError, match="Unsupported backend"):
            build_knowledge_layer(
                athena_path=athena_dir,
                output_path=tmp_path / "output",
                backend="invalid",
            )

    def test_empty_concepts_raises(self, tmp_path):
        # Create a CONCEPT.csv with no standard concepts
        concept_rows = [
            "concept_id\tconcept_name\tdomain_id\tvocabulary_id\tconcept_class_id\tstandard_concept\tconcept_code\tvalid_start_date\tvalid_end_date\tinvalid_reason",
            "0\tNon-standard\tMetadata\tNone\tMetadata\t\t0\t19700101\t20991231\t",
        ]
        athena = tmp_path / "athena"
        athena.mkdir()
        (athena / "CONCEPT.csv").write_text("\n".join(concept_rows))

        output = tmp_path / "output"
        with pytest.raises(ValueError, match="No standard concepts found"):
            build_knowledge_layer(athena_path=athena, output_path=output)

    def test_creates_output_directory(self, athena_dir, tmp_path):
        output = tmp_path / "deep" / "nested" / "output"
        paths = build_knowledge_layer(
            athena_path=athena_dir,
            output_path=output,
            backend="bm25s",
        )
        assert output.exists()
        assert Path(paths["bm25s_corpus_path"]).exists()

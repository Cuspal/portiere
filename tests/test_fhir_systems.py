"""Tests for FHIR terminology system URL mappings."""

from portiere.models.fhir_systems import VOCABULARY_SYSTEMS, create_codeable_concept, get_system_url


class TestGetSystemUrl:
    def test_known_vocabularies(self):
        assert get_system_url("SNOMED") == "http://snomed.info/sct"
        assert get_system_url("LOINC") == "http://loinc.org"
        assert get_system_url("RxNorm") == "http://www.nlm.nih.gov/research/umls/rxnorm"
        assert get_system_url("ICD10CM") == "http://hl7.org/fhir/sid/icd-10-cm"
        assert get_system_url("ICD10") == "http://hl7.org/fhir/sid/icd-10"
        assert get_system_url("ICD9CM") == "http://hl7.org/fhir/sid/icd-9-cm"
        assert get_system_url("NDC") == "http://hl7.org/fhir/sid/ndc"
        assert get_system_url("UCUM") == "http://unitsofmeasure.org"
        assert get_system_url("CVX") == "http://hl7.org/fhir/sid/cvx"
        assert get_system_url("CPT4") is not None
        assert get_system_url("ATC") is not None

    def test_unknown_vocabulary_returns_none(self):
        assert get_system_url("UNKNOWN") is None
        assert get_system_url("") is None
        assert get_system_url("custom_vocab") is None

    def test_all_vocabularies_have_https_or_http_urls(self):
        for vocab_id, url in VOCABULARY_SYSTEMS.items():
            assert url.startswith("http"), f"{vocab_id} URL must start with http"


class TestCreateCodeableConcept:
    def test_creates_valid_codeable_concept(self):
        result = create_codeable_concept(
            concept_code="44054006",
            concept_name="Type 2 diabetes mellitus",
            vocabulary_id="SNOMED",
        )
        assert result is not None
        assert "coding" in result
        assert "text" in result
        assert result["text"] == "Type 2 diabetes mellitus"
        coding = result["coding"][0]
        assert coding["system"] == "http://snomed.info/sct"
        assert coding["code"] == "44054006"
        assert coding["display"] == "Type 2 diabetes mellitus"

    def test_returns_none_for_unknown_vocabulary(self):
        result = create_codeable_concept(
            concept_code="12345",
            concept_name="Some concept",
            vocabulary_id="UNKNOWN_VOCAB",
        )
        assert result is None

    def test_loinc_codeable_concept(self):
        result = create_codeable_concept("2093-3", "Cholesterol [Mass/volume]", "LOINC")
        assert result is not None
        assert result["coding"][0]["system"] == "http://loinc.org"

    def test_rxnorm_codeable_concept(self):
        result = create_codeable_concept("860975", "metformin 500 mg", "RxNorm")
        assert result is not None
        assert "rxnorm" in result["coding"][0]["system"].lower()

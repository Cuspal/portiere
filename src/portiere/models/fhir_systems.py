"""
FHIR Terminology System URL mappings.

Maps OMOP vocabulary IDs to FHIR CodeSystem URLs for CodeableConcept generation.
"""

# Vocabulary ID → FHIR CodeSystem URL
VOCABULARY_SYSTEMS = {
    "SNOMED": "http://snomed.info/sct",
    "LOINC": "http://loinc.org",
    "RxNorm": "http://www.nlm.nih.gov/research/umls/rxnorm",
    "ICD10CM": "http://hl7.org/fhir/sid/icd-10-cm",
    "ICD10": "http://hl7.org/fhir/sid/icd-10",
    "ICD9CM": "http://hl7.org/fhir/sid/icd-9-cm",
    "CPT4": "http://www.ama-assn.org/go/cpt",
    "HCPCS": "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets",
    "NDC": "http://hl7.org/fhir/sid/ndc",
    "ATC": "http://www.whocc.no/atc",
    "CVX": "http://hl7.org/fhir/sid/cvx",
    "UCUM": "http://unitsofmeasure.org",
}


def get_system_url(vocabulary_id: str) -> str | None:
    """
    Get FHIR CodeSystem URL for a given OMOP vocabulary ID.

    Args:
        vocabulary_id: OMOP vocabulary ID (e.g., "SNOMED", "LOINC")

    Returns:
        FHIR CodeSystem URL or None if not found

    Example:
        >>> get_system_url("SNOMED")
        'http://snomed.info/sct'

        >>> get_system_url("RxNorm")
        'http://www.nlm.nih.gov/research/umls/rxnorm'
    """
    return VOCABULARY_SYSTEMS.get(vocabulary_id)


def create_codeable_concept(
    concept_code: str,
    concept_name: str,
    vocabulary_id: str,
) -> dict | None:
    """
    Create a FHIR CodeableConcept from OMOP concept information.

    Args:
        concept_code: OMOP concept code
        concept_name: OMOP concept name (display text)
        vocabulary_id: OMOP vocabulary ID

    Returns:
        FHIR CodeableConcept dict or None if vocabulary not mapped

    Example:
        >>> create_codeable_concept(
        ...     concept_code="44054006",
        ...     concept_name="Type 2 diabetes mellitus",
        ...     vocabulary_id="SNOMED"
        ... )
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "44054006",
                    "display": "Type 2 diabetes mellitus"
                }
            ],
            "text": "Type 2 diabetes mellitus"
        }
    """
    system_url = get_system_url(vocabulary_id)
    if not system_url:
        return None

    return {
        "coding": [
            {
                "system": system_url,
                "code": concept_code,
                "display": concept_name,
            }
        ],
        "text": concept_name,
    }

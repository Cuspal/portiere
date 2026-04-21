"""
Portiere VocabularyBridge — Cross-vocabulary concept mapping via Athena relationships.

Uses CONCEPT_RELATIONSHIP.csv from OHDSI Athena downloads to map concepts
between vocabularies (e.g., OMOP → SNOMED, ICD10 → SNOMED, SNOMED → LOINC).

Also uses CONCEPT.csv for concept lookups by ID.

Example:
    >>> from portiere.knowledge.vocabulary_bridge import VocabularyBridge
    >>> bridge = VocabularyBridge("./data/athena/")
    >>> bridge.map_concept(4329847, "SNOMED")
    [{'concept_id': 4329847, 'concept_name': 'Blood pressure', 'vocabulary_id': 'SNOMED', ...}]
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)

# Relationship types used for cross-vocabulary mapping
MAPPING_RELATIONSHIPS = {
    "Maps to",
    "Mapped from",
    "Is a",
    "Subsumes",
}

# Relationship types for equivalence
EQUIVALENCE_RELATIONSHIPS = {
    "Maps to",
    "Mapped from",
}


class VocabularyBridge:
    """
    Cross-vocabulary concept mapping using OHDSI Athena relationships.

    Loads CONCEPT_RELATIONSHIP.csv to build a lookup index for mapping
    concepts between vocabularies. Supports lazy loading and optional
    SQLite caching.
    """

    def __init__(
        self,
        athena_path: str | Path,
        relationship_types: set[str] | None = None,
        vocabularies: list[str] | None = None,
    ):
        """
        Initialize VocabularyBridge.

        Args:
            athena_path: Path to extracted Athena download directory.
                Must contain CONCEPT.csv and CONCEPT_RELATIONSHIP.csv.
            relationship_types: Relationship types to index.
                Defaults to MAPPING_RELATIONSHIPS.
            vocabularies: Optional filter — only index relationships
                involving these vocabularies.
        """
        self._athena_path = Path(athena_path)
        self._relationship_types = relationship_types or MAPPING_RELATIONSHIPS
        self._vocabularies = set(vocabularies) if vocabularies else None

        # Lazy-loaded indexes
        self._concepts: dict[int, dict] = {}
        self._relationships: dict[int, list[dict]] = defaultdict(list)
        self._reverse_relationships: dict[int, list[dict]] = defaultdict(list)
        self._loaded = False

    def _ensure_loaded(self):
        """Lazy-load indexes on first use."""
        if not self._loaded:
            self._load_concepts()
            self._load_relationships()
            self._loaded = True

    def _load_concepts(self):
        """Load CONCEPT.csv into memory for fast lookups."""
        concept_file = self._athena_path / "CONCEPT.csv"
        if not concept_file.exists():
            raise FileNotFoundError(
                f"CONCEPT.csv not found in {self._athena_path}. "
                "Download vocabularies from athena.ohdsi.org."
            )

        logger.info("vocabulary_bridge.loading_concepts", path=str(concept_file))
        count = 0

        with open(concept_file, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                try:
                    concept_id = int(row["concept_id"])
                except (ValueError, KeyError):
                    continue

                vocab_id = row.get("vocabulary_id", "")
                if self._vocabularies and vocab_id not in self._vocabularies:
                    continue

                self._concepts[concept_id] = {
                    "concept_id": concept_id,
                    "concept_name": row.get("concept_name", "").strip(),
                    "vocabulary_id": vocab_id,
                    "domain_id": row.get("domain_id", ""),
                    "concept_class_id": row.get("concept_class_id", ""),
                    "standard_concept": row.get("standard_concept", ""),
                    "concept_code": row.get("concept_code", ""),
                }
                count += 1

        logger.info("vocabulary_bridge.concepts_loaded", count=count)

    def _load_relationships(self):
        """Load CONCEPT_RELATIONSHIP.csv into forward+reverse indexes."""
        rel_file = self._athena_path / "CONCEPT_RELATIONSHIP.csv"
        if not rel_file.exists():
            logger.warning(
                "vocabulary_bridge.no_relationships",
                path=str(rel_file),
            )
            return

        logger.info("vocabulary_bridge.loading_relationships", path=str(rel_file))
        count = 0

        with open(rel_file, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                rel_id = row.get("relationship_id", "")
                if rel_id not in self._relationship_types:
                    continue

                try:
                    cid_1 = int(row["concept_id_1"])
                    cid_2 = int(row["concept_id_2"])
                except (ValueError, KeyError):
                    continue

                # Skip self-references
                if cid_1 == cid_2:
                    continue

                # Only index if both concepts are in our vocabulary filter
                if self._vocabularies:
                    c1 = self._concepts.get(cid_1)
                    c2 = self._concepts.get(cid_2)
                    if not c1 or not c2:
                        continue

                entry = {
                    "concept_id_1": cid_1,
                    "concept_id_2": cid_2,
                    "relationship_id": rel_id,
                }
                self._relationships[cid_1].append(entry)
                self._reverse_relationships[cid_2].append(entry)
                count += 1

        logger.info("vocabulary_bridge.relationships_loaded", count=count)

    # ── Public API ─────────────────────────────────────────

    def get_concept(self, concept_id: int) -> dict | None:
        """
        Lookup a concept by ID.

        Args:
            concept_id: OMOP concept_id

        Returns:
            Concept dict or None if not found.
        """
        self._ensure_loaded()
        return self._concepts.get(concept_id)

    def map_concept(
        self,
        concept_id: int,
        target_vocabulary: str | None = None,
        relationship_types: set[str] | None = None,
    ) -> list[dict]:
        """
        Map a concept to equivalent/related concepts.

        Args:
            concept_id: Source concept ID
            target_vocabulary: Filter target to this vocabulary (e.g., "SNOMED")
            relationship_types: Specific relationship types to use.
                Defaults to EQUIVALENCE_RELATIONSHIPS.

        Returns:
            List of target concept dicts.
        """
        self._ensure_loaded()
        rel_types = relationship_types or EQUIVALENCE_RELATIONSHIPS

        results = []
        for rel in self._relationships.get(concept_id, []):
            if rel["relationship_id"] not in rel_types:
                continue

            target_concept = self._concepts.get(rel["concept_id_2"])
            if not target_concept:
                continue

            if target_vocabulary and target_concept["vocabulary_id"] != target_vocabulary:
                continue

            results.append(
                {
                    **target_concept,
                    "relationship": rel["relationship_id"],
                }
            )

        return results

    def get_crosswalk(
        self,
        source_vocabulary: str,
        target_vocabulary: str,
        relationship_types: set[str] | None = None,
    ) -> list[dict]:
        """
        Build a full crosswalk between two vocabularies.

        Args:
            source_vocabulary: Source vocabulary ID (e.g., "ICD10CM")
            target_vocabulary: Target vocabulary ID (e.g., "SNOMED")
            relationship_types: Specific relationship types.
                Defaults to EQUIVALENCE_RELATIONSHIPS.

        Returns:
            List of mapping dicts with source and target concept info.
        """
        self._ensure_loaded()
        rel_types = relationship_types or EQUIVALENCE_RELATIONSHIPS
        crosswalk = []

        for concept_id, concept in self._concepts.items():
            if concept["vocabulary_id"] != source_vocabulary:
                continue

            targets = self.map_concept(
                concept_id,
                target_vocabulary=target_vocabulary,
                relationship_types=rel_types,
            )
            for target in targets:
                crosswalk.append(
                    {
                        "source_concept_id": concept_id,
                        "source_concept_name": concept["concept_name"],
                        "source_concept_code": concept.get("concept_code", ""),
                        "source_vocabulary_id": source_vocabulary,
                        "target_concept_id": target["concept_id"],
                        "target_concept_name": target["concept_name"],
                        "target_concept_code": target.get("concept_code", ""),
                        "target_vocabulary_id": target_vocabulary,
                        "relationship": target["relationship"],
                    }
                )

        return crosswalk

    def concept_to_codeable_concept(self, concept_id: int) -> dict:
        """
        Convert an OMOP concept_id to a FHIR CodeableConcept structure.

        Looks up the concept and constructs a CodeableConcept with system
        URL derived from vocabulary_id.

        Args:
            concept_id: OMOP concept ID

        Returns:
            FHIR CodeableConcept dict
        """
        self._ensure_loaded()
        concept = self._concepts.get(concept_id)
        if not concept:
            return {"text": str(concept_id)}

        # Map vocabulary to FHIR system URL
        system = VOCABULARY_TO_SYSTEM.get(concept["vocabulary_id"], "")

        return {
            "coding": [
                {
                    "system": system,
                    "code": concept.get("concept_code", str(concept_id)),
                    "display": concept["concept_name"],
                }
            ],
            "text": concept["concept_name"],
        }

    def concept_to_dv_coded_text(self, concept_id: int) -> dict:
        """
        Convert an OMOP concept_id to an openEHR DV_CODED_TEXT structure.

        Args:
            concept_id: OMOP concept ID

        Returns:
            openEHR DV_CODED_TEXT dict
        """
        self._ensure_loaded()
        concept = self._concepts.get(concept_id)
        if not concept:
            return {
                "_type": "DV_CODED_TEXT",
                "value": str(concept_id),
                "defining_code": {
                    "terminology_id": {"value": "local"},
                    "code_string": str(concept_id),
                },
            }

        terminology = VOCABULARY_TO_TERMINOLOGY.get(
            concept["vocabulary_id"], concept["vocabulary_id"]
        )

        return {
            "_type": "DV_CODED_TEXT",
            "value": concept["concept_name"],
            "defining_code": {
                "terminology_id": {"value": terminology},
                "code_string": concept.get("concept_code", str(concept_id)),
            },
        }

    def stats(self) -> dict:
        """Return summary statistics about loaded data."""
        self._ensure_loaded()
        return {
            "concepts": len(self._concepts),
            "forward_relationships": sum(len(v) for v in self._relationships.values()),
            "reverse_relationships": sum(len(v) for v in self._reverse_relationships.values()),
            "vocabularies": sorted(set(c["vocabulary_id"] for c in self._concepts.values())),
        }


# ── Vocabulary → FHIR system URL mappings ──────────────────

VOCABULARY_TO_SYSTEM = {
    "SNOMED": "http://snomed.info/sct",
    "LOINC": "http://loinc.org",
    "RxNorm": "http://www.nlm.nih.gov/research/umls/rxnorm",
    "ICD10CM": "http://hl7.org/fhir/sid/icd-10-cm",
    "ICD10PCS": "http://www.cms.gov/Medicare/Coding/ICD10",
    "ICD9CM": "http://hl7.org/fhir/sid/icd-9-cm",
    "CPT4": "http://www.ama-assn.org/go/cpt",
    "HCPCS": "https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets",
    "NDC": "http://hl7.org/fhir/sid/ndc",
    "CVX": "http://hl7.org/fhir/sid/cvx",
    "Gender": "http://hl7.org/fhir/administrative-gender",
    "Race": "http://terminology.hl7.org/CodeSystem/v3-Race",
    "Ethnicity": "http://terminology.hl7.org/CodeSystem/v3-Ethnicity",
    "UCUM": "http://unitsofmeasure.org",
}

VOCABULARY_TO_TERMINOLOGY = {
    "SNOMED": "SNOMED-CT",
    "LOINC": "LOINC",
    "RxNorm": "RxNorm",
    "ICD10CM": "ICD10-CM",
    "ICD10PCS": "ICD10-PCS",
    "ICD9CM": "ICD9-CM",
    "CPT4": "CPT4",
    "HCPCS": "HCPCS",
    "NDC": "NDC",
    "UCUM": "UCUM",
}

"""
Portiere Concept Mapping Model — Local codes to standard concepts.

This is the core value proposition of Portiere:
- Map hospital drug codes to RxNorm
- Map local lab codes to LOINC
- Map diagnosis codes to SNOMED CT / ICD-10
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, Field, model_validator

from portiere.models.mapping_policy import (
    ConceptReviewDecision,
    concept_is_approved,
    concept_is_executable,
)

if TYPE_CHECKING:
    from portiere.models.project import Project

logger = structlog.get_logger(__name__)


class ConceptMappingMethod(str, Enum):
    """Method used to determine the mapping."""

    AUTO = "auto"  # High confidence, auto-accepted
    REVIEW = "review"  # Medium confidence, needs human review
    MANUAL = "manual"  # Low confidence, requires manual mapping
    OVERRIDE = "override"  # Human selected different concept
    UNMAPPED = "unmapped"  # No suitable match found


class ConceptCandidate(BaseModel):
    """A candidate concept for mapping."""

    concept_id: int
    concept_name: str
    vocabulary_id: str
    domain_id: str
    concept_class_id: str
    standard_concept: str
    score: float = 0.0
    rrf_score: float | None = None
    cross_encoder_score: float | None = None


class ConceptMappingItem(BaseModel):
    """A single concept mapping from source code to standard concept."""

    # Source (local code from hospital system)
    source_code: str
    source_description: str | None = None
    source_column: str | None = None
    source_count: int = 1  # How often this code appears

    # Target (standard concept)
    target_concept_id: int | None = None
    target_concept_name: str | None = None
    target_vocabulary_id: str | None = None
    target_domain_id: str | None = None

    # AI inference
    confidence: float = 0.0
    method: ConceptMappingMethod = ConceptMappingMethod.REVIEW
    inference_method: ConceptMappingMethod | None = None
    review_decision: ConceptReviewDecision | None = None

    # All candidates considered
    candidates: list[ConceptCandidate] = Field(default_factory=list)

    # Provenance (audit trail)
    provenance: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def _preserve_inference_method(self):
        if self.inference_method is None:
            self.inference_method = self.method
        return self

    @property
    def is_mapped(self) -> bool:
        """Check if this item has a valid mapping."""
        return self.target_concept_id is not None

    @property
    def approved(self) -> bool:
        """Check if this item has been approved (AUTO or OVERRIDE)."""
        return concept_is_approved(self)

    @property
    def rejected(self) -> bool:
        """Check if this item has been rejected (UNMAPPED)."""
        if self.review_decision is not None:
            return self.review_decision == ConceptReviewDecision.REJECTED
        return self.method == ConceptMappingMethod.UNMAPPED

    def approve(self, candidate_index: int = 0):
        """Approve this mapping using the specified candidate."""
        if candidate_index < 0 or candidate_index >= max(1, len(self.candidates)):
            raise ValueError("Invalid candidate index; select an available candidate.")
        if self.candidates and candidate_index < len(self.candidates):
            candidate = self.candidates[candidate_index]
            self.target_concept_id = candidate.concept_id
            self.target_concept_name = candidate.concept_name
            self.target_vocabulary_id = candidate.vocabulary_id
            self.target_domain_id = candidate.domain_id
            self.method = (
                ConceptMappingMethod.OVERRIDE if candidate_index > 0 else ConceptMappingMethod.AUTO
            )
        else:
            # No candidates — approve with current target
            self.method = ConceptMappingMethod.AUTO
        self.review_decision = (
            ConceptReviewDecision.OVERRIDDEN
            if candidate_index > 0
            else ConceptReviewDecision.APPROVED
        )

    def reject(self):
        """Reject this mapping (mark as unmapped)."""
        self.mark_unmapped()

    def override(self, concept_id: int, concept_name: str = "", vocabulary_id: str = ""):
        """Override with a specific concept ID."""
        self.target_concept_id = concept_id
        self.target_concept_name = concept_name
        self.target_vocabulary_id = vocabulary_id
        self.method = ConceptMappingMethod.OVERRIDE
        self.review_decision = ConceptReviewDecision.OVERRIDDEN

    def mark_unmapped(self):
        """Mark this code as unmapped (no suitable match)."""
        self.method = ConceptMappingMethod.UNMAPPED
        self.review_decision = ConceptReviewDecision.REJECTED


class ConceptMapping(BaseModel):
    """
    Collection of concept mappings for a source.

    Concept mapping translates local clinical codes to standard
    vocabularies (SNOMED CT, LOINC, RxNorm, ICD-10, etc.).
    """

    model_config = {"arbitrary_types_allowed": True}

    items: list[ConceptMappingItem] = Field(default_factory=list)
    source_id: str | None = None
    revision: str | None = None

    # Internal references
    project: Project | None = Field(default=None, exclude=True)
    source: object | None = Field(default=None, exclude=True)
    finalized: bool = False

    @classmethod
    def from_api_response(cls, response: dict, project: Project) -> ConceptMapping:
        """Create ConceptMapping from API response."""
        items = []
        for item_data in response.get("items", []):
            candidates = [ConceptCandidate(**c) for c in item_data.get("candidates", [])]
            items.append(
                ConceptMappingItem(
                    source_code=item_data["source_code"],
                    source_description=item_data.get("source_description"),
                    source_column=item_data.get("source_column"),
                    target_concept_id=item_data.get("target_concept_id"),
                    target_concept_name=item_data.get("target_concept_name"),
                    target_vocabulary_id=item_data.get("target_vocabulary_id"),
                    target_domain_id=item_data.get("target_domain_id"),
                    confidence=item_data.get("confidence", 0.0),
                    method=ConceptMappingMethod(item_data.get("method", "review")),
                    inference_method=item_data.get("inference_method"),
                    review_decision=item_data.get("review_decision"),
                    candidates=candidates,
                    provenance=item_data.get("provenance", {}),
                )
            )
        return cls(items=items, project=project)

    @property
    def review_url(self) -> str | None:
        """URL for web-based review UI (Portiere Cloud only)."""
        return None

    def needs_review(self) -> list[ConceptMappingItem]:
        """Return items that need human review."""
        return [
            item
            for item in self.items
            if item.review_decision == ConceptReviewDecision.PENDING
            or (item.review_decision is None and item.method == ConceptMappingMethod.REVIEW)
        ]

    def auto_mapped(self) -> list[ConceptMappingItem]:
        """Return items that were auto-mapped."""
        return [item for item in self.items if item.method == ConceptMappingMethod.AUTO]

    def unmapped(self) -> list[ConceptMappingItem]:
        """Return items that require manual mapping."""
        return [
            item
            for item in self.items
            if item.method in (ConceptMappingMethod.MANUAL, ConceptMappingMethod.UNMAPPED)
        ]

    def get_item(self, source_code: str) -> ConceptMappingItem:
        """Find an item by source code."""
        for item in self.items:
            if item.source_code == source_code:
                return item
        raise KeyError(f"No mapping found for source code '{source_code}'")

    def approve(self, source_code: str, candidate_index: int = 0) -> None:
        """Approve a mapping by source code."""
        item = self.get_item(source_code)
        item.approve(candidate_index=candidate_index)

    def reject(self, source_code: str) -> None:
        """Reject a mapping by source code."""
        item = self.get_item(source_code)
        item.reject()

    def override(
        self,
        source_code: str,
        concept_id: int,
        concept_name: str = "",
        vocabulary_id: str = "",
    ) -> None:
        """Override a mapping with a specific concept."""
        item = self.get_item(source_code)
        item.override(concept_id, concept_name, vocabulary_id)

    def approve_all(self):
        """Approve all items that need review (using first candidate)."""
        for item in self.needs_review():
            if item.candidates:
                item.approve(candidate_index=0)

    @property
    def _engine(self):
        """Return the project's engine if available."""
        if self.project and hasattr(self.project, "_engine") and self.project._engine:
            return self.project._engine
        return None

    def _review_records(self) -> list[dict]:
        """Flat review rows shared by CSV and dataframe exports."""
        rows = []
        for item in self.items:
            row = {
                "mapping_source_id": self.source_id,
                "mapping_revision": self.revision,
                "source_code": item.source_code,
                "source_description": item.source_description,
                "source_column": item.source_column,
                "source_count": item.source_count,
                "target_concept_id": item.target_concept_id,
                "target_concept_name": item.target_concept_name,
                "target_vocabulary_id": item.target_vocabulary_id,
                "target_domain_id": item.target_domain_id,
                "confidence": item.confidence,
                "method": item.method.value,
                "inference_method": item.inference_method.value if item.inference_method else None,
                "review_decision": item.review_decision.value if item.review_decision else None,
            }
            rows.append(row)
        return rows

    def to_dataframe(self):
        """Export mappings as a DataFrame using the project's engine."""
        rows = self._review_records()
        if self._engine:
            return self._engine.from_records(rows)

        import pandas as pd

        return pd.DataFrame(rows)

    def to_csv(self, path: str) -> None:
        """Export mappings to CSV for review."""
        from pathlib import Path

        from portiere.models._mapping_csv import write_mapping_records

        if not self.items:
            raise ValueError("Use JSON to export an empty mapping with its source and revision.")
        if self._engine and Path(path).suffix.lower() not in {".gz", ".bz2", ".xz", ".zip"}:
            self._engine.write_csv(self.to_dataframe(), path)
        else:
            rows = self._review_records()
            write_mapping_records(path, rows, list(rows[0]))

    def to_json(self, path: str) -> None:
        """Export mappings to JSON file."""
        import json

        data = {"format_version": 1, **self.model_dump(mode="json")}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def from_json(cls, path: str) -> ConceptMapping:
        """Import mappings from a JSON file."""
        import json

        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return cls(items=data)
        if data.get("format_version", 1) != 1:
            raise ValueError("Unsupported concept mapping format_version")
        return cls.model_validate(data)

    @classmethod
    def _items_from_records(cls, records: list[dict]) -> list[ConceptMappingItem]:
        """
        Build ConceptMappingItem list from record dicts.

        Handles column aliases (vocabulary_id ↔ target_vocabulary_id,
        domain_id ↔ target_domain_id) and NaN/None values.
        """
        import math

        def _clean(val):
            """Return None for NaN/empty values."""
            if val is None:
                return None
            if isinstance(val, float) and math.isnan(val):
                return None
            if isinstance(val, str) and val.strip() == "":
                return None
            return val

        items = []
        for r in records:
            if "source_code" not in r:
                raise ValueError(
                    f"Each record must have a 'source_code' column. Found columns: {list(r.keys())}"
                )

            # Handle column aliases
            vocab_id = _clean(r.get("target_vocabulary_id") or r.get("vocabulary_id"))
            domain_id = _clean(r.get("target_domain_id") or r.get("domain_id"))

            target_id = _clean(r.get("target_concept_id"))
            # Let Pydantic validate integer identity, including integral CSV
            # floats ("123.0") emitted by pandas for nullable integer columns.

            confidence = _clean(r.get("confidence"))
            confidence = float(confidence) if confidence is not None else 0.0

            method_val = _clean(r.get("method"))
            method = ConceptMappingMethod(method_val) if method_val else ConceptMappingMethod.REVIEW

            items.append(
                ConceptMappingItem(
                    source_code=str(r["source_code"]),
                    source_description=_clean(r.get("source_description")),
                    source_column=_clean(r.get("source_column")),
                    source_count=int(r.get("source_count", 1)),
                    target_concept_id=target_id,
                    target_concept_name=_clean(r.get("target_concept_name")),
                    target_vocabulary_id=vocab_id,
                    target_domain_id=domain_id,
                    confidence=confidence,
                    method=method,
                    inference_method=_clean(r.get("inference_method")),
                    review_decision=_clean(r.get("review_decision")),
                )
            )
        return items

    @classmethod
    def _dataframe_to_records(cls, df) -> list[dict]:
        """Convert any DataFrame (Pandas, Polars, Spark) to list of dicts."""
        df_type = type(df).__module__.split(".")[0]

        if df_type == "polars":
            return df.to_dicts()
        elif df_type == "pyspark":
            return df.toPandas().to_dict("records")
        else:
            # Pandas or pandas-like
            return df.to_dict("records")

    @classmethod
    def from_csv(cls, path: str, *, engine=None) -> ConceptMapping:
        """
        Import mappings from a CSV file.

        Accepts CSVs with standard column names (source_code, target_concept_id,
        etc.) or aliases (vocabulary_id for target_vocabulary_id).

        Args:
            path: Path to CSV file.
            engine: Optional compute engine for its supported CSV transports.
        """
        from portiere.models._mapping_csv import read_mapping_records

        records = read_mapping_records(path, engine=engine)

        return cls.from_records(records)

    @classmethod
    def from_dataframe(cls, df) -> ConceptMapping:
        """
        Import mappings from a DataFrame (Pandas, Polars, or Spark).

        The DataFrame must have at least a ``source_code`` column.
        """
        records = cls._dataframe_to_records(df)
        return cls.from_records(records)

    @classmethod
    def from_records(cls, records: list[dict]) -> ConceptMapping:
        """
        Import mappings from a list of dicts.

        Each dict must have at least a ``source_code`` key.
        """
        import math

        context = {}
        for field in ("source_id", "revision"):
            values = set()
            for record in records:
                value = record.get(f"mapping_{field}")
                if value == "" or (isinstance(value, float) and math.isnan(value)):
                    value = None
                values.add(value)
            if len(values) > 1:
                raise ValueError(f"Inconsistent mapping {field} metadata across rows.")
            context[field] = next(iter(values), None)
        items = cls._items_from_records(records)
        return cls(items=items, source_id=context["source_id"], revision=context["revision"])

    def finalize(self):
        """Finalize the concept mapping."""
        if not self.finalized:
            pending = self.needs_review()
            if pending:
                logger.warning(
                    f"{len(pending)} items still need review. "
                    "Call approve_all() or review individual items first."
                )

            self.finalized = True

    def summary(self) -> dict:
        """Return summary statistics."""
        total = len(self.items)
        auto = len(self.auto_mapped())
        review = len(self.needs_review())
        manual = len(self.unmapped())

        return {
            "total": total,
            "auto_mapped": auto,
            "needs_review": review,
            "manual_required": manual,
            "auto_rate": auto / total * 100 if total > 0 else 0,
            "coverage": (auto + review) / total * 100 if total > 0 else 0,
        }

    def to_source_to_concept_map(self) -> list[dict]:
        """
        Export as OMOP source_to_concept_map format.

        Returns data suitable for loading into the OMOP
        source_to_concept_map table.
        """
        rows = []
        for item in self.items:
            if concept_is_executable(item):
                rows.append(
                    {
                        "source_code": item.source_code,
                        "source_concept_id": 0,  # Placeholder
                        "source_vocabulary_id": item.source_column or "Hospital",
                        "source_description": item.source_description or "",
                        "source_code_description": item.source_description,
                        "target_concept_id": item.target_concept_id,
                        "target_concept_name": item.target_concept_name or "",
                        "target_vocabulary_id": item.target_vocabulary_id,
                        "confidence": item.confidence,
                        "method": item.method.value,
                        "inference_method": item.inference_method.value
                        if item.inference_method
                        else None,
                        "review_decision": item.review_decision.value
                        if item.review_decision
                        else None,
                        "valid_start_date": "1970-01-01",
                        "valid_end_date": "2099-12-31",
                        "invalid_reason": None,
                    }
                )
        return rows

    def __repr__(self) -> str:
        stats = self.summary()
        return (
            f"ConceptMapping(total={stats['total']}, "
            f"auto={stats['auto_mapped']} ({stats['auto_rate']:.0f}%), "
            f"review={stats['needs_review']}, "
            f"manual={stats['manual_required']})"
        )

"""
Portiere Schema Mapping Model — Source to target schema mapping.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, Field, field_validator

if TYPE_CHECKING:
    from portiere.models.project import Project

logger = structlog.get_logger(__name__)


class MappingStatus(str, Enum):
    """Status of a mapping item."""

    AUTO_ACCEPTED = "auto_accepted"
    NEEDS_REVIEW = "needs_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    OVERRIDDEN = "overridden"
    UNMAPPED = "unmapped"


class SchemaMappingItem(BaseModel):
    """A single schema mapping from source to target."""

    # Source
    source_column: str
    source_table: str = ""

    @field_validator("source_table", "source_column", mode="before")
    @classmethod
    def _coerce_nan_to_str(cls, v: object) -> str:
        """Coerce NaN/None from CSV round-trips to empty string."""
        if v is None or (isinstance(v, float) and v != v):
            return ""
        return str(v)

    # Target
    target_table: str | None = None
    target_column: str | None = None

    @field_validator("target_table", "target_column", mode="before")
    @classmethod
    def _coerce_nan_to_none(cls, v: object) -> str | None:
        """Coerce NaN/empty from CSV round-trips to None."""
        if v is None or (isinstance(v, float) and v != v):
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return str(v)

    # AI inference
    confidence: float = 0.0
    status: MappingStatus = MappingStatus.NEEDS_REVIEW

    # Candidates (for review)
    candidates: list[dict] = Field(default_factory=list)

    # Override (if user selected different target)
    override_target_table: str | None = None
    override_target_column: str | None = None

    @property
    def effective_target_table(self) -> str | None:
        """Return the effective target table (override or AI suggestion)."""
        return self.override_target_table or self.target_table

    @property
    def effective_target_column(self) -> str | None:
        """Return the effective target column (override or AI suggestion)."""
        return self.override_target_column or self.target_column

    def approve(self, target_table: str | None = None, target_column: str | None = None):
        """Approve this mapping, optionally with an override."""
        if target_table:
            self.override_target_table = target_table
            self.override_target_column = target_column
            self.status = MappingStatus.OVERRIDDEN
        else:
            self.status = MappingStatus.APPROVED

    def reject(self):
        """Reject this mapping."""
        self.status = MappingStatus.REJECTED


class SchemaMapping(BaseModel):
    """
    Collection of schema mappings for a source.

    Schema mapping aligns source table/column structures to
    target data model entities (e.g., OMOP CDM tables).
    """

    model_config = {"arbitrary_types_allowed": True}

    items: list[SchemaMappingItem] = Field(default_factory=list)

    # Internal references
    project: Project | None = Field(default=None, exclude=True)
    source: object | None = Field(default=None, exclude=True)  # Source model
    finalized: bool = False

    @classmethod
    def from_api_response(cls, response: dict, project: Project) -> SchemaMapping:
        """Create SchemaMapping from API response."""
        # API suggest endpoint returns "mappings", project endpoints return "items"
        raw_items = response.get("mappings", response.get("items", []))
        items = []
        for item in raw_items:
            confidence = item.get("confidence", 0.0)
            if confidence >= 0.95:
                status = MappingStatus.AUTO_ACCEPTED
            elif confidence >= 0.70:
                status = MappingStatus.NEEDS_REVIEW
            else:
                status = MappingStatus(item.get("status", "needs_review"))
            items.append(
                SchemaMappingItem(
                    source_table=item.get("source_table", ""),
                    source_column=item["source_column"],
                    target_table=item.get("target_table"),
                    target_column=item.get("target_column"),
                    confidence=confidence,
                    status=status,
                    candidates=item.get("candidates", []),
                )
            )
        return cls(items=items, project=project)

    @property
    def review_url(self) -> str | None:
        """URL for web-based review UI (Portiere Cloud only)."""
        return None

    def needs_review(self) -> list[SchemaMappingItem]:
        """Return items that need human review."""
        return [item for item in self.items if item.status == MappingStatus.NEEDS_REVIEW]

    def auto_accepted(self) -> list[SchemaMappingItem]:
        """Return items that were auto-accepted."""
        return [item for item in self.items if item.status == MappingStatus.AUTO_ACCEPTED]

    def rejected(self) -> list[SchemaMappingItem]:
        """Return items that were rejected."""
        return [item for item in self.items if item.status == MappingStatus.REJECTED]

    def overridden(self) -> list[SchemaMappingItem]:
        """Return items that were overridden."""
        return [item for item in self.items if item.status == MappingStatus.OVERRIDDEN]

    def get_item(self, source_column: str) -> SchemaMappingItem:
        """Find an item by source column name."""
        for item in self.items:
            if item.source_column == source_column:
                return item
        raise KeyError(f"No mapping found for source column '{source_column}'")

    def approve(self, source_column: str) -> None:
        """Approve a mapping by source column name."""
        item = self.get_item(source_column)
        item.approve()

    def reject(self, source_column: str) -> None:
        """Reject a mapping by source column name."""
        item = self.get_item(source_column)
        item.reject()

    def override(self, source_column: str, target_table: str, target_column: str) -> None:
        """Override a mapping with a custom target."""
        item = self.get_item(source_column)
        item.approve(target_table=target_table, target_column=target_column)

    def approve_all(self):
        """Approve all items that need review."""
        for item in self.needs_review():
            item.approve()

    @property
    def _engine(self):
        """Return the project's engine if available."""
        if self.project and hasattr(self.project, "_engine") and self.project._engine:
            return self.project._engine
        return None

    def to_dataframe(self):
        """Export mappings as a DataFrame using the project's engine."""
        rows = []
        for item in self.items:
            row = {
                "source_column": item.source_column,
                "source_table": item.source_table,
                "target_table": item.effective_target_table,
                "target_column": item.effective_target_column,
                "confidence": item.confidence,
                "status": item.status.value,
            }
            rows.append(row)

        if self._engine:
            return self._engine.from_records(rows)

        import pandas as pd

        return pd.DataFrame(rows)

    def to_csv(self, path: str) -> None:
        """Export mappings to CSV for review."""
        df = self.to_dataframe()
        if self._engine:
            self._engine.write_csv(df, path)
        else:
            df.to_csv(path, index=False)

    @classmethod
    def from_csv(cls, path: str, *, engine=None) -> SchemaMapping:
        """Import mappings from a reviewed CSV file."""
        if engine:
            df = engine.read_csv(path)
            records = engine.to_dict_records(df, limit=999999)
        else:
            import pandas as pd

            df = pd.read_csv(path)
            records = df.to_dict("records")

        items = []
        for row in records:
            items.append(
                SchemaMappingItem(
                    source_column=row["source_column"],
                    source_table=row.get("source_table", ""),
                    target_table=row.get("target_table"),
                    target_column=row.get("target_column"),
                    confidence=float(row.get("confidence", 0.0)),
                    status=MappingStatus(row.get("status", "needs_review")),
                )
            )
        return cls(items=items)

    def finalize(self):
        """Finalize the schema mapping."""
        if not self.finalized:
            # Check if any items still need review
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
        auto = len([i for i in self.items if i.status == MappingStatus.AUTO_ACCEPTED])
        review = len([i for i in self.items if i.status == MappingStatus.NEEDS_REVIEW])
        approved = len([i for i in self.items if i.status == MappingStatus.APPROVED])
        unmapped = len([i for i in self.items if i.status == MappingStatus.UNMAPPED])

        return {
            "total": total,
            "auto_accepted": auto,
            "needs_review": review,
            "approved": approved,
            "unmapped": unmapped,
            "auto_rate": auto / total * 100 if total > 0 else 0,
        }

    def __repr__(self) -> str:
        stats = self.summary()
        return (
            f"SchemaMapping(total={stats['total']}, "
            f"auto={stats['auto_accepted']}, "
            f"review={stats['needs_review']})"
        )

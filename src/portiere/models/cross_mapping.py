"""
Portiere Cross Mapping Model — Track cross-standard mapping runs.

Cross-mapping is deterministic (YAML-based transforms), so there is no
approval workflow. We track *runs* — execution records that capture what
was mapped, how many records, and which crossmap file was used.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class CrossMappingRun(BaseModel):
    """A single cross-mapping execution."""

    source_standard: str  # e.g., "omop_cdm_v5.4"
    target_standard: str  # e.g., "fhir_r4"
    source_entity: str  # e.g., "person"
    target_entity: str | None = None  # e.g., "Patient"
    record_count: int = 0
    status: str = "completed"  # completed, failed
    crossmap_file: str | None = None  # e.g., "omop_to_fhir_r4.yaml"
    created_at: str | None = None


class CrossMapping(BaseModel):
    """Collection of cross-mapping runs for a project."""

    runs: list[CrossMappingRun] = Field(default_factory=list)

    def summary(self) -> dict:
        """Return summary statistics."""
        total_runs = len(self.runs)
        total_records = sum(r.record_count for r in self.runs)
        standards_used = set()
        for r in self.runs:
            standards_used.add(f"{r.source_standard} → {r.target_standard}")
        return {
            "total_runs": total_runs,
            "total_records": total_records,
            "standard_pairs": sorted(standards_used),
        }

    def __repr__(self) -> str:
        stats = self.summary()
        return f"CrossMapping(runs={stats['total_runs']}, records={stats['total_records']})"

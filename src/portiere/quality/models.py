"""
Quality report dataclasses for profiling and validation results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ProfileReport:
    """Result of GX-based data profiling."""

    source_name: str
    columns: list[dict[str, Any]]
    gx_result: dict[str, Any]
    expectations: dict[str, Any]
    row_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "columns": self.columns,
            "gx_result": self.gx_result,
            "expectations": self.expectations,
            "row_count": self.row_count,
            "created_at": self.created_at,
        }


@dataclass
class ValidationReport:
    """Result of GX-based post-ETL validation."""

    table_name: str
    passed: bool
    completeness_score: float
    conformance_score: float
    plausibility_score: float
    gx_result: dict[str, Any]
    thresholds: dict[str, float]
    created_at: str = field(default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "passed": self.passed,
            "completeness_score": self.completeness_score,
            "conformance_score": self.conformance_score,
            "plausibility_score": self.plausibility_score,
            "gx_result": self.gx_result,
            "thresholds": self.thresholds,
            "created_at": self.created_at,
        }

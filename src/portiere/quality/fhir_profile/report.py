"""Validation report models for FHIR profile checks."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, computed_field


class ResourceFailure(BaseModel):
    resource_type: str
    resource_index: int
    invariant_id: str
    message: str
    severity: Literal["error", "warning"] = "error"


class ProfileValidationReport(BaseModel):
    profile: str
    total_resources: int = 0
    failures: list[ResourceFailure] = []
    skipped: list[str] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def passed(self) -> bool:
        return all(f.severity != "error" for f in self.failures)

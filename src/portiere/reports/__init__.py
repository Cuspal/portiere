"""Portiere Reports — styled Excel deliverables for the mapping workflow.

Generated replacements for the workbooks OMOP conversion teams hand-build:
- :func:`build_profile_workbook` — White-Rabbit-parity data scan
- :func:`build_schema_workbook` — per-target-table mapping working document
- :func:`build_concept_workbook` — OMOP source_to_concept_map + status

Requires the ``xlsx`` extra: ``pip install "portiere-health[xlsx]"``.
"""

from portiere.reports.concept_workbook import build_concept_workbook
from portiere.reports.profile_workbook import build_profile_workbook
from portiere.reports.schema_workbook import build_schema_workbook

__all__ = [
    "build_concept_workbook",
    "build_profile_workbook",
    "build_schema_workbook",
]

"""
Portiere Quality — Data quality and profiling using Great Expectations.

Provides:
- GXProfiler: Data profiling for source data
- GXValidator: Post-ETL validation against target model expectations
- ProfileReport / ValidationReport: Result dataclasses
- SourceProfile / ColumnProfile + build_source_profile / export_profile_report:
  aggregate engine profiles into an HTML + CSV data-profile report
"""

from portiere.quality.models import ProfileReport, ValidationReport
from portiere.quality.profile_report import (
    ColumnProfile,
    SourceProfile,
    build_source_profile,
    export_profile_report,
)
from portiere.quality.profiler import GXProfiler
from portiere.quality.validator import GXValidator

__all__ = [
    "ColumnProfile",
    "GXProfiler",
    "GXValidator",
    "ProfileReport",
    "SourceProfile",
    "ValidationReport",
    "build_source_profile",
    "export_profile_report",
]

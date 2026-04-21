"""
Portiere Quality — Data quality and profiling using Great Expectations.

Provides:
- GXProfiler: Data profiling for source data
- GXValidator: Post-ETL validation against target model expectations
- ProfileReport / ValidationReport: Result dataclasses
"""

from portiere.quality.models import ProfileReport, ValidationReport
from portiere.quality.profiler import GXProfiler
from portiere.quality.validator import GXValidator

__all__ = ["GXProfiler", "GXValidator", "ProfileReport", "ValidationReport"]

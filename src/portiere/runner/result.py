"""
Portiere ETL Runner — Result models for ETL execution.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class TableResult(BaseModel):
    """Result for a single target table produced by ETL."""

    table_name: str
    rows_written: int = 0
    columns: list[str] = Field(default_factory=list)
    output_path: str = ""
    concept_columns_mapped: list[str] = Field(default_factory=list)
    unmapped_concept_count: int = 0


class ETLResult(BaseModel):
    """Result of an ETL pipeline execution."""

    success: bool = False
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float = 0.0
    source_path: str = ""
    source_rows_read: int = 0
    output_path: str = ""
    tables: list[TableResult] = Field(default_factory=list)
    total_rows_written: int = 0
    schema_mappings_applied: int = 0
    concept_mappings_applied: int = 0
    unmapped_columns: list[str] = Field(default_factory=list)
    engine_name: str = ""
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    def summary(self) -> str:
        """Return a human-readable summary of the ETL result."""
        lines = []
        status = "SUCCESS" if self.success else "FAILED"
        lines.append(f"ETL Result: {status}")
        lines.append(f"  Engine: {self.engine_name}")
        lines.append(f"  Duration: {self.duration_seconds:.1f}s")
        lines.append(f"  Source: {self.source_path} ({self.source_rows_read} rows)")
        lines.append(f"  Output: {self.output_path}")
        lines.append(f"  Tables written: {len(self.tables)}")

        for table in self.tables:
            concept_info = ""
            if table.concept_columns_mapped:
                concept_info = f", concepts: {len(table.concept_columns_mapped)} cols"
            lines.append(
                f"    - {table.table_name}: {table.rows_written} rows, "
                f"{len(table.columns)} cols{concept_info}"
            )

        lines.append(f"  Total rows written: {self.total_rows_written}")
        lines.append(f"  Schema mappings applied: {self.schema_mappings_applied}")
        lines.append(f"  Concept mappings applied: {self.concept_mappings_applied}")

        if self.unmapped_columns:
            lines.append(f"  Unmapped columns: {', '.join(self.unmapped_columns)}")

        if self.warnings:
            lines.append(f"  Warnings: {len(self.warnings)}")
            for w in self.warnings:
                lines.append(f"    - {w}")

        if self.errors:
            lines.append(f"  Errors: {len(self.errors)}")
            for e in self.errors:
                lines.append(f"    - {e}")

        return "\n".join(lines)

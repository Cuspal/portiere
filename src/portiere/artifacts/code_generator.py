"""
Portiere Code Generator — Generates standalone ETL scripts from finalized mappings.

Translates approved schema and concept mappings into executable ETL code
that can run without the Portiere SDK installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from jinja2 import Environment, PackageLoader, select_autoescape

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)


class CodeGenerator:
    """
    Generates standalone ETL scripts from finalized mappings.

    Supports multiple compute engines (Polars, PySpark, Pandas)
    and produces scripts that require only the target engine library.
    """

    def __init__(self) -> None:
        self._jinja: Environment | None = None
        try:
            self._jinja = Environment(
                loader=PackageLoader("portiere.artifacts", "templates"),
                autoescape=select_autoescape(["html", "xml"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        except Exception:
            self._jinja = None
            logger.warning("Jinja2 templates not found, code generation will use fallback mode")

    def generate_etl_script(
        self,
        engine_type: str,
        schema_mappings: list[dict],
        concept_mappings: list[dict],
        project_name: str = "portiere_project",
        target_model: str = "omop_cdm_v5.4",
        source_path: str = "source_data.csv",
        output_path: str = "omop_output.parquet",
    ) -> str:
        """
        Generate a complete ETL transformation script.

        Args:
            engine_type: Target engine (polars, spark, pandas)
            schema_mappings: List of {source_column, target_table, target_column, ...}
            concept_mappings: List of {source_code, target_concept_id, ...}
            project_name: Project name for script header
            target_model: Target data model
            source_path: Default source data path
            output_path: Default output path

        Returns:
            Generated Python script as a string
        """
        template_name = f"{engine_type}_etl.py.j2"

        # Determine which columns have concept mappings
        concept_columns = list(
            {m.get("source_column", "") for m in concept_mappings if m.get("source_column")}
        )

        context = {
            "project_name": project_name,
            "target_model": target_model,
            "source_path": source_path,
            "output_path": output_path,
            "schema_mappings": schema_mappings,
            "concept_mappings": concept_mappings,
            "concept_columns": concept_columns,
        }

        if self._jinja:
            try:
                template = self._jinja.get_template(template_name)
                return template.render(**context)
            except Exception as e:
                logger.warning(f"Template rendering failed: {e}, using fallback")

        return self._generate_fallback(engine_type, context)

    def generate_ddl(
        self,
        target_model: str = "omop_cdm_v5.4",
        project_name: str = "portiere_project",
        tables: list[dict] | None = None,
    ) -> str:
        """
        Generate SQL DDL for target OMOP tables.

        Args:
            target_model: Target data model
            project_name: Project name for header
            tables: Optional custom table definitions

        Returns:
            SQL DDL string
        """
        if tables is None:
            tables = self._default_omop_tables()

        context = {
            "target_model": target_model,
            "project_name": project_name,
            "tables": tables,
        }

        if self._jinja:
            try:
                template = self._jinja.get_template("sql_ddl.sql.j2")
                return template.render(**context)
            except Exception:
                pass

        # Fallback DDL
        lines = [f"-- OMOP CDM DDL ({target_model})", f"-- Project: {project_name}", ""]
        for table in tables:
            cols = ", ".join(
                f"{c['name']} {c['type']}" + (" NOT NULL" if c.get("not_null") else "")
                for c in table["columns"]
            )
            lines.append(f"CREATE TABLE IF NOT EXISTS {table['name']} ({cols});")
            lines.append("")
        return "\n".join(lines)

    def generate_validation_script(
        self,
        engine_type: str = "polars",
        concept_mappings: list[dict] | None = None,
        project_name: str = "portiere_project",
        target_model: str = "omop_cdm_v5.4",
        thresholds: dict | None = None,
    ) -> str:
        """Generate a standalone validation script."""
        if thresholds is None:
            thresholds = {
                "completeness": 0.95,
                "conformance": 0.98,
                "plausibility": 0.90,
                "mapping_coverage": 0.95,
            }

        context = {
            "engine": engine_type,
            "project_name": project_name,
            "target_model": target_model,
            "concept_mappings": concept_mappings or [],
            "thresholds": thresholds,
        }

        if self._jinja:
            try:
                template = self._jinja.get_template("validation_script.py.j2")
                return template.render(**context)
            except Exception:
                pass

        return (
            f'"""Validation script for {project_name}"""\n# Use portiere SDK for full validation\n'
        )

    def generate_source_to_concept_csv(self, concept_mappings: list[dict]) -> str:
        """Generate OMOP source_to_concept_map CSV content."""
        lines = [
            "source_code,source_description,source_vocabulary_id,"
            "target_concept_id,target_concept_name,target_vocabulary_id,"
            "confidence,method"
        ]
        for c in concept_mappings:
            lines.append(
                f'"{c.get("source_code", "")}","{c.get("source_description", "")}",'
                f'"source",{c.get("target_concept_id", 0)},'
                f'"{c.get("target_concept_name", "")}","{c.get("target_vocabulary_id", "")}",'
                f"{c.get('confidence', 0.0)},{c.get('method', 'manual')}"
            )
        return "\n".join(lines)

    def _generate_fallback(self, engine_type: str, context: dict) -> str:
        """Fallback code generation when templates aren't available."""
        return f'''"""
Portiere Generated ETL Script ({engine_type})
Project: {context["project_name"]}
"""
# Install: pip install {"polars" if engine_type == "polars" else "pyspark" if engine_type == "spark" else "pandas"}
# Run: python this_script.py source.csv output.parquet

raise NotImplementedError("Template-based generation not available. Install portiere with templates.")
'''

    @staticmethod
    def _default_omop_tables() -> list[dict]:
        """Return default OMOP CDM table definitions."""
        return [
            {
                "name": "person",
                "columns": [
                    {"name": "person_id", "type": "BIGINT", "not_null": True, "primary_key": True},
                    {"name": "gender_concept_id", "type": "INTEGER", "not_null": True},
                    {"name": "year_of_birth", "type": "INTEGER", "not_null": True},
                    {"name": "race_concept_id", "type": "INTEGER", "not_null": True},
                    {"name": "ethnicity_concept_id", "type": "INTEGER", "not_null": True},
                    {"name": "person_source_value", "type": "VARCHAR(50)"},
                ],
            },
            {
                "name": "condition_occurrence",
                "columns": [
                    {
                        "name": "condition_occurrence_id",
                        "type": "BIGINT",
                        "not_null": True,
                        "primary_key": True,
                    },
                    {"name": "person_id", "type": "BIGINT", "not_null": True},
                    {"name": "condition_concept_id", "type": "INTEGER", "not_null": True},
                    {"name": "condition_start_date", "type": "DATE", "not_null": True},
                    {"name": "condition_source_value", "type": "VARCHAR(50)"},
                ],
            },
            {
                "name": "drug_exposure",
                "columns": [
                    {
                        "name": "drug_exposure_id",
                        "type": "BIGINT",
                        "not_null": True,
                        "primary_key": True,
                    },
                    {"name": "person_id", "type": "BIGINT", "not_null": True},
                    {"name": "drug_concept_id", "type": "INTEGER", "not_null": True},
                    {"name": "drug_exposure_start_date", "type": "DATE", "not_null": True},
                    {"name": "drug_source_value", "type": "VARCHAR(50)"},
                ],
            },
            {
                "name": "measurement",
                "columns": [
                    {
                        "name": "measurement_id",
                        "type": "BIGINT",
                        "not_null": True,
                        "primary_key": True,
                    },
                    {"name": "person_id", "type": "BIGINT", "not_null": True},
                    {"name": "measurement_concept_id", "type": "INTEGER", "not_null": True},
                    {"name": "measurement_date", "type": "DATE", "not_null": True},
                    {"name": "value_as_number", "type": "NUMERIC"},
                    {"name": "measurement_source_value", "type": "VARCHAR(50)"},
                ],
            },
        ]

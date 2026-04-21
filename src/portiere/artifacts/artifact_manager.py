"""
Portiere Artifact Manager — Generate runnable ETL scripts.

Artifacts are standalone scripts that can run without the Portiere SDK.
They are generated from approved schema and concept mappings.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
from jinja2 import Environment, PackageLoader, select_autoescape

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)


class ArtifactManager:
    """
    Manages ETL artifact generation.

    Generates:
    - Runnable PySpark/Polars scripts
    - Configuration YAML files
    - SQL DDL for target tables
    - Documentation
    """

    def __init__(
        self,
        engine: AbstractEngine | None = None,
        output_dir: str = "./artifacts",
    ) -> None:
        """
        Initialize artifact manager.

        Args:
            engine: Target engine for generated scripts
            output_dir: Directory to save artifacts
        """
        self._engine = engine
        self._output_dir = Path(output_dir)
        self._artifacts: list[dict] = []

        # Initialize Jinja2 environment
        self._jinja: Environment | None = None
        try:
            self._jinja = Environment(
                loader=PackageLoader("portiere.artifacts", "templates"),
                autoescape=select_autoescape(["html", "xml"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        except Exception:
            # Fallback if templates not found
            self._jinja = None

    @classmethod
    def from_api_response(
        cls,
        response: dict,
        engine: AbstractEngine,
    ) -> ArtifactManager:
        """Create from API response."""
        manager = cls(engine=engine)
        manager._artifacts = response.get("artifacts", [])
        return manager

    def generate_etl_script(
        self,
        schema_mapping: dict,
        concept_mapping: dict,
        source_path: str,
        output_path: str,
        project_name: str = "portiere_project",
        target_model: str = "omop_cdm_v5.4",
    ) -> str:
        """
        Generate ETL transformation script.

        Args:
            schema_mapping: Approved schema mappings (list of dicts or dict with 'mappings' key)
            concept_mapping: Approved concept mappings (list of dicts or dict with 'mappings' key)
            source_path: Path to source data
            output_path: Path for output data
            project_name: Project name for script header
            target_model: Target data model

        Returns:
            Generated script content
        """
        from portiere.artifacts.code_generator import CodeGenerator

        engine_type = self._engine.engine_name if self._engine else "polars"

        # Normalize mappings to lists
        schema_list = (
            schema_mapping
            if isinstance(schema_mapping, list)
            else schema_mapping.get("mappings", [])
        )
        concept_list = (
            concept_mapping
            if isinstance(concept_mapping, list)
            else concept_mapping.get("mappings", [])
        )

        gen = CodeGenerator()
        script = gen.generate_etl_script(
            engine_type=engine_type,
            schema_mappings=schema_list,
            concept_mappings=concept_list,
            project_name=project_name,
            target_model=target_model,
            source_path=source_path,
            output_path=output_path,
        )

        self._artifacts.append(
            {
                "type": "etl_script",
                "engine": engine_type,
                "content": script,
            }
        )

        return script

    def _generate_fallback_script(
        self,
        engine_type: str,
        schema_mapping: dict,
        concept_mapping: dict,
        source_path: str,
        output_path: str,
    ) -> str:
        """Generate basic ETL script without templates."""
        if engine_type == "spark":
            return f'''"""
Portiere Generated ETL Script (PySpark)
Generated from approved mappings.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("portiere_etl").getOrCreate()

    # Read source
    df = spark.read.csv("{source_path}", header=True)

    # Apply mappings
    # TODO: Apply schema and concept mappings

    # Write output
    df.write.parquet("{output_path}", mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    main()
'''
        else:  # polars
            return f'''"""
Portiere Generated ETL Script (Polars)
Generated from approved mappings.
"""

import polars as pl

def main():
    # Read source
    df = pl.read_csv("{source_path}")

    # Apply mappings
    # TODO: Apply schema and concept mappings

    # Write output
    df.write_parquet("{output_path}")

if __name__ == "__main__":
    main()
'''

    def generate_ddl(
        self,
        target_model: str = "omop_cdm_v5.4",
        project_name: str = "portiere_project",
    ) -> str:
        """Generate SQL DDL for OMOP target tables."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        ddl = gen.generate_ddl(target_model=target_model, project_name=project_name)

        self._artifacts.append({"type": "ddl", "content": ddl})
        return ddl

    def generate_validation_script(
        self,
        concept_mapping: list[dict] | None = None,
        project_name: str = "portiere_project",
        target_model: str = "omop_cdm_v5.4",
    ) -> str:
        """Generate a standalone validation script."""
        from portiere.artifacts.code_generator import CodeGenerator

        engine_type = self._engine.engine_name if self._engine else "polars"
        gen = CodeGenerator()
        script = gen.generate_validation_script(
            engine_type=engine_type,
            concept_mappings=concept_mapping,
            project_name=project_name,
            target_model=target_model,
        )

        self._artifacts.append(
            {
                "type": "validation_script",
                "engine": engine_type,
                "content": script,
            }
        )
        return script

    def generate_source_to_concept_map(
        self,
        concept_mapping: list[dict],
    ) -> str:
        """Generate source_to_concept_map CSV."""
        from portiere.artifacts.code_generator import CodeGenerator

        gen = CodeGenerator()
        csv_content = gen.generate_source_to_concept_csv(concept_mapping)

        self._artifacts.append({"type": "source_to_concept_map", "content": csv_content})
        return csv_content

    def generate_runner_config(
        self,
        schema_mapping,
        concept_mapping,
        source_path: str = "",
        output_path: str = "./omop_output",
        project_name: str = "portiere_project",
        target_model: str = "omop_cdm_v5.4",
    ) -> str:
        """
        Generate etl_config.yaml for ETLRunner.from_artifacts().

        Saves full schema mapping data and engine type so the runner
        can reconstruct the pipeline from saved artifacts.

        Args:
            schema_mapping: SchemaMapping model instance
            concept_mapping: ConceptMapping model instance
            source_path: Path to source data
            output_path: Path for output data
            project_name: Project name
            target_model: Target data model

        Returns:
            Generated YAML content
        """
        import yaml

        actionable_statuses = {"auto_accepted", "approved", "overridden"}
        schema_items = []
        for item in schema_mapping.items:
            status = item.status.value if hasattr(item.status, "value") else str(item.status)
            if status in actionable_statuses:
                schema_items.append(
                    {
                        "source_column": item.source_column,
                        "target_table": item.effective_target_table,
                        "target_column": item.effective_target_column,
                    }
                )

        config = {
            "version": "1.0",
            "engine": self._engine.engine_name if self._engine else "polars",
            "project_name": project_name,
            "target_model": target_model,
            "source": {
                "path": source_path,
                "format": "csv",
            },
            "output": {
                "path": output_path,
                "format": "parquet",
            },
            "schema_mappings": schema_items,
            "concept_lookup_file": "source_to_concept_map.csv",
        }

        content = yaml.dump(config, default_flow_style=False, sort_keys=False)
        self._artifacts.append({"type": "runner_config", "content": content})
        return content

    def generate_config(self) -> str:
        """Generate configuration YAML."""
        import yaml

        config = {
            "version": "1.0",
            "engine": self._engine.engine_name if self._engine else "polars",
            "artifacts": [{"type": a["type"], "engine": a.get("engine")} for a in self._artifacts],
        }

        content = yaml.dump(config, default_flow_style=False)
        self._artifacts.append({"type": "config", "content": content})

        return content

    def save_artifacts(self, output_dir: str | None = None) -> list[Path]:
        """
        Save all artifacts to disk.

        Args:
            output_dir: Override output directory

        Returns:
            List of saved file paths
        """
        out = Path(output_dir) if output_dir else self._output_dir
        out.mkdir(parents=True, exist_ok=True)

        saved = []
        for i, artifact in enumerate(self._artifacts):
            atype = artifact["type"]
            if atype == "etl_script":
                filename = f"etl_{artifact.get('engine', 'script')}.py"
            elif atype == "config":
                filename = "portiere_config.yaml"
            elif atype == "ddl":
                filename = "omop_ddl.sql"
            elif atype == "validation_script":
                filename = "run_validation.py"
            elif atype == "source_to_concept_map":
                filename = "source_to_concept_map.csv"
            elif atype == "runner_config":
                filename = "etl_config.yaml"
            else:
                filename = f"artifact_{i}.txt"

            filepath = out / filename
            filepath.write_text(artifact["content"])
            saved.append(filepath)

            logger.info(f"Saved artifact: {filepath}")

        return saved

    def list_artifacts(self) -> list[dict[str, Any]]:
        """List all generated artifacts."""
        return [{"type": a["type"], "engine": a.get("engine")} for a in self._artifacts]

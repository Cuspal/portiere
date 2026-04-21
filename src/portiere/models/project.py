"""
Portiere Project Model — Represents a mapping project.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.engines.base import AbstractEngine
    from portiere.models.concept_mapping import ConceptMapping
    from portiere.models.schema_mapping import SchemaMapping
    from portiere.models.source import Source
    from portiere.runner.result import ETLResult

logger = structlog.get_logger(__name__)


class Project(BaseModel):
    """
    A mapping project.

    Projects contain:
    - One or more data sources
    - Schema mappings (source → target)
    - Concept mappings (local codes → standard concepts)
    - Generated ETL artifacts
    """

    model_config = {"arbitrary_types_allowed": True}

    id: str
    name: str
    target_model: str = "omop_cdm_v5.4"
    vocabularies: list[str] = Field(default_factory=list)
    description: str | None = None

    # Internal: reference to client (excluded from serialization)
    client: Client | None = Field(default=None, exclude=True)

    def add_source(
        self,
        name: str,
        engine: AbstractEngine,
        path: str,
        format: str = "csv",
    ) -> Source:
        """
        Add a data source to the project.

        Args:
            name: Human-readable source name
            engine: Compute engine to use
            path: Path to source data (file or glob)
            format: Data format (csv, parquet, json)

        Returns:
            Source instance
        """
        from portiere.models.source import Source

        logger.info("Adding source", name=name, path=path, format=format)

        # Register source with SaaS
        if self.client:
            response = self.client._request(
                "POST",
                f"/projects/{self.id}/sources",
                json={"name": name, "path": path, "format": format},
            )
            source_id = response["id"]
        else:
            source_id = f"local_{name}"

        return Source(
            id=source_id,
            name=name,
            project=self,
            engine=engine,
            path=path,
            format=format,
        )

    def map_schema(
        self,
        source: Source,
    ) -> SchemaMapping:
        """
        Create AI-powered schema mapping for a source.

        Uses the source profile to suggest source→target mappings.

        Args:
            source: Source to map

        Returns:
            SchemaMapping with AI suggestions
        """
        from portiere.models.schema_mapping import SchemaMapping

        logger.info("Starting schema mapping", source=source.name)

        # Get profile if not already done
        if source.profile_result is None:
            source.profile()

        # Request AI schema mapping suggestions from SaaS
        if self.client:
            # Build column info from profile
            columns = []
            for col in source.profile_result.columns if source.profile_result else []:
                columns.append(
                    {
                        "name": col["name"],
                        "type": col.get("type", "unknown"),
                        "nullable": col.get("null_pct", 0) > 0,
                        "n_unique": col.get("n_unique"),
                        "sample_values": [str(v) for v in col.get("top_values", [])[:5]],
                    }
                )

            response = self.client._request(
                "POST",
                "/schema-mapping/suggest",
                json={
                    "columns": columns,
                    "target_model": self.target_model,
                },
            )
            return SchemaMapping.from_api_response(response, project=self)
        else:
            return SchemaMapping(project=self, source=source, items=[])

    def map_concepts(
        self,
        source: Source,
        engine: AbstractEngine,
        schema_mapping: SchemaMapping | None = None,
        batch_size: int = 50,
        code_columns: list[str] | None = None,
        auto_threshold: float = 0.95,
        review_threshold: float = 0.70,
    ) -> ConceptMapping:
        """
        Create AI-powered concept mapping for source codes.

        This is the core value proposition — mapping local codes to
        standard vocabularies (SNOMED CT, LOINC, RxNorm, etc.).

        Args:
            source: Source containing codes to map
            engine: Compute engine for local processing
            schema_mapping: Optional schema mapping to use
            batch_size: Number of codes per API batch
            code_columns: Override which columns to map (default: auto-detected).
                          Example: code_columns=["icd_code"]
            auto_threshold: Confidence >= this → auto-mapped (default 0.95)
            review_threshold: Confidence >= this → needs review (default 0.70)

        Returns:
            ConceptMapping with AI suggestions
        """
        from portiere.models.concept_mapping import ConceptMapping

        logger.info("Starting concept mapping", source=source.name)

        # Extract code columns from source
        # This runs locally on customer's engine
        all_code_columns = source.get_code_columns(engine)

        # Filter to user-specified columns if provided
        if code_columns is not None:
            filtered = {k: v for k, v in all_code_columns.items() if k in code_columns}
            skipped = set(code_columns) - set(filtered.keys())
            if skipped:
                logger.warning(f"Requested code columns not found in source: {skipped}")
            all_code_columns = filtered

        # Batch send to SaaS for AI mapping
        if self.client:
            all_items = []
            for col_name, codes in all_code_columns.items():
                total_batches = (len(codes) + batch_size - 1) // batch_size
                # Send in batches with scaled timeout
                for i in range(0, len(codes), batch_size):
                    batch = codes[i : i + batch_size]
                    batch_num = i // batch_size + 1
                    batch_timeout = max(120.0, len(batch) * 3.0)

                    logger.info(
                        f"Mapping {col_name} batch {batch_num}/{total_batches} "
                        f"({len(batch)} codes, timeout={batch_timeout:.0f}s)"
                    )

                    response = self.client._request(
                        "POST",
                        "/concepts/map",
                        json={
                            "source_id": source.id,
                            "column": col_name,
                            "codes": batch,
                            "vocabularies": self.vocabularies,
                            "auto_threshold": auto_threshold,
                            "review_threshold": review_threshold,
                        },
                        timeout=batch_timeout,
                    )
                    all_items.extend(response.get("items", []))

            logger.info(f"Concept mapping complete: {len(all_items)} codes mapped")

            return ConceptMapping.from_api_response({"items": all_items}, project=self)
        else:
            return ConceptMapping(project=self, source=source, items=[])

    def generate_etl(
        self,
        source: Source,
        engine: AbstractEngine,
        schema_mapping: SchemaMapping,
        concept_mapping: ConceptMapping,
        output_format: str = "omop_cdm_v5.4",
    ):
        """
        Generate ETL artifacts from approved mappings.

        Creates runnable PySpark/Polars scripts that transform
        source data to the target model.

        Args:
            source: Source to transform
            engine: Target engine for generated code
            schema_mapping: Approved schema mappings
            concept_mapping: Approved concept mappings
            output_format: Target format

        Returns:
            ETL artifact manager
        """
        from portiere.artifacts.artifact_manager import ArtifactManager

        logger.info("Generating ETL artifacts", engine=engine.engine_name)

        # Request ETL generation from SaaS
        if self.client:
            response = self.client._request(
                "POST",
                f"/projects/{self.id}/etl/generate",
                json={
                    "source_id": source.id,
                    "engine_type": engine.engine_name,
                    "output_format": output_format,
                },
            )
            return ArtifactManager.from_api_response(response, engine=engine)
        else:
            return ArtifactManager(engine=engine)

    def run_etl(
        self,
        source: Source,
        schema_mapping: SchemaMapping | None = None,
        concept_mapping: ConceptMapping | None = None,
        output_path: str = "./omop_output",
        source_format: str | None = None,
        output_format: str = "parquet",
        on_progress=None,
    ) -> ETLResult:
        """
        Execute ETL pipeline using the source's pre-configured engine.

        Args:
            source: Source with engine already configured via add_source()
            schema_mapping: Approved schema mappings (required)
            concept_mapping: Approved concept mappings (required)
            output_path: Directory for output files
            source_format: Source format override (defaults to source.format)
            output_format: Output format (parquet, csv, json)
            on_progress: Optional callback(table_name, current, total)

        Returns:
            ETLResult with execution details
        """
        from portiere.runner.etl_runner import ETLRunner

        if source.engine is None:
            raise ValueError(
                "Source has no engine configured. Pass an engine when calling add_source()."
            )

        if schema_mapping is None or concept_mapping is None:
            raise ValueError(
                "Both schema_mapping and concept_mapping are required. "
                "Run map_schema() and map_concepts() first."
            )

        logger.info(
            "Running ETL",
            source=source.name,
            engine=source.engine.engine_name,
            output_path=output_path,
        )

        runner = ETLRunner.from_mappings(
            engine=source.engine,
            schema_mapping=schema_mapping,
            concept_mapping=concept_mapping,
            target_model=self.target_model,
            project_name=self.name,
        )

        return runner.run(
            source_path=source.path,
            output_path=output_path,
            source_format=source_format or source.format,
            output_format=output_format,
            on_progress=on_progress,
        )

    def validate(  # type: ignore[override]
        self,
        engine: AbstractEngine,
        output_path: str,
    ):
        """
        Run validation checks on transformed data.

        Args:
            engine: Compute engine
            output_path: Path to transformed data

        Returns:
            Validation report
        """
        from portiere.stages.stage5_validate import validate_output

        logger.info("Running validation", output_path=output_path)

        return validate_output(
            engine=engine,
            output_path=output_path,
            target_model=self.target_model,
        )

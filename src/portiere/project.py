"""
Portiere Unified Project — Single entry point for the mapping pipeline.

Combines storage (local or cloud) with pipeline execution (local or cloud)
behind a single API. Created via ``portiere.init()``.

Example:
    import portiere
    from portiere.engines import PolarsEngine

    project = portiere.init(name="Hospital Migration", engine=PolarsEngine())
    source = project.add_source("patients.csv")
    profile = project.profile(source)
    schema_map = project.map_schema(source)
    concept_map = project.map_concepts(codes=["E11.9", "I10"])
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.config import PortiereConfig
    from portiere.engines.base import AbstractEngine
    from portiere.models.concept_mapping import ConceptMapping
    from portiere.models.schema_mapping import SchemaMapping
    from portiere.runner.result import ETLResult
    from portiere.storage.base import StorageBackend

logger = structlog.get_logger(__name__)


class Project:
    """
    Unified Portiere project.

    Provides a single API for the full mapping pipeline regardless of
    storage mode (local/cloud) or pipeline mode (local/cloud AI).

    Not a Pydantic model — plain Python class to avoid forward-ref issues.
    """

    def __init__(
        self,
        name: str,
        target_model: str,
        vocabularies: list[str],
        config: PortiereConfig,
        storage: StorageBackend,
        project_id: str,
        engine: AbstractEngine,
        client: Client | None = None,
        task: str = "standardize",
        source_standard: str | None = None,
    ) -> None:
        self.name = name
        self.target_model = target_model
        self.vocabularies = vocabularies
        self.task = task
        self.source_standard = source_standard
        self.config = config
        self._storage = storage
        self.id = project_id
        self._engine = engine
        self._client = client

    # --- Properties ---

    @property
    def engine(self) -> AbstractEngine:
        """Return the compute engine."""
        return self._engine

    @property
    def client(self) -> Client:
        """Cloud client (not available in open-source SDK)."""
        raise NotImplementedError(
            "Cloud features are not available in the open-source SDK. "
            "For cloud storage and managed inference, see https://portiere.io"
        )

    @property
    def storage(self) -> StorageBackend:
        """Access the underlying storage backend."""
        return self._storage

    # --- Pipeline Methods ---

    def add_source(
        self,
        path: str | None = None,
        name: str | None = None,
        format: str | None = None,
        *,
        connection_string: str | None = None,
        table: str | None = None,
        query: str | None = None,
    ) -> dict:
        """
        Register a data source with the project.

        Supports both file-based and database sources. Provide either ``path``
        for files or ``connection_string`` for databases (not both).

        Args:
            path: Path to the source data file.
            name: Optional source name (defaults to filename or table name).
            format: Data format (csv, parquet, json). Auto-detected if omitted.
            connection_string: Database connection URI
                (e.g., ``postgresql://user:pass@host/db``).
            table: Database table to read (mutually exclusive with query).
            query: SQL query to execute (mutually exclusive with table).

        Returns:
            Source metadata dict.
        """
        if path and connection_string:
            raise ValueError("Provide either 'path' or 'connection_string', not both.")
        if not path and not connection_string:
            raise ValueError("Either 'path' or 'connection_string' must be provided.")

        if connection_string:
            # Database source
            if not table and not query:
                raise ValueError("Database sources require 'table' or 'query'.")
            if name is None:
                name = table or "query_source"

            metadata: dict[str, Any] = {
                "name": name,
                "format": "database",
                "connection_string": connection_string,
                "added_at": datetime.now(tz=timezone.utc).isoformat(),
            }
            if table:
                metadata["table"] = table
            if query:
                metadata["query"] = query
        else:
            # File source (original behavior)
            path_obj = Path(path)  # type: ignore[arg-type]
            if name is None:
                name = path_obj.stem
            if format is None:
                format = path_obj.suffix.lstrip(".") or "csv"

            metadata = {
                "name": name,
                "path": str(path),
                "format": format,
                "added_at": datetime.now(tz=timezone.utc).isoformat(),
            }

        # Read file to populate columns and row_count
        try:
            df = self._read_source_data(metadata)
            col_info = self._extract_columns(df)
            columns = [c["name"] for c in col_info]
            if columns and all(isinstance(c, str) for c in columns):
                metadata["columns"] = columns
            if hasattr(df, "shape"):
                row_count = df.shape[0]
            elif hasattr(df, "count"):
                row_count = df.count()
            else:
                row_count = len(df)
            if isinstance(row_count, int):
                metadata["row_count"] = row_count
        except Exception:
            logger.debug("project.source_introspection_skipped", source=name)

        self._storage.save_source(self.name, name, metadata)
        logger.info("project.source_added", project=self.name, source=name)

        # Auto-profile if configured
        if (
            self.config.quality
            and self.config.quality.enabled
            and self.config.quality.profile_on_ingest
        ):
            try:
                self.profile(metadata)
            except ImportError:
                logger.debug(
                    "project.auto_profile_skipped",
                    reason="great_expectations not installed",
                )

        return metadata

    def profile(self, source: dict) -> dict:
        """
        Profile a data source using Great Expectations.

        Args:
            source: Source metadata dict (from add_source()).

        Returns:
            Profile report dict with column stats and GX expectations.

        Raises:
            ImportError: If great_expectations is not installed.
        """
        try:
            from portiere.quality.profiler import GXProfiler
        except ImportError:
            raise ImportError(
                "Great Expectations is required for profiling. "
                "Install it with: pip install portiere-health[quality]"
            )

        # Read data using engine
        df = self._read_source_data(source)

        # Only convert to pandas if engine is Polars (GX doesn't support Polars natively).
        # Spark DataFrames pass through directly — GXProfiler handles them via add_spark().
        if hasattr(df, "to_pandas") and not hasattr(df, "toPandas"):
            # Polars DataFrame → convert to pandas
            profiler_df = df.to_pandas()
        else:
            # Pandas or Spark DataFrame → pass through directly
            profiler_df = df

        profiler = GXProfiler(self.config.quality)
        report = profiler.profile(profiler_df, source["name"])

        # Save as artifact
        self._storage.save_profile(self.name, source["name"], report)
        logger.info("project.profiled", source=source["name"])
        return report

    def map_schema(self, source: dict) -> SchemaMapping:
        """
        Map source columns to target model schema.

        Uses local AI or Portiere Cloud API based on ``config.pipeline``.

        Args:
            source: Source metadata dict (from add_source()).

        Returns:
            SchemaMapping instance.
        """
        from portiere.stages.stage2_schema import map_schema

        # Read source to get columns
        df = self._read_source_data(source)
        columns = self._extract_columns(df)

        result = map_schema(
            client=None,
            target_model=self.target_model,
            config=self.config,
            columns=columns,
        )

        # Build SchemaMapping model from result
        from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem

        items = []
        for m in result.get("mappings", []):
            items.append(
                SchemaMappingItem(
                    source_column=m.get("source_column", ""),
                    source_table=m.get("source_table", ""),
                    target_table=m.get("target_table", ""),
                    target_column=m.get("target_column", ""),
                    confidence=m.get("confidence", 0.0),
                    status=m.get("status", "needs_review"),
                )
            )
        mapping = SchemaMapping(items=items)
        mapping.project = self  # type: ignore[assignment]

        # Persist
        self._storage.save_schema_mapping(self.name, mapping)
        logger.info(
            "project.schema_mapped",
            project=self.name,
            total=len(items),
            auto=result.get("stats", {}).get("auto_accepted", 0),
        )
        return mapping

    # Column name patterns that indicate clinical codes
    _CODE_COLUMN_PATTERNS = {
        "diagnosis_code",
        "icd_code",
        "icd10_code",
        "icd9_code",
        "condition_code",
        "dx_code",
        "drug_code",
        "medication_code",
        "ndc_code",
        "rxnorm_code",
        "procedure_code",
        "cpt_code",
        "hcpcs_code",
        "lab_code",
        "test_code",
        "loinc_code",
    }

    def _detect_code_columns(self, source: dict) -> list[str]:
        """Auto-detect columns likely containing clinical codes."""
        columns = source.get("columns", [])
        if not columns:
            return []
        detected = []
        for col in columns:
            col_lower = col.lower()
            if col_lower in self._CODE_COLUMN_PATTERNS:
                detected.append(col)
            elif col_lower.endswith("_code") and col_lower not in (
                "zip_code",
                "postal_code",
                "area_code",
                "country_code",
            ):
                detected.append(col)
        if detected:
            logger.info(
                "project.code_columns_detected",
                columns=detected,
            )
        return detected

    def map_concepts(
        self,
        source: dict | None = None,
        codes: list[dict | str] | None = None,
        code_columns: list[str] | None = None,
        vocabularies: list[str] | None = None,
    ) -> ConceptMapping:
        """
        Map source codes to standard concepts.

        Uses local AI or Portiere Cloud API based on ``config.pipeline``.

        When ``source`` is provided without ``code_columns``, Portiere
        auto-detects columns likely to contain clinical codes (e.g.,
        ``diagnosis_code``, ``drug_code``, ``test_code``). If no code
        columns are found, an empty ConceptMapping is returned.

        Args:
            source: Source metadata dict (from add_source()). Needed if code_columns used.
            codes: Pre-extracted codes to map. Each can be a string or dict with
                   'code', 'description', 'count'.
            code_columns: Column names containing codes (requires source).
                If None and source is given, auto-detection is attempted.
            vocabularies: Target vocabularies. Defaults to project vocabularies.

        Returns:
            ConceptMapping instance.
        """
        from portiere.stages.stage3_concepts import map_concepts

        if vocabularies is None:
            vocabularies = self.vocabularies

        # Auto-detect code columns when source is given but no code_columns specified
        if source is not None and code_columns is None and codes is None:
            code_columns = self._detect_code_columns(source)

        # Normalize codes if passed as strings
        normalized_codes = None
        if codes is not None:
            normalized_codes = []
            for c in codes:
                if isinstance(c, str):
                    normalized_codes.append({"code": c, "description": c, "count": 1})
                else:
                    normalized_codes.append(c)

        # For database sources with code_columns, pre-extract codes
        if source and source.get("format") == "database" and code_columns:
            from portiere.stages.stage3_concepts import (
                _extract_description_map,
                _find_description_column,
            )

            df = self._read_source_data(source)
            normalized_codes = normalized_codes or []
            for col in code_columns:
                distinct = self.engine.get_distinct_values(df, col)
                # Look for companion description column
                desc_col = _find_description_column(list(df.columns), col)
                code_to_desc: dict[str, str] = {}
                if desc_col:
                    code_to_desc = _extract_description_map(df, col, desc_col)
                for item in distinct:
                    code_val = str(item["value"])
                    normalized_codes.append(
                        {
                            "code": code_val,
                            "description": code_to_desc.get(code_val, code_val),
                            "count": item["count"],
                        }
                    )
            # Codes extracted — no need to pass engine/path to stage
            stage_engine = None
            source_path = ""
            source_format = "csv"
        else:
            stage_engine = self.engine if source else None
            source_path = source["path"] if source else ""
            source_format = source.get("format", "csv") if source else "csv"

        result = map_concepts(
            client=None,
            engine=stage_engine,
            source_path=source_path,
            code_columns=code_columns or [],
            vocabularies=vocabularies,
            format=source_format,
            config=self.config,
            codes=normalized_codes,
        )

        # Build ConceptMapping model from result
        from portiere.models.concept_mapping import (
            ConceptCandidate,
            ConceptMapping,
            ConceptMappingItem,
        )

        items = []
        for column_key, column_data in result.get("mappings", {}).items():
            for item_data in column_data.get("items", []):
                # Parse candidates from search results
                raw_candidates = item_data.get("candidates", [])
                parsed_candidates = []
                for c in raw_candidates:
                    try:
                        parsed_candidates.append(
                            ConceptCandidate(
                                concept_id=c.get("concept_id", 0),
                                concept_name=c.get("concept_name", ""),
                                vocabulary_id=c.get("vocabulary_id", ""),
                                domain_id=c.get("domain_id", ""),
                                concept_class_id=c.get("concept_class_id", ""),
                                standard_concept=c.get("standard_concept", ""),
                                score=c.get("score", 0.0),
                                rrf_score=c.get("rrf_score"),
                                cross_encoder_score=c.get("cross_encoder_score"),
                            )
                        )
                    except Exception:
                        continue

                items.append(
                    ConceptMappingItem(
                        source_code=item_data.get("source_code", ""),
                        source_description=item_data.get("source_description", ""),
                        source_column=item_data.get("source_column"),
                        source_count=item_data.get("source_count", 1),
                        target_concept_id=item_data.get("target_concept_id", 0),
                        target_concept_name=item_data.get("target_concept_name", ""),
                        target_vocabulary_id=item_data.get("target_vocabulary_id", ""),
                        target_domain_id=item_data.get("target_domain_id", ""),
                        confidence=item_data.get("confidence", 0.0),
                        method=item_data.get("method", "manual"),
                        candidates=parsed_candidates,
                    )
                )
        mapping = ConceptMapping(items=items)
        mapping.project = self  # type: ignore[assignment]

        # Persist
        self._storage.save_concept_mapping(self.name, mapping)
        logger.info(
            "project.concepts_mapped",
            project=self.name,
            total=len(items),
            auto_rate=result.get("auto_rate", 0),
        )
        return mapping

    def run_etl(
        self,
        source: dict,
        output_dir: str,
        schema_mapping: SchemaMapping | None = None,
        concept_mapping: ConceptMapping | None = None,
        output_format: str = "csv",
    ) -> ETLResult:
        """
        Generate and execute ETL pipeline.

        Args:
            source: Source metadata dict.
            output_dir: Output directory for ETL results.
            schema_mapping: Schema mapping to use. Loaded from storage if None.
            concept_mapping: Concept mapping to use. Loaded from storage if None.
            output_format: Output file format (csv, parquet, json). Default: csv.

        Returns:
            ETLResult with execution details.
        """
        from portiere.runner import ETLRunner

        if schema_mapping is None:
            schema_mapping = self._storage.load_schema_mapping(self.name)
        if concept_mapping is None:
            concept_mapping = self._storage.load_concept_mapping(self.name)

        if source.get("format") == "database":
            raise NotImplementedError(
                "ETL from database sources is not yet supported. "
                "Export your query results to a file first, then use "
                "add_source(path=...) for ETL execution."
            )

        runner = ETLRunner.from_mappings(
            engine=self.engine,
            schema_mapping=schema_mapping,
            concept_mapping=concept_mapping,
            target_model=self.target_model,
            project_name=self.name,
        )
        result = runner.run(
            source_path=source["path"],
            output_path=output_dir,
            source_format=source.get("format", "csv"),
            output_format=output_format,
        )

        logger.info("project.etl_complete", project=self.name, output_dir=output_dir)
        return result

    def validate(self, etl_result=None, output_path: str | None = None) -> dict:
        """
        Validate ETL output using Great Expectations.

        Args:
            etl_result: ETLResult or dict from run_etl().
            output_path: Path to ETL output directory. Used if etl_result is None.

        Returns:
            Validation report dict with GX results.

        Raises:
            ImportError: If great_expectations is not installed.
        """
        try:
            from portiere.quality.validator import GXValidator
        except ImportError:
            raise ImportError(
                "Great Expectations is required for validation. "
                "Install it with: pip install portiere-health[quality]"
            )

        validator = GXValidator(self.config.quality, self.config.thresholds)

        # Determine output path
        if etl_result is not None:
            if isinstance(etl_result, dict):
                output_path = etl_result.get("output_dir") or etl_result.get("output_path")
            elif hasattr(etl_result, "output_path"):
                output_path = etl_result.output_path
        if output_path is None:
            raise ValueError("Either etl_result or output_path must be provided.")

        # Find output files and validate each table
        reports = []
        output_dir = Path(output_path)
        output_files = list(output_dir.glob("*.csv")) + list(output_dir.glob("*.parquet"))
        for output_file in output_files:
            # Use engine to read output files — preserves DataFrame type (Spark/Pandas)
            fmt = "parquet" if output_file.suffix == ".parquet" else "csv"
            df = self._engine.read_source(str(output_file), format=fmt)
            # Only convert to pandas for Polars (GX doesn't support Polars natively)
            if hasattr(df, "to_pandas") and not hasattr(df, "toPandas"):
                df = df.to_pandas()
            table_name = output_file.stem
            report = validator.validate(df, table_name, self.target_model)
            reports.append(report)
            self._storage.save_quality_report(self.name, report)

        combined = {
            "tables": reports,
            "total_tables": len(reports),
            "all_passed": all(r.get("passed", False) for r in reports),
            "validated_at": datetime.now(tz=timezone.utc).isoformat(),
        }

        logger.info(
            "project.validated",
            project=self.name,
            tables=len(reports),
            all_passed=combined["all_passed"],
        )
        return combined

    # --- Cloud Sync (Portiere Cloud only) ---

    _CLOUD_MSG = (
        "Cloud sync is not available in the open-source SDK. "
        "For cloud storage, sync, and collaboration, see https://portiere.io"
    )

    def push(self) -> str:
        """Push project to Portiere Cloud (not available in open-source SDK)."""
        raise NotImplementedError(self._CLOUD_MSG)

    def pull(self) -> None:
        """Pull from Portiere Cloud (not available in open-source SDK)."""
        raise NotImplementedError(self._CLOUD_MSG)

    def sync_status(self) -> dict:
        """Return local-only sync status."""
        return {
            "mode": "local",
            "pipeline": "local",
            "synced": False,
            "cloud_project_id": None,
            "last_synced": None,
        }

    # --- Cross-Standard Mapping ---

    def cross_map(
        self,
        source_standard: str | None = None,
        target_standard: str | None = None,
        source_entity: str = "",
        data: Any = None,
        custom_crossmap: Path | None = None,
    ) -> Any:
        """
        Cross-map data from one clinical standard to another.

        Transforms records already in one standard (e.g., OMOP CDM) into
        another standard (e.g., FHIR R4) using crossmap definitions.

        For ``task="cross_map"`` projects, ``source_standard`` and
        ``target_standard`` are inferred from the project settings and
        can be omitted.

        Args:
            source_standard: Source standard name (e.g., "omop_cdm_v5.4").
                Defaults to ``self.source_standard`` for cross_map projects,
                or ``self.target_model`` for standardize projects.
            target_standard: Target standard name (e.g., "fhir_r4").
                Defaults to ``self.target_model`` for cross_map projects.
            source_entity: Source entity name (e.g., "person", "PID").
            data: Input data — dict (single record), list[dict] (multiple),
                  or pandas/polars DataFrame.
            custom_crossmap: Path to custom crossmap YAML (optional).

        Returns:
            Mapped data in the same format as input (dict, list, or DataFrame).

        Example:
            # Cross-map project: standards inferred
            >>> result = project.cross_map(source_entity="person", data=record)

            # Explicit standards (backward compatible)
            >>> result = project.cross_map(
            ...     "omop_cdm_v5.4", "fhir_r4", "person",
            ...     {"person_id": 123, "gender_concept_id": 8507}
            ... )
        """
        from portiere.local.cross_mapper import CrossStandardMapper
        from portiere.models.cross_mapping import CrossMappingRun

        # Infer source_standard from project context
        if source_standard is None:
            if self.task == "cross_map" and self.source_standard:
                source_standard = self.source_standard
            elif self.target_model:
                source_standard = self.target_model
            else:
                raise ValueError(
                    "source_standard is required. Either pass it explicitly "
                    "or use task='cross_map' with source_standard at init."
                )

        # Infer target_standard from project context
        if target_standard is None:
            if self.task == "cross_map":
                target_standard = self.target_model
            else:
                raise ValueError(
                    "target_standard is required for standardize projects. "
                    "Pass it explicitly or use task='cross_map' at init."
                )

        mapper = CrossStandardMapper(
            source_standard, target_standard, custom_crossmap=custom_crossmap
        )
        entity_map = mapper.get_entity_map()
        target_entity = entity_map.get(source_entity)

        # Handle different input types
        result: Any
        if isinstance(data, dict):
            result = mapper.map_record(source_entity, data)
            record_count = 1
        elif isinstance(data, list):
            result = mapper.map_records(source_entity, data)
            record_count = len(data)
        else:
            # Assume DataFrame (pandas or polars)
            result = mapper.map_dataframe(source_entity, data)
            record_count = len(data) if hasattr(data, "__len__") else 0  # type: ignore[arg-type]

        # Persist run record
        run = CrossMappingRun(
            source_standard=source_standard,
            target_standard=target_standard,
            source_entity=source_entity,
            target_entity=target_entity,
            record_count=record_count,
        )
        try:
            existing = self._storage.load_cross_mapping(self.name)
            existing.runs.append(run)
            self._storage.save_cross_mapping(self.name, existing)
        except Exception:
            logger.debug("cross_map.persist_skipped", reason="storage unavailable")

        return result

    # --- Save Methods (for review workflow) ---

    def save_schema_mapping(self, mapping: SchemaMapping) -> None:
        """Save a (reviewed) schema mapping back to storage."""
        self._storage.save_schema_mapping(self.name, mapping)
        logger.info(
            "project.schema_mapping_saved",
            project=self.name,
            items=len(mapping.items),
        )

    def save_concept_mapping(self, mapping: ConceptMapping) -> None:
        """Save a (reviewed) concept mapping back to storage."""
        self._storage.save_concept_mapping(self.name, mapping)
        logger.info(
            "project.concept_mapping_saved",
            project=self.name,
            items=len(mapping.items),
        )

    # --- Convenience ---

    def load_schema_mapping(self) -> SchemaMapping:
        """Load the current schema mapping from storage."""
        return self._storage.load_schema_mapping(self.name)

    def load_concept_mapping(self) -> ConceptMapping:
        """Load the current concept mapping from storage."""
        return self._storage.load_concept_mapping(self.name)

    def import_concept_mapping(
        self,
        path: str | None = None,
        dataframe: Any = None,
        records: list[dict] | None = None,
    ) -> ConceptMapping:
        """
        Import an existing concept mapping table into this project.

        Use this when you already have a mapping table (e.g., from a
        previous migration or manual curation) and want to continue
        the pipeline from there.

        Provide exactly one of ``path``, ``dataframe``, or ``records``.

        Args:
            path: Path to a CSV or JSON file.
            dataframe: A Pandas, Polars, or Spark DataFrame.
            records: A list of dicts, each with at least ``source_code``.

        Returns:
            ConceptMapping instance persisted to project storage.
        """
        from portiere.models.concept_mapping import ConceptMapping

        if path is not None:
            if path.endswith(".json"):
                mapping = ConceptMapping.from_json(path)
            else:
                mapping = ConceptMapping.from_csv(path, engine=self._engine)
        elif dataframe is not None:
            mapping = ConceptMapping.from_dataframe(dataframe)
        elif records is not None:
            mapping = ConceptMapping.from_records(records)
        else:
            raise ValueError("Provide one of: path (CSV/JSON file), dataframe, or records.")

        mapping.project = self  # type: ignore[assignment]
        self._storage.save_concept_mapping(self.name, mapping)
        logger.info(
            "project.concept_mapping_imported",
            project=self.name,
            items=len(mapping.items),
        )
        return mapping

    def export_concept_mapping(
        self,
        path: str,
        *,
        omop_format: bool = False,
    ) -> str:
        """
        Export the project's concept mapping to a file.

        Args:
            path: Output file path (.csv or .json).
            omop_format: If True, export in OMOP source_to_concept_map
                format (always CSV, ignores file extension).

        Returns:
            The output file path.
        """
        mapping = self.load_concept_mapping()

        if omop_format:
            import pandas as pd

            rows = mapping.to_source_to_concept_map()
            pd.DataFrame(rows).to_csv(path, index=False)
        elif path.endswith(".json"):
            mapping.to_json(path)
        else:
            mapping.to_csv(path)

        logger.info(
            "project.concept_mapping_exported",
            project=self.name,
            path=path,
            omop_format=omop_format,
        )
        return path

    # --- Internal ---

    def _read_source_data(self, source: dict) -> Any:
        """Read data from source — handles both file and database sources."""
        if source.get("format") == "database":
            return self.engine.read_database(
                connection_string=source["connection_string"],
                query=source.get("query"),
                table=source.get("table"),
            )
        return self.engine.read_source(source["path"], format=source.get("format", "csv"))

    def _extract_columns(self, df: Any) -> list[dict]:
        """Extract column metadata from a DataFrame for schema mapping."""
        columns = []

        # Polars
        if hasattr(df, "schema") and hasattr(df, "head"):
            try:
                head = df.head(5)
                for col_name in df.columns:
                    col_type = str(df.schema.get(col_name, "unknown"))
                    sample_values = head[col_name].to_list()
                    columns.append(
                        {
                            "name": col_name,
                            "type": col_type,
                            "sample_values": [str(v) for v in sample_values if v is not None],
                        }
                    )
                return columns
            except Exception:
                pass

        # Pandas
        if hasattr(df, "dtypes") and hasattr(df, "head"):
            head = df.head(5)
            for col_name in df.columns:
                col_type = str(df[col_name].dtype)
                sample_values = head[col_name].dropna().tolist()
                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "sample_values": [str(v) for v in sample_values],
                    }
                )
            return columns

        # PySpark
        if hasattr(df, "dtypes") and hasattr(df, "take"):
            rows = df.take(5)
            for col_name, col_type in df.dtypes:
                sample_values = [str(getattr(row, col_name, "")) for row in rows]
                columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "sample_values": sample_values,
                    }
                )
            return columns

        return columns

    def __repr__(self) -> str:
        parts = [
            f"Project(name='{self.name}'",
            f"task='{self.task}'",
            f"target_model='{self.target_model}'",
        ]
        if self.source_standard:
            parts.append(f"source_standard='{self.source_standard}'")
        parts.append(f"mode='{self.config.effective_mode}'")
        return ", ".join(parts) + ")"

"""
Portiere ETL Runner — Execute ETL/ELT pipelines from the SDK.

Supports three loading modes:
- from_mappings(): From in-memory SchemaMapping/ConceptMapping objects
- from_artifacts(): From saved artifact files (etl_config.yaml + CSV)
- from_project(): From an API project (fetches mappings remotely)
"""

from __future__ import annotations

import csv
import os
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from portiere.runner.result import ETLResult, TableResult

if TYPE_CHECKING:
    from portiere.client import Client
    from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)

# Actionable schema mapping statuses
_ACTIONABLE_SCHEMA_STATUSES = {"auto_accepted", "approved", "overridden"}


class ETLRunner:
    """
    Executes ETL/ELT pipelines using approved schema and concept mappings.

    The runner reads source data, routes columns to target tables,
    applies renames, performs concept lookups, and writes output files.

    Engine Consistency:
        The runner always uses the developer's pre-configured engine.
        It never silently creates a default engine.
    """

    def __init__(
        self,
        engine: AbstractEngine,
        schema_items: list[dict],
        concept_items: list[dict],
        target_model: str = "omop_cdm_v5.4",
        project_name: str = "portiere_project",
    ) -> None:
        """
        Initialize ETLRunner.

        Args:
            engine: Compute engine (required — never silently defaulted)
            schema_items: List of schema mapping dicts with keys:
                source_column, target_table, target_column
            concept_items: List of concept mapping dicts with keys:
                source_code, source_column, target_concept_id
            target_model: Target data model name
            project_name: Project name for logging
        """
        self.engine = engine
        self.schema_items = schema_items
        self.concept_items = concept_items
        self.target_model = target_model
        self.project_name = project_name

        # Pre-compute routing and lookup tables
        self._table_routes = self._build_table_routes()
        self._concept_lookups = self._build_concept_lookups()

    def _build_table_routes(self) -> dict[str, list[dict]]:
        """Group schema items by target table."""
        routes: dict[str, list[dict]] = defaultdict(list)
        for item in self.schema_items:
            table = item.get("target_table")
            if table:
                routes[table].append(item)
        return dict(routes)

    def _build_concept_lookups(self) -> dict[str, dict]:
        """Build per-column concept lookup dicts: {source_column: {code: concept_id}}."""
        lookups: dict[str, dict] = defaultdict(dict)
        for item in self.concept_items:
            col = item.get("source_column")
            code = item.get("source_code")
            concept_id = item.get("target_concept_id")
            if col and code is not None and concept_id is not None:
                lookups[col][str(code)] = concept_id
        return dict(lookups)

    # ──────────────────────────────────────────────────────────
    # Factory methods
    # ──────────────────────────────────────────────────────────

    @classmethod
    def from_mappings(
        cls,
        engine: AbstractEngine,
        schema_mapping: Any,
        concept_mapping: Any,
        target_model: str = "omop_cdm_v5.4",
        project_name: str = "portiere_project",
    ) -> ETLRunner:
        """
        Create ETLRunner from in-memory SchemaMapping/ConceptMapping models.

        Filters to actionable statuses only:
        - Schema: AUTO_ACCEPTED, APPROVED, OVERRIDDEN
        - Concepts: non-UNMAPPED with is_mapped=True

        Args:
            engine: Compute engine (required)
            schema_mapping: SchemaMapping model instance
            concept_mapping: ConceptMapping model instance
            target_model: Target data model
            project_name: Project name
        """
        schema_items = []
        for item in schema_mapping.items:
            status = item.status.value if hasattr(item.status, "value") else str(item.status)
            if status in _ACTIONABLE_SCHEMA_STATUSES:
                schema_items.append(
                    {
                        "source_column": item.source_column,
                        "target_table": item.effective_target_table,
                        "target_column": item.effective_target_column,
                    }
                )

        concept_items = []
        for item in concept_mapping.items:
            if item.is_mapped and item.method.value != "unmapped":
                concept_items.append(
                    {
                        "source_code": item.source_code,
                        "source_column": item.source_column,
                        "target_concept_id": item.target_concept_id,
                    }
                )

        return cls(
            engine=engine,
            schema_items=schema_items,
            concept_items=concept_items,
            target_model=target_model,
            project_name=project_name,
        )

    @classmethod
    def from_artifacts(
        cls,
        artifacts_dir: str,
        engine: AbstractEngine | None = None,
        engine_type: str | None = None,
    ) -> ETLRunner:
        """
        Create ETLRunner from saved artifact files.

        Reads etl_config.yaml for schema mappings and engine type.
        Reads source_to_concept_map.csv for concept lookups.

        Engine resolution order:
        1. Use `engine` if provided (developer's existing engine)
        2. Read engine type from etl_config.yaml and create via get_engine()
        3. Raise ValueError if neither is available

        Args:
            artifacts_dir: Path to directory containing saved artifacts
            engine: Optional pre-configured engine instance
            engine_type: Optional engine type override (e.g., "polars")
        """
        import yaml

        artifacts_path = Path(artifacts_dir)

        # Load config
        config_path = artifacts_path / "etl_config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(
                f"etl_config.yaml not found in {artifacts_dir}. "
                "Run generate_runner_config() first or check the artifacts directory."
            )

        with open(config_path) as f:
            config = yaml.safe_load(f)

        # Resolve engine
        resolved_engine = engine
        if resolved_engine is None:
            from portiere.engines import get_engine

            etype = engine_type or config.get("engine")
            if not etype:
                raise ValueError(
                    "No engine provided and no engine type found in etl_config.yaml. "
                    "Pass engine= or engine_type= explicitly."
                )
            resolved_engine = get_engine(etype)

        # Load schema mappings from config
        schema_items = config.get("schema_mappings", [])

        # Load concept lookups from CSV
        concept_items = []
        lookup_file = config.get("concept_lookup_file", "source_to_concept_map.csv")
        lookup_path = artifacts_path / lookup_file
        if lookup_path.exists():
            with open(lookup_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    concept_id = row.get("target_concept_id")
                    if concept_id:
                        try:
                            concept_id = int(concept_id)
                        except (ValueError, TypeError):
                            continue
                        concept_items.append(
                            {
                                "source_code": row.get("source_code", ""),
                                "source_column": row.get("source_column", ""),
                                "target_concept_id": concept_id,
                            }
                        )

        return cls(
            engine=resolved_engine,
            schema_items=schema_items,
            concept_items=concept_items,
            target_model=config.get("target_model", "omop_cdm_v5.4"),
            project_name=config.get("project_name", "medmap_project"),
        )

    @classmethod
    def from_project(
        cls,
        client: Client,
        project_id: str,
        engine: AbstractEngine | None = None,
        engine_type: str | None = None,
    ) -> ETLRunner:
        """
        Create ETLRunner from an API project.

        Fetches schema and concept mappings from the Portiere API.

        Engine resolution order:
        1. Use `engine` if provided (developer's existing engine)
        2. Use `engine_type` to create via get_engine()
        3. Raise ValueError — never silently default

        Args:
            client: Portiere API client
            project_id: Project ID to load mappings from
            engine: Optional pre-configured engine instance
            engine_type: Optional engine type (e.g., "polars")
        """
        # Resolve engine
        resolved_engine = engine
        if resolved_engine is None:
            if engine_type:
                from portiere.engines import get_engine

                resolved_engine = get_engine(engine_type)
            else:
                raise ValueError(
                    "No engine provided. Pass engine= with your configured engine "
                    "or engine_type= to create one."
                )

        # Fetch project info
        project_data = client._request("GET", f"/projects/{project_id}")

        # Fetch schema mappings
        schema_response = client._request(
            "GET", f"/schema-mapping/projects/{project_id}/schema-mapping"
        )
        schema_items = []
        schema_list = (
            schema_response
            if isinstance(schema_response, list)
            else schema_response.get("items", [])
        )
        for item in schema_list:
            status = item.get("status", "")
            if status in _ACTIONABLE_SCHEMA_STATUSES:
                target_table = item.get("override_target_table") or item.get("target_table")
                target_column = item.get("override_target_column") or item.get("target_column")
                schema_items.append(
                    {
                        "source_column": item["source_column"],
                        "target_table": target_table,
                        "target_column": target_column,
                    }
                )

        # Fetch concept mappings
        concept_response = client._request(
            "GET", f"/concepts/projects/{project_id}/concept-mapping"
        )
        concept_items = []
        concept_list = (
            concept_response
            if isinstance(concept_response, list)
            else concept_response.get("items", [])
        )
        for item in concept_list:
            method = item.get("method", "")
            concept_id = item.get("target_concept_id")
            if method != "unmapped" and concept_id is not None:
                concept_items.append(
                    {
                        "source_code": item.get("source_code", ""),
                        "source_column": item.get("source_column", ""),
                        "target_concept_id": concept_id,
                    }
                )

        return cls(
            engine=resolved_engine,
            schema_items=schema_items,
            concept_items=concept_items,
            target_model=project_data.get("target_model", "omop_cdm_v5.4"),
            project_name=project_data.get("name", "medmap_project"),
        )

    # ──────────────────────────────────────────────────────────
    # Execution
    # ──────────────────────────────────────────────────────────

    def run(
        self,
        source_path: str,
        output_path: str = "./omop_output",
        source_format: str = "csv",
        output_format: str = "parquet",
        on_progress: Callable[[str, int, int], None] | None = None,
    ) -> ETLResult:
        """
        Execute the ETL pipeline.

        Reads source data, routes columns to OMOP target tables,
        applies renames and concept lookups, writes output files.

        Args:
            source_path: Path to source data file
            output_path: Directory for output files (one per target table)
            source_format: Source file format (csv, parquet, json)
            output_format: Output file format (parquet, csv, json)
            on_progress: Optional callback(table_name, current, total)

        Returns:
            ETLResult with execution details
        """
        from portiere.exceptions import ETLExecutionError

        started_at = datetime.now(tz=timezone.utc)
        errors: list[str] = []
        warnings: list[str] = []
        table_results: list[TableResult] = []

        try:
            # Read source data
            logger.info("Reading source", path=source_path, format=source_format)
            df = self.engine.read_source(source_path, format=source_format)
            source_rows = self.engine.count(df)

            # Create output directory
            os.makedirs(output_path, exist_ok=True)

            # Track which source columns are used
            used_source_columns = set()

            total_tables = len(self._table_routes)
            for idx, (table_name, items) in enumerate(self._table_routes.items()):
                if on_progress:
                    on_progress(table_name, idx + 1, total_tables)

                logger.info("Processing table", table=table_name, columns=len(items))

                try:
                    table_result = self._process_table(
                        df, table_name, items, output_path, output_format
                    )
                    table_results.append(table_result)
                    for item in items:
                        used_source_columns.add(item["source_column"])
                except Exception as e:
                    errors.append(f"Table {table_name}: {e}")
                    logger.error("Table processing failed", table=table_name, error=str(e))

            # Identify unmapped source columns
            all_source_columns = set()
            try:
                schema = self.engine.schema(df)
                all_source_columns = {col["name"] for col in schema}
            except Exception:
                pass
            unmapped_columns = sorted(all_source_columns - used_source_columns)

            completed_at = datetime.now(tz=timezone.utc)
            duration = (completed_at - started_at).total_seconds()

            total_rows_written = sum(t.rows_written for t in table_results)
            concept_cols_applied = sum(len(t.concept_columns_mapped) for t in table_results)

            result = ETLResult(
                success=len(errors) == 0,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                source_path=source_path,
                source_rows_read=source_rows,
                output_path=output_path,
                tables=table_results,
                total_rows_written=total_rows_written,
                schema_mappings_applied=len(self.schema_items),
                concept_mappings_applied=concept_cols_applied,
                unmapped_columns=unmapped_columns,
                engine_name=self.engine.engine_name,
                errors=errors,
                warnings=warnings,
            )

            logger.info("ETL complete", success=result.success, duration=duration)
            return result

        except Exception as e:
            completed_at = datetime.now(tz=timezone.utc)
            duration = (completed_at - started_at).total_seconds()
            result = ETLResult(
                success=False,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                source_path=source_path,
                output_path=output_path,
                engine_name=self.engine.engine_name,
                errors=[str(e)],
            )
            raise ETLExecutionError(str(e), result=result) from e

    def _process_table(
        self,
        df: Any,
        table_name: str,
        items: list[dict],
        output_path: str,
        output_format: str,
    ) -> TableResult:
        """Process a single target table: select, rename, concept lookup, write."""
        # Collect source columns needed for this table
        source_cols = []
        renames = {}
        seen_targets = set()  # Prevent duplicate target columns
        for item in items:
            src = item["source_column"]
            tgt = item.get("target_column", src)
            # Skip if another source already maps to this target column
            if tgt in seen_targets:
                logger.warning(f"Skipping duplicate target '{tgt}' from '{src}' in {table_name}")
                continue
            seen_targets.add(tgt)
            if src not in source_cols:
                source_cols.append(src)
            if src != tgt:
                renames[src] = tgt

        # Select columns
        table_df = self.engine.transform(df, {"select": source_cols})

        # Rename columns
        if renames:
            table_df = self.engine.transform(table_df, {"renames": renames})

        # Apply concept lookups
        concept_cols_mapped = []
        for src_col, lookup in self._concept_lookups.items():
            # Check if this source column is used in this table
            if src_col in source_cols:
                # Determine the renamed column name
                col_name = renames.get(src_col, src_col)
                target_col = f"{col_name}_concept_id"
                table_df = self.engine.map_column(table_df, col_name, lookup, target_col, default=0)
                concept_cols_mapped.append(col_name)

        # Write output
        file_path = os.path.join(output_path, f"{table_name}.{output_format}")
        self.engine.write(table_df, file_path, format=output_format)

        rows_written = self.engine.count(table_df)
        output_columns = [col["name"] for col in self.engine.schema(table_df)]

        return TableResult(
            table_name=table_name,
            rows_written=rows_written,
            columns=output_columns,
            output_path=file_path,
            concept_columns_mapped=concept_cols_mapped,
        )

    def dry_run(
        self,
        source_path: str,
        source_format: str = "csv",
    ) -> dict:
        """
        Preview the ETL plan without writing any files.

        Args:
            source_path: Path to source data
            source_format: Source file format

        Returns:
            Dict with table routing plan, column mappings, and concept lookups
        """
        df = self.engine.read_source(source_path, format=source_format)
        source_rows = self.engine.count(df)
        source_schema = self.engine.schema(df)
        source_columns = {col["name"] for col in source_schema}

        tables_plan = {}
        used_columns = set()

        for table_name, items in self._table_routes.items():
            cols = []
            for item in items:
                src = item["source_column"]
                tgt = item.get("target_column", src)
                available = src in source_columns
                has_concept = src in self._concept_lookups
                cols.append(
                    {
                        "source_column": src,
                        "target_column": tgt,
                        "available": available,
                        "has_concept_lookup": has_concept,
                        "concept_codes_count": len(self._concept_lookups.get(src, {}))
                        if has_concept
                        else 0,
                    }
                )
                if available:
                    used_columns.add(src)

            tables_plan[table_name] = {
                "columns": cols,
                "column_count": len(cols),
            }

        unmapped_columns = sorted(source_columns - used_columns)

        return {
            "source_path": source_path,
            "source_rows": source_rows,
            "source_columns": sorted(source_columns),
            "tables": tables_plan,
            "table_count": len(tables_plan),
            "schema_mappings": len(self.schema_items),
            "concept_lookups": len(self._concept_lookups),
            "unmapped_columns": unmapped_columns,
            "engine": self.engine.engine_name,
        }

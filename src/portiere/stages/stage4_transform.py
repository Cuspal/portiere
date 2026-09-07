"""
Portiere Stage 4 — ETL Code Generation.

This stage:
1. Takes finalized schema and concept mappings
2. Generates runnable ETL code (Spark/Polars)
3. Produces standalone artifact that runs without Portiere SDK
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from portiere.models.mapping_policy import concept_is_executable, schema_is_executable

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)


def generate_etl(
    engine: "AbstractEngine",
    schema_mapping: dict[str, Any],
    concept_mapping: dict[str, Any],
    source_path: str,
    output_path: str,
    artifact_dir: str | None = None,
) -> dict[str, Any]:
    """
    Generate ETL transformation code.

    Args:
        engine: Target compute engine
        schema_mapping: Finalized schema mappings
        concept_mapping: Finalized concept mappings
        source_path: Path to source data
        output_path: Path for transformed output
        artifact_dir: Directory for generated artifacts

    Returns:
        Generation result with artifact paths
    """
    logger.info("Stage 4: Generating ETL artifacts")

    schema_mapping = {
        **schema_mapping,
        "items": [
            item
            for item in schema_mapping.get("items", [])
            if "status" not in item or schema_is_executable(item)
        ],
    }
    concept_mapping = {
        **concept_mapping,
        "items": [item for item in concept_mapping.get("items", []) if concept_is_executable(item)],
        "mappings": {
            column: {
                **data,
                "items": [item for item in data.get("items", []) if concept_is_executable(item)],
            }
            for column, data in concept_mapping.get("mappings", {}).items()
            if any(concept_is_executable(item) for item in data.get("items", []))
        },
    }

    artifact_path = Path(artifact_dir or "./medmap_artifacts")
    artifact_path.mkdir(parents=True, exist_ok=True)

    engine_name = engine.engine_name

    # Generate ETL script
    if engine_name == "spark":
        script = _generate_spark_etl(schema_mapping, concept_mapping, source_path, output_path)
    elif engine_name == "polars":
        script = _generate_polars_etl(schema_mapping, concept_mapping, source_path, output_path)
    else:
        script = _generate_pandas_etl(schema_mapping, concept_mapping, source_path, output_path)

    # Save ETL script
    script_path = artifact_path / f"etl_{engine_name}.py"
    script_path.write_text(script)

    # Generate mapping lookup table
    lookup_path = artifact_path / "concept_lookup.csv"
    _generate_lookup_table(concept_mapping, lookup_path)

    # Generate config
    config_path = artifact_path / "etl_config.yaml"
    _generate_config(schema_mapping, concept_mapping, source_path, output_path, config_path)

    result = {
        "artifacts": [
            {"type": "etl_script", "path": str(script_path)},
            {"type": "lookup_table", "path": str(lookup_path)},
            {"type": "config", "path": str(config_path)},
        ],
        "engine": engine_name,
    }

    logger.info("Stage 4 complete", artifacts=len(result["artifacts"]))
    return result


def _generate_spark_etl(
    schema_mapping: dict,
    concept_mapping: dict,
    source_path: str,
    output_path: str,
) -> str:
    """Generate PySpark ETL script."""
    # Build column rename lines from schema mapping
    rename_lines = []
    for item in schema_mapping.get("items", []):
        src_col = item.get("source_column", "")
        tgt_col = item.get("effective_target_column") or item.get("target_column", "")
        if src_col and tgt_col and src_col != tgt_col:
            rename_lines.append(f'    df = df.withColumnRenamed("{src_col}", "{tgt_col}")')

    rename_block = (
        "\n".join(rename_lines) if rename_lines else "    pass  # No column renames needed"
    )

    # Build concept join lines from concept mapping
    join_columns = set()
    for item in concept_mapping.get("items", []):
        col = item.get("source_column")
        if col:
            join_columns.add(col)
    # Also check nested mappings dict format
    for col in concept_mapping.get("mappings", {}).keys():
        join_columns.add(col)

    join_lines = []
    for col in sorted(join_columns):
        safe = col.replace(" ", "_")
        join_lines.append(f'''
    # Join concept lookup for column: {col}
    lookup_{safe} = lookup.filter(F.col("source_column") == "{col}")
    df = df.join(
        lookup_{safe}.select(
            F.col("source_code").alias("{col}"),
            F.col("target_concept_id").alias("{safe}_concept_id"),
            F.col("target_concept_name").alias("{safe}_concept_name"),
        ),
        on="{col}",
        how="left",
    )''')

    join_block = "\n".join(join_lines) if join_lines else "    pass  # No concept joins needed"

    return f'''#!/usr/bin/env python3
"""
Portiere Generated ETL — PySpark
Generated from finalized mappings.
Run: spark-submit etl_spark.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuration
SOURCE_PATH = {source_path!r}
OUTPUT_PATH = {output_path!r}
LOOKUP_PATH = "concept_lookup.csv"


def main():
    spark = SparkSession.builder \\
        .appName("Portiere ETL") \\
        .getOrCreate()

    # Load source data
    df = spark.read.csv(SOURCE_PATH, header=True, inferSchema=True)

    # Load concept mapping lookup
    lookup = spark.read.csv(LOOKUP_PATH, header=True)

    # Apply schema mappings (column renames)
{rename_block}

    # Apply concept mappings (join with lookup)
{join_block}

    # Write output
    df.write.parquet(OUTPUT_PATH, mode="overwrite")

    print(f"ETL complete. Output: {{OUTPUT_PATH}}")
    spark.stop()


if __name__ == "__main__":
    main()
'''


def _generate_polars_etl(
    schema_mapping: dict,
    concept_mapping: dict,
    source_path: str,
    output_path: str,
) -> str:
    """Generate Polars ETL script."""
    # Build column rename dict from schema mapping
    renames = {}
    for item in schema_mapping.get("items", []):
        src_col = item.get("source_column", "")
        tgt_col = item.get("effective_target_column") or item.get("target_column", "")
        if src_col and tgt_col and src_col != tgt_col:
            renames[src_col] = tgt_col

    if renames:
        rename_block = f"    df = df.rename({renames!r})"
    else:
        rename_block = "    pass  # No column renames needed"

    # Build concept join lines
    join_columns = set()
    for item in concept_mapping.get("items", []):
        col = item.get("source_column")
        if col:
            join_columns.add(col)
    for col in concept_mapping.get("mappings", {}).keys():
        join_columns.add(col)

    join_lines = []
    for col in sorted(join_columns):
        safe = col.replace(" ", "_")
        join_lines.append(f'''
    # Join concept lookup for column: {col}
    lookup_{safe} = lookup.filter(pl.col("source_column") == "{col}").select([
        pl.col("source_code").alias("{col}"),
        pl.col("target_concept_id").alias("{safe}_concept_id"),
        pl.col("target_concept_name").alias("{safe}_concept_name"),
    ])
    df = df.join(lookup_{safe}, on="{col}", how="left")''')

    join_block = "\n".join(join_lines) if join_lines else "    pass  # No concept joins needed"

    return f'''#!/usr/bin/env python3
"""
Portiere Generated ETL — Polars
Generated from finalized mappings.
Run: python etl_polars.py
"""

import polars as pl

# Configuration
SOURCE_PATH = {source_path!r}
OUTPUT_PATH = {output_path!r}
LOOKUP_PATH = "concept_lookup.csv"


def main():
    # Load source data
    df = pl.read_csv(SOURCE_PATH)
    print(f"Loaded {{len(df):,}} rows from {{SOURCE_PATH}}")

    # Load concept mapping lookup
    lookup = pl.read_csv(LOOKUP_PATH)

    # Apply schema mappings (column renames)
{rename_block}

    # Apply concept mappings (join with lookup)
{join_block}

    # Write output
    df.write_parquet(OUTPUT_PATH)
    print(f"ETL complete. Output: {{OUTPUT_PATH}}")


if __name__ == "__main__":
    main()
'''


def _generate_pandas_etl(
    schema_mapping: dict,
    concept_mapping: dict,
    source_path: str,
    output_path: str,
) -> str:
    """Generate Pandas ETL script."""
    # Build column rename dict from schema mapping
    renames = {}
    for item in schema_mapping.get("items", []):
        src_col = item.get("source_column", "")
        tgt_col = item.get("effective_target_column") or item.get("target_column", "")
        if src_col and tgt_col and src_col != tgt_col:
            renames[src_col] = tgt_col

    if renames:
        rename_block = f"    df = df.rename(columns={renames!r})"
    else:
        rename_block = "    pass  # No column renames needed"

    # Build concept merge lines
    join_columns = set()
    for item in concept_mapping.get("items", []):
        col = item.get("source_column")
        if col:
            join_columns.add(col)
    for col in concept_mapping.get("mappings", {}).keys():
        join_columns.add(col)

    merge_lines = []
    for col in sorted(join_columns):
        safe = col.replace(" ", "_")
        merge_lines.append(f'''
    # Merge concept lookup for column: {col}
    lookup_{safe} = lookup[lookup["source_column"] == "{col}"][["source_code", "target_concept_id", "target_concept_name"]].copy()
    lookup_{safe} = lookup_{safe}.rename(columns={{
        "source_code": "{col}",
        "target_concept_id": "{safe}_concept_id",
        "target_concept_name": "{safe}_concept_name",
    }})
    df = df.merge(lookup_{safe}, on="{col}", how="left")''')

    merge_block = "\n".join(merge_lines) if merge_lines else "    pass  # No concept merges needed"

    return f'''#!/usr/bin/env python3
"""
Portiere Generated ETL — Pandas
Generated from finalized mappings.
Run: python etl_pandas.py
"""

import pandas as pd

# Configuration
SOURCE_PATH = {source_path!r}
OUTPUT_PATH = {output_path!r}
LOOKUP_PATH = "concept_lookup.csv"


def main():
    # Load source data
    df = pd.read_csv(SOURCE_PATH)
    print(f"Loaded {{len(df):,}} rows from {{SOURCE_PATH}}")

    # Load concept mapping lookup
    lookup = pd.read_csv(LOOKUP_PATH)

    # Apply schema mappings (column renames)
{rename_block}

    # Apply concept mappings (merge with lookup)
{merge_block}

    # Write output
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"ETL complete. Output: {{OUTPUT_PATH}}")


if __name__ == "__main__":
    main()
'''


def _generate_lookup_table(concept_mapping: dict, path: Path) -> None:
    """Generate concept lookup CSV for ETL.

    Uses the ``csv`` module so that codes or names containing commas, quotes, or
    newlines are quoted correctly rather than splitting or truncating a row —
    a mis-split row would silently drop or mis-map concepts in the generated
    join. ``lineterminator="\\n"`` keeps the output byte-stable across platforms.
    """
    import csv
    import io

    header = [
        "source_code",
        "source_column",
        "target_concept_id",
        "target_concept_name",
        "confidence",
        "method",
    ]
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)

    # Handle nested dict format: {mappings: {column: {items: [...]}}}
    for column, data in concept_mapping.get("mappings", {}).items():
        for item in data.get("items", []):
            if not concept_is_executable(item):
                continue
            writer.writerow(
                [
                    item.get("source_code", ""),
                    column,
                    item.get("target_concept_id", ""),
                    item.get("target_concept_name", ""),
                    item.get("confidence", 0),
                    item.get("method", ""),
                ]
            )

    # Handle flat list format: {items: [...]} from ConceptMapping.to_source_to_concept_map()
    for item in concept_mapping.get("items", []):
        if not concept_is_executable(item):
            continue
        writer.writerow(
            [
                item.get("source_code", ""),
                item.get("source_column", item.get("source_vocabulary_id", "")),
                item.get("target_concept_id", ""),
                item.get("target_concept_name", ""),
                item.get("confidence", 0),
                item.get("method", ""),
            ]
        )

    path.write_text(buf.getvalue().rstrip("\n"))


def _generate_config(
    schema_mapping: dict,
    concept_mapping: dict,
    source_path: str,
    output_path: str,
    path: Path,
) -> None:
    """Generate ETL config YAML."""
    import yaml

    config = {
        "version": "1.0",
        "source": {"path": source_path},
        "output": {"path": output_path, "format": "parquet"},
        "schema_mapping": {
            "columns": len(schema_mapping.get("mappings", [])),
        },
        "concept_mapping": {
            "columns": list(concept_mapping.get("mappings", {}).keys()),
            "stats": concept_mapping.get("stats", {}),
        },
    }

    path.write_text(yaml.dump(config, default_flow_style=False))

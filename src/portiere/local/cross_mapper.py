"""
Portiere Cross-Standard Mapper — Map data between clinical data standards.

Supports mapping between any pair of registered standards using YAML-based
crossmap definitions. For example:
- OMOP CDM → FHIR R4
- HL7 v2 → FHIR R4
- FHIR R4 → OpenEHR
- OMOP CDM → OpenEHR

Crossmap definitions are declarative YAML files that specify:
- Entity-level correspondences (e.g., person → Patient)
- Field-level mappings with transforms
- Value transformations (static maps, format conversions, vocabulary lookups)

Example:
    >>> from portiere.local.cross_mapper import CrossStandardMapper
    >>> mapper = CrossStandardMapper("omop_cdm_v5.4", "fhir_r4")
    >>> result = mapper.map_record("person", {
    ...     "person_id": 12345,
    ...     "gender_concept_id": 8507,
    ...     "birth_datetime": "1990-05-15",
    ... })
    >>> result
    {'id': '12345', 'gender': 'male', 'birthDate': '1990-05-15'}
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog
import yaml

from portiere.local.transforms import TransformRegistry
from portiere.models.target_model import get_target_model

logger = structlog.get_logger(__name__)

# Directory containing built-in crossmap YAML files
CROSSMAPS_DIR = Path(__file__).parent.parent / "standards" / "crossmaps"


def list_crossmaps() -> list[dict[str, str]]:
    """List all available crossmap definitions."""
    crossmaps = []
    if CROSSMAPS_DIR.exists():
        for f in sorted(CROSSMAPS_DIR.glob("*.yaml")):
            data = yaml.safe_load(f.read_text())
            crossmaps.append(
                {
                    "source": data.get("source", ""),
                    "target": data.get("target", ""),
                    "file": f.name,
                }
            )
    return crossmaps


class CrossStandardMapper:
    """
    Map data from one clinical standard to another.

    Uses YAML-based crossmap definitions with pluggable transforms.
    """

    def __init__(
        self,
        source_standard: str,
        target_standard: str,
        custom_crossmap: Path | None = None,
        vocabulary_bridge: Any | None = None,
    ):
        """
        Initialize cross-standard mapper.

        Args:
            source_standard: Source standard name (e.g., "omop_cdm_v5.4")
            target_standard: Target standard name (e.g., "fhir_r4")
            custom_crossmap: Path to custom crossmap YAML (optional)
            vocabulary_bridge: VocabularyBridge instance for vocabulary lookups (optional)
        """
        self.source_model = get_target_model(source_standard)
        self.target_model = get_target_model(target_standard)
        self.vocabulary_bridge = vocabulary_bridge
        self.transforms = TransformRegistry()

        # Load crossmap definition
        self._crossmap = self._load_crossmap(source_standard, target_standard, custom_crossmap)
        self._entity_map = self._crossmap.get("entity_map", {})
        self._field_map = self._crossmap.get("field_map", {})
        self._transform_defs = self._crossmap.get("transforms", {})

    def _load_crossmap(
        self,
        source: str,
        target: str,
        custom_path: Path | None = None,
    ) -> dict:
        """Load crossmap YAML definition."""
        if custom_path and custom_path.exists():
            return yaml.safe_load(custom_path.read_text())

        # Search built-in crossmaps
        source_lower = source.lower().replace(".", "_")
        target_lower = target.lower().replace(".", "_")

        # Try exact filename match
        for pattern in [
            f"{source_lower}_to_{target_lower}.yaml",
            f"{source}_to_{target}.yaml",
        ]:
            candidate = CROSSMAPS_DIR / pattern
            if candidate.exists():
                return yaml.safe_load(candidate.read_text())

        # Search by source/target fields in YAML
        if CROSSMAPS_DIR.exists():
            for f in CROSSMAPS_DIR.glob("*.yaml"):
                data = yaml.safe_load(f.read_text())
                if data.get("source") == source and data.get("target") == target:
                    return data

        logger.warning(f"No crossmap found for {source} → {target}, using empty mapping")
        return {"source": source, "target": target, "entity_map": {}, "field_map": {}}

    def map_record(self, source_entity: str, record: dict) -> dict:
        """
        Map a single record from source entity to target entity.

        Args:
            source_entity: Source entity name (e.g., "person", "PID")
            record: Source record as dict

        Returns:
            Mapped record in target standard format
        """
        target_entity = self._entity_map.get(source_entity)
        if not target_entity:
            logger.warning(f"No entity mapping for {source_entity}")
            return record

        result: dict[str, Any] = {}

        # Apply field-level mappings
        for field_key, mapping_def in self._field_map.items():
            # field_key format: "source_entity.source_field"
            if not field_key.startswith(f"{source_entity}."):
                continue

            source_field = field_key.split(".", 1)[1]
            source_value = record.get(source_field)

            if isinstance(mapping_def, dict):
                target_path = mapping_def.get("target", "")
                transform_name = mapping_def.get("transform", "passthrough")
                transform_config = mapping_def.get("config")
            elif isinstance(mapping_def, str):
                target_path = mapping_def
                transform_name = "passthrough"
                transform_config = None
            else:
                continue

            # Resolve named transforms from the crossmap's transforms section
            if transform_name in self._transform_defs:
                transform_def = self._transform_defs[transform_name]
                actual_transform = transform_def.get("type", transform_name)
                transform_config = transform_def
            else:
                actual_transform = transform_name

            # Execute transform
            transformed = self.transforms.execute(
                actual_transform,
                source_value,
                config=transform_config,
                record=record,
                vocabulary_bridge=self.vocabulary_bridge,
            )

            # Set value in result (handle nested paths like "Patient.gender")
            if "." in target_path:
                _, target_field = target_path.split(".", 1)
            else:
                target_field = target_path

            # Handle array notation (e.g., "code.coding[0].code")
            self._set_nested(result, target_field, transformed)

        return result

    def map_records(self, source_entity: str, records: list[dict]) -> list[dict]:
        """Map multiple records from source entity to target entity."""
        return [self.map_record(source_entity, r) for r in records]

    def map_dataframe(self, source_entity: str, df: Any) -> Any:
        """
        Map a DataFrame from source to target schema.

        Supports Polars, Pandas, and Spark DataFrames. The return type
        matches the input type so engine pipelines stay consistent.

        Args:
            source_entity: Source entity name
            df: Polars, Pandas, or Spark DataFrame with source data

        Returns:
            DataFrame in the same type as input with target schema columns
        """
        # Detect DataFrame type and convert to records
        df_type = type(df).__module__.split(".")[0]  # "polars", "pandas", "pyspark"

        if df_type == "polars":
            records = df.to_dicts()
            mapped = self.map_records(source_entity, records)
            import polars as pl

            return pl.DataFrame(mapped)
        elif df_type == "pyspark":
            records = df.toPandas().to_dict("records")
            mapped = self.map_records(source_entity, records)
            import pandas as pd

            return df.sparkSession.createDataFrame(pd.DataFrame(mapped))
        else:
            # Default: treat as pandas
            import pandas as pd

            records = df.to_dict("records")
            mapped = self.map_records(source_entity, records)
            return pd.DataFrame(mapped)

    def get_entity_map(self) -> dict[str, str]:
        """Return the entity-level mapping (source → target)."""
        return dict(self._entity_map)

    def get_field_map(self, source_entity: str) -> dict[str, dict]:
        """Return field-level mappings for a specific source entity."""
        prefix = f"{source_entity}."
        return {k[len(prefix) :]: v for k, v in self._field_map.items() if k.startswith(prefix)}

    def get_mapping_report(self) -> dict:
        """
        Generate a report on mapping coverage.

        Returns dict with:
        - entity_mappings: list of source→target entity pairs
        - field_mappings: count of field-level mappings
        - unmapped_source_fields: source fields with no mapping
        - unmapped_target_fields: target fields with no mapping
        """
        source_schema = self.source_model.get_schema()
        target_schema = self.target_model.get_schema()

        mapped_source_fields = set()
        mapped_target_fields = set()

        for field_key, mapping_def in self._field_map.items():
            if "." in field_key:
                entity, field = field_key.split(".", 1)
                mapped_source_fields.add((entity, field))

            if isinstance(mapping_def, dict):
                target_path = mapping_def.get("target", "")
            else:
                target_path = str(mapping_def)

            if "." in target_path:
                t_entity, t_field = target_path.split(".", 1)
                mapped_target_fields.add((t_entity, t_field))

        # Find unmapped fields
        unmapped_source = []
        for entity, fields in source_schema.items():
            for field in fields:
                if (entity, field) not in mapped_source_fields:
                    unmapped_source.append(f"{entity}.{field}")

        unmapped_target = []
        for entity, fields in target_schema.items():
            for field in fields:
                if (entity, field) not in mapped_target_fields:
                    unmapped_target.append(f"{entity}.{field}")

        return {
            "source_standard": self.source_model.name,
            "target_standard": self.target_model.name,
            "entity_mappings": [{"source": k, "target": v} for k, v in self._entity_map.items()],
            "field_mappings": len(self._field_map),
            "unmapped_source_fields": unmapped_source,
            "unmapped_target_fields": unmapped_target,
        }

    @staticmethod
    def _set_nested(d: dict, path: str, value: Any):
        """Set a value in a nested dict using dot-separated path."""
        # Strip array notation for now (e.g., "coding[0].code" → "code")
        # Simple implementation — handles one level of nesting
        parts = path.replace("[0]", "").replace("[1]", "").split(".")
        if len(parts) == 1:
            d[parts[0]] = value
        else:
            current = d
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value

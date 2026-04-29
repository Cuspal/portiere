"""
Portiere Standards — Externalized clinical data standard definitions.

Provides YAML-driven standard definitions for OMOP CDM, FHIR R4,
HL7 v2.5.1, and OpenEHR. Users can add/replace YAML files to support
new standard versions without code changes.

Example:
    >>> from portiere.standards import YAMLTargetModel, get_standards_dir, list_standards
    >>> model = YAMLTargetModel.from_name("omop_cdm_v5.4")
    >>> model.get_schema()
    {'person': ['person_id', 'gender_concept_id', ...], ...}
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog
import yaml

from portiere.models.target_model import TargetModel

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine

logger = structlog.get_logger(__name__)

# Directory containing built-in standard YAML definitions
STANDARDS_DIR = Path(__file__).parent


def get_standards_dir() -> Path:
    """Return path to the built-in standards directory."""
    return STANDARDS_DIR


def list_standards() -> list[str]:
    """List all available standard definition names."""
    standards = []
    for f in sorted(STANDARDS_DIR.glob("*.yaml")):
        data = yaml.safe_load(f.read_text())
        if "name" in data:
            standards.append(data["name"])
    return standards


# Maps raw YAML field type strings to validation categories.
# Used by YAMLTargetModel.get_field_types() for standards-aware validation.
_TYPE_CATEGORY_MAP: dict[str, str] = {
    # Temporal
    "date": "temporal",
    "datetime": "temporal",
    "DateTime": "temporal",
    "instant": "temporal",
    "Period": "temporal",
    "TS": "temporal",
    "DV_DATE": "temporal",
    "DV_DATE_TIME": "temporal",
    # Code / vocabulary
    "code": "code",
    "CodeableConcept": "code",
    "CodeableConcept[]": "code",
    "Coding": "code",
    "CE": "code",
    "IS": "code",
    "ID": "code",
    "CNE": "code",
    "CWE": "code",
    "DV_CODED_TEXT": "code",
    # Numeric
    "integer": "numeric",
    "float": "numeric",
    "NM": "numeric",
    "DV_QUANTITY": "numeric",
    "DV_COUNT": "numeric",
    # Reference
    "Reference": "reference",
    "CX": "reference",
    "PARTY_IDENTIFIED": "reference",
    # String
    "string": "string",
    "ST": "string",
    "DV_TEXT": "string",
    "text": "string",
    "XPN": "string",
    "XAD": "string",
    "XTN": "string",
}


class YAMLTargetModel(TargetModel):
    """
    Generic target model loader from YAML definition files.

    Loads any clinical data standard (OMOP, FHIR, HL7 v2, OpenEHR, etc.)
    from a structured YAML file. Implements the full TargetModel interface.

    The YAML schema supports:
    - Entity definitions (tables/resources/segments/archetypes) with fields
    - Source patterns for fast column-name matching
    - Embedding descriptions for AI-powered semantic matching
    - Vocabulary system URL mappings
    - DDL type information for schema generation
    - Required field tracking for validation
    - Field type metadata for standards-aware quality validation
    """

    def __init__(self, yaml_path: Path):
        """
        Load a standard definition from a YAML file.

        Args:
            yaml_path: Path to the YAML definition file.
        """
        if not yaml_path.exists():
            raise FileNotFoundError(f"Standard definition not found: {yaml_path}")

        self._path = yaml_path
        self._def = yaml.safe_load(yaml_path.read_text())
        self._validate_definition()

    def _validate_definition(self):
        """Validate that required YAML keys are present."""
        required = ["name", "version", "standard_type", "entities"]
        missing = [k for k in required if k not in self._def]
        if missing:
            raise ValueError(
                f"Standard definition {self._path.name} missing required keys: {missing}"
            )

    @classmethod
    def from_name(cls, name: str) -> YAMLTargetModel:
        """
        Load a standard by its registered name.

        Searches the built-in standards directory for a YAML file
        whose 'name' field matches.

        Args:
            name: Standard name (e.g., "omop_cdm_v5.4", "fhir_r4")

        Returns:
            YAMLTargetModel instance

        Raises:
            ValueError: If no matching standard is found.
        """
        for f in STANDARDS_DIR.glob("*.yaml"):
            data = yaml.safe_load(f.read_text())
            if data.get("name") == name:
                return cls(f)

        available = list_standards()
        raise ValueError(f"Standard '{name}' not found. Available: {', '.join(available)}")

    # ── TargetModel interface ──────────────────────────────────────

    @property
    def name(self) -> str:
        return self._def["name"]

    @property
    def version(self) -> str:
        return self._def["version"]

    @property
    def standard_type(self) -> str:
        """Return the standard type (relational, resource, segment, archetype)."""
        return self._def.get("standard_type", "relational")

    @property
    def organization(self) -> str:
        """Return the standards organization (OHDSI, HL7, openEHR, etc.)."""
        return self._def.get("organization", "")

    @property
    def description(self) -> str:
        """Return human-readable description of the standard."""
        return self._def.get("description", "")

    def get_schema(self) -> dict[str, list[str]]:
        """
        Return schema: entity_name → list of field names.

        For OMOP: table → columns
        For FHIR: resource → fields
        For HL7 v2: segment → fields
        For OpenEHR: archetype → elements
        """
        schema = {}
        for entity_name, entity_def in self._def.get("entities", {}).items():
            fields = list(entity_def.get("fields", {}).keys())
            schema[entity_name] = fields
        return schema

    def get_target_descriptions(self) -> dict[str, str]:
        """
        Return AI-friendly descriptions for schema mapping.

        Keys are "entity.field" format (e.g., "person.person_id").
        Values are human-readable descriptions optimized for embedding similarity.
        """
        descriptions = {}
        for entity_name, entity_def in self._def.get("entities", {}).items():
            # Use embedding_descriptions if available (preferred for AI)
            embed_descs = entity_def.get("embedding_descriptions", {})
            if embed_descs:
                for field_name, desc in embed_descs.items():
                    descriptions[f"{entity_name}.{field_name}"] = desc
            else:
                # Fall back to field-level descriptions
                for field_name, field_def in entity_def.get("fields", {}).items():
                    if isinstance(field_def, dict) and "description" in field_def:
                        descriptions[f"{entity_name}.{field_name}"] = field_def["description"]
        return descriptions

    def get_target_descriptions_tupled(self) -> dict[tuple[str, str], str]:
        """
        Return descriptions keyed by (entity, field) tuples.

        Compatible with the existing LocalSchemaMapper format
        (OMOP_TARGET_DESCRIPTIONS uses tuple keys).
        """
        descriptions = {}
        for entity_name, entity_def in self._def.get("entities", {}).items():
            embed_descs = entity_def.get("embedding_descriptions", {})
            if embed_descs:
                for field_name, desc in embed_descs.items():
                    descriptions[(entity_name, field_name)] = desc
            else:
                for field_name, field_def in entity_def.get("fields", {}).items():
                    if isinstance(field_def, dict) and "description" in field_def:
                        descriptions[(entity_name, field_name)] = field_def["description"]
        return descriptions

    def get_source_patterns(self) -> dict[str, tuple[str, str]]:
        """
        Return source column patterns for fast matching.

        Keys are source column name patterns (lowercase).
        Values are (entity_name, field_name) tuples.

        This replaces the hardcoded OMOP_COLUMN_PATTERNS dict.
        """
        patterns = {}
        for entity_name, entity_def in self._def.get("entities", {}).items():
            for pattern, field_name in entity_def.get("source_patterns", {}).items():
                patterns[pattern] = (entity_name, field_name)
        return patterns

    def get_required_fields(self) -> dict[str, list[str]]:
        """Return required fields per entity (for validation)."""
        # Check top-level required_fields key first
        if "required_fields" in self._def:
            return self._def["required_fields"]

        # Otherwise, derive from field definitions
        required = {}
        for entity_name, entity_def in self._def.get("entities", {}).items():
            req_fields = []
            for field_name, field_def in entity_def.get("fields", {}).items():
                if isinstance(field_def, dict) and field_def.get("required"):
                    req_fields.append(field_name)
            if req_fields:
                required[entity_name] = req_fields
        return required

    def get_field_types(self, entity: str) -> dict[str, str]:
        """
        Return field name → validation category for an entity.

        Categories:
            "code"      — vocabulary/coded field (concept_id, CodeableConcept, CE, DV_CODED_TEXT)
            "temporal"  — date/datetime field
            "numeric"   — numeric field (integer, float) that is NOT a code field
            "string"    — text/string field
            "reference" — reference to another entity
            "other"     — anything not classified above

        Fields with a ``vocabulary`` or ``valueset`` key are always classified
        as ``"code"`` regardless of their raw type (e.g., an OMOP ``integer``
        field with ``vocabulary: "Gender"`` becomes ``"code"``).
        """
        entity_def = self._def.get("entities", {}).get(entity, {})
        fields = entity_def.get("fields", {})
        result: dict[str, str] = {}
        for field_name, field_def in fields.items():
            if isinstance(field_def, dict):
                raw_type = field_def.get("type", "string")
                if field_def.get("vocabulary") or field_def.get("valueset"):
                    result[field_name] = "code"
                elif raw_type.startswith("Reference("):
                    result[field_name] = "reference"
                else:
                    result[field_name] = _TYPE_CATEGORY_MAP.get(raw_type, "other")
            else:
                result[field_name] = "other"
        return result

    def get_plausibility_rules(self, entity: str) -> list:
        """Return parsed plausibility rules for an entity.

        Pulls the entity's ``plausibility:`` YAML block (if any) and parses
        each rule via :func:`portiere.quality.plausibility.dsl.parse_rule`.

        Returns
        -------
        list[PlausibilityRule]
            Empty list if the entity is missing or has no ``plausibility:``
            block. Otherwise a list of typed rule models (RangeRule,
            RegexRule, EnumRule, TemporalOrderRule, FkExistsRule).
        """
        from portiere.quality.plausibility.dsl import parse_rules

        entity_def = self._def.get("entities", {}).get(entity, {})
        return parse_rules(entity_def.get("plausibility", []))

    def get_vocabulary_systems(self) -> dict[str, str]:
        """Return vocabulary_id → system URI mapping (e.g., SNOMED → http://snomed.info/sct)."""
        return self._def.get("vocabulary_systems", {})

    def get_default_entity(self) -> str:
        """Return the default fallback entity for unmapped columns."""
        return self._def.get("default_entity", next(iter(self._def.get("entities", {})), ""))

    def get_default_field(self) -> str:
        """Return the default fallback field for unmapped columns."""
        return self._def.get("default_field", "")

    def validate_output(self, engine: AbstractEngine, output_path: str) -> dict[str, Any]:
        """
        Validate transformed output against this standard's specification.

        Uses required fields and entity definitions from the YAML.
        """
        from portiere.stages.stage5_validate import validate_output

        return validate_output(engine, output_path, target_model=self.name)

    def generate_ddl(self) -> str:
        """
        Generate schema definition language for this standard.

        For relational models (OMOP): SQL CREATE TABLE statements
        For resource models (FHIR): JSON StructureDefinition template
        For segment models (HL7 v2): Segment definition summary
        For archetype models (OpenEHR): Archetype definition summary
        """
        std_type = self.standard_type

        if std_type == "relational":
            return self._generate_sql_ddl()
        elif std_type == "resource":
            return self._generate_fhir_structure()
        elif std_type == "segment":
            return self._generate_segment_summary()
        elif std_type == "archetype":
            return self._generate_archetype_summary()
        else:
            return self._generate_generic_summary()

    # ── DDL generators ─────────────────────────────────────────────

    def _generate_sql_ddl(self) -> str:
        """Generate SQL DDL for relational standards."""
        statements = []
        for entity_name, entity_def in self._def.get("entities", {}).items():
            columns = []
            fks = []
            for field_name, field_def in entity_def.get("fields", {}).items():
                if isinstance(field_def, dict):
                    ddl_type = field_def.get(
                        "ddl", self._type_to_sql(field_def.get("type", "string"))
                    )
                    columns.append(f"    {field_name} {ddl_type}")
                    if "fk" in field_def:
                        ref_table, ref_col = field_def["fk"].split(".")
                        fks.append(
                            f"    FOREIGN KEY ({field_name}) REFERENCES {ref_table}({ref_col})"
                        )
                else:
                    columns.append(f"    {field_name} VARCHAR(255)")

            all_lines = columns + fks
            stmt = f"CREATE TABLE {entity_name} (\n" + ",\n".join(all_lines) + "\n);"
            statements.append(stmt)

        return "\n\n".join(statements)

    def _generate_fhir_structure(self) -> str:
        """Generate FHIR StructureDefinition summary."""
        import json

        resources = {}
        for entity_name, entity_def in self._def.get("entities", {}).items():
            fields = {}
            for field_name, field_def in entity_def.get("fields", {}).items():
                if isinstance(field_def, dict):
                    fields[field_name] = {
                        "type": field_def.get("type", "string"),
                        "required": field_def.get("required", False),
                        "description": field_def.get("description", ""),
                    }
            resources[entity_name] = {
                "resourceType": entity_name,
                "description": entity_def.get("description", ""),
                "fields": fields,
            }

        return json.dumps(resources, indent=2)

    def _generate_segment_summary(self) -> str:
        """Generate HL7 v2 segment definition summary."""
        lines = [f"# {self.name} Segment Definitions\n"]
        for entity_name, entity_def in self._def.get("entities", {}).items():
            lines.append(f"\n## {entity_name} — {entity_def.get('description', '')}")
            for field_name, field_def in entity_def.get("fields", {}).items():
                if isinstance(field_def, dict):
                    position = field_def.get("position", "")
                    desc = field_def.get("description", "")
                    lines.append(f"  {position:>6s} {field_name}: {desc}")
        return "\n".join(lines)

    def _generate_archetype_summary(self) -> str:
        """Generate OpenEHR archetype summary."""
        lines = [f"# {self.name} Archetype Definitions\n"]
        for entity_name, entity_def in self._def.get("entities", {}).items():
            lines.append(f"\n## {entity_name}")
            lines.append(f"   {entity_def.get('description', '')}")
            for field_name, field_def in entity_def.get("fields", {}).items():
                if isinstance(field_def, dict):
                    path = field_def.get("path", "")
                    desc = field_def.get("description", "")
                    lines.append(f"  {path:>60s} → {field_name}: {desc}")
        return "\n".join(lines)

    def _generate_generic_summary(self) -> str:
        """Generate a generic summary for unknown standard types."""
        lines = [f"# {self.name} ({self.version})\n"]
        schema = self.get_schema()
        for entity, fields in schema.items():
            lines.append(f"\n{entity}:")
            for f in fields:
                lines.append(f"  - {f}")
        return "\n".join(lines)

    @staticmethod
    def _type_to_sql(type_str: str) -> str:
        """Map YAML type strings to SQL types."""
        mapping = {
            "integer": "INTEGER",
            "float": "FLOAT",
            "string": "VARCHAR(255)",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "boolean": "BOOLEAN",
            "text": "TEXT",
        }
        return mapping.get(type_str.lower(), "VARCHAR(255)")

    def __repr__(self) -> str:
        schema = self.get_schema()
        total_fields = sum(len(fields) for fields in schema.values())
        return (
            f"YAMLTargetModel(name='{self.name}', version='{self.version}', "
            f"type='{self.standard_type}', entities={len(schema)}, "
            f"fields={total_fields})"
        )

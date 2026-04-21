"""
Portiere Transforms — Built-in data transformation functions for cross-standard mapping.

Transform types:
- passthrough: Copy value as-is
- str/int/float: Type casting
- value_map: Static lookup (e.g., concept_id → code string)
- format: Date/string formatting
- vocabulary_lookup: Cross-vocabulary via Athena CONCEPT_RELATIONSHIP
- concat: Combine multiple source fields
- split: Split a source field
- codeable_concept: Wrap code + display into FHIR CodeableConcept
- archetype_wrap: Wrap value into openEHR DV_* data types

Users can register custom transforms via TransformRegistry.register().
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class TransformRegistry:
    """
    Registry of transform functions for cross-standard mapping.

    Built-in transforms are registered automatically. Users can add
    custom transforms via register().
    """

    def __init__(self):
        self._transforms: dict[str, Callable] = {}
        self._register_builtins()

    def _register_builtins(self):
        """Register all built-in transform functions."""
        self.register("passthrough", transform_passthrough)
        self.register("str", transform_str)
        self.register("int", transform_int)
        self.register("float", transform_float)
        self.register("bool", transform_bool)
        self.register("value_map", transform_value_map)
        self.register("format", transform_format)
        self.register("codeable_concept", transform_codeable_concept)
        self.register("fhir_reference", transform_fhir_reference)
        self.register("fhir_date", transform_fhir_date)
        self.register("fhir_period", transform_fhir_period)
        self.register("hl7v2_field", transform_hl7v2_field)
        self.register("dv_quantity", transform_dv_quantity)
        self.register("dv_coded_text", transform_dv_coded_text)
        self.register("vocabulary_lookup", transform_vocabulary_lookup)

    def register(self, name: str, func: Callable):
        """Register a transform function."""
        self._transforms[name] = func

    def get(self, name: str) -> Callable | None:
        """Get a transform function by name."""
        return self._transforms.get(name)

    def execute(
        self,
        transform_name: str,
        value: Any,
        config: dict | None = None,
        record: dict | None = None,
        vocabulary_bridge: Any | None = None,
    ) -> Any:
        """
        Execute a named transform.

        Args:
            transform_name: Name of the transform to execute
            value: Input value to transform
            config: Transform-specific configuration from crossmap YAML
            record: Full source record (for transforms that need multiple fields)
            vocabulary_bridge: VocabularyBridge instance (for vocabulary_lookup)

        Returns:
            Transformed value
        """
        func = self._transforms.get(transform_name)
        if func is None:
            logger.warning(f"Unknown transform: {transform_name}, using passthrough")
            return value

        try:
            return func(value, config=config, record=record, vocabulary_bridge=vocabulary_bridge)
        except Exception as e:
            logger.warning(f"Transform {transform_name} failed: {e}, returning original value")
            return value

    def list_transforms(self) -> list[str]:
        """List all registered transform names."""
        return sorted(self._transforms.keys())


# ── Built-in transform functions ────────────────────────────────


def transform_passthrough(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> Any:
    """Copy value as-is."""
    return value


def transform_str(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> str:
    """Convert to string."""
    if value is None:
        return ""
    return str(value)


def transform_int(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> int | None:
    """Convert to integer."""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def transform_float(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> float | None:
    """Convert to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def transform_bool(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> bool | None:
    """Convert to boolean."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1", "y", "t")
    return bool(value)


def transform_value_map(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> Any:
    """
    Static value mapping lookup.

    Config:
        mapping: dict of source_value → target_value
        default: fallback value if not found (optional)
    """
    if config is None:
        return value
    mapping = config.get("mapping", {})
    default = config.get("default", value)

    # Try exact match, then string match
    if value in mapping:
        return mapping[value]
    str_val = str(value)
    if str_val in mapping:
        return mapping[str_val]

    # Try numeric key match (YAML may parse "8507" as int)
    for k, v in mapping.items():
        if str(k) == str_val:
            return v

    return default


def transform_format(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> str:
    """
    Format a value using a pattern.

    Config:
        pattern: strftime pattern for dates, or Python format string
        input_format: input date format (optional)
    """
    if value is None:
        return ""
    if config is None:
        return str(value)

    pattern = config.get("pattern", "%Y-%m-%d")
    input_format = config.get("input_format")

    # Try date formatting
    if isinstance(value, (datetime,)):
        return value.strftime(pattern)

    if isinstance(value, str) and input_format:
        try:
            dt = datetime.strptime(value, input_format)
            return dt.strftime(pattern)
        except ValueError:
            pass

    # Try general string formatting
    try:
        return pattern.format(value)
    except (ValueError, KeyError):
        return str(value)


def transform_codeable_concept(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> dict:
    """
    Wrap a code into a FHIR CodeableConcept structure.

    Config:
        system: CodeSystem URL (e.g., "http://snomed.info/sct")
        display_field: field name in record for display text (optional)
    """
    if value is None:
        return {}

    system = (config or {}).get("system", "")
    display_field = (config or {}).get("display_field")
    display = ""
    if display_field and record:
        display = str(record.get(display_field, ""))

    return {
        "coding": [
            {
                "system": system,
                "code": str(value),
                "display": display,
            }
        ],
        "text": display or str(value),
    }


def transform_fhir_reference(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> dict:
    """
    Create a FHIR Reference.

    Config:
        resource_type: e.g., "Patient", "Encounter"
    """
    if value is None:
        return {}
    resource_type = (config or {}).get("resource_type", "Patient")
    return {"reference": f"{resource_type}/{value}"}


def transform_fhir_date(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> str | None:
    """Convert a date/datetime to FHIR date format (YYYY-MM-DD)."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    # Try common date formats
    str_val = str(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(str_val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    return str_val


def transform_fhir_period(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> dict:
    """
    Create a FHIR Period from start/end fields.

    Config:
        start_field: field name in record for start time
        end_field: field name in record for end time
    """
    if config is None or record is None:
        return {}

    start_field = config.get("start_field", "")
    end_field = config.get("end_field", "")

    period = {}
    start_val = record.get(start_field)
    end_val = record.get(end_field)

    if start_val:
        period["start"] = transform_fhir_date(start_val)
    if end_val:
        period["end"] = transform_fhir_date(end_val)

    return period


def transform_hl7v2_field(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> str:
    """
    Format a value as an HL7 v2 field component.

    Config:
        component_separator: separator char (default "^")
        subcomponent_separator: separator char (default "&")
    """
    if value is None:
        return ""
    return str(value)


def transform_dv_quantity(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> dict:
    """
    Wrap a value into openEHR DV_QUANTITY structure.

    Config:
        unit_field: field name in record for units
        units: static unit string (fallback)
    """
    if value is None:
        return {}

    units = ""
    if config:
        unit_field = config.get("unit_field")
        if unit_field and record:
            units = str(record.get(unit_field, ""))
        if not units:
            units = config.get("units", "")

    return {
        "_type": "DV_QUANTITY",
        "magnitude": float(value) if value else 0.0,
        "units": units,
    }


def transform_dv_coded_text(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> dict:
    """
    Wrap a value into openEHR DV_CODED_TEXT structure.

    Config:
        terminology_id: e.g., "SNOMED-CT", "LOINC"
        display_field: field name in record for display text
    """
    if value is None:
        return {}

    terminology = (config or {}).get("terminology_id", "local")
    display_field = (config or {}).get("display_field")
    display = ""
    if display_field and record:
        display = str(record.get(display_field, ""))

    return {
        "_type": "DV_CODED_TEXT",
        "value": display or str(value),
        "defining_code": {
            "terminology_id": {"value": terminology},
            "code_string": str(value),
        },
    }


def transform_vocabulary_lookup(
    value: Any, config: dict = None, record: dict = None, vocabulary_bridge: Any = None
) -> Any:
    """
    Cross-vocabulary concept mapping via VocabularyBridge.

    Requires a VocabularyBridge instance to be passed in. If not available,
    falls through to passthrough.

    Config:
        target_vocabulary: Target vocabulary (e.g., "SNOMED", "LOINC")
        output: What to return — "concept_id", "concept_code",
                "concept_name", "codeable_concept", "dv_coded_text".
                Default: "concept_code".
    """
    if value is None:
        return None

    if vocabulary_bridge is None:
        logger.debug("vocabulary_lookup: no VocabularyBridge available, passing through")
        return value

    target_vocab = (config or {}).get("target_vocabulary", "")
    output_type = (config or {}).get("output", "concept_code")

    try:
        concept_id = int(value)
    except (ValueError, TypeError):
        return value

    results = vocabulary_bridge.map_concept(concept_id, target_vocabulary=target_vocab)
    if not results:
        return value

    target = results[0]

    if output_type == "concept_id":
        return target["concept_id"]
    elif output_type == "concept_name":
        return target["concept_name"]
    elif output_type == "concept_code":
        return target.get("concept_code", str(target["concept_id"]))
    elif output_type == "codeable_concept":
        return vocabulary_bridge.concept_to_codeable_concept(target["concept_id"])
    elif output_type == "dv_coded_text":
        return vocabulary_bridge.concept_to_dv_coded_text(target["concept_id"])
    else:
        return target.get("concept_code", str(target["concept_id"]))

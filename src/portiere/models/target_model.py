"""
Abstract base class and factory for target data models.

All clinical standards (OMOP CDM, FHIR R4, HL7 v2.5.1, OpenEHR 1.0.4, custom)
are loaded from YAML definitions via ``YAMLTargetModel``.  The ``get_target_model()``
factory resolves standard names and shorthand aliases to the correct YAML file in
``src/portiere/standards/``.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine


class TargetModel(ABC):
    """
    Abstract base class for target data models.

    Implementations must provide:
    - Schema definition (tables/columns or resources/fields)
    - Target descriptions for AI-powered mapping
    - Validation logic for transformed output
    - DDL/schema generation
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Model name (e.g., 'omop_cdm_v5.4', 'fhir_r4').

        Returns:
            String identifier for the model
        """
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """
        Model version (e.g., 'v5.4', 'R4').

        Returns:
            Version string
        """
        pass

    @abstractmethod
    def get_schema(self) -> dict[str, list[str]]:
        """
        Return schema definition.

        For relational models (OMOP):
            Returns dict of table_name -> list of column names

        For resource-based models (FHIR):
            Returns dict of resource_type -> list of required fields

        Returns:
            Schema definition as dict
        """
        pass

    @abstractmethod
    def get_target_descriptions(self) -> dict[str, str]:
        """
        Return embedding-friendly descriptions for AI mapping.

        Keys should be in format "table.column" or "Resource.field"
        Values should be human-readable descriptions optimized for semantic search.

        Example:
            {
                "person.gender_concept_id": "Patient's administrative gender",
                "Patient.gender": "Administrative gender (male, female, other, unknown)",
            }

        Returns:
            Dict mapping field paths to descriptions
        """
        pass

    @abstractmethod
    def validate_output(self, engine: "AbstractEngine", output_path: str) -> dict[str, Any]:
        """
        Validate transformed output against model specification.

        Args:
            engine: Compute engine for reading data
            output_path: Path to transformed data (directory or file)

        Returns:
            Validation result dict with keys:
                - valid: bool (True if passed)
                - issues: list[dict] (validation issues)
                - stats: dict (validation statistics)
        """
        pass

    @abstractmethod
    def generate_ddl(self) -> str:
        """
        Generate schema definition (SQL DDL, FHIR StructureDefinition, etc.).

        For OMOP:
            Returns SQL DDL for creating tables

        For FHIR:
            Returns JSON StructureDefinition or Bundle template

        Returns:
            Schema definition as string
        """
        pass


# Shorthand aliases for common model names
_ALIASES: dict[str, str] = {
    "omop": "omop_cdm_v5.4",
    "omop_cdm": "omop_cdm_v5.4",
    "omop_v5.3": "omop_cdm_v5.3",
    "omop_v5.4": "omop_cdm_v5.4",
    "fhir": "fhir_r4",
}


def get_target_model(model_name: str) -> "TargetModel":
    """
    Load a target model from the built-in YAML standards directory.

    All standards (OMOP CDM, FHIR R4, HL7 v2.5.1, OpenEHR 1.0.4) are loaded
    from YAML definitions in ``src/portiere/standards/``.  Shorthand aliases
    (e.g., ``"omop"`` → ``"omop_cdm_v5.4"``) are resolved automatically.

    Args:
        model_name: Standard name, alias, or ``"custom:/path/to/file.yaml"``.

    Returns:
        YAMLTargetModel instance.

    Raises:
        ValueError: If no matching standard is found.

    Example:
        >>> get_target_model("omop_cdm_v5.4").name
        'omop_cdm_v5.4'
        >>> get_target_model("fhir_r4").name
        'fhir_r4'
        >>> get_target_model("hl7v2_2.5.1").name
        'hl7v2_2.5.1'
    """
    from pathlib import Path

    from portiere.standards import YAMLTargetModel, list_standards

    # Custom YAML path: "custom:/path/to/file.yaml"
    if model_name.startswith("custom:"):
        return YAMLTargetModel(Path(model_name[7:]))

    # Resolve shorthand aliases
    resolved = _ALIASES.get(model_name.lower(), model_name)

    # Load from built-in standards directory (src/portiere/standards/*.yaml)
    try:
        return YAMLTargetModel.from_name(resolved)
    except ValueError:
        pass

    available = list_standards()
    raise ValueError(
        f"Unsupported target model: {model_name}. "
        f"Available: {', '.join(available)}. "
        f"Or use 'custom:/path/to/your_standard.yaml' for custom standards."
    )

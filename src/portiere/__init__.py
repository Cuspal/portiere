"""
Portiere SDK — AI-Powered Clinical Data Mapping (Open Source)

Portiere automates clinical data mapping using AI. Use ``portiere.init()``
to create a project and run the full mapping pipeline locally.

Example:
    import portiere
    from portiere.engines import PolarsEngine

    project = portiere.init(
        name="Hospital OMOP Migration",
        engine=PolarsEngine(),
    )
    source = project.add_source("patients.csv")
    schema_map = project.map_schema(source)
    concept_map = project.map_concepts(codes=["E11.9", "I10"])
"""

from __future__ import annotations

from typing import TYPE_CHECKING

# Cloud modules — importable but raise NotImplementedError in open-source SDK.
# These are placeholders for Portiere Cloud (https://portiere.io).
from portiere.client import Client
from portiere.config import (
    EmbeddingConfig,
    EngineConfig,
    KnowledgeLayerConfig,
    LLMConfig,
    PortiereConfig,
    ProjectTask,
    QualityConfig,
    RerankerConfig,
    ThresholdsConfig,
)
from portiere.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ETLExecutionError,
    MappingError,
    PortiereError,
    RateLimitError,
)
from portiere.knowledge import build_knowledge_layer
from portiere.project import Project as PortiereProject
from portiere.runner import ETLRunner
from portiere.sync import SyncManager

if TYPE_CHECKING:
    from portiere.engines.base import AbstractEngine

__version__ = "0.2.0"


def init(
    name: str,
    *,
    engine: AbstractEngine | None = None,
    task: str = "standardize",
    target_model: str = "omop_cdm_v5.4",
    source_standard: str | None = None,
    vocabularies: list[str] | None = None,
    config: PortiereConfig | None = None,
) -> PortiereProject:
    """
    Initialize a Portiere project.

    Creates or loads a project with the specified configuration.
    The ``task`` parameter declares the project's purpose:

    - ``"standardize"`` (default) — Map raw source data to a target standard
      (full pipeline: ingest → profile → schema map → concept map → ETL).
    - ``"cross_map"`` — Transform data between clinical data standards
      (e.g. OMOP CDM → FHIR R4). Requires ``source_standard``.

    Args:
        name: Project name (used as identifier).
        engine: Compute engine instance. Create one of:
            - ``PolarsEngine()`` — lightweight local processing (default choice)
            - ``SparkEngine(spark)`` — large-scale PySpark / Databricks
            - ``PandasEngine()`` — small datasets / prototyping
        task: Project task type. "standardize" or "cross_map".
        target_model: Target data standard (omop_cdm_v5.4, fhir_r4, etc.).
        source_standard: Source standard for cross_map tasks
            (e.g. "omop_cdm_v5.4"). Required when task="cross_map".
        vocabularies: Standard vocabularies to use. Defaults to
            ["SNOMED", "LOINC", "RxNorm", "ICD10CM"].
        config: Full configuration. Auto-discovered if not provided.

    Returns:
        PortiereProject instance ready for pipeline operations.

    Example:
        import portiere
        from portiere.engines import PolarsEngine

        # Standardize: map raw data to OMOP CDM
        project = portiere.init(
            name="Hospital Migration",
            task="standardize",
            target_model="omop_cdm_v5.4",
            engine=PolarsEngine(),
        )

        # Cross-map: transform OMOP → FHIR
        project = portiere.init(
            name="FHIR Export",
            task="cross_map",
            source_standard="omop_cdm_v5.4",
            target_model="fhir_r4",
            engine=PolarsEngine(),
        )
    """
    if engine is None:
        from portiere.engines import PolarsEngine

        engine = PolarsEngine()
    if config is None:
        config = PortiereConfig.discover()
    if vocabularies is None:
        vocabularies = ["SNOMED", "LOINC", "RxNorm", "ICD10CM"]

    # Validate task type
    task = task.lower()
    if task not in ("standardize", "cross_map"):
        raise ConfigurationError(f"Invalid task '{task}'. Must be 'standardize' or 'cross_map'.")
    if task == "cross_map" and source_standard is None:
        raise ConfigurationError(
            "task='cross_map' requires source_standard (e.g. source_standard='omop_cdm_v5.4')."
        )

    # Warn if api_key is provided (not supported in open-source SDK)
    if config.api_key:
        import warnings

        warnings.warn(
            "api_key is ignored in the open-source SDK. "
            "All processing runs locally. "
            "For cloud features, see https://portiere.io",
            stacklevel=2,
        )

    # Always use local storage in open-source SDK
    from portiere.storage import LocalStorageBackend

    storage = LocalStorageBackend(base_dir=config.local_project_dir)

    # Create or load project (idempotent)
    if storage.project_exists(name):
        metadata = storage.load_project(name)
    else:
        metadata = storage.create_project(
            name,
            target_model,
            vocabularies,
            task=task,
            source_standard=source_standard,
        )

    return PortiereProject(
        name=name,
        target_model=metadata.get("target_model", target_model),
        vocabularies=metadata.get("vocabularies", vocabularies),
        task=metadata.get("task", task),
        source_standard=metadata.get("source_standard", source_standard),
        config=config,
        storage=storage,
        project_id=metadata["id"],
        engine=engine,
    )


__all__ = [  # noqa: RUF022
    "__version__",
    # New unified API
    "build_knowledge_layer",
    "init",
    "PortiereProject",
    # Config
    "EmbeddingConfig",
    "EngineConfig",
    "KnowledgeLayerConfig",
    "LLMConfig",
    "PortiereConfig",
    "ProjectTask",
    "QualityConfig",
    "RerankerConfig",
    "ThresholdsConfig",
    # Infrastructure
    "ETLRunner",
    # Cloud (Portiere Cloud only — raises NotImplementedError in open-source SDK)
    "Client",
    "SyncManager",
    # Exceptions
    "AuthenticationError",
    "ConfigurationError",
    "ETLExecutionError",
    "MappingError",
    "PortiereError",
    "RateLimitError",
]

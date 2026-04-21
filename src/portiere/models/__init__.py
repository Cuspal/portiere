"""
Portiere Models — Pydantic data models.
"""

from typing import Any

from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
from portiere.models.cross_mapping import CrossMapping, CrossMappingRun
from portiere.models.project import Project
from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem
from portiere.models.source import Source, SourceProfile

__all__ = [
    "ConceptMapping",
    "ConceptMappingItem",
    "CrossMapping",
    "CrossMappingRun",
    "Project",
    "SchemaMapping",
    "SchemaMappingItem",
    "Source",
    "SourceProfile",
]

# Resolve Pydantic forward references after all models are imported.
# We must provide the actual classes for TYPE_CHECKING-guarded imports
# since they aren't available in the module namespace at import time.
_rebuild_ns: dict[str, Any] = {
    "Project": Project,
    "Source": Source,
    "SchemaMapping": SchemaMapping,
    "ConceptMapping": ConceptMapping,
}

try:
    from portiere.client import Client

    _rebuild_ns["Client"] = Client
except ImportError:
    # Client may not be importable during model-only usage
    _rebuild_ns["Client"] = type(None)

try:
    from portiere.engines.base import AbstractEngine

    _rebuild_ns["AbstractEngine"] = AbstractEngine
except ImportError:
    _rebuild_ns["AbstractEngine"] = type(None)

Project.model_rebuild(_types_namespace=_rebuild_ns)
Source.model_rebuild(_types_namespace=_rebuild_ns)
SchemaMapping.model_rebuild(_types_namespace=_rebuild_ns)
ConceptMapping.model_rebuild(_types_namespace=_rebuild_ns)

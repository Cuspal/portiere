"""
Portiere Configuration — YAML config and LLM settings.

Supports loading from portiere.yaml files and environment variables.
"""

from __future__ import annotations

import warnings
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProjectTask(str, Enum):
    """Project task type — declared at init to define the project's purpose."""

    STANDARDIZE = "standardize"  # Raw source data → target standard (full pipeline)
    CROSS_MAP = "cross_map"  # Transform between clinical data standards


class LLMConfig(BaseModel):
    """
    LLM provider configuration for BYO-LLM support.

    Supports:
    - openai: OpenAI API
    - azure_openai: Azure OpenAI Service
    - anthropic: Anthropic Claude
    - bedrock: AWS Bedrock
    - ollama: Local Ollama
    - none: No LLM (cloud pipeline handles it via API endpoints)
    """

    provider: Literal["openai", "azure_openai", "anthropic", "bedrock", "ollama", "none"] = "none"
    endpoint: str | None = None
    api_key: str | None = None
    model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 1000

    model_config = ConfigDict(extra="allow")


class EmbeddingConfig(BaseModel):
    """
    Embedding provider configuration.

    Supports:
    - huggingface: Local sentence-transformers model (default)
    - ollama: Local Ollama embedding endpoint
    - openai: OpenAI / OpenAI-compatible embedding endpoint
    - bedrock: AWS Bedrock (Amazon Titan, Cohere Embed)
    """

    provider: Literal["huggingface", "ollama", "openai", "bedrock"] = "huggingface"
    model: str = "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
    endpoint: str | None = None
    api_key: str | None = None
    batch_size: int = 64

    model_config = ConfigDict(extra="allow")


class RerankerConfig(BaseModel):
    """
    Reranker provider configuration.

    Supports:
    - huggingface: Local cross-encoder model (default)
    - none: Disable reranking entirely
    """

    provider: Literal["huggingface", "none"] = "huggingface"
    model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    endpoint: str | None = None
    api_key: str | None = None

    model_config = ConfigDict(extra="allow")


class ThresholdConfig(BaseModel):
    """Confidence thresholds for auto-accept vs. review routing."""

    auto_accept: float = Field(default=0.95, ge=0.0, le=1.0)
    needs_review: float = Field(default=0.70, ge=0.0, le=1.0)


class SchemaMappingThresholds(ThresholdConfig):
    """Schema mapping specific thresholds."""

    auto_accept: float = 0.95
    needs_review: float = 0.70


class ConceptMappingThresholds(ThresholdConfig):
    """Concept mapping specific thresholds."""

    auto_accept: float = 0.95
    needs_review: float = 0.70


class ValidationThresholds(BaseModel):
    """Validation pass/fail thresholds."""

    min_completeness: float = Field(default=0.95, ge=0.0, le=1.0)
    min_conformance: float = Field(default=0.98, ge=0.0, le=1.0)
    min_plausibility: float = Field(default=0.90, ge=0.0, le=1.0)


class ThresholdsConfig(BaseModel):
    """All confidence and validation thresholds."""

    schema_mapping: SchemaMappingThresholds = Field(default_factory=SchemaMappingThresholds)
    concept_mapping: ConceptMappingThresholds = Field(default_factory=ConceptMappingThresholds)
    validation: ValidationThresholds = Field(default_factory=ValidationThresholds)


class KnowledgeLayerConfig(BaseModel):
    """Configuration for local knowledge layer backends.

    Supported backends:
        - ``bm25s`` — Pure-Python BM25 lexical search (default, no external service)
        - ``faiss`` — FAISS vector similarity search (requires faiss-cpu)
        - ``elasticsearch`` — Elasticsearch multi-match (requires ES cluster)
        - ``chromadb`` — ChromaDB vector store (requires chromadb)
        - ``pgvector`` — PostgreSQL + pgvector extension (requires psycopg + pgvector)
        - ``mongodb`` — MongoDB Atlas Vector Search (requires pymongo)
        - ``qdrant`` — Qdrant vector database (requires qdrant-client)
        - ``milvus`` — Milvus vector database (requires pymilvus)
        - ``hybrid`` — Combine multiple backends with RRF fusion
    """

    backend: Literal[
        "bm25s",
        "faiss",
        "elasticsearch",
        "hybrid",
        "chromadb",
        "pgvector",
        "mongodb",
        "qdrant",
        "milvus",
    ] = "bm25s"

    # FAISS settings
    faiss_index_path: Path | None = None
    faiss_metadata_path: Path | None = None

    # Elasticsearch settings
    elasticsearch_url: str | None = None
    elasticsearch_index: str = "portiere_concepts"

    # BM25s settings (pure Python, no external dependencies)
    bm25s_corpus_path: Path | None = None

    # ChromaDB settings
    chroma_collection: str = "portiere_concepts"
    chroma_persist_path: Path | None = None

    # PGVector settings
    pgvector_connection_string: str | None = None
    pgvector_table: str = "portiere_concepts"

    # MongoDB settings (Atlas Vector Search)
    mongodb_connection_string: str | None = None
    mongodb_database: str = "portiere"
    mongodb_collection: str = "concepts"

    # Qdrant settings
    qdrant_url: str | None = None
    qdrant_collection: str = "portiere_concepts"
    qdrant_api_key: str | None = None

    # Milvus settings
    milvus_uri: str | None = None
    milvus_collection: str = "portiere_concepts"

    # Hybrid settings — when backend="hybrid", specify sub-backends to combine
    # e.g. hybrid_backends=["bm25s", "chromadb"] → BM25s + ChromaDB with RRF fusion
    hybrid_backends: list[str] = Field(default_factory=lambda: ["bm25s", "faiss"])
    fusion_method: Literal["rrf", "weighted"] = "rrf"
    rrf_k: int = 60
    fusion_weights: list[float] | None = None


class QualityConfig(BaseModel):
    """Data quality and profiling settings (Great Expectations)."""

    enabled: bool = True
    profile_on_ingest: bool = True
    expectation_suite: str | None = None
    output_format: Literal["json", "html"] = "json"


class ArtifactConfig(BaseModel):
    """Artifact generation settings."""

    output_dir: str = "./artifacts"
    engine_type: Literal["spark", "polars", "duckdb", "pandas"] = "spark"
    include_standalone_scripts: bool = True
    include_tests: bool = True
    include_documentation: bool = True


class EngineConfig(BaseModel):
    """Compute engine configuration."""

    type: Literal["spark", "polars", "duckdb", "snowpark", "pandas"] = "polars"
    config: dict[str, Any] = Field(default_factory=dict)


class PortiereConfig(BaseSettings):
    """
    Full Portiere configuration.

    Can be loaded from:
    - portiere.yaml file
    - Environment variables (PORTIERE_* prefix)
    """

    # DEPRECATED: Use `storage` instead for explicit control, or omit entirely
    # and let Portiere infer from api_key and local AI configuration.
    # See effective_mode and effective_pipeline properties below.
    mode: Literal["local", "cloud", "hybrid"] = "local"
    pipeline: Literal["local", "cloud"] = "local"

    # Explicit storage override (replaces mode for new code)
    # "local" = artifacts stored locally, "cloud" = synced to Portiere Cloud,
    # "auto" = inferred from api_key presence
    storage: Literal["local", "cloud", "auto"] = "auto"

    # SaaS connection — when provided, enables cloud storage and/or inference
    api_key: str | None = None
    endpoint: str = "https://api.portiere.io"

    # Target data standard (OMOP CDM, FHIR R4, HL7 v2, OpenEHR, etc.)
    # Supported: "omop_cdm_v5.4", "fhir_r4", "hl7v2_2.5.1", "openehr_1.0.4"
    # Custom: "custom:/path/to/my_standard.yaml"
    target_model: str = "omop_cdm_v5.4"

    # Path to custom standard definition YAML (alternative to custom: prefix)
    custom_standard_path: Path | None = None

    # Local project storage
    local_project_dir: Path = Field(default_factory=lambda: Path.home() / ".portiere" / "projects")

    # Knowledge layer configuration
    knowledge_layer: KnowledgeLayerConfig | None = None

    # Custom model paths (legacy — prefer embedding/reranker configs below)
    embedding_model: str = "cambridgeltl/SapBERT-from-PubMedBERT-fulltext"
    reranker_model: str | None = "cross-encoder/ms-marco-MiniLM-L-6-v2"

    # Structured embedding/reranker config (preferred)
    embedding: EmbeddingConfig = Field(default_factory=EmbeddingConfig)
    reranker: RerankerConfig = Field(default_factory=RerankerConfig)

    # Model cache directory
    model_cache_dir: Path = Field(default_factory=lambda: Path.home() / ".portiere" / "models")

    # LLM settings
    llm: LLMConfig = Field(default_factory=LLMConfig)

    # Engine settings
    engine: EngineConfig = Field(default_factory=EngineConfig)

    # Thresholds
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)

    # Artifacts
    artifacts: ArtifactConfig = Field(default_factory=ArtifactConfig)

    # Data quality (Great Expectations)
    quality: QualityConfig = Field(default_factory=QualityConfig)

    model_config = SettingsConfigDict(env_prefix="PORTIERE_", extra="allow")

    @model_validator(mode="after")
    def _resolve_embedding_reranker(self) -> PortiereConfig:
        """Backward compat + smart defaults for embedding/reranker config.

        Priority:
        1. Explicit embedding=EmbeddingConfig(...) → use as-is
        2. Legacy embedding_model=<str> → coerce to EmbeddingConfig(huggingface)
        3. api_key present + no explicit config → set pipeline="cloud"
        4. No api_key + no explicit config → keep default (huggingface + SapBERT)
        """
        embedding_set = "embedding" in self.model_fields_set
        reranker_set = "reranker" in self.model_fields_set
        embedding_model_set = "embedding_model" in self.model_fields_set
        reranker_model_set = "reranker_model" in self.model_fields_set

        # --- Embedding resolution ---
        if embedding_set:
            # User provided explicit EmbeddingConfig — sync legacy field
            self.embedding_model = self.embedding.model
        elif embedding_model_set:
            # Legacy: embedding_model="some/model" → coerce to EmbeddingConfig
            self.embedding = EmbeddingConfig(
                provider="huggingface",
                model=self.embedding_model,
            )
        elif self.api_key:
            # Cloud pipeline — server handles embedding via mapping endpoints
            # Only set if no local AI components are configured
            has_local_ai = self.knowledge_layer is not None or self.llm.provider != "none"
            if "pipeline" not in self.model_fields_set and not has_local_ai:
                self.pipeline = "cloud"
        # else: keep default (huggingface + SapBERT)

        # --- Reranker resolution ---
        if reranker_set:
            # User provided explicit RerankerConfig — sync legacy field
            self.reranker_model = self.reranker.model if self.reranker.provider != "none" else None
        elif reranker_model_set:
            # Legacy: reranker_model=None or reranker_model="some/model"
            if self.reranker_model is None:
                self.reranker = RerankerConfig(provider="none", model="")
            else:
                self.reranker = RerankerConfig(
                    provider="huggingface",
                    model=self.reranker_model,
                )
        # else: keep default (huggingface + ms-marco)

        # Deprecation warnings for mode/pipeline
        if "mode" in self.model_fields_set:
            warnings.warn(
                "PortiereConfig(mode=...) is deprecated. "
                "Portiere now infers storage mode from api_key and local AI "
                "configuration. Use storage='local'|'cloud' for explicit control.",
                DeprecationWarning,
                stacklevel=4,
            )
        if "pipeline" in self.model_fields_set:
            warnings.warn(
                "PortiereConfig(pipeline=...) is deprecated. "
                "Portiere now infers pipeline mode from llm, embedding, reranker, "
                "and knowledge_layer configuration.",
                DeprecationWarning,
                stacklevel=4,
            )

        return self

    @property
    def _has_local_ai(self) -> bool:
        """Check if any local AI component is explicitly configured."""
        return (
            self.knowledge_layer is not None
            or "embedding" in self.model_fields_set
            or "reranker" in self.model_fields_set
            or self.llm.provider != "none"
        )

    @property
    def effective_pipeline(self) -> str:
        """Pipeline mode — always 'local' in the open-source SDK."""
        return "local"

    @property
    def effective_mode(self) -> str:
        """Storage mode — always 'local' in the open-source SDK."""
        return "local"

    @classmethod
    def from_yaml(cls, path: str | Path) -> PortiereConfig:
        """
        Load configuration from a YAML file.

        Supports environment variable interpolation: ${VAR_NAME}

        Args:
            path: Path to portiere.yaml

        Returns:
            PortiereConfig instance
        """
        import os
        import re

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path) as f:
            content = f.read()

        # Interpolate environment variables
        def replace_env(match: re.Match) -> str:
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))

        content = re.sub(r"\$\{(\w+)\}", replace_env, content)

        data = yaml.safe_load(content)
        return cls(**data)

    @classmethod
    def discover(cls) -> PortiereConfig:
        """
        Auto-discover configuration.

        Looks for portiere.yaml in:
        1. Current directory
        2. Parent directories (up to 5 levels)
        3. ~/.portiere/config.yaml

        Returns:
            PortiereConfig instance (defaults if no file found)
        """
        # Check current and parent directories
        cwd = Path.cwd()
        for _ in range(5):
            config_path = cwd / "portiere.yaml"
            if config_path.exists():
                return cls.from_yaml(config_path)
            cwd = cwd.parent

        # Check home directory
        home_config = Path.home() / ".portiere" / "config.yaml"
        if home_config.exists():
            return cls.from_yaml(home_config)

        # Return defaults
        return cls()

"""Pydantic v1 schema for the reproducibility manifest.

Every field is strict: unknown keys raise. The schema is versioned
(``manifest_version``) for forward compatibility — bumping the version
on a breaking change is the documented migration path.

Design rationale (Slice 4 deferred decisions, locked 2026-04-29):

* **Embedding identity** — record name + ``hf_revision`` + ``dimension``,
  not weight bytes. Hashing 440 MB of SapBERT on every run would add
  seconds and convey no more identity than the HF revision SHA already
  does. ``sha256_of_config`` is an optional fallback for local-only
  models without HF metadata.
* **Git state** — both ``git_sha`` and ``git_dirty`` are nullable; ``None``
  means "not a git repo" (acceptable). A dirty tree is recorded but does
  not block the run.
* **Credentials** — ``connection_string_redacted`` is the only field for
  database sources; the unredacted string never enters the schema, so
  it cannot leak via ``model_dump()``.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    """All manifest models forbid unknown fields."""

    model_config = ConfigDict(extra="forbid")


class RunInfo(_Strict):
    """Per-run timing and identity."""

    run_id: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None


class EmbeddingFingerprint(_Strict):
    """Identity tuple for the embedding model used during the run.

    Bytes are intentionally not hashed — see module docstring.
    """

    name: str
    hf_revision: str | None = None
    sha256_of_config: str | None = None
    dimension: int


class KnowledgeBackendFingerprint(_Strict):
    """Identity for the knowledge-layer index (FAISS / BM25s / hybrid / ...)."""

    type: str
    index_hash: str | None = None


class VocabularyFingerprint(_Strict):
    """Identity for one vocabulary file (e.g., Athena CONCEPT.csv)."""

    name: str
    version_date: str | None = None
    sha256_of_source_file: str | None = None
    path: str | None = None


class PromptTemplateFingerprint(_Strict):
    """Identity for a prompt template (LLM verifier prompt, etc.)."""

    name: str
    sha256: str


class SourceDataFingerprint(_Strict):
    """Identity for the source data the pipeline ran against.

    Two mutually-exclusive shapes:

    * File: ``path`` + ``sha256`` set
    * Database: ``connection_string_redacted`` + ``table_or_query`` set;
      credentials are stripped before reaching this model.
    """

    path: str | None = None
    sha256: str | None = None
    connection_string_redacted: str | None = None
    table_or_query: str | None = None


class StageEntry(_Strict):
    """One pipeline stage's record (ingest / schema / concept / etl / validate)."""

    stage: str
    started_at: str
    finished_at: str
    inputs: dict = Field(default_factory=dict)
    outputs: dict = Field(default_factory=dict)
    metrics: dict = Field(default_factory=dict)


class Manifest(_Strict):
    """Complete v1 reproducibility manifest.

    Written as ``<project>/runs/<run_id>/manifest.lock.json``.
    """

    manifest_version: str = "1"
    run: RunInfo
    portiere_version: str
    python_version: str
    os_string: str
    git_sha: str | None
    git_dirty: bool | None
    project_name: str
    target_model: str
    task: str = "standardize"  # "standardize" | "cross_map"
    source_standard: str | None = None  # set when task == "cross_map"
    vocabularies_requested: list[str] = Field(default_factory=list)
    embedding: EmbeddingFingerprint
    knowledge_backend: KnowledgeBackendFingerprint | None = None
    vocabularies: list[VocabularyFingerprint] = Field(default_factory=list)
    prompt_templates: list[PromptTemplateFingerprint] = Field(default_factory=list)
    thresholds: dict = Field(default_factory=dict)
    source_data: SourceDataFingerprint | None = None
    stages: list[StageEntry] = Field(default_factory=list)

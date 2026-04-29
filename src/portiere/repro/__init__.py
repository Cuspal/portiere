"""Reproducibility manifest — every Portiere pipeline run captures
exactly what produced its output.

Records what's recorded; doesn't promise byte-deterministic replay
(LLM sampling, BM25 ties, FAISS thread effects make that brittle).
The replay command re-runs the pipeline using the manifest's pinned
inputs and configuration, validating that all referenced artifacts
still exist and match their recorded sha256.

See ``specs/2026-04-29-v0.2.0-release-design.md`` §4.2 for the design.
"""

from portiere.repro.hashing import (
    sha256_bytes,
    sha256_file,
    sha256_file_or_metadata,
    sha256_text,
)
from portiere.repro.manifest import (
    EmbeddingFingerprint,
    KnowledgeBackendFingerprint,
    Manifest,
    PromptTemplateFingerprint,
    RunInfo,
    SourceDataFingerprint,
    StageEntry,
    VocabularyFingerprint,
)
from portiere.repro.recorder import ManifestRecorder
from portiere.repro.replay import ManifestReplayError, replay

__all__ = [
    "EmbeddingFingerprint",
    "KnowledgeBackendFingerprint",
    "Manifest",
    "ManifestRecorder",
    "ManifestReplayError",
    "PromptTemplateFingerprint",
    "RunInfo",
    "SourceDataFingerprint",
    "StageEntry",
    "VocabularyFingerprint",
    "replay",
    "sha256_bytes",
    "sha256_file",
    "sha256_file_or_metadata",
    "sha256_text",
]

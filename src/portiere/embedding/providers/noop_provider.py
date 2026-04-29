"""No-op embedding provider — returns deterministic zero vectors.

Used when ``EmbeddingConfig.provider == "none"`` (e.g., the bundled
``portiere quickstart`` demo runs fully offline without
``sentence-transformers`` installed). Embedding-similarity scores
collapse to zero, so the schema mapper falls back to source-pattern
matching and the concept mapper falls back to lexical (BM25) search.
"""

from __future__ import annotations

import numpy as np

from portiere.embedding.providers.base import BaseEmbeddingProvider


class NoOpEmbeddingProvider(BaseEmbeddingProvider):
    """Returns a fixed-dimension zero vector for any input.

    Dimension defaults to 384 (matches all-MiniLM-L6-v2's shape so a
    NoOp result is interchangeable with a small real model). Use this
    only when you specifically don't want embeddings — production
    pipelines should use a real provider.
    """

    _DEFAULT_DIM = 384

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        **kwargs,
    ) -> np.ndarray:
        return np.zeros((len(texts), self._DEFAULT_DIM), dtype=np.float32)

    @property
    def dimension(self) -> int:
        return self._DEFAULT_DIM

"""HuggingFace sentence-transformers embedding provider."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import structlog

from portiere.embedding.providers.base import BaseEmbeddingProvider

if TYPE_CHECKING:
    from portiere.config import EmbeddingConfig

logger = structlog.get_logger(__name__)


class HuggingFaceEmbeddingProvider(BaseEmbeddingProvider):
    """Local HuggingFace sentence-transformers provider.

    Wraps SentenceTransformer with lazy loading. Identical behavior
    to the previous EmbeddingModelLoader, but as an instance-based provider.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        super().__init__(config)
        self._model: Any = None
        self._dimension: int | None = None

    def _load_model(self):
        """Lazy-load the sentence-transformer model."""
        if self._model is not None:
            return

        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError(
                "sentence-transformers is required for HuggingFace embeddings. "
                "Install with: pip install sentence-transformers"
            )

        logger.info("embedding.huggingface.loading", model=self.config.model)
        self._model = SentenceTransformer(self.config.model)
        self._dimension = self._model.get_sentence_embedding_dimension()
        logger.info(
            "embedding.huggingface.loaded",
            model=self.config.model,
            dimension=self._dimension,
        )

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        **kwargs,
    ) -> np.ndarray:
        self._load_model()
        return self._model.encode(
            texts,
            normalize_embeddings=normalize_embeddings,
            show_progress_bar=show_progress_bar,
            **kwargs,
        )

    @property
    def dimension(self) -> int:
        self._load_model()
        return self._dimension or 0

"""
Portiere Embedding Gateway — Routes to appropriate embedding provider.

All methods are SYNCHRONOUS. See BaseEmbeddingProvider docstring for rationale.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import structlog

if TYPE_CHECKING:
    from portiere.config import EmbeddingConfig
    from portiere.embedding.providers.base import BaseEmbeddingProvider

logger = structlog.get_logger(__name__)


class EmbeddingGateway:
    """Embedding gateway for routing encode() calls to different providers.

    The gateway's encode() API matches SentenceTransformer.encode(), so
    consumers can switch from SentenceTransformer to EmbeddingGateway
    without changing call sites.

    Example::

        from portiere.config import EmbeddingConfig
        from portiere.embedding import EmbeddingGateway

        config = EmbeddingConfig(provider="huggingface",
                                 model="cambridgeltl/SapBERT-from-PubMedBERT-fulltext")
        gateway = EmbeddingGateway(config)
        embeddings = gateway.encode(["hypertension", "diabetes"])  # np.ndarray
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        self.config = config
        self._provider: BaseEmbeddingProvider = self._create_provider()
        logger.info(
            "embedding_gateway.initialized",
            provider=config.provider,
            model=config.model,
        )

    def _create_provider(self) -> BaseEmbeddingProvider:
        """Create the appropriate provider based on config."""
        if self.config.provider == "huggingface":
            from portiere.embedding.providers.huggingface_provider import (
                HuggingFaceEmbeddingProvider,
            )

            return HuggingFaceEmbeddingProvider(self.config)

        elif self.config.provider == "ollama":
            from portiere.embedding.providers.ollama_provider import (
                OllamaEmbeddingProvider,
            )

            return OllamaEmbeddingProvider(self.config)

        elif self.config.provider == "openai":
            from portiere.embedding.providers.openai_provider import (
                OpenAIEmbeddingProvider,
            )

            return OpenAIEmbeddingProvider(self.config)

        elif self.config.provider == "bedrock":
            from portiere.embedding.providers.bedrock_provider import (
                BedrockEmbeddingProvider,
            )

            return BedrockEmbeddingProvider(self.config)

        elif self.config.provider == "none":
            from portiere.embedding.providers.noop_provider import NoOpEmbeddingProvider

            return NoOpEmbeddingProvider(self.config)

        else:
            raise ValueError(f"Unsupported embedding provider: {self.config.provider}")

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Encode texts to embedding vectors.

        Args:
            texts: List of texts to embed.
            normalize_embeddings: L2-normalize output (default True).
            show_progress_bar: Show progress (local only).

        Returns:
            np.ndarray of shape (len(texts), dimension)
        """
        return self._provider.encode(
            texts,
            normalize_embeddings=normalize_embeddings,
            show_progress_bar=show_progress_bar,
            **kwargs,
        )

    @property
    def dimension(self) -> int:
        """Embedding dimension for the current model."""
        return self._provider.dimension

    def get_sentence_embedding_dimension(self) -> int:
        """Backward-compatible alias (matches SentenceTransformer API)."""
        return self.dimension

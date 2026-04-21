"""Base embedding provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from portiere.config import EmbeddingConfig


class BaseEmbeddingProvider(ABC):
    """Abstract base class for embedding providers.

    All methods are SYNCHRONOUS. Embedding encode() is called in tight loops
    by LocalSchemaMapper and LocalFAISSBackend. Unlike LLM completions
    (which are long-running and benefit from async), embedding calls
    integrate with sync code without asyncio overhead.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        self.config = config

    @abstractmethod
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
            texts: List of strings to embed.
            normalize_embeddings: L2-normalize output vectors.
            show_progress_bar: Show progress (local models only).

        Returns:
            np.ndarray of shape (len(texts), dimension)
        """
        ...

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the embedding dimension for this model."""
        ...

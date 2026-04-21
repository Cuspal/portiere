"""OpenAI / OpenAI-compatible embedding provider."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import structlog

from portiere.embedding.providers.base import BaseEmbeddingProvider

if TYPE_CHECKING:
    from portiere.config import EmbeddingConfig

logger = structlog.get_logger(__name__)


class OpenAIEmbeddingProvider(BaseEmbeddingProvider):
    """OpenAI embedding provider using the sync OpenAI client.

    Also supports OpenAI-compatible APIs (e.g., vLLM, LiteLLM, Together)
    by setting ``endpoint`` to the base URL.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        super().__init__(config)
        try:
            import openai

            kwargs: dict = {"api_key": config.api_key}
            if config.endpoint:
                kwargs["base_url"] = config.endpoint
            self._client = openai.OpenAI(**kwargs)
        except ImportError:
            raise ImportError(
                "openai is required for OpenAI embeddings. Install with: pip install openai"
            )
        self._dimension: int | None = None

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        **kwargs,
    ) -> np.ndarray:
        response = self._client.embeddings.create(
            model=self.config.model,
            input=texts,
        )
        embeddings = [item.embedding for item in response.data]
        result = np.array(embeddings, dtype="float32")

        if normalize_embeddings and result.size > 0:
            norms = np.linalg.norm(result, axis=1, keepdims=True)
            norms = np.where(norms == 0, 1, norms)
            result = result / norms

        if self._dimension is None and result.shape[0] > 0:
            self._dimension = result.shape[1]

        return result

    @property
    def dimension(self) -> int:
        if self._dimension is None:
            probe = self.encode(["dimension probe"], normalize_embeddings=False)
            self._dimension = probe.shape[1]
        return self._dimension

"""Ollama local embedding provider."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import structlog

from portiere.embedding.providers.base import BaseEmbeddingProvider

if TYPE_CHECKING:
    from portiere.config import EmbeddingConfig

logger = structlog.get_logger(__name__)


class OllamaEmbeddingProvider(BaseEmbeddingProvider):
    """Ollama embedding provider using sync HTTP.

    Calls Ollama's ``/api/embeddings`` endpoint per-text.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        super().__init__(config)
        try:
            import httpx

            self._client = httpx.Client(timeout=120.0)
        except ImportError:
            raise ImportError(
                "httpx is required for Ollama embeddings. Install with: pip install httpx"
            )
        self._endpoint = config.endpoint or "http://localhost:11434"
        self._dimension: int | None = None

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        **kwargs,
    ) -> np.ndarray:
        import httpx

        embeddings: list[list[float]] = []

        for text in texts:
            try:
                response = self._client.post(
                    f"{self._endpoint}/api/embeddings",
                    json={"model": self.config.model, "prompt": text},
                )
                response.raise_for_status()
                data = response.json()
                embeddings.append(data["embedding"])
            except httpx.ConnectError as e:
                raise ConnectionError(
                    f"Cannot connect to Ollama at {self._endpoint}. "
                    "Is Ollama running? Start with: ollama serve"
                ) from e
            except httpx.HTTPStatusError as e:
                error_msg = e.response.json().get("error", str(e))
                raise RuntimeError(
                    f"Ollama embedding error (model={self.config.model}): {error_msg}"
                ) from e

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

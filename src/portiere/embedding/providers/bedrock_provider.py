"""AWS Bedrock embedding provider (Amazon Titan, Cohere Embed)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import numpy as np
import structlog

from portiere.embedding.providers.base import BaseEmbeddingProvider

if TYPE_CHECKING:
    from portiere.config import EmbeddingConfig

logger = structlog.get_logger(__name__)


class BedrockEmbeddingProvider(BaseEmbeddingProvider):
    """AWS Bedrock embedding provider using sync boto3.

    Supports Amazon Titan Embeddings and Cohere Embed models via
    the Bedrock Runtime ``invoke_model()`` API.

    Credentials are loaded from the standard AWS credential chain:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. ~/.aws/credentials profile
    3. IAM role (EC2/ECS/Lambda)

    Configuration::

        EmbeddingConfig(
            provider="bedrock",
            model="amazon.titan-embed-text-v2:0",
            endpoint="us-west-2",          # AWS region (optional)
        )

    The ``endpoint`` field is reused as the AWS region. If not set,
    falls back to ``aws_region`` extra field or ``"us-east-1"``.
    """

    def __init__(self, config: EmbeddingConfig) -> None:
        super().__init__(config)
        try:
            import boto3

            self._boto3 = boto3
        except ImportError:
            raise ImportError(
                "boto3 is required for Bedrock embeddings. "
                "Install with: pip install portiere[bedrock]"
            )

        # Region resolution: endpoint → aws_region extra → default
        self.region = config.endpoint or getattr(config, "aws_region", None) or "us-east-1"
        self._client = self._boto3.client("bedrock-runtime", region_name=self.region)
        self._dimension: int | None = None

    def _is_cohere_model(self) -> bool:
        """Check if the model is a Cohere embedding model."""
        return "cohere" in self.config.model.lower()

    def _invoke_titan(self, text: str) -> list[float]:
        """Invoke Amazon Titan Embeddings model for a single text."""
        body: dict[str, Any] = {"inputText": text}

        # Titan v2 supports dimensions and normalize params
        if "v2" in self.config.model:
            body["dimensions"] = getattr(self.config, "dimensions", 1024)
            body["normalize"] = True

        response = self._client.invoke_model(
            modelId=self.config.model,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json",
        )
        result = json.loads(response["body"].read())
        return result["embedding"]

    def _invoke_cohere(self, texts: list[str]) -> list[list[float]]:
        """Invoke Cohere Embed model (supports batch input)."""
        body = {
            "texts": texts,
            "input_type": "search_document",
        }
        response = self._client.invoke_model(
            modelId=self.config.model,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json",
        )
        result = json.loads(response["body"].read())
        return result["embeddings"]

    def encode(
        self,
        texts: list[str],
        *,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        **kwargs,
    ) -> np.ndarray:
        try:
            if self._is_cohere_model():
                embeddings = self._invoke_cohere(texts)
            else:
                # Titan: one call per text (like Ollama provider)
                embeddings = [self._invoke_titan(text) for text in texts]
        except self._boto3.exceptions.Boto3Error as e:
            raise RuntimeError(
                f"Bedrock embedding error (model={self.config.model}, region={self.region}): {e}"
            ) from e
        except Exception as e:
            if "botocore" in type(e).__module__:
                raise RuntimeError(
                    f"Bedrock embedding error (model={self.config.model}, "
                    f"region={self.region}): {e}"
                ) from e
            raise

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

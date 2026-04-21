"""Ollama local LLM provider."""

from typing import TYPE_CHECKING

import httpx

from portiere.llm.providers.base import BaseLLMProvider

if TYPE_CHECKING:
    from portiere.config import LLMConfig


class OllamaProvider(BaseLLMProvider):
    """
    Ollama local LLM provider.

    Supports local Ollama deployments for air-gapped / on-premise use.
    No authentication needed for local endpoints.

    Configuration:
        provider: "ollama"
        endpoint: "http://localhost:11434"  # Default
        model: "llama3:70b"  # Any Ollama model

    Prerequisites:
        1. Install Ollama: https://ollama.com
        2. Pull a model: ollama pull llama3:70b
        3. Start server: ollama serve

    Example:
        from portiere import LLMConfig
        from portiere.llm.providers.ollama_provider import OllamaProvider

        config = LLMConfig(
            provider="ollama",
            endpoint="http://localhost:11434",
            model="llama3:8b",
        )
        provider = OllamaProvider(config)
        response = await provider.complete("What is SNOMED CT?")
    """

    def __init__(self, config: "LLMConfig") -> None:
        super().__init__(config)
        # Default endpoint for Ollama
        self.endpoint = config.endpoint or "http://localhost:11434"

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1000,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """
        Generate completion via Ollama API.

        Args:
            prompt: User prompt
            system: Optional system prompt
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature
            json_mode: If True, request JSON output

        Returns:
            Generated text

        Raises:
            ConnectionError: If cannot connect to Ollama
            RuntimeError: If Ollama API call fails
        """
        # Build the prompt (combine system and user if needed)
        full_prompt = prompt
        if system:
            full_prompt = f"{system}\n\n{prompt}"

        if json_mode:
            full_prompt += "\n\nRespond only with valid JSON, no additional text."

        # Ollama API request
        request_body = {
            "model": self.config.model,
            "prompt": full_prompt,
            "stream": False,  # Get complete response at once
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,  # Ollama uses num_predict instead of max_tokens
            },
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.endpoint}/api/generate",
                    json=request_body,
                    timeout=120.0,  # Longer timeout for local models
                )
                response.raise_for_status()

                data = response.json()
                return data["response"]

            except httpx.ConnectError as e:
                raise ConnectionError(
                    f"Cannot connect to Ollama at {self.endpoint}. "
                    f"Is Ollama running? Start with: ollama serve"
                ) from e
            except httpx.HTTPStatusError as e:
                # Parse Ollama error message
                try:
                    error_data = e.response.json()
                    error_msg = error_data.get("error", str(e))
                except Exception:
                    error_msg = str(e)

                raise RuntimeError(
                    f"Ollama API error (model={self.config.model}): {error_msg}"
                ) from e

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """
        Generate embeddings via Ollama API.

        Args:
            texts: List of texts to embed

        Returns:
            List of embedding vectors

        Raises:
            ConnectionError: If cannot connect to Ollama
            RuntimeError: If Ollama API call fails
        """
        async with httpx.AsyncClient() as client:
            try:
                # Ollama embeddings API requires one text at a time
                # Batch them for efficiency
                embeddings = []

                for text in texts:
                    response = await client.post(
                        f"{self.endpoint}/api/embeddings",
                        json={
                            "model": self.config.model,
                            "prompt": text,
                        },
                        timeout=60.0,
                    )
                    response.raise_for_status()

                    data = response.json()
                    embeddings.append(data["embedding"])

                return embeddings

            except httpx.ConnectError as e:
                raise ConnectionError(
                    f"Cannot connect to Ollama at {self.endpoint}. "
                    f"Is Ollama running? Start with: ollama serve"
                ) from e
            except httpx.HTTPStatusError as e:
                try:
                    error_data = e.response.json()
                    error_msg = error_data.get("error", str(e))
                except Exception:
                    error_msg = str(e)

                raise RuntimeError(
                    f"Ollama embeddings error (model={self.config.model}): {error_msg}"
                ) from e

"""Base LLM provider interface."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portiere.config import LLMConfig


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers."""

    def __init__(self, config: "LLMConfig") -> None:
        self.config = config

    @abstractmethod
    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1000,
        temperature: float = 0.0,
        json_mode: bool = False,
    ) -> str:
        """Generate completion."""
        ...

    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings (optional)."""
        raise NotImplementedError("Embeddings not supported by this provider")

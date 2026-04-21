"""
Portiere Embedding — Multi-provider embedding support.

Routes embedding calls to:
- HuggingFace sentence-transformers (local, default)
- Ollama (local)
- OpenAI / OpenAI-compatible
- AWS Bedrock (Amazon Titan, Cohere Embed)
"""

from portiere.embedding.gateway import EmbeddingGateway

__all__ = ["EmbeddingGateway"]

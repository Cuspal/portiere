"""
Portiere Local Mode — Offline mapping capabilities.

This module provides local AI mapping capabilities for Portiere SDK,
allowing users to run the full pipeline offline without cloud dependency.

Key components:
- LocalConceptMapper: Concept mapping with pluggable knowledge backends
- LocalSchemaMapper: Schema mapping with embeddings + pattern matching
- LocalReranker: Cross-encoder reranking
- LocalLLMVerifier: LLM-based verification via BYO-LLM
"""

__all__: list[str] = []

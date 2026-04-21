"""
Portiere LLM — BYO-LLM support.

Routes LLM calls to:
- Portiere hosted inference (default)
- Customer's own LLM endpoint
"""

from portiere.llm.gateway import LLMGateway

__all__ = ["LLMGateway"]

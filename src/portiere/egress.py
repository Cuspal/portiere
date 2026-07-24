"""
Egress classification — determine which configured providers would send data
off the local machine.

This is the shared engine behind the ``offline`` guarantee (``PortiereConfig``
validation) and the ``portiere doctor`` preflight. It is intentionally
dependency-free and does not import ``portiere.config`` so it can be reused from
anywhere without import cycles; it duck-types the config object it is given.

A component counts as *remote* (data leaves the machine) when:
- its ``provider`` is a known cloud provider whose client does not honor a
  local endpoint override, or
- it carries an endpoint/URL/connection-string pointing at a non-local host, or
- (knowledge layer) its backend is configured against a remote service URL.

Endpoint locality is decided by **parsing the URL and comparing the hostname
exactly** against loopback names — substring checks are spoofable
(``localhost.evil.example.com``, markers hidden in paths/queries).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

# Cloud providers, per the Literal fields in portiere.config.
REMOTE_LLM_PROVIDERS = frozenset({"openai", "azure_openai", "anthropic", "bedrock"})
REMOTE_EMBEDDING_PROVIDERS = frozenset({"openai", "bedrock"})
# reranker providers are only huggingface/none — both local.
REMOTE_RERANKER_PROVIDERS: frozenset[str] = frozenset()

_REMOTE_BY_KIND = {
    "llm": REMOTE_LLM_PROVIDERS,
    "embedding": REMOTE_EMBEDDING_PROVIDERS,
    "reranker": REMOTE_RERANKER_PROVIDERS,
}

# Providers whose client honors ``endpoint`` as a base URL, so a loopback
# endpoint genuinely keeps traffic local (OpenAI-compatible local servers:
# vLLM, LM Studio, llama.cpp). The LLM ``openai`` provider does NOT honor
# ``endpoint`` (it always talks to api.openai.com), so it is deliberately
# absent here.
_LOCAL_ENDPOINT_OVERRIDABLE = {("embedding", "openai")}

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "::1", "[::1]"})


def provider_is_remote(kind: str, provider: str) -> bool:
    """True if ``provider`` for the given component kind sends data off-machine.

    Args:
        kind: One of ``"llm"``, ``"embedding"``, ``"reranker"``.
        provider: The provider string from the component config.
    """
    return provider in _REMOTE_BY_KIND.get(kind, frozenset())


def _hostname(endpoint: str) -> str | None:
    """Extract the hostname from a URL / connection string / bare host:port."""
    candidate = endpoint.strip()
    if "//" not in candidate:
        # Bare "host:port" or "host" — give urlsplit a netloc to parse.
        candidate = "//" + candidate
    try:
        host = urlsplit(candidate).hostname
    except ValueError:
        return None
    return host


def endpoint_is_remote(endpoint: str | None) -> bool:
    """True if ``endpoint`` targets a non-local host.

    ``None``/empty is local. The URL is parsed and its **hostname** compared
    exactly against loopback names — ``localhost.evil.example.com`` or a
    ``localhost`` buried in the path/query does not count as local.
    ``mongodb+srv://`` seedlist URIs are always remote (DNS-based discovery).
    Unparseable endpoints are conservatively treated as remote.
    """
    if not endpoint:
        return False
    lowered = endpoint.strip().lower()
    if lowered.startswith("mongodb+srv://"):
        return True
    host = _hostname(lowered)
    if host is None:
        return True  # unparseable — refuse to call it local
    return host not in _LOOPBACK_HOSTS and not host.endswith(".localhost")


def _component_violation(kind: str, component: Any) -> str | None:
    """Return a human-readable violation string for one component, or None."""
    provider = getattr(component, "provider", None)
    endpoint = getattr(component, "endpoint", None)
    if provider is not None and provider_is_remote(kind, provider):
        if (
            (kind, provider) in _LOCAL_ENDPOINT_OVERRIDABLE
            and endpoint
            and not endpoint_is_remote(endpoint)
        ):
            return None  # OpenAI-compatible client pointed at a local server
        return f"{kind}.provider={provider!r} (remote)"
    if endpoint_is_remote(endpoint):
        return f"{kind}.endpoint={endpoint!r} (non-local host)"
    return None


# Knowledge-layer fields that may carry a remote service URL / connection string.
_KNOWLEDGE_URL_FIELDS = (
    "elasticsearch_url",
    "pgvector_connection_string",
    "mongodb_connection_string",
    "qdrant_url",
    "milvus_uri",
)


def _knowledge_violations(knowledge: Any) -> list[str]:
    """Violations from knowledge-layer backends configured against remote hosts."""
    violations: list[str] = []
    for field in _KNOWLEDGE_URL_FIELDS:
        value = getattr(knowledge, field, None)
        if value and endpoint_is_remote(str(value)):
            violations.append(f"knowledge_layer.{field}={value!r} (non-local host)")
    return violations


def egress_violations(config: Any) -> list[str]:
    """List every configured component that would send data off-machine.

    Inspects the LLM, embedding, and reranker providers/endpoints **and** the
    knowledge-layer service URLs (Elasticsearch, pgvector, MongoDB, Qdrant,
    Milvus). Returns human-readable violation strings (empty when the whole
    stack is local). Used both to enforce ``offline=True`` and to report egress
    posture in ``portiere doctor``.
    """
    violations: list[str] = []
    for kind, attr in (("llm", "llm"), ("embedding", "embedding"), ("reranker", "reranker")):
        component = getattr(config, attr, None)
        if component is None:
            continue
        v = _component_violation(kind, component)
        if v:
            violations.append(v)
    knowledge = getattr(config, "knowledge_layer", None)
    if knowledge is not None:
        violations.extend(_knowledge_violations(knowledge))
    return violations

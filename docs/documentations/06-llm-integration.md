# LLM Integration

Portiere uses Large Language Models (LLMs) to verify and disambiguate concept mappings that
fall below the auto-acceptance confidence threshold. The SDK supports a bring-your-own-LLM
architecture: you can use the Portiere hosted gateway (default, zero configuration), connect
to major cloud providers, or run a local model via Ollama for fully offline operation.

---

## Table of Contents

1. [Overview](#overview)
2. [Portiere Hosted (Default)](#portiere-hosted-default)
3. [OpenAI](#openai)
4. [Azure OpenAI](#azure-openai)
5. [Anthropic Claude](#anthropic-claude)
6. [AWS Bedrock](#aws-bedrock)
7. [Ollama (Local)](#ollama-local)
8. [LLM Verification in the Pipeline](#llm-verification-in-the-pipeline)
9. [Confidence Routing](#confidence-routing)
10. [Configuration Reference](#configuration-reference)
11. [Best Practices](#best-practices)

---

## Overview

The `LLMConfig` model controls which LLM provider and model the SDK uses for mapping
verification. The LLM is invoked only for mappings in the "verify" or "review" confidence
band -- high-confidence mappings are auto-accepted and low-confidence mappings are routed
to manual review without consuming LLM tokens.

```python
from portiere.config import PortiereConfig, LLMConfig

config = PortiereConfig(
    llm=LLMConfig(
        provider="openai",
        api_key="sk-...",
        model="gpt-4o",
    )
)
```

The LLM gateway is accessed through the `LLMGateway` class, which takes a `config: LLMConfig`
parameter (not a `model=` keyword argument):

```python
from portiere.models import LLMGateway

gateway = LLMGateway(config=llm_config)
```

---

## Cloud Pipeline (Default with API Key)

When you provide an API key without local AI components, Portiere infers cloud pipeline mode. The server handles all inference (embedding, reranking, LLM verification) — no local model configuration needed.

```python
config = PortiereConfig(api_key="pt_sk_your_key")
# → effective_pipeline="cloud", llm.provider="none"
# Server handles all AI inference via /schema-mapping/suggest and /concepts/map
```

**Characteristics:**
- Zero configuration beyond the Portiere API key
- Optimized for medical terminology verification
- Usage counted against your Portiere plan quota
- Suitable for getting started and prototyping

---

## OpenAI

Connect directly to the OpenAI API with your own API key.

```python
config = PortiereConfig(
    llm=LLMConfig(
        provider="openai",
        api_key="sk-proj-...",
        model="gpt-4o",
        temperature=0.0,
        max_tokens=1000,
    )
)
```

### Supported Models

| Model | Context Window | Recommended For |
|-------|---------------|-----------------|
| `gpt-4o` | 128K | Best accuracy for medical mapping verification |
| `gpt-4o-mini` | 128K | Cost-effective for high-volume mapping |
| `gpt-4-turbo` | 128K | Previous-generation alternative |

### Environment Variable Configuration

```bash
export PORTIERE_LLM_PROVIDER=openai
export PORTIERE_LLM_API_KEY=sk-proj-...
export PORTIERE_LLM_MODEL=gpt-4o
```

---

## Azure OpenAI

For organizations using Azure-hosted OpenAI deployments. Azure OpenAI requires additional
configuration fields beyond the standard OpenAI setup.

```python
config = PortiereConfig(
    llm=LLMConfig(
        provider="azure_openai",
        endpoint="https://your-resource.openai.azure.com/",
        api_key="your-azure-api-key",
        model="gpt-4o",
        temperature=0.0,
        max_tokens=1000,
        # Azure-specific extra fields (allowed by Config extra="allow")
        api_version="2024-02-15-preview",
        deployment_name="your-gpt4o-deployment",
    )
)
```

### Required Azure-Specific Fields

| Field | Description |
|-------|-------------|
| `endpoint` | Azure OpenAI resource endpoint URL |
| `api_key` | Azure API key for the resource |
| `api_version` | Azure OpenAI API version string |
| `deployment_name` | Name of the deployed model in your Azure resource |

These extra fields are accepted because `LLMConfig` is configured with `extra = "allow"`,
enabling provider-specific parameters without schema changes.

---

## Anthropic Claude

Use Anthropic's Claude models for mapping verification.

```python
config = PortiereConfig(
    llm=LLMConfig(
        provider="anthropic",
        api_key="sk-ant-...",
        model="claude-sonnet-4-5-20250929",
        temperature=0.0,
        max_tokens=1000,
    )
)
```

### Supported Models

| Model | Context Window | Recommended For |
|-------|---------------|-----------------|
| `claude-sonnet-4-5-20250929` | 200K | Balanced accuracy and cost |
| `claude-opus-4-20250514` | 200K | Maximum reasoning capability |
| `claude-3-5-haiku-20241022` | 200K | Fast, cost-effective verification |

---

## AWS Bedrock

For organizations running within AWS that want to use foundation models through Bedrock.
Authentication uses your AWS credentials (IAM role, environment variables, or AWS profile).

```python
config = PortiereConfig(
    llm=LLMConfig(
        provider="bedrock",
        model="anthropic.claude-3-sonnet-20240229-v1:0",
        temperature=0.0,
        max_tokens=1000,
        # Bedrock-specific extra fields
        region_name="us-east-1",
        model_id="anthropic.claude-3-sonnet-20240229-v1:0",
    )
)
```

### Required Bedrock-Specific Fields

| Field | Description |
|-------|-------------|
| `region_name` | AWS region where Bedrock is available |
| `model_id` | Full Bedrock model identifier |

### AWS Credentials

Bedrock uses standard AWS credential resolution:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM role (when running on EC2, ECS, Lambda, etc.)

No `api_key` is needed in `LLMConfig` when using IAM-based authentication.

---

## Ollama (Local)

Run a local LLM for fully offline, air-gapped operation. Ollama serves open-source models
through a local HTTP API.

### Prerequisites

1. Install Ollama: https://ollama.ai
2. Pull a model:
   ```bash
   ollama pull llama3
   ```
3. Ensure the Ollama server is running (default: `http://localhost:11434`)

### Configuration

```python
config = PortiereConfig(
    llm=LLMConfig(
        provider="ollama",
        endpoint="http://localhost:11434",
        model="llama3",
        temperature=0.0,
        max_tokens=1000,
    )
)
```

### Recommended Local Models

| Model | Parameters | VRAM Required | Notes |
|-------|-----------|--------------|-------|
| `llama3` | 8B | 6 GB | Good balance of speed and accuracy |
| `llama3:70b` | 70B | 40 GB | Higher accuracy, requires significant GPU |
| `mistral` | 7B | 5 GB | Fast, suitable for simple verifications |
| `mixtral` | 8x7B | 26 GB | MoE architecture, strong reasoning |
| `medllama2` | 7B | 5 GB | Fine-tuned for medical text |

### Offline Operation

When combined with the `bm25s` or `faiss` knowledge backend, Ollama enables a fully offline
mapping pipeline with no external network calls:

```python
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="bm25s",
        bm25s_corpus_path="/path/to/corpus/",
    ),
    llm=LLMConfig(
        provider="ollama",
        endpoint="http://localhost:11434",
        model="llama3",
    ),
)
```

---

## LLM Verification in the Pipeline

The LLM is used specifically in Stage 4 (Concept Mapping) of the pipeline to verify mappings
whose confidence score falls in the "verify" or "review" band. The verification prompt includes:

1. **Source term** -- the original code or description from the source data
2. **Top candidates** -- the highest-scoring candidates from the knowledge layer
3. **Context** -- column name, source table, and any profiling information
4. **Instructions** -- domain-specific verification criteria

The LLM returns a structured response indicating:
- Whether the top candidate is a valid mapping
- An optional re-ranking of candidates
- A confidence adjustment (the LLM may raise or lower confidence)

### Example Verification Flow

```python
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="llm_demo", engine=PolarsEngine(), config=config)

# During concept mapping, the LLM is invoked automatically
# for mappings in the verify/review confidence band
concept_mapping = project.map_concepts(
    source=source,
    schema_mapping=schema_mapping,
)

# Check which mappings were LLM-verified
for item in concept_mapping.items:
    if item.provenance and "llm" in item.provenance:
        print(f"{item.source_code}: verified by LLM -> {item.target_concept_name}")
```

---

## Confidence Routing

The LLM is one component of Portiere's confidence routing system. Not every mapping goes
through LLM verification -- only those in specific confidence bands:

### Concept Mapping Confidence Bands

| Confidence Range | Action | LLM Involved? |
|-----------------|--------|---------------|
| >= 0.95 | **Auto-accept** -- mapping is accepted without verification | No |
| 0.80 - 0.95 | **Verify** -- LLM reviews and confirms or adjusts the mapping | Yes |
| 0.70 - 0.80 | **Review** -- LLM provides analysis, but human review is flagged | Yes |
| < 0.70 | **Manual** -- routed directly to human review | No |

### Schema Mapping Confidence Bands

| Confidence Range | Action | LLM Involved? |
|-----------------|--------|---------------|
| >= 0.90 | **Auto-accept** | No |
| 0.70 - 0.90 | **Needs review** | Optional |
| < 0.70 | **Unmapped** | No |

The confidence thresholds ensure that LLM tokens are spent only where they provide the most
value -- on mappings that are likely correct but need confirmation.

---

## Configuration Reference

The complete `LLMConfig` model:

```python
class LLMConfig(BaseModel):
    provider: Literal[
        "openai", "azure_openai", "anthropic",
        "bedrock", "ollama", "none"
    ] = "none"
    endpoint: Optional[str] = None
    api_key: Optional[str] = None
    model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 1000

    class Config:
        extra = "allow"  # Accepts provider-specific fields
```

### Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `provider` | str | `"none"` | LLM provider identifier. `"none"` = no local LLM (use cloud pipeline). |
| `endpoint` | str | None | API endpoint URL (required for Azure, Ollama) |
| `api_key` | str | None | API key (not needed for Bedrock with IAM or Ollama) |
| `model` | str | `"gpt-4o"` | Model name or identifier |
| `temperature` | float | `0.0` | Sampling temperature (0.0 = deterministic) |
| `max_tokens` | int | `1000` | Maximum response tokens |

### Accessing the Model in Config

The LLM model name is accessed through the config hierarchy:

```python
# Correct
model_name = config.llm.model

# Incorrect -- config.EMBEDDING_MODEL is an API-level setting, not the SDK LLM model
```

---

## Best Practices

### Temperature

Always use `temperature=0.0` for mapping verification. Non-zero temperatures introduce
randomness that can lead to inconsistent mapping decisions across runs.

### Token Budget

Set `max_tokens` to a reasonable value (500-1000) for verification tasks. The LLM only needs
to confirm or reject a mapping, not generate long text.

### Cost Optimization

- Use the confidence routing thresholds to minimize LLM invocations
- Choose smaller models (`gpt-4o-mini`, `claude-3-5-haiku`) for high-volume mapping jobs
- Use Ollama for development and testing to avoid API costs entirely

### Model Selection by Use Case

| Use Case | Recommended Provider | Model |
|----------|---------------------|-------|
| Getting started | `none` (cloud pipeline) | Set `api_key` — server handles it |
| Production (cloud) | `openai` or `anthropic` | `gpt-4o` or `claude-sonnet-4-5-20250929` |
| Enterprise (Azure) | `azure_openai` | `gpt-4o` deployment |
| AWS infrastructure | `bedrock` | Claude or Titan models |
| Offline / air-gapped | `ollama` | `llama3` or `medllama2` |
| Development / testing | `ollama` | `llama3` |

---

## See Also

- [Knowledge Layer](05-knowledge-layer.md) -- Search backends that produce candidates for LLM verification
- [Data Models](07-data-models.md) -- `ConceptMappingItem.provenance` field and confidence scoring
- [Pipeline Architecture](08-pipeline-architecture.md) -- Where LLM verification fits in the 5-stage pipeline
- [Exceptions](09-exceptions.md) -- `RateLimitError` and `QuotaExceededError` for LLM API issues

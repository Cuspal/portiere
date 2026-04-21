# Operating Modes

The open-source Portiere SDK operates in **local mode only**. All data processing, storage, and AI inference happen on your machine. Cloud and hybrid modes are available through [Portiere Cloud](https://portiere.io).

---

## Table of Contents

- [Overview](#overview)
- [Local Mode (Default)](#local-mode-default)
- [Intent-Based Configuration](#intent-based-configuration)
- [Hybrid Sync: push() and pull()](#hybrid-sync-push-and-pull)
- [Decision Matrix](#decision-matrix)

---

## Intent-Based Configuration

In the open-source SDK, `effective_mode` and `effective_pipeline` always return `"local"`. If an `api_key` is provided to `PortiereConfig`, it is ignored and a warning is emitted.

| What You Configure | Inferred Mode | Inferred Pipeline |
|-------------------|---------------|-------------------|
| Nothing (defaults) | `local` | `local` |
| `knowledge_layer` and/or `llm` | `local` | `local` |
| `api_key` (ignored with warning) | `local` | `local` |

```python
# Fully local — configure your local AI components
config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(backend="bm25s", bm25s_corpus_path="./vocab.json"),
)

# api_key is ignored in the open-source SDK (warning emitted)
config = PortiereConfig(api_key="pt_sk_...")
# effective_mode -> "local", effective_pipeline -> "local"
```

---

## Local Mode (Default)

Zero cloud dependency. All data stays on your machine.

### When to Use

- Data cannot leave your environment (regulatory, compliance, air-gapped networks).
- You want full control over every component.
- You have sufficient local compute for embedding generation and LLM inference.
- Single-user workflow with no collaboration requirement.

### Configuration

```python
import portiere
from portiere.config import PortiereConfig, LLMConfig
from portiere.engines import PolarsEngine

# Option A: Local with Ollama for LLM
config = PortiereConfig(
    llm=LLMConfig(
        provider="ollama",
        endpoint="http://localhost:11434",
        model="llama3"
    )
)
# effective_mode="local", effective_pipeline="local"
project = portiere.init(name="Air-Gapped Migration", engine=PolarsEngine(), config=config)

# Option B: Local storage but using OpenAI directly (you manage the API key)
config = PortiereConfig(
    llm=LLMConfig(
        provider="openai",
        api_key="sk-...",
        model="gpt-4o"
    )
)
project = portiere.init(name="Local + OpenAI", engine=PolarsEngine(), config=config)
```

### YAML

```yaml
llm:
  provider: ollama
  endpoint: http://localhost:11434
  model: llama3
```

### What Happens

1. Project artifacts are stored under `~/.portiere/projects/<project-name>/`.
2. Embedding models (SapBERT) are downloaded to `~/.portiere/models/` on first use and cached.
3. LLM calls go to the configured provider (Ollama, OpenAI, etc.) -- Portiere Cloud is never contacted.
4. Knowledge layer search runs entirely in-process (BM25s or FAISS).

### Storage Layout

```
~/.portiere/
  projects/
    Local Migration/
      project.json          # Project metadata
      sources/              # Registered source file references
      schema_mappings/      # Schema mapping artifacts
      concept_mappings/     # Concept mapping artifacts
      etl/                  # Generated ETL scripts and logs
  models/
    SapBERT-from-PubMedBERT-fulltext/   # Cached embedding model
    ms-marco-MiniLM-L-6-v2/            # Cached reranker model
```

---

## Hybrid Sync: push() and pull()

In the open-source SDK, `push()` and `pull()` raise `NotImplementedError`. These methods require Portiere Cloud for team-based synchronization and collaborative review.

```python
project.push()   # raises NotImplementedError
project.pull()   # raises NotImplementedError
```

For cloud sync, team collaboration, and the web review UI, visit [Portiere Cloud](https://portiere.io).

---

## Decision Matrix

| Requirement | Configuration |
|-------------|---------------|
| Air-gapped or regulated environment | `knowledge_layer` + `llm` (no `api_key`) |
| Single user, full local control | `knowledge_layer` + `llm` |
| PHI/PII data, even column names are sensitive | `llm=LLMConfig(provider="ollama")` |
| CI/CD pipeline, automated mapping in Docker | `knowledge_layer` + `llm` |
| Team collaboration, web-based review UI | Use [Portiere Cloud](https://portiere.io) |

---

## See Also

- [01-quickstart.md](./01-quickstart.md) -- Get running in 5 minutes
- [02-unified-api-reference.md](./02-unified-api-reference.md) -- Full SDK API reference
- [03-configuration.md](./03-configuration.md) -- Configuration deep dive for all settings

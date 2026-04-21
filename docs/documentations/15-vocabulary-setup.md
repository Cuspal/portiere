# Vocabulary Setup Guide

How to prepare and index standard clinical vocabularies for local concept mapping. This guide covers downloading OMOP vocabularies from Athena, building knowledge layer indexes, and configuring Portiere to use them.

---

## Table of Contents

- [Overview](#overview)
- [What Are Vocabularies?](#what-are-vocabularies)
- [Quick Start: Build a Local Knowledge Layer](#quick-start-build-a-local-knowledge-layer)
- [Step 1: Download Vocabularies from Athena](#step-1-download-vocabularies-from-athena)
- [Step 2: Build a Knowledge Layer Index](#step-2-build-a-knowledge-layer-index)
- [Step 3: Use the Knowledge Layer in Your Project](#step-3-use-the-knowledge-layer-in-your-project)
- [Cross-Vocabulary Mapping with VocabularyBridge](#cross-vocabulary-mapping-with-vocabularybridge)
- [Advanced: Loading Concepts Programmatically](#advanced-loading-concepts-programmatically)
- [Concept Record Format](#concept-record-format)
- [Adding Custom Vocabularies](#adding-custom-vocabularies)
- [Backend Comparison](#backend-comparison)

---

## Overview

Portiere maps clinical codes (ICD-10, SNOMED, LOINC, etc.) to standard OMOP concepts using a **knowledge layer** -- a searchable index of vocabulary data. The knowledge layer requires pre-indexed vocabulary data to function.

Three search backends are available:

| Backend | Best For | Requirements |
|---------|----------|-------------|
| **BM25s** | Quick setup, keyword matching | None (pure Python) |
| **FAISS** | Semantic similarity, higher accuracy | `pip install portiere-health[faiss]` |
| **Hybrid** | Best accuracy (combines both) | FAISS + BM25s |

The SDK provides `build_knowledge_layer()` to automate the entire setup from an OHDSI Athena download.

---

## What Are Vocabularies?

Clinical vocabularies are standardized coding systems used in healthcare. OMOP CDM uses these as target vocabularies:

| Vocabulary | Full Name | Domain | Example Codes |
|-----------|-----------|--------|---------------|
| **SNOMED** | SNOMED CT | Clinical findings, procedures, anatomy | 73211009 (Diabetes mellitus) |
| **LOINC** | LOINC | Lab tests, clinical observations | 4548-4 (Hemoglobin A1c) |
| **RxNorm** | RxNorm | Medications, drug ingredients | 860975 (Metformin 500mg) |
| **ICD10CM** | ICD-10-CM | Diagnosis codes (US billing) | E11.9 (Type 2 diabetes) |
| **CPT4** | CPT-4 | Procedure codes (US) | 99213 (Office visit) |
| **HCPCS** | HCPCS | Healthcare supplies and services | J0170 (Adrenalin injection) |
| **NDC** | National Drug Code | Drug packaging identifiers | 0378-4000-01 |

When Portiere maps a source code like `E11.9`, it searches the knowledge layer for the matching standard concept (e.g., SNOMED concept 201826 "Type 2 diabetes mellitus").

---

## Quick Start: Build a Local Knowledge Layer

If you already have an Athena download, you can build a knowledge layer in three lines:

```python
from portiere.knowledge import build_knowledge_layer

# Parse Athena CSVs and create a BM25s index
paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="bm25s",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
)
# Returns: {"bm25s_corpus_path": "./data/vocab/concepts.json"}
```

Then use the returned paths in your project config:

```python
import portiere
from portiere.config import PortiereConfig, KnowledgeLayerConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(backend="bm25s", **paths)
)
project = portiere.init(
    name="My Project",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
    config=config,
)
```

If you don't have an Athena download yet, follow the steps below.

---

## Step 1: Download Vocabularies from Athena

OHDSI Athena ([athena.ohdsi.org](https://athena.ohdsi.org)) is the official source for OMOP standard vocabularies.

### 1. Create an Account

Go to [athena.ohdsi.org](https://athena.ohdsi.org) and register for a free account.

### 2. Select Vocabularies

Click **"Download"** in the top navigation. You'll see a list of available vocabularies. Check the ones you need:

- **SNOMED CT** -- Required for clinical findings and procedures. Requires a UMLS license (free for US users via [uts.nlm.nih.gov](https://uts.nlm.nih.gov)).
- **LOINC** -- Required for lab test mappings.
- **RxNorm** -- Required for medication mappings.
- **ICD10CM** -- Required for US diagnosis code mappings.
- **CPT4** -- Optional, for procedure codes. Requires an AMA license.
- **HCPCS**, **NDC**, **ATC** -- Optional, for additional coverage.

### 3. Download the Bundle

Click **"Download Vocabularies"**. Athena bundles your selection into a zip file and sends a download link to your registered email. This may take a few minutes.

### 4. Extract the Download

```bash
mkdir -p ./data/athena
unzip vocabulary_download_*.zip -d ./data/athena/
```

### 5. Verify the Contents

Your `./data/athena/` directory should contain these key files:

| File | Description |
|------|-------------|
| `CONCEPT.csv` | All concept records (concept_id, concept_name, domain_id, vocabulary_id, ...) |
| `CONCEPT_SYNONYM.csv` | Alternative names for concepts |
| `CONCEPT_RELATIONSHIP.csv` | Relationships between concepts (Maps to, Is a, etc.) |
| `CONCEPT_ANCESTOR.csv` | Hierarchical ancestry |
| `VOCABULARY.csv` | Vocabulary metadata |

> **Note:** These are tab-delimited CSV files, not comma-delimited. The SDK handles this automatically.

---

## Step 2: Build a Knowledge Layer Index

Use the SDK's `build_knowledge_layer()` function to parse the Athena CSV files and create backend-specific indexes.

### Option A: BM25s (Simplest -- Recommended for Getting Started)

BM25s uses keyword-based search. No external services or GPU required.

```python
from portiere.knowledge import build_knowledge_layer

paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/vocab/",
    backend="bm25s",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
)
# paths = {"bm25s_corpus_path": "./data/vocab/concepts.json"}
```

### Option B: FAISS (Semantic Search -- Higher Accuracy)

FAISS uses dense vector embeddings for semantic similarity matching. Requires `pip install portiere-health[faiss]`.

```python
from portiere.knowledge import build_knowledge_layer

paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/faiss/",
    backend="faiss",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
)
# paths = {"faiss_index_path": "...", "faiss_metadata_path": "..."}
```

This encodes all concept names using the SapBERT biomedical embedding model (768-dimensional vectors). The first run downloads the model (~400 MB) and may take several minutes for large vocabularies.

### Option C: Hybrid (Best Accuracy)

Combines BM25s (keyword) and FAISS (semantic) search via Reciprocal Rank Fusion (RRF). This is the recommended production setup.

```python
from portiere.knowledge import build_knowledge_layer

paths = build_knowledge_layer(
    athena_path="./data/athena/",
    output_path="./data/hybrid/",
    backend="hybrid",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
)
# paths = {
#     "bm25s_corpus_path": "./data/hybrid/concepts.json",
#     "faiss_index_path": "./data/hybrid/concepts.index",
#     "faiss_metadata_path": "./data/hybrid/concepts.meta.json",
# }
```

---

## Step 3: Use the Knowledge Layer in Your Project

Pass the returned paths to `KnowledgeLayerConfig`:

```python
import portiere
from portiere.config import PortiereConfig, KnowledgeLayerConfig
from portiere.engines import PolarsEngine

config = PortiereConfig(
    knowledge_layer=KnowledgeLayerConfig(
        backend="hybrid",    # or "bm25s", "faiss"
        **paths,             # Paths returned from build_knowledge_layer()
        fusion_method="rrf",
        rrf_k=60,
    )
)

project = portiere.init(
    name="Hospital Migration",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],
    config=config,
)

# Concept mapping now uses your local vocabulary index
source = project.add_source("patients.csv")
concept_map = project.map_concepts(source=source)
```

You can also persist this configuration in `portiere.yaml`:

```yaml
knowledge_layer:
  backend: hybrid
  bm25s_corpus_path: ./data/hybrid/concepts.json
  faiss_index_path: ./data/hybrid/concepts.index
  faiss_metadata_path: ./data/hybrid/concepts.meta.json
  fusion_method: rrf
  rrf_k: 60
```

---

## Cross-Vocabulary Mapping with VocabularyBridge

In addition to building a knowledge layer index for concept search, the Athena download includes
`CONCEPT_RELATIONSHIP.csv` which contains pre-computed relationships between concepts across
vocabularies. The `VocabularyBridge` class uses these relationships for direct cross-vocabulary
mapping -- no search or embedding required.

### When to Use VocabularyBridge vs Knowledge Layer

| Use Case | Tool | Why |
|----------|------|-----|
| Map source terms to standard concepts | Knowledge Layer (BM25s/FAISS/Hybrid) | Fuzzy search, handles typos and synonyms |
| Map known concept IDs between vocabularies | VocabularyBridge | Direct lookup via Athena relationships |
| Build crosswalk tables (e.g., ICD10 → SNOMED) | VocabularyBridge | Complete mapping from relationships |
| Cross-standard mapping transforms | VocabularyBridge | Translates concept IDs in field transforms |

### Setup

VocabularyBridge uses the same Athena download directory -- specifically `CONCEPT.csv` (for concept
lookups) and `CONCEPT_RELATIONSHIP.csv` (for cross-vocabulary relationships).

```python
from portiere.knowledge import VocabularyBridge

bridge = VocabularyBridge(
    athena_path="./data/athena/",
    vocabularies=["SNOMED", "LOINC", "RxNorm", "ICD10CM"],  # optional filter
)
```

The `vocabularies` parameter is optional. When provided, only concepts from those vocabularies
are loaded into memory, reducing memory usage for large Athena downloads.

### Key Files Used

| File | Used By | Purpose |
|------|---------|---------|
| `CONCEPT.csv` | Both Knowledge Layer and VocabularyBridge | Concept metadata (ID, name, vocabulary, domain) |
| `CONCEPT_SYNONYM.csv` | Knowledge Layer only | Alternative names for search |
| `CONCEPT_RELATIONSHIP.csv` | VocabularyBridge only | Cross-vocabulary relationships |
| `CONCEPT_ANCESTOR.csv` | Not yet used | Hierarchical ancestry (future) |

### Relationship Types

VocabularyBridge indexes these relationship types:

- **`Maps to`** / **`Mapped from`** -- Equivalence mappings (default for `map_concept()`)
- **`Is a`** / **`Subsumes`** -- Hierarchical relationships (used for broader/narrower lookups)

### Examples

```python
# Map an OMOP concept to SNOMED
results = bridge.map_concept(4329847, target_vocabulary="SNOMED")

# Build a full ICD10CM → SNOMED crosswalk
crosswalk = bridge.get_crosswalk("ICD10CM", "SNOMED")

# Convert to FHIR CodeableConcept
fhir_cc = bridge.concept_to_codeable_concept(201826)

# Convert to openEHR DV_CODED_TEXT
ehr_ct = bridge.concept_to_dv_coded_text(201826)
```

### Memory Considerations

`CONCEPT_RELATIONSHIP.csv` can be very large (39M+ rows for a full Athena download). To manage
memory:

1. **Filter vocabularies**: Pass `vocabularies=["SNOMED", "LOINC"]` to only load relevant concepts and relationships
2. **Lazy loading**: VocabularyBridge loads data on first use, not at initialization
3. **Subset your download**: Only select the vocabularies you need from Athena

See [Knowledge Layer -- VocabularyBridge](./05-knowledge-layer.md#vocabularybridge----cross-vocabulary-mapping)
for the complete API reference.

---

## Advanced: Loading Concepts Programmatically

For custom processing or inspection, use `load_athena_concepts()` to parse Athena CSVs into structured records without building an index:

```python
from portiere.knowledge import load_athena_concepts

concepts = load_athena_concepts(
    athena_path="./data/athena/",
    vocabularies=["SNOMED", "LOINC"],
)

print(f"Loaded {len(concepts)} concepts")
print(concepts[0])
# {
#     "concept_id": 201826,
#     "concept_name": "Type 2 diabetes mellitus",
#     "vocabulary_id": "SNOMED",
#     "domain_id": "Condition",
#     "concept_class_id": "Clinical Finding",
#     "standard_concept": "S",
#     "synonyms": ["diabetes type 2", "DM2", "T2DM"],
# }
```

You can then filter, transform, or index these concepts manually:

```python
# Filter to conditions only
conditions = [c for c in concepts if c["domain_id"] == "Condition"]

# Index into a specific backend
from portiere.knowledge.bm25s_backend import BM25sBackend

backend = BM25sBackend(corpus_path="./data/conditions.json")
backend.index_concepts(conditions)
```

---

## Concept Record Format

Each concept in the knowledge layer corpus has these fields:

```json
{
    "concept_id": 201826,
    "concept_name": "Type 2 diabetes mellitus",
    "vocabulary_id": "SNOMED",
    "domain_id": "Condition",
    "concept_class_id": "Clinical Finding",
    "standard_concept": "S",
    "synonyms": ["diabetes type 2", "DM2", "T2DM"]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `concept_id` | `int` | Yes | OMOP concept ID |
| `concept_name` | `str` | Yes | Display name for the concept |
| `vocabulary_id` | `str` | Yes | Source vocabulary (SNOMED, LOINC, etc.) |
| `domain_id` | `str` | Recommended | Clinical domain (Condition, Drug, Measurement, etc.) |
| `concept_class_id` | `str` | Optional | Concept type (Clinical Finding, Lab Test, Ingredient, etc.) |
| `standard_concept` | `str` | Optional | "S" for standard concepts |
| `synonyms` | `list[str]` | Optional | Alternative names (improves search recall) |

---

## Adding Custom Vocabularies

To add a custom vocabulary (e.g., institution-specific codes), create a JSON file with your concepts in the standard format:

```python
import json

custom_concepts = [
    {
        "concept_id": 2000000001,
        "concept_name": "Hospital Admission Score",
        "vocabulary_id": "CUSTOM_HOSPITAL",
        "domain_id": "Observation",
        "concept_class_id": "Clinical Observation",
        "standard_concept": "S",
    },
    # ... more concepts
]

with open("./data/custom_vocab.json", "w") as f:
    json.dump(custom_concepts, f, indent=2)
```

Then merge with existing vocabularies:

```python
from portiere.knowledge import load_athena_concepts
from portiere.knowledge.bm25s_backend import BM25sBackend

# Load standard concepts
standard = load_athena_concepts("./data/athena/", vocabularies=["SNOMED", "LOINC"])

# Add custom concepts
all_concepts = standard + custom_concepts

# Build index
backend = BM25sBackend(corpus_path="./data/merged_vocab.json")
backend.index_concepts(all_concepts)
```

Include your custom vocabulary ID in the `vocabularies` parameter:

```python
project = portiere.init(
    name="My Project",
    engine=PolarsEngine(),
    vocabularies=["SNOMED", "LOINC", "CUSTOM_HOSPITAL"],
    config=config,
)
```

---

## Backend Comparison

| Feature | BM25s | FAISS | Hybrid |
|---------|-------|-------|--------|
| **Setup** | Easiest | Moderate | Moderate |
| **Dependencies** | None | faiss-cpu, sentence-transformers | Both |
| **Search Type** | Keyword (BM25) | Semantic (vector) | Both (RRF fusion) |
| **Best For** | Exact code matches | Conceptual similarity | Production accuracy |
| **Speed** | Fast | Fast (after model load) | Moderate |
| **Disk Space** | Small (JSON only) | Larger (vectors + metadata) | Largest |
| **Accuracy** | Good | Better | Best |

**Recommendation:** Start with BM25s for development, switch to Hybrid for production.

---

## See Also

- [03-configuration.md](./03-configuration.md) -- Knowledge layer configuration reference
- [05-knowledge-layer.md](./05-knowledge-layer.md) -- Search backend deep dive
- [01-quickstart.md](./01-quickstart.md) -- Configuring vocabularies in `portiere.init()`
- [21-cross-standard-mapping.md](./21-cross-standard-mapping.md) -- VocabularyBridge in cross-standard transforms

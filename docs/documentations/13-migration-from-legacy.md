# Migration from Legacy SDK API

This guide helps you migrate from the legacy Portiere SDK (Client-based API) to the new unified SDK API. The new API introduces a local-first architecture, simplified project management, and a streamlined pipeline interface.

---

## Table of Contents

1. [Overview of Changes](#overview-of-changes)
2. [Old API Pattern](#old-api-pattern)
3. [New API Pattern](#new-api-pattern)
4. [Side-by-Side Comparison](#side-by-side-comparison)
5. [Key Differences](#key-differences)
6. [Deprecation Warnings and Timeline](#deprecation-warnings-and-timeline)
7. [Step-by-Step Migration Checklist](#step-by-step-migration-checklist)

---

## Overview of Changes

The legacy SDK used a `Client` class that communicated directly with the Portiere API for all operations. Every action -- creating a project, adding sources, running mappings -- required an active API connection and round-trip network calls.

The new SDK introduces a **local-first** architecture where projects can run entirely on your machine, with optional cloud synchronization. The API surface has been simplified from a collection of client methods to a fluent, project-centric interface.

### Why Migrate?

- **Local-first by default**: Run the full mapping pipeline without an API connection. Sync to cloud when ready.
- **Simplified API**: Fewer method calls, more intuitive project lifecycle.
- **Explicit engine selection**: The `engine` parameter is now required in `portiere.init()`, giving you explicit control over the compute engine used for ETL.
- **Storage abstraction**: Transparent handling of local file storage, FAISS indices, and cloud state.
- **Pipeline modes**: Choose between local execution, cloud execution, or hybrid modes.
- **Better error handling**: Structured exceptions with actionable messages.
- **Type safety**: Full Pydantic model validation on all inputs and outputs.

### Key API Change: Required `engine` Parameter

The new `portiere.init()` requires an `engine` parameter that accepts an `AbstractEngine` instance. This replaces the previous implicit engine configuration:

```python
# Old API: engine was configured implicitly via EngineConfig
from portiere import Client
client = Client(api_key="pt_sk_...")

# New API: engine is an explicit required parameter
import portiere
from portiere.engines import PolarsEngine

project = portiere.init(name="My Project", engine=PolarsEngine())
```

Available engine classes (imported from `portiere.engines`):

| Engine Class | Constructor | Use Case |
|-------------|-------------|----------|
| `PolarsEngine` | `PolarsEngine()` | Default, fast local processing |
| `SparkEngine` | `SparkEngine(spark_session)` | Large-scale distributed processing |
| `PandasEngine` | `PandasEngine()` | Compatibility with existing pandas workflows |

---

## Old API Pattern

The legacy SDK required creating a `Client` instance with an API key, then calling methods on the client to interact with the remote Portiere API.

```python
from portiere import Client

# Initialize client with API key
client = Client(api_key="pt_sk_your_api_key_here")

# Create a project
project = client.create_project("My Project")
project_id = project["id"]

# List sources
sources = client.list_sources(project_id)

# Add a source
source = client.add_source(
    project_id,
    file_path="patients.csv",
    source_type="csv"
)
source_id = source["id"]

# Run schema mapping
schema_result = client.map_schema(project_id, source_id)

# Get schema mapping results
mappings = client.get_schema_mappings(project_id, source_id)
for mapping in mappings:
    print(f"{mapping['source_column']} -> {mapping['target_field']} "
          f"(confidence: {mapping['confidence']})")

# Approve a mapping
client.approve_mapping(project_id, mapping_id=mappings[0]["id"])

# Run concept mapping
concept_result = client.map_concepts(
    project_id,
    codes=["E11.9", "I10", "J45.0"]
)

# Get concept mapping results
concepts = client.get_concept_mappings(project_id)
for concept in concepts:
    print(f"{concept['source_code']} -> {concept['target_concept']} "
          f"(method: {concept['method']})")

# Override a concept mapping
client.override_mapping(
    project_id,
    mapping_id=concepts[0]["id"],
    target_concept_id="12345"
)

# Generate ETL
etl = client.generate_etl(project_id)

# Validate
validation = client.validate(project_id)
```

### Characteristics of the Old API

- All operations require an API key and network connectivity.
- Returns raw dictionaries (not typed models).
- Project and source IDs are opaque strings managed server-side.
- No local execution capability.
- No pipeline state management.

---

## New API Pattern

The new SDK uses a project-centric, local-first approach. Initialize a project with `portiere.init()` and call methods directly on the project object.

```python
import portiere
from portiere.engines import PolarsEngine

# Initialize a project (local-first by default)
project = portiere.init(
    name="My Project",
    engine=PolarsEngine(),
    config=portiere.PortiereConfig(
        api_key="pt_sk_your_api_key_here"  # Optional: only needed for cloud sync
    )
)

# Add a source
source = project.add_source("patients.csv")

# Run schema mapping
schema_map = project.map_schema(source)

# Review schema mapping results
for item in schema_map.items:
    print(f"{item.source_column} -> {item.target_table}.{item.target_column} "
          f"(confidence: {item.confidence}, status: {item.status})")

# Approve a mapping (without candidates sets method to AUTO)
schema_map.items[0].approve()

# Override a mapping (sets status to OVERRIDDEN)
schema_map.items[1].approve(target_table="person", target_column="year_of_birth")

# Run concept mapping
concept_map = project.map_concepts(codes=["E11.9", "I10", "J45.0"])

# Review concept mapping results
summary = concept_map.summary()
print(f"Auto-mapped: {summary['auto_mapped']}")
print(f"Needs review: {summary['needs_review']}")
print(f"Manual required: {summary['manual_required']}")

# Generate ETL
etl = project.run_etl(source, output_dir="./output")

# Validate
validation = project.validate(etl_result=etl)

# Push to cloud (if API key is configured)
project.push()
```

### Using Environment Variables

The new SDK reads configuration from environment variables, so you can omit them from code:

```bash
export PORTIERE_API_KEY="pt_sk_your_api_key_here"
export PORTIERE_API_URL="https://api.portiere.io"
```

```python
import portiere
from portiere.engines import PolarsEngine

# Config is automatically loaded from environment variables
project = portiere.init(name="My Project", engine=PolarsEngine())
```

---

## Side-by-Side Comparison

| Operation                  | Legacy API                                          | New API                                         |
|---------------------------|-----------------------------------------------------|-------------------------------------------------|
| **Initialize**            | `client = Client(api_key="...")`                   | `project = portiere.init(name="...", engine=PolarsEngine(), config=...)` |
| **Create project**        | `client.create_project("name")`                    | `portiere.init(name="...", engine=PolarsEngine())` (project is the entry point) |
| **Add source**            | `client.add_source(project_id, file_path="...")`   | `project.add_source("file.csv")`                |
| **List sources**          | `client.list_sources(project_id)`                  | `project.sources`                               |
| **Map schema**            | `client.map_schema(project_id, source_id)`         | `project.map_schema(source)`                    |
| **Get schema mappings**   | `client.get_schema_mappings(project_id, source_id)` | `schema_map.items`                             |
| **Approve mapping**       | `client.approve_mapping(project_id, mapping_id)`   | `item.approve()`                                |
| **Override mapping**      | `client.override_mapping(project_id, mapping_id, target)` | `item.approve(target_table="...", target_column="...")`       |
| **Map concepts**          | `client.map_concepts(project_id, codes=[...])`     | `project.map_concepts(codes=[...])`             |
| **Get concept mappings**  | `client.get_concept_mappings(project_id)`          | `concept_map.summary()`                         |
| **Generate ETL**          | `client.generate_etl(project_id)`                  | `project.run_etl(source, output_dir="./output")`|
| **Validate**              | `client.validate(project_id)`                      | `project.validate(etl_result=etl)`              |
| **Push to cloud**         | N/A (always remote)                                | `project.push()`                                |
| **Return types**          | Raw dictionaries                                   | Typed Pydantic models                           |
| **Network required**      | Always                                             | Only for sync                                   |
| **LLM configuration**     | N/A                                                | `config.llm.model`                              |

---

## Key Differences

### 1. Storage Abstraction

**Legacy**: All data is stored on the Portiere API server. The client is a thin wrapper around REST API calls.

**New**: Projects are stored locally by default. The SDK manages local file storage, FAISS indices, and project state on your machine. Cloud synchronization is opt-in.

```python
from portiere.engines import PolarsEngine

# Local-only project (no API key needed)
project = portiere.init(name="Local Project", engine=PolarsEngine())

# Cloud-synced project
project = portiere.init(
    name="Cloud Project",
    engine=PolarsEngine(),
    config=portiere.PortiereConfig(api_key="pt_sk_...")
)
project.push()  # Push local state to cloud
```

### 2. Pipeline Modes

**Legacy**: The pipeline always runs on the server. You send data to the API and receive results.

**New**: Choose where the pipeline executes:

- **Local mode** (default): All processing happens on your machine using local models and indices.
- **Cloud mode**: Processing is offloaded to the Portiere API.
- **Hybrid mode**: Local processing with cloud-based review and collaboration.

### 3. Local-First Default

**Legacy**: Requires an API key and network connection for any operation.

**New**: Works offline by default. An API key is only required for cloud synchronization, and the SDK gracefully handles connectivity issues.

### 4. Project-Centric API

**Legacy**: Operations are performed through client methods that require explicit project and source IDs.

**New**: The project object is the primary interface. Sources, mappings, and pipeline operations are accessed through the project, eliminating the need to track IDs manually.

### 5. Typed Return Values

**Legacy**: Returns raw Python dictionaries. No IDE autocompletion or type checking.

**New**: Returns Pydantic models with full type annotations. Enables IDE autocompletion, type checking, and validation.

```python
# Legacy: raw dict, no type safety
mapping = client.get_schema_mappings(project_id, source_id)[0]
print(mapping["source_column"])  # KeyError if typo

# New: typed model, full IDE support
item = schema_map.items[0]
print(item.source_column)  # Autocomplete and type checking
print(item.status)         # MappingStatus enum
```

### 6. Method Semantics

Be aware of the following method behavior changes:

- `approve()` called without candidates sets the mapping method to `AUTO` (not `MANUAL`).
- `override()` sets the mapping method to `OVERRIDE` (not `MANUAL`).
- `ConceptMapping.summary()` returns keys: `auto_mapped`, `needs_review`, `manual_required`.

### 7. Configuration

**Legacy**: Configuration is limited to the API key passed to the `Client` constructor.

**New**: Rich configuration through `PortiereConfig` and `PortiereConfig`:

```python
import portiere
from portiere.engines import PolarsEngine

config = portiere.PortiereConfig(
    api_key="pt_sk_...",
    api_url="https://api.portiere.io"
)
project = portiere.init(name="My Project", engine=PolarsEngine(), config=config)

# Access LLM settings through config.llm.model
# Access embedding settings through the SDK's PortiereConfig
```

**Important**: Use `config.llm.model` for SDK-level LLM configuration. The `EMBEDDING_MODEL` setting is specific to the API service's `Settings` class and should not be confused with SDK configuration.

---

## Deprecation Warnings and Timeline

### Current Status

The legacy `Client` class is **deprecated** but still functional. When you import and use it, you will see deprecation warnings:

```
DeprecationWarning: portiere.Client is deprecated and will be removed in v2.0.
Use portiere.init() instead. See migration guide: https://docs.portiere.io/migration
```

### Timeline

| Date              | Action                                                        |
|-------------------|---------------------------------------------------------------|
| **Now**           | Legacy API is deprecated. New API is the recommended approach.|
| **+3 months**     | Legacy API enters maintenance mode. Bug fixes only.           |
| **+6 months**     | Legacy API emits louder warnings. New features are new API only.|
| **+12 months**    | Legacy API is removed in SDK v2.0.                            |

### Suppressing Warnings

If you need to suppress deprecation warnings temporarily while migrating:

```python
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="portiere")
```

This is not recommended for long-term use. Complete your migration before the legacy API is removed.

---

## Step-by-Step Migration Checklist

Follow this checklist to migrate your codebase from the legacy API to the new API.

### Phase 1: Preparation

- [ ] **Inventory legacy usage**: Search your codebase for all instances of `from portiere import Client` and `Client(`.
- [ ] **Review the new API**: Read through the [New API Pattern](#new-api-pattern) section and familiarize yourself with the project-centric interface.
- [ ] **Check SDK version**: Ensure you are running SDK version 1.x or later, which includes both the legacy and new APIs.
  ```bash
  pip install --upgrade portiere
  ```
- [ ] **Set up environment variables**: Configure `PORTIERE_API_KEY` and `PORTIERE_API_URL` as environment variables rather than hardcoding them.
  ```bash
  export PORTIERE_API_KEY="pt_sk_your_api_key_here"
  export PORTIERE_API_URL="https://api.portiere.io"
  ```

### Phase 2: Replace Client Initialization

- [ ] **Replace `Client` instantiation** with `portiere.init()`:

  ```python
  # Before
  from portiere import Client
  client = Client(api_key="pt_sk_...")

  # After
  import portiere
  from portiere.engines import PolarsEngine
  project = portiere.init(
      name="My Project",
      engine=PolarsEngine(),
      config=portiere.PortiereConfig(api_key="pt_sk_...")
  )
  ```

- [ ] **Remove explicit project creation**: The `portiere.init()` call replaces `client.create_project()`.

### Phase 3: Update Source Management

- [ ] **Replace `add_source`**:

  ```python
  # Before
  source = client.add_source(project_id, file_path="patients.csv", source_type="csv")
  source_id = source["id"]

  # After
  source = project.add_source("patients.csv")
  ```

- [ ] **Replace `list_sources`**:

  ```python
  # Before
  sources = client.list_sources(project_id)

  # After
  sources = project.sources
  ```

### Phase 4: Update Schema Mapping

- [ ] **Replace schema mapping calls**:

  ```python
  # Before
  client.map_schema(project_id, source_id)
  mappings = client.get_schema_mappings(project_id, source_id)

  # After
  schema_map = project.map_schema(source)
  items = schema_map.items
  ```

- [ ] **Replace approval and override calls**:

  ```python
  # Before
  client.approve_mapping(project_id, mapping_id=mappings[0]["id"])
  client.override_mapping(project_id, mapping_id=mappings[1]["id"], target="...")

  # After
  schema_map.items[0].approve()
  schema_map.items[1].approve(target_table="...", target_column="...")
  ```

- [ ] **Note**: `SchemaMappingItem.source_table` has a default value of `""`. If your tests omit this field, they will continue to work.

### Phase 5: Update Concept Mapping

- [ ] **Replace concept mapping calls**:

  ```python
  # Before
  client.map_concepts(project_id, codes=["E11.9", "I10"])
  concepts = client.get_concept_mappings(project_id)

  # After
  concept_map = project.map_concepts(codes=["E11.9", "I10"])
  summary = concept_map.summary()
  # Keys: auto_mapped, needs_review, manual_required
  ```

### Phase 6: Update ETL and Validation

- [ ] **Replace ETL generation**:

  ```python
  # Before
  etl = client.generate_etl(project_id)

  # After
  etl = project.run_etl(source, output_dir="./output")
  ```

- [ ] **Replace validation**:

  ```python
  # Before
  validation = client.validate(project_id)

  # After
  validation = project.validate(etl_result=etl)
  ```

### Phase 7: Update Return Type Handling

- [ ] **Replace dictionary access with attribute access**:

  ```python
  # Before (raw dicts)
  name = mapping["source_column"]
  score = mapping["confidence"]

  # After (Pydantic models)
  name = item.source_column
  score = item.confidence
  ```

- [ ] **Update any JSON serialization**: If you serialize results to JSON, use the Pydantic `.model_dump()` method:

  ```python
  # Before
  import json
  json.dumps(mapping)

  # After
  item.model_dump()
  # or
  item.model_dump_json()
  ```

### Phase 8: Push to Cloud (Optional)

- [ ] **Add push/pull calls** if you want to sync local results with the cloud:

  ```python
  # After running the pipeline locally
  project.push()
  ```

### Phase 9: Testing

- [ ] **Run your test suite**: Verify all tests pass with the new API.
- [ ] **Test locally**: Confirm the pipeline works without an API connection (local mode).
- [ ] **Test with cloud sync**: If using cloud features, verify synchronization works correctly.
- [ ] **Check for deprecation warnings**: Ensure no legacy `Client` usage remains.

### Phase 10: Cleanup

- [ ] **Remove legacy imports**: Delete all `from portiere import Client` statements.
- [ ] **Remove project ID tracking**: The new API manages project identity internally.
- [ ] **Remove source ID tracking**: Sources are accessed through the project object.
- [ ] **Update documentation**: Update any internal documentation or runbooks that reference the legacy API.
- [ ] **Remove warning suppression**: If you added `warnings.filterwarnings` during migration, remove it.

---

## Troubleshooting

### Common Migration Issues

**Issue: `ModuleNotFoundError: No module named 'portiere.init'`**

Ensure you are calling `portiere.init()` as a function, not importing it as a module:

```python
# Wrong
from portiere import init

# Correct
import portiere
from portiere.engines import PolarsEngine
project = portiere.init(name="...", engine=PolarsEngine())
```

**Issue: `AttributeError: 'dict' object has no attribute 'source_column'`**

You are still using the legacy API which returns dictionaries. Update to the new API which returns Pydantic models.

**Issue: Pydantic forward reference errors**

If you encounter forward reference resolution errors, ensure `model_rebuild()` is called with the correct `_types_namespace`. The SDK's `models/__init__.py` handles this automatically, but custom model extensions may need manual rebuilding:

```python
from portiere.models import Client, AbstractEngine
MyModel.model_rebuild(_types_namespace={"Client": Client, "AbstractEngine": AbstractEngine})
```

**Issue: `LLMGateway` configuration errors**

The `LLMGateway` takes a `config: LLMConfig` parameter, not a `model=` keyword argument:

```python
# Wrong
gateway = LLMGateway(model="gpt-4")

# Correct
from portiere.config import LLMConfig
gateway = LLMGateway(config=LLMConfig(model="gpt-4"))
```

---

## Related Documentation

- [Quickstart Guide](01-quickstart.md) -- Get started with the new Portiere SDK
- [Portiere Cloud Guide](10-portiere-cloud-guide.md) -- Cloud dashboard for team collaboration
- [Portiere Mapper Guide](11-portiere-mapper-guide.md) -- Crowdsourced concept mapping
- [Deployment Guide](12-deployment.md) -- Deploy the Portiere platform

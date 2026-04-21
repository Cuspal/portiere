# Exceptions and Error Handling

Portiere defines a structured exception hierarchy rooted at `PortiereError`. All exceptions
raised by the SDK are subclasses of this base class, enabling both broad and fine-grained
error handling. This guide covers every exception type, when it is raised, its attributes,
and practical error handling patterns.

---

## Table of Contents

1. [Exception Hierarchy](#exception-hierarchy)
2. [Base Exception](#base-exception)
3. [Authentication Errors](#authentication-errors)
4. [Configuration Errors](#configuration-errors)
5. [Mapping Errors](#mapping-errors)
6. [Rate Limit and Quota Errors](#rate-limit-and-quota-errors)
7. [Validation Errors](#validation-errors)
8. [Engine Errors](#engine-errors)
9. [Artifact Errors](#artifact-errors)
10. [ETL Execution Errors](#etl-execution-errors)
11. [Error Handling Patterns](#error-handling-patterns)
12. [Common Error Scenarios](#common-error-scenarios)

---

## Exception Hierarchy

```
PortiereError (base)
|
+-- AuthenticationError
|
+-- ConfigurationError
|
+-- MappingError
|
+-- RateLimitError
|
+-- QuotaExceededError
|       attributes: usage_info (dict)
|
+-- ValidationError
|
+-- EngineError
|
+-- ArtifactError
|
+-- ETLExecutionError
        attributes: result
```

All exceptions inherit from `PortiereError`, which itself inherits from Python's built-in
`Exception`. This means you can catch all Portiere-specific errors with a single handler:

```python
from portiere.exceptions import PortiereError

try:
    result = project.map_concepts(source=source, schema_mapping=schema_mapping)
except PortiereError as e:
    print(f"Portiere error: {e}")
```

---

## Base Exception

### PortiereError

The root exception class for all Portiere SDK errors.

```python
from portiere.exceptions import PortiereError
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `message` | str | Human-readable error description |

**When raised:** Never raised directly. Always one of the specific subclasses is raised.

**Usage:** Catch `PortiereError` as a fallback handler for any SDK error:

```python
try:
    # Any Portiere operation
    ...
except PortiereError as e:
    logger.error(f"Unexpected Portiere error: {e}")
    raise
```

---

## Authentication Errors

### AuthenticationError

Raised when API authentication fails.

```python
from portiere.exceptions import AuthenticationError
```

**When raised:**
- Invalid or expired API key
- Missing API key when connecting to Portiere Cloud
- Insufficient permissions for the requested operation
- Invalid `HTTPAuthorizationCredentials` (the API checks `isinstance(credentials, HTTPAuthorizationCredentials)` for safety)

**Example:**

```python
import portiere
from portiere.engines import PolarsEngine
from portiere.exceptions import AuthenticationError

try:
    project = portiere.init(name="my_project", engine=PolarsEngine(), config=config)
    source = project.add_source("data/patients.csv")
except AuthenticationError as e:
    print(f"Authentication failed: {e}")
    print("Please check your API key and ensure it has not expired.")
```

---

## Configuration Errors

### ConfigurationError

Raised when the SDK configuration is invalid or incomplete.

```python
from portiere.exceptions import ConfigurationError
```

**When raised:**
- Missing required configuration fields (e.g., `faiss_index_path` when backend is `"faiss"`)
- Invalid configuration values (e.g., unknown backend name, invalid URL format)
- Incompatible configuration combinations (e.g., hybrid backend without FAISS or ES settings)
- Missing dependencies for the selected backend (e.g., `faiss-cpu` not installed)

**Example:**

```python
import portiere
from portiere.config import PortiereConfig, KnowledgeLayerConfig
from portiere.engines import PolarsEngine
from portiere.exceptions import ConfigurationError

try:
    config = PortiereConfig(
        knowledge_layer=KnowledgeLayerConfig(
            backend="faiss",
            # Missing: faiss_index_path, faiss_metadata_path
        )
    )
    project = portiere.init(name="my_project", engine=PolarsEngine(), config=config)
except ConfigurationError as e:
    print(f"Configuration error: {e}")
    print("Ensure all required fields for the selected backend are provided.")
```

---

## Mapping Errors

### MappingError

Raised when an error occurs during schema or concept mapping operations.

```python
from portiere.exceptions import MappingError
```

**When raised:**
- Attempting to modify a finalized mapping
- Invalid candidate index in `approve(candidate_index=...)`
- Knowledge layer search failure (index not found, corrupted index)
- Attempting to approve or reject an item that is already in a terminal state

**Example:**

```python
from portiere.exceptions import MappingError

try:
    # Attempt to approve with an invalid candidate index
    item.approve(candidate_index=99)
except MappingError as e:
    print(f"Mapping error: {e}")

try:
    # Attempt to modify a finalized mapping
    schema_mapping.finalize()
    schema_mapping.items[0].approve()  # Raises MappingError
except MappingError as e:
    print(f"Cannot modify finalized mapping: {e}")
```

---

## Rate Limit and Quota Errors

### RateLimitError

Raised when API rate limits are exceeded.

```python
from portiere.exceptions import RateLimitError
```

**When raised:**
- Too many API requests in a short time window
- LLM provider rate limiting (OpenAI, Anthropic, etc.)
- Knowledge layer query rate exceeded

**Example:**

```python
import time
from portiere.exceptions import RateLimitError

def map_with_retry(project, source, schema_mapping, max_retries=3):
    for attempt in range(max_retries):
        try:
            return project.map_concepts(
                source=source,
                schema_mapping=schema_mapping,
            )
        except RateLimitError as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Rate limited. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise
```

### QuotaExceededError

Raised when the account's usage quota has been exhausted.

```python
from portiere.exceptions import QuotaExceededError
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `usage_info` | dict | Dictionary containing quota and usage details |

The `usage_info` dictionary contains information about the current usage and quota limits:

```python
# Example usage_info structure:
{
    "current_usage": 10000,
    "quota_limit": 10000,
    "period": "monthly",
    "resets_at": "2025-02-01T00:00:00Z",
    "plan": "starter",
}
```

**When raised:**
- Monthly API call quota exhausted
- LLM token quota exceeded
- Storage quota exceeded

**Example:**

```python
from portiere.exceptions import QuotaExceededError

try:
    concept_mapping = project.map_concepts(
        source=source,
        schema_mapping=schema_mapping,
    )
except QuotaExceededError as e:
    print(f"Quota exceeded: {e}")
    print(f"Usage details: {e.usage_info}")
    print(f"Current usage: {e.usage_info.get('current_usage')}")
    print(f"Quota limit: {e.usage_info.get('quota_limit')}")
    print(f"Resets at: {e.usage_info.get('resets_at')}")
    print("Consider upgrading your plan or waiting for the quota to reset.")
```

---

## Validation Errors

### ValidationError

Raised when data validation fails during pipeline operations.

```python
from portiere.exceptions import ValidationError
```

**When raised:**
- Input data fails schema validation (wrong column types, missing required columns)
- Mapping data fails consistency checks
- Configuration values fail validation (e.g., confidence threshold out of [0, 1] range)
- Pydantic model validation failures

**Example:**

```python
from portiere.exceptions import ValidationError

try:
    source = project.add_source("data/malformed.csv")
    profile = project.profile(source)
except ValidationError as e:
    print(f"Validation error: {e}")
    print("The source data does not meet the expected format requirements.")
```

---

## Engine Errors

### EngineError

Raised when the data processing engine encounters an error.

```python
from portiere.exceptions import EngineError
```

**When raised:**
- File read failure (corrupted file, unsupported encoding)
- Data type conversion errors during ingestion
- Memory errors when processing large files
- Engine-specific errors (pandas, polars, pyarrow)

**Example:**

```python
from portiere.exceptions import EngineError

try:
    source = project.add_source("data/huge_file.parquet")
except EngineError as e:
    print(f"Engine error: {e}")
    print("The data engine could not process the source file.")
    print("Consider using a different engine or reducing file size.")
```

---

## Artifact Errors

### ArtifactError

Raised when artifact storage or retrieval fails.

```python
from portiere.exceptions import ArtifactError
```

**When raised:**
- Failed to save a pipeline artifact (disk full, permission denied)
- Failed to load a previously saved artifact (file not found, corrupted)
- Storage backend connection failure
- Artifact version mismatch (attempting to load an artifact from an incompatible SDK version)

**Example:**

```python
from portiere.exceptions import ArtifactError

try:
    # Resume a pipeline from saved artifacts
    schema_mapping = project.load_artifact("schema_mapping")
except ArtifactError as e:
    print(f"Artifact error: {e}")
    print("Could not load the saved artifact. You may need to re-run the pipeline stage.")
```

---

## ETL Execution Errors

### ETLExecutionError

Raised when the ETL transformation process encounters a critical error that prevents
completion. Unlike validation warnings (which are reported in the result), this exception
indicates a fatal failure.

```python
from portiere.exceptions import ETLExecutionError
```

| Attribute | Type | Description |
|-----------|------|-------------|
| `result` | object | Partial ETL result containing any successfully processed data and error details |

The `result` attribute provides access to whatever was processed before the failure, enabling
partial recovery and debugging.

**When raised:**
- Critical type conversion failure that cannot be handled
- Target table generation failure
- Mandatory column mapping missing from the finalized schema mapping
- Database write failure during ETL output

**Example:**

```python
from portiere.exceptions import ETLExecutionError

try:
    result = project.run_etl(
        source=source,
        schema_mapping=schema_mapping,
        concept_mapping=concept_mapping,
    )
except ETLExecutionError as e:
    print(f"ETL execution failed: {e}")

    # Access partial results for debugging
    partial = e.result
    if partial:
        print(f"Tables processed before failure: {list(partial.table_results.keys())}")
        print(f"Rows processed: {partial.statistics.get('rows_processed', 0)}")
        for table_name, table_result in partial.table_results.items():
            print(f"  {table_name}: {table_result.row_count} rows")

    # Decide how to proceed
    print("Review the partial results and fix the mapping before retrying.")
```

---

## Error Handling Patterns

### Pattern 1: Catch-All with Specific Handlers

Handle known errors specifically and catch unexpected errors with the base class:

```python
import portiere
from portiere.engines import PolarsEngine
from portiere.exceptions import (
    PortiereError,
    AuthenticationError,
    ConfigurationError,
    MappingError,
    RateLimitError,
    QuotaExceededError,
    ValidationError,
    EngineError,
    ArtifactError,
    ETLExecutionError,
)

try:
    project = portiere.init(name="my_project", engine=PolarsEngine(), config=config)
    source = project.add_source("data/patients.csv")
    profile = project.profile(source)
    schema_mapping = project.map_schema(source=source)
    concept_mapping = project.map_concepts(
        source=source, schema_mapping=schema_mapping
    )
    result = project.run_etl(
        source=source,
        schema_mapping=schema_mapping,
        concept_mapping=concept_mapping,
    )

except AuthenticationError:
    print("Check your API key and permissions.")

except ConfigurationError as e:
    print(f"Fix your configuration: {e}")

except QuotaExceededError as e:
    print(f"Quota exceeded. Resets at: {e.usage_info.get('resets_at')}")

except RateLimitError:
    print("Rate limited. Implement backoff and retry.")

except MappingError as e:
    print(f"Mapping issue: {e}")

except ValidationError as e:
    print(f"Data validation failed: {e}")

except EngineError as e:
    print(f"Data engine failure: {e}")

except ArtifactError as e:
    print(f"Artifact storage issue: {e}")

except ETLExecutionError as e:
    print(f"ETL failed. Partial result available: {e.result is not None}")

except PortiereError as e:
    # Catch-all for any Portiere error not handled above
    print(f"Unexpected Portiere error: {e}")
```

### Pattern 2: Stage-by-Stage Error Handling

Wrap each pipeline stage independently to enable partial recovery:

```python
import portiere
from portiere.engines import PolarsEngine
from portiere.exceptions import PortiereError, ETLExecutionError

project = portiere.init(name="my_project", engine=PolarsEngine(), config=config)

# Stage 1: Ingest
try:
    source = project.add_source("data/patients.csv")
except PortiereError as e:
    print(f"Ingestion failed: {e}")
    raise SystemExit(1)

# Stage 2: Profile
try:
    profile = project.profile(source)
except PortiereError as e:
    print(f"Profiling failed: {e}")
    raise SystemExit(1)

# Stage 3: Schema Map
try:
    schema_mapping = project.map_schema(source=source)
except PortiereError as e:
    print(f"Schema mapping failed: {e}")
    raise SystemExit(1)

# Review and finalize schema mapping
for item in schema_mapping.needs_review():
    item.approve()
schema_mapping.finalize()

# Stage 4: Concept Map
try:
    concept_mapping = project.map_concepts(
        source=source, schema_mapping=schema_mapping
    )
except PortiereError as e:
    print(f"Concept mapping failed: {e}")
    raise SystemExit(1)

# Review and finalize concept mapping
concept_mapping.approve_all()
concept_mapping.finalize()

# Stage 5: ETL + Validate
try:
    result = project.run_etl(
        source=source,
        schema_mapping=schema_mapping,
        concept_mapping=concept_mapping,
    )
except ETLExecutionError as e:
    print(f"ETL failed with partial results: {e}")
    # Save partial results for investigation
    partial = e.result
except PortiereError as e:
    print(f"ETL failed: {e}")
    raise SystemExit(1)
```

### Pattern 3: Retry with Exponential Backoff

For transient errors (rate limits, network issues):

```python
import time
from portiere.exceptions import RateLimitError, PortiereError

def retry_with_backoff(func, *args, max_retries=5, base_delay=1.0, **kwargs):
    """Execute a function with exponential backoff on transient errors."""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except RateLimitError as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"Rate limited (attempt {attempt + 1}/{max_retries}). "
                      f"Retrying in {delay:.1f}s...")
                time.sleep(delay)
            else:
                print(f"Max retries exceeded: {e}")
                raise
        except PortiereError:
            # Non-transient errors should not be retried
            raise

# Usage
concept_mapping = retry_with_backoff(
    project.map_concepts,
    source=source,
    schema_mapping=schema_mapping,
)
```

### Pattern 4: Graceful Quota Handling

Check quota status and handle exhaustion gracefully:

```python
import portiere
from portiere.engines import PolarsEngine
from portiere.exceptions import QuotaExceededError

try:
    concept_mapping = project.map_concepts(
        source=source,
        schema_mapping=schema_mapping,
    )
except QuotaExceededError as e:
    usage = e.usage_info
    print(f"Quota exceeded ({usage.get('current_usage')}/{usage.get('quota_limit')})")
    print(f"Plan: {usage.get('plan')}")
    print(f"Resets at: {usage.get('resets_at')}")

    # Fallback: switch to local LLM to avoid further API usage
    from portiere.config import LLMConfig
    config.llm = LLMConfig(
        provider="ollama",
        endpoint="http://localhost:11434",
        model="llama3",
    )
    project = portiere.init(name="my_project", engine=PolarsEngine(), config=config)

    # Retry with local LLM
    concept_mapping = project.map_concepts(
        source=source,
        schema_mapping=schema_mapping,
    )
```

---

## Common Error Scenarios

### Scenario 1: Invalid API Key

**Error:** `AuthenticationError: Invalid API key`

**Cause:** The API key is incorrect, expired, or revoked.

**Solution:**
```python
# Verify your API key
import os
api_key = os.environ.get("PORTIERE_API_KEY")
if not api_key:
    print("PORTIERE_API_KEY environment variable not set")
else:
    print(f"API key starts with: {api_key[:8]}...")
```

### Scenario 2: FAISS Index Not Found

**Error:** `ConfigurationError: FAISS index file not found at /path/to/faiss.index`

**Cause:** The FAISS index path in the configuration does not point to a valid index file.

**Solution:**
```python
from pathlib import Path

index_path = Path("/path/to/faiss.index")
if not index_path.exists():
    print(f"Index file not found at {index_path}")
    print("Build the index first:")
    print("  backend.build_index(concepts_df)")
```

### Scenario 3: Modifying a Finalized Mapping

**Error:** `MappingError: Cannot modify a finalized mapping`

**Cause:** Attempting to approve, reject, or override a mapping item after `finalize()` has
been called.

**Solution:**
Complete all reviews *before* calling `finalize()`. Finalization is a one-way operation.

```python
# Correct order:
for item in schema_mapping.needs_review():
    item.approve()

# Finalize only after all reviews are complete
schema_mapping.finalize()
```

### Scenario 4: LLM Provider Rate Limiting

**Error:** `RateLimitError: OpenAI rate limit exceeded (429)`

**Cause:** Too many LLM verification requests sent in a short window.

**Solution:** Use the retry pattern with exponential backoff (see [Pattern 3](#pattern-3-retry-with-exponential-backoff)), or reduce the number of
LLM-verified mappings by adjusting confidence thresholds.

### Scenario 5: ETL Type Conversion Failure

**Error:** `ETLExecutionError: Cannot convert column 'age' from string to integer`

**Cause:** Source data contains values that cannot be cast to the target column type.

**Solution:**
```python
from portiere.exceptions import ETLExecutionError

try:
    result = project.run_etl(
        source=source,
        schema_mapping=schema_mapping,
        concept_mapping=concept_mapping,
    )
except ETLExecutionError as e:
    # Inspect the partial result to identify the failing column
    print(f"Error: {e}")
    if e.result:
        print("Review the source data for invalid values in the failing column.")
        print("Consider adding a data cleaning step before ETL.")
```

### Scenario 6: Pydantic Forward Reference Errors

**Error:** Errors related to unresolved forward references in Pydantic models.

**Cause:** Portiere uses `TYPE_CHECKING` imports with Pydantic models, which require explicit
forward reference resolution.

**Solution:** If you are extending or subclassing Portiere models, ensure you call
`model_rebuild()` with the proper type namespace:

```python
from portiere.models import Client, AbstractEngine

# Rebuild models with actual classes in the namespace
YourModel.model_rebuild(_types_namespace={
    "Client": Client,
    "AbstractEngine": AbstractEngine,
})
```

This is handled automatically in `models/__init__.py` for all built-in models.

---

## See Also

- [Pipeline Architecture](08-pipeline-architecture.md) -- Pipeline stages where each exception may be raised
- [LLM Integration](06-llm-integration.md) -- LLM-related errors (RateLimitError, QuotaExceededError)
- [Data Models](07-data-models.md) -- Mapping operations that may raise MappingError
- [Knowledge Layer](05-knowledge-layer.md) -- Backend configuration errors (ConfigurationError)

# Review API Endpoints

The Portiere Cloud API provides REST endpoints for approving, rejecting, and overriding individual schema and concept mappings. These endpoints power the cloud dashboard's review interface and can be called directly from scripts or custom integrations.

> **Note:** These cloud API endpoints are part of the **Portiere Cloud** platform, which is currently on the development roadmap. In the open-source SDK, mapping review is performed locally via the Python API (e.g., `schema_map.approve(index)`, `concept_map.reject(index)`). See the [Mapping Review Workflow](18-mapping-review-workflow.md) for the local review guide.

---

## Table of Contents

1. [Authentication](#authentication)
2. [Schema Mapping Review](#schema-mapping-review)
3. [Concept Mapping Review](#concept-mapping-review)
4. [Response Format](#response-format)
5. [Error Handling](#error-handling)
6. [Workflow Integration](#workflow-integration)
7. [Batch Review via SDK](#batch-review-via-sdk)

---

## Authentication

All review endpoints require a valid API key in the `Authorization` header:

```
Authorization: Bearer pt_sk_your_api_key_here
```

The API key must have write access to the project being modified.

---

## Schema Mapping Review

### Approve Schema Mapping

Accept the AI-suggested mapping for a schema mapping item.

```
POST /v1/projects/{project_id}/schema-mapping/{mapping_id}/approve
```

**Parameters:**

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `project_id` | path | `string` | Yes | Project UUID |
| `mapping_id` | path | `string` | Yes | Schema mapping item UUID |

**Request body:** None

**Response:**

```json
{
  "id": "sm_abc123",
  "source_column": "patient_id",
  "target_table": "person",
  "target_column": "person_id",
  "confidence": 0.97,
  "status": "approved"
}
```

**Example:**

```bash
curl -X POST \
  https://api.portiere.dev/v1/projects/proj_123/schema-mapping/sm_abc/approve \
  -H "Authorization: Bearer pt_sk_..."
```

---

### Reject Schema Mapping

Mark a schema mapping item as not mappable to the target model.

```
POST /v1/projects/{project_id}/schema-mapping/{mapping_id}/reject
```

**Parameters:**

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `project_id` | path | `string` | Yes | Project UUID |
| `mapping_id` | path | `string` | Yes | Schema mapping item UUID |

**Request body:** None

**Response:**

```json
{
  "id": "sm_abc123",
  "source_column": "zip_code",
  "target_table": null,
  "target_column": null,
  "confidence": 0.45,
  "status": "rejected"
}
```

---

### Override Schema Mapping

Replace the AI suggestion with a manually specified target table and column.

```
POST /v1/projects/{project_id}/schema-mapping/{mapping_id}/override
```

**Parameters:**

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `project_id` | path | `string` | Yes | Project UUID |
| `mapping_id` | path | `string` | Yes | Schema mapping item UUID |

**Request body:**

```json
{
  "target_table": "person",
  "target_column": "birth_datetime"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `target_table` | `string` | Yes | OMOP CDM target table name |
| `target_column` | `string` | Yes | OMOP CDM target column name |

**Response:**

```json
{
  "id": "sm_abc123",
  "source_column": "date_of_birth",
  "target_table": "person",
  "target_column": "birth_datetime",
  "confidence": 0.88,
  "status": "overridden"
}
```

**Example:**

```bash
curl -X POST \
  https://api.portiere.dev/v1/projects/proj_123/schema-mapping/sm_abc/override \
  -H "Authorization: Bearer pt_sk_..." \
  -H "Content-Type: application/json" \
  -d '{"target_table": "person", "target_column": "birth_datetime"}'
```

---

## Concept Mapping Review

### Approve Concept Mapping

Accept the AI-suggested concept for a concept mapping item.

```
POST /v1/projects/{project_id}/concept-mapping/{mapping_id}/approve
```

**Parameters:**

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `project_id` | path | `string` | Yes | Project UUID |
| `mapping_id` | path | `string` | Yes | Concept mapping item UUID |

**Request body:** None

**Response:**

```json
{
  "id": "cm_xyz789",
  "source_code": "E11.9",
  "source_description": "Type 2 diabetes mellitus without complications",
  "target_concept_id": 201826,
  "target_concept_name": "Type 2 diabetes mellitus",
  "target_vocabulary_id": "SNOMED",
  "confidence": 0.96,
  "method": "auto"
}
```

---

### Reject Concept Mapping

Mark a concept as unmappable. Clears the target concept fields.

```
POST /v1/projects/{project_id}/concept-mapping/{mapping_id}/reject
```

**Parameters:**

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `project_id` | path | `string` | Yes | Project UUID |
| `mapping_id` | path | `string` | Yes | Concept mapping item UUID |

**Request body:** None

**Response:**

```json
{
  "id": "cm_xyz789",
  "source_code": "Z87.891",
  "source_description": "Personal history of nicotine dependence",
  "target_concept_id": null,
  "target_concept_name": null,
  "target_vocabulary_id": null,
  "confidence": 0.55,
  "method": "unmapped"
}
```

---

### Override Concept Mapping

Replace the AI suggestion with a manually specified target concept.

```
POST /v1/projects/{project_id}/concept-mapping/{mapping_id}/override
```

**Parameters:**

| Parameter | Location | Type | Required | Description |
|-----------|----------|------|----------|-------------|
| `project_id` | path | `string` | Yes | Project UUID |
| `mapping_id` | path | `string` | Yes | Concept mapping item UUID |

**Request body:**

```json
{
  "target_concept_id": 320128,
  "target_concept_name": "Essential hypertension",
  "target_vocabulary_id": "SNOMED"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `target_concept_id` | `integer` | Yes | OMOP concept ID |
| `target_concept_name` | `string` | No | Human-readable concept name |
| `target_vocabulary_id` | `string` | No | Source vocabulary (e.g., SNOMED, LOINC) |

**Response:**

```json
{
  "id": "cm_xyz789",
  "source_code": "I10",
  "source_description": "Essential (primary) hypertension",
  "target_concept_id": 320128,
  "target_concept_name": "Essential hypertension",
  "target_vocabulary_id": "SNOMED",
  "confidence": 0.82,
  "method": "override"
}
```

**Example:**

```bash
curl -X POST \
  https://api.portiere.dev/v1/projects/proj_123/concept-mapping/cm_xyz/override \
  -H "Authorization: Bearer pt_sk_..." \
  -H "Content-Type: application/json" \
  -d '{"target_concept_id": 320128, "target_concept_name": "Essential hypertension", "target_vocabulary_id": "SNOMED"}'
```

---

## Response Format

All review endpoints return the updated mapping item as JSON. The response includes the full item with updated status/method fields.

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| `200` | Success — item updated |
| `401` | Unauthorized — invalid or missing API key |
| `403` | Forbidden — API key does not have access to this project |
| `404` | Not found — project or mapping item does not exist |
| `422` | Validation error — invalid request body |

---

## Error Handling

### Item Not Found

```json
{
  "detail": "Schema mapping sm_abc123 not found in project proj_123"
}
```

### Invalid Override

```json
{
  "detail": [
    {
      "loc": ["body", "target_table"],
      "msg": "field required",
      "type": "value_error.missing"
    }
  ]
}
```

---

## Workflow Integration

### Typical Cloud Review Flow

1. **SDK pushes mappings** to cloud via `project.push()` (hybrid mode) or direct API
2. **SME reviews** on the cloud dashboard — clicks approve/reject/override per item
3. **Dashboard calls** these API endpoints to persist review decisions
4. **SDK pulls** updated mappings via `project.pull()`

### Direct API Integration

For custom review UIs or automated pipelines:

```python
import requests

API_URL = "https://api.portiere.dev/v1"
TOKEN = "pt_sk_..."
PROJECT_ID = "proj_123"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# Approve a schema mapping
resp = requests.post(
    f"{API_URL}/projects/{PROJECT_ID}/schema-mapping/sm_abc/approve",
    headers=headers,
)
print(resp.json())

# Override a concept mapping
resp = requests.post(
    f"{API_URL}/projects/{PROJECT_ID}/concept-mapping/cm_xyz/override",
    headers=headers,
    json={
        "target_concept_id": 320128,
        "target_concept_name": "Essential hypertension",
        "target_vocabulary_id": "SNOMED",
    },
)
print(resp.json())
```

---

## Batch Review via SDK

For bulk review, use the SDK's collection-level methods rather than individual API calls:

```python
# Approve all needs_review items at once
schema_map.approve_all()
concept_map.approve_all()

# Then push to cloud
project.save_schema_mapping(schema_map)
project.save_concept_mapping(concept_map)
project.push()
```

This is more efficient than calling the API endpoint for each item individually.

---

## See Also

- [02-unified-api-reference.md](./02-unified-api-reference.md) — Full API reference
- [18-mapping-review-workflow.md](./18-mapping-review-workflow.md) — SDK review workflow guide
- [17-hybrid-mode.md](./17-hybrid-mode.md) — Push/pull sync for team collaboration
- [10-portiere-cloud-guide.md](./10-portiere-cloud-guide.md) — Portiere Cloud guide

# FHIR Bundle + NDJSON Export

v0.3.0 adds two FHIR-compliant export shapes for generated resources:

- **Bundle (transaction)** — one JSON file containing all resources, suitable for `POST /` to a FHIR server.
- **NDJSON (Bulk Data)** — one newline-delimited JSON file per resource type, matching the FHIR Bulk Data `$export` operation output.

Both are exposed through `portiere export` and the `portiere.export.fhir` Python module.

## When to use which

| Use case | Format |
|---|---|
| Loading a small dataset into a HAPI / Smile / Azure FHIR server in one round-trip | **Bundle** |
| Round-tripping a small fixture through a FHIR test server | **Bundle** |
| Large datasets (millions of resources) | **NDJSON** |
| Feeding a `$import` operation or another tool that expects Bulk Data shape | **NDJSON** |
| Streaming export from a CI job to object storage | **NDJSON** |

Bundle is bounded by FHIR server entry limits (typically a few thousand). NDJSON has no such ceiling.

## Bundle (transaction)

Each resource becomes one entry with:

- `fullUrl`: `urn:uuid:<random>` — a fresh UUID per entry, FHIR-compliant, no cross-resource reference resolution at export time. If your resources contain references in `urn:uuid:` or canonical-URL form upstream, those are preserved verbatim.
- `request.method`: `POST`
- `request.url`: the resource type (e.g. `"Patient"`)

Example output:

```json
{
  "resourceType": "Bundle",
  "type": "transaction",
  "entry": [
    {
      "fullUrl": "urn:uuid:a3f7c1de-...",
      "resource": {
        "resourceType": "Patient",
        "id": "p1",
        "identifier": [{"system": "urn:oid:1", "value": "x"}],
        "name": [{"family": "Doe"}],
        "gender": "female"
      },
      "request": {"method": "POST", "url": "Patient"}
    }
  ]
}
```

Consume in a HAPI FHIR test server:

```bash
curl -X POST -H 'Content-Type: application/fhir+json' \
  --data-binary @bundle.json \
  https://hapi.fhir.org/baseR4
```

## NDJSON Bulk Data

Resources are grouped by `resourceType`, one file per type, one resource per line:

```
out_dir/
  Patient.ndjson
  Observation.ndjson
  Condition.ndjson
```

Each line is a compact JSON-encoded resource (no indentation). Empty input writes no files.

File naming follows the FHIR Bulk Data `$export` convention; downstream tools that expect `<ResourceType>.ndjson` can ingest this directly.

## Profile validation hook

`--fhir-profile us-core-6.1.0` runs the [Slice 3 profile validator](fhir-profile-validation.md) **before** writing. On any error-severity failure:

- The command exits with code `1`.
- **No bytes are written to `--out`.**
- First 5 failures are printed to stderr; remaining count is summarized.

Without `--fhir-profile`, export proceeds unconditionally (resources are written as-is).

## CLI examples

```bash
# Bundle, no validation
portiere export \
  --input resources.json \
  --format bundle \
  --out out/bundle.json

# Bundle, validated against US Core 6.1.0
portiere export \
  --input resources.json \
  --format bundle \
  --out out/bundle.json \
  --fhir-profile us-core-6.1.0

# NDJSON
portiere export \
  --input resources.json \
  --format ndjson \
  --out out/ndjson_dir/

# NDJSON with validation
portiere export \
  --input resources.json \
  --format ndjson \
  --out out/ndjson_dir/ \
  --fhir-profile us-core-6.1.0
```

`--input` is a JSON file containing a list of FHIR resource dicts.

## Python API

```python
from portiere.export.fhir import to_transaction_bundle, to_ndjson_files

resources = [
    {"resourceType": "Patient", "id": "p1", "gender": "female"},
    {"resourceType": "Observation", "id": "o1", "status": "final"},
]

# Bundle
bundle = to_transaction_bundle(resources)
# bundle is a dict; serialize with json.dumps as needed.

# NDJSON
from pathlib import Path
files = to_ndjson_files(resources, out_dir=Path("out/"))
# files is a list[Path] of files written.
```

## Round-trip guarantee

The exporter is round-trip tested against `fhir.resources` v8.2.0: both `to_transaction_bundle()` output and each NDJSON line parse back through the Pydantic models without error. See `tests/test_fhir_export.py::TestFhirExportRoundTrip`.

## Not in v0.3.0

- **Collection / searchset Bundles** — only `transaction` is emitted; collection lands in v0.3.x if there's demand.
- **Reference rewriting** — cross-resource references aren't recomputed against the new `fullUrl`s. Resources must already carry `urn:uuid:` or canonical-URL references upstream.
- **PUT semantics** — all requests are `POST`. `PUT` (upsert by id) lands in v0.3.x.

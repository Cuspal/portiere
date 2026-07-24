# Pipeline Tutorial: HL7 v2 → FHIR R4

Raw ADT^A01 messages → parsed segments → crossmapped FHIR resources →
transaction Bundle + NDJSON. Structural conversion via the shipped crossmap —
no AI required, no network.

Runnable notebook: [`24_pipeline_hl7v2_to_fhir.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/24_pipeline_hl7v2_to_fhir.ipynb).

## The route

```
ER7 text ──parse──► segment dicts ──CrossStandardMapper──► FHIR resources ──► Bundle / NDJSON
 (MSH/PID/PV1/         (PID, PV1,       hl7v2_2.5.1 →         (Patient,
  DG1/OBX lines)        DG1, OBX)         fhir_r4              Encounter, …)
```

## Step 1 — Parse ER7

Portiere's `hl7v2_2.5.1` standard models segments as entities with named
fields (`PID.patient_id`, `PID.date_of_birth`, `DG1.diagnosis_code`, …). The
notebook includes a compact parser mapping pipe-delimited field positions to
those names — swap in `hl7apy`/your interface engine's parser for production;
only the output dict shape matters:

```python
{"PID": [{"patient_id": "MRN-10001", "patient_name": "DOE^JANE",
          "date_of_birth": "19850322", "administrative_sex": "F"}],
 "DG1": [{"diagnosis_code": "I10", "diagnosis_description": "…"}], ...}
```

## Step 2 — Crossmap

```python
from portiere.local.cross_mapper import CrossStandardMapper

mapper = CrossStandardMapper("hl7v2_2.5.1", "fhir_r4")
mapper.get_entity_map()
# {'PID': 'Patient', 'PV1': 'Encounter', 'DG1': 'Condition',
#  'OBX': 'Observation', 'OBR': 'DiagnosticReport',
#  'RXE': 'MedicationRequest', 'AL1': 'AllergyIntolerance'}
```

The crossmap (based on the HL7 v2-to-FHIR IG) applies per-field transforms —
`v2_gender` (F→female), `fhir_date`, `codeable_concept`. Two things to know:

- **`map_record` returns the mapped field dict** — inject `resourceType`
  yourself from `get_entity_map()` (the notebook shows the idiom).
- **Unmapped v2 fields are dropped, not guessed.** `get_mapping_report()`
  documents exactly what is covered — paste it into your interface spec.

```python
entity_map = mapper.get_entity_map()
resources = [
    {"resourceType": entity_map[seg],
     **{k: v for k, v in mapper.map_record(seg, rec).items() if v not in (None, "", {})}}
    for seg, records in message.items() for rec in records
]
```

## Step 3 — Bundle + NDJSON

```python
from portiere.export.fhir.bundle import to_transaction_bundle
from portiere.export.fhir.ndjson import to_ndjson_files

bundle = to_transaction_bundle(resources)   # each entry: fullUrl + POST request
to_ndjson_files(resources, out_dir=out)
```

Gate on profile conformance when needed:
`portiere validate --fhir-profile us-core-6.1.0`.

## Production notes

- **Batch feeds**: split on `MSH` boundaries (the [HL7 v2 → OMOP notebook](26-pipeline-hl7v2-to-omop.md)
  shows the splitter) and stream messages through the same mapper instance.
- **References**: v0.4.0 crossmaps do not rewrite cross-resource references
  (Observation.subject → Patient). Assign `urn:uuid:` fullUrls upstream if your
  server requires resolvable references within the transaction.
- **Segments beyond PID/PV1/DG1/OBX**: OBR, RXE, AL1 are also mapped; extend
  the crossmap YAML (`src/portiere/standards/crossmaps/hl7v2_to_fhir_r4.yaml`)
  for site-specific Z-segments.

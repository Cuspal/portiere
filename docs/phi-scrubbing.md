# Value-Level PHI Scrubbing (v0.4.0)

Portiere v0.4.0 adds **value-level** PHI detection and scrubbing —
`portiere.deid.PHIScrubber` — replacing reliance on the column-*name*
heuristic (`phi_columns` in Stage 1), which remains as a fast pre-filter only.

## Quick start

```python
from portiere.deid import PHIScrubber

scrubber = PHIScrubber(strategy="redact")          # backend="auto"
scrubber.scrub_column(["call 02-123-4567", "E11.9"])
# -> ['call [PHONE]', 'E11.9']
```

Wired into Stage 1 (opt-in for v0.4.0; planned default-on in v1.0):

```python
from portiere.stages.stage1_ingest import ingest_source

profile = ingest_source(engine, "data/patients.csv", scrub_phi=True)
# example / sample_values / top_values in the profile are scrubbed
```

## Backends

| Backend | Detects | Dependencies |
|---|---|---|
| `regex` (built-in) | EMAIL, PHONE, MRN/HN-labeled IDs, NATIONAL_ID (SSN shape, 13-digit shape), DATE | none |
| `presidio` | all of the above **plus PERSON names, locations** (NER) | `pip install "portiere-health[phi]"` |
| `auto` (default) | presidio when installed, else regex | — |

## Strategies

- `redact` — replace the span with a `[TYPE]` marker (default)
- `hash` — **salted** SHA-256 token (`<TYPE:ab12cd34ef>`). Deterministic within one scrubber instance, so joins survive; salted so low-entropy PHI (DOBs, SSNs, phone numbers) cannot be recovered by offline enumeration. Pass `salt=` for tokens that are stable across runs.
- `surrogate` — **keyed** shape-preserving substitution (labels like `MRN:` are kept; only the value span is replaced). Substitutions are derived from the instance salt — not invertible without it. Note: surrogate output looks like real data by design; prefer `redact` when artifacts leave your team.

## Measured behaviour (regex backend)

Measured on the bundled synthetic clinical dataset (602 values across
patients / conditions / medications / observations), 2026-07-24:

| Aspect | Result |
|---|---|
| Recall on true PHI columns | **100%** — `dob` 20/20, `phone` 20/20 |
| False positives on code/value columns | **0** — no findings on ICD-10 codes, drug names, LOINC codes, lab values, units |
| Clinical-event dates (`onset_date`, `prescribed_date`, `lab_dt`) | flagged as DATE (30/30, 25/25, 37/37) |

**On dates:** HIPAA Safe Harbor treats all dates related to an individual as
identifiers, so flagging clinical-event dates is conservative-by-design. If your
governance permits keeping event dates, narrow the scrubber:

```python
PHIScrubber(entities=["EMAIL", "PHONE", "MRN", "NATIONAL_ID", "PERSON"])
```

Re-run this measurement on your own data before relying on it — detection rates
are data-dependent, and the regex backend cannot detect unlabeled free-text names
(use the `[phi]` extra for NER).

## Scope in v0.4.0 — what is and is not scrubbed

Be precise about the boundary:

- **Scrubbed** (with `scrub_phi=True`): the Stage-1 profile's value-bearing
  fields — `example`, `sample_values`, `top_values` — i.e. profile artifacts
  (HTML/CSV reports) and anything built *from the profile dict*.
- **NOT scrubbed in v0.4.0**: Stage-2 schema mapping reads sample values
  directly from the raw dataframe (not from the scrubbed profile), so an
  LLM-assisted schema-mapping payload can carry raw values. Wiring the scrubber
  into that path is planned for v0.4.x.

Until then, the control for BYO-LLM configurations is structural:
[`portiere doctor --assert-no-egress`](./compliance-threat-model.md) /
`offline=True` prove that no remote LLM can be configured at all. Scrub-then-send
is defence-in-depth for the profile-derived payloads; offline mode is the
guarantee.

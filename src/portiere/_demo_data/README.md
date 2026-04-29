# Portiere Demo Data

This directory ships inside the `portiere-health` wheel as `portiere/_demo_data/`. It powers the `portiere quickstart` CLI — a fully offline, end-to-end demo of the OMOP mapping pipeline against ~20 synthetic patients.

## What's here

```
_demo_data/
├── synthetic_patients.csv        20 patients with deliberately messy column names
├── synthetic_conditions.csv      30 condition records (ICD-10-CM)
├── synthetic_observations.csv    37 measurement/observation records (LOINC)
├── synthetic_medications.csv     25 medication records (RxNorm)
└── vocabulary/                   Athena-format subset (tab-delimited)
    ├── CONCEPT.csv
    ├── VOCABULARY.csv
    ├── DOMAIN.csv
    ├── CONCEPT_CLASS.csv
    └── CONCEPT_RELATIONSHIP.csv
```

## Source data design

Column names are deliberately mixed:

- **Clean** (~40%): `patient_id`, `gender`, `state`, `prescribed_date`, `lab_value`. Should hit the source-pattern lookup directly.
- **Abbreviated** (~40%): `dob`, `pt_zip`, `phone`, `lab_dt`, `onset_date`. Should hit pattern aliases.
- **Rephrased** (~20%): `dx_code`, `med_code`, `lab_description`. Forces the embedding-similarity path.

This way the quickstart demo produces a healthy mix of confidence tiers (auto-accept / needs-review / manual) so users see the full routing.

## Vocabulary subset

Three vocabularies, each with 8–10 concepts representing common conditions / labs / medications:

- **ICD-10-CM** (10 codes): E11.9 diabetes, I10 hypertension, J45.909 asthma, …
- **LOINC** (9 codes): 4548-4 HbA1c, 8480-6 systolic BP, 718-7 hemoglobin, …
- **RxNorm** (8 codes): 860975 metformin, 314076 lisinopril, 197361 albuterol, …

**Important deviation from a real Athena export:** all bundled concepts are marked `standard_concept = 'S'`, including ICD-10-CM. In a real OMOP setup, ICD-10-CM is non-standard and would map to SNOMED CT. We can't bundle SNOMED (license-restricted), so for the demo, every code maps to itself — the pipeline runs end-to-end and shows confidence routing, but the mapping target is intentionally simplified.

For real ICD-10-CM → SNOMED CT mapping, download Athena (free with registration) and use it as the vocabulary directory. See [docs/documentations/15-vocabulary-setup.md](../../../docs/documentations/15-vocabulary-setup.md).

## Curation invariant

`tests/test_demo_data_invariant.py` asserts on every CI run that **every** code in the source CSVs (`dx_code`, `lab_code`, `med_code`) exists in `vocabulary/CONCEPT.csv`. A violation fails the build.

## Regenerating from upstream

`scripts/build_demo_data.py` documents how this bundle could be regenerated reproducibly from upstream sources (Synthea + CMS ICD-10-CM + LOINC + RxNorm). For v0.2.0 the bundle is hand-curated and committed directly; the script is a v0.3.0 enhancement.

## Licensing

- Synthetic CSVs: synthesized for this project (no upstream license).
- ICD-10-CM: U.S. CMS, public domain.
- LOINC: Regenstrief Institute, [LOINC license](https://loinc.org/license/) — public-domain code reuse permitted.
- RxNorm: NLM, [public domain in the U.S.](https://www.nlm.nih.gov/research/umls/rxnorm/docs/2025/rxnorm_doco_full_2025-1.html).
- SNOMED CT: NOT bundled (national license required in many jurisdictions; users add via Athena).
- CPT: NOT bundled (AMA-licensed).

# Pipeline Tutorial: OMOP CDM → Custom Analytics CDM

You already converted to OMOP; the analytics team wants a flat, query-friendly
mart. Define the mart as a **custom YAML standard**, map OMOP into it with the
normal pipeline, then run cohort / prevalence / age analytics on the result.

Runnable notebook: [`26_pipeline_omop_to_custom_cdm.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/26_pipeline_omop_to_custom_cdm.ipynb)
(fully offline, executed with outputs).

## The route

```
OMOP person / condition_occurrence ──map_schema──► custom mart entities ──ETL──► flat CSVs ──► analytics
                                     (target_model="custom:<yaml>")
```

## Step 1 — Define the mart as YAML

A custom standard is a YAML file: entities, typed fields, and
`source_patterns` — where each pattern maps a **source column name** to a
**target field name** in that entity:

```yaml
name: "patients_mart_v1"
version: "1.0"
standard_type: "relational"
entities:
  patient_summary:
    description: "One row per patient with demographics"
    fields:
      subject_id:  {type: integer, required: true,  description: "…"}
      gender_code: {type: integer, required: false, description: "…"}
      birth_year:  {type: integer, required: true,  description: "…"}
    source_patterns:
      person_id: "subject_id"          # value = target FIELD NAME (a string)
      gender_concept_id: "gender_code"
      year_of_birth: "birth_year"
default_entity: "patient_summary"
default_field: "subject_id"
```

**The modeling rule that bites:** `source_patterns` are global per standard —
each pattern resolves to exactly one `(entity, field)`. A source column shared
by several tables (`person_id` appears in both `person` and
`condition_occurrence`) must therefore be claimed by exactly **one** entity per
standard. The clean pattern: **one mart standard per extraction unit** — the
notebook defines `patients_mart_v1` and `events_mart_v1` separately and runs
one lightweight project per mart.

## Step 2 — Map and ETL

```python
proj = portiere.init(name="omop-to-patients-mart",
                     target_model=f"custom:{patients_yaml}",
                     vocabularies=["ICD10CM"], config=offline_config)
src = proj.add_source("person.csv", name="person")
schema_map = proj.map_schema(src)      # AUTO_ACCEPTED via source_patterns
proj.run_etl(src, output_dir=out, schema_mapping=schema_map)
```

Because OMOP column names are stable, `source_patterns` catch everything at
full confidence with **no embedding model** — this pipeline is deterministic
and instant. `get_target_model(f"custom:{path}")` also gives you `generate_ddl()`
for the mart's SQL schema.

> Requires v0.4.0+ — earlier versions ignored custom targets in local schema
> mapping (fixed; regression-tested).

## Step 3 — Analytics on the mart

The point of the flat shape — cohort logic becomes one-liners (polars):

```python
# patients born before 1970 with >= 2 distinct condition concepts
cohort = (events.group_by("subject_id")
          .agg(pl.col("event_concept").n_unique().alias("n_conditions"))
          .join(patients, on="subject_id")
          .filter((pl.col("birth_year") < 1970) & (pl.col("n_conditions") >= 2)))

# prevalence per concept by gender
prev = (events.join(patients, on="subject_id")
        .group_by(["event_concept", "gender_code"])
        .agg(pl.col("subject_id").n_unique()))
```

## Production notes

- Add mart entities incrementally (drug eras, visit windows, lab pivots) —
  each stays a small YAML diff under version control, with `required:` driving
  Stage-5 completeness validation of the mart itself.
- For marts that need *value* transformation (age bands, code rollups), land
  the mapped mart first, then apply analytics transforms downstream — keep the
  standard purely structural.
- Cross-standard YAML reference: [multi-standard support](20-multi-standard-support.md)
  · custom-standard deep dive: [`15_custom_standards.ipynb`](https://github.com/Cuspal/portiere/blob/main/docs/notebooks_examples/15_custom_standards.ipynb).

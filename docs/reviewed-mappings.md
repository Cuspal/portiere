# Review and reuse mappings

**Introduced in 0.6.0.** This guide covers source-scoped reviews and migration from 0.5.x. To test a release candidate from this checkout, follow the [contributor instructions](../CONTRIBUTING.md).

Each registered source now has a stable `id`. Schema and concept mappings belong to that source, so adding another hospital's file no longer overwrites the first hospital's decisions. The SDK and Streamlit review UI read and save the same current snapshot.

## Select a source explicitly

```python
first = project.add_source("clinic-a/patients.csv", name="clinic-a")
second = project.add_source("clinic-b/patients.csv", name="clinic-b")

schema = project.map_schema(first)
concepts = project.map_concepts(first, code_columns=["diagnosis_code"])

# Inspect proposals, then record each reviewed decision.
schema.get_item("patient_id").approve("person", "person_id")
project.save_schema_mapping(schema)

# Another session, notebook or process reads the same saved decisions.
schema = project.load_schema_mapping(source=first)
concepts = project.load_concept_mapping(source_id=first["id"])
```

Use distinct names for different source systems. Re-registering an existing name preserves its ID. Passing that name explicitly also allows changing its file binding for the next delivery from that source. An implicit filename collision with a different path raises an error and asks for a distinct `name=`. Source schema fingerprints and compatibility checks are still planned, so review the new delivery's columns before applying an existing mapping.

Unscoped load methods still work for unambiguous projects. With multiple sources, pass `source=` or `source_id=`. Supplying both requires matching identities. Pre-extracted concept lists can be attached with `project.map_concepts(first, codes=[...])`; include `source_column` in each record to identify its field.

## Review decisions control execution

Schema mappings execute only with `auto_accepted`, `approved` or `overridden` status and a target table/column. Concept mappings need a positive target ID and an accepted decision. An explicit `pending` or `rejected` decision overrides the legacy `method` field. Older concepts without a decision retain the previous `auto`/`override` acceptance convention. `inference_method` preserves how the original proposal was routed when a reviewer changes the decision.

Rejecting a concept retains its suggested target for inspection, but excludes it from execution lookups, generated scripts, dbt seeds and approved exports. dbt generation requires a fresh output directory for every revision, preventing older generated files from surviving into a new project. `finalize()` is not an approval operation. An ETL plan with no approved schema routes fails before creating output.

Launch `portiere review <project-directory>` after installing `[review]`. Select the source in the sidebar. Each session keeps the revision it displayed until you click **Reload mappings**. Reloading resets editable inputs for the new revision. If another reviewer saved first, your save reports a conflict; reload, compare the latest decisions, and reapply the edits you still intend. Do not remove revision metadata to bypass a conflict.

## Exchange concept reviews through files

```python
# JSON retains items, candidates, provenance, source and revision.
project.export_concept_mapping("clinic-a-review.json", source=first)

# CSV is convenient for row-level review; preserve both metadata columns.
project.export_concept_mapping("clinic-a-review.csv", source=first)

# After editing review_decision/target fields, import the reviewed file.
reviewed = project.import_concept_mapping("clinic-a-review.csv", source=first)

# Approved downstream projection: this is not a lossless review backup.
project.export_concept_mapping("source_to_concept_map.csv", source=first, omop_format=True)
```

Concept JSON uses an envelope with `format_version: 1`, `items`, `source_id`, `revision` and `finalized`. Legacy JSON lists remain readable. CSV repeats `mapping_source_id` and `mapping_revision` on each row; conflicting row metadata is rejected. CSV retains decisions and inference method, but does not preserve candidates or provenance. Empty mapping CSV export is rejected with guidance to use JSON. Use JSON when those details must round-trip. Keep literal codes as text in spreadsheet editors too: Portiere cannot recover zeros already removed by another application.

Imports use the exported revision. If the stored mapping has changed, import fails instead of overwriting the later review. A file for source A cannot be imported as source B. For a legacy table without metadata, first import into the intended source. Replacing an existing snapshot requires an explicit expected revision after inspecting the current mapping:

```python
current = project.load_concept_mapping(source=first)
# Compare current.items with the external table before accepting replacement.
replacement = project.import_concept_mapping(
    "legacy-review.csv", source=first, expected_revision=current.revision,
)
```

The revision is a content digest for concurrent edits, not an identity signature or immutable historical bundle. In-memory standalone mappings remain supported; versioned mappings supplied to `Project.run_etl()` must match current storage. The standalone runner rejects schema/concept objects with different source IDs, but does not consult a project store.

## Migrate existing projects

1. Keep a backup of your project and mapping files.
2. Re-register each source with a clear, stable name. Existing named sources acquire an ID when needed.
3. Load and inspect mappings before saving. New writes go to `schema_mappings/sources/<source-hash>/schema_mapping_reviewed.json` and the equivalent concept path. The filename does not imply every item is approved; item decisions are authoritative.
4. For a single registered source, existing unscoped YAML or reviewed JSON can be read and bound on the next save. The original files remain intact. A legacy draft modified after its unversioned review raises a reconciliation error rather than assuming the review applies.
5. If multiple legacy sources exist, the old global mapping cannot be assigned reliably. Import each verified concept table explicitly into its source. For schema mappings, load a verified CSV with `SchemaMapping.from_csv()`, set its `source_id`, then save; inspect effective targets because legacy CSV is a projection. If replacing a current schema snapshot, set the revision from the current mapping only after reconciling the content. Do not copy one global mapping to every source automatically.
6. Update scripts that read the old YAML/CSV output locations. Use SDK loaders or explicit exports. Saving a concept mapping no longer refreshes an implicit `source_to_concept_map.csv`; an older copy on disk is not the current review.

Legacy YAML is a fallback; a current versioned JSON snapshot takes precedence. Editing retained YAML after migration does not update the active mapping. JSON interchange and the shared store are the review path for this slice; portable, lossless schema/concept bundles are next.

## Scope and operational limits

Mapping writes use atomic file replacement and a project-level exclusive lock on a local filesystem. A competing write fails with `MappingConflictError`; reload and retry. A process killed during a write can leave `.mapping.write.lock`. After confirming no writer is running and inspecting the last snapshot, remove that stale lock and reload. A leftover `.mapping-*` temporary file is not an active revision. Shared network filesystems and distributed writers have not been certified. Custom storage backends must implement atomic source registration and the source-aware load contract; cloud storage remains unimplemented.

Concept extraction no longer silently caps distinct values at 1,000 or 5,000. CSV code extraction preserves strings on the supported engines. The resulting mapping collection still lives in memory; this is completeness work, not bounded-memory or distributed mapping. Pandas/Polars paths are covered locally; Spark requires its CI/runtime validation.

Direct search respects vocabulary/domain filters and retains the vocabulary's standard/nonstandard designation. Automatic standard targets require `standard_concept="S"`. Athena loading excludes concepts marked invalid, and local BM25/FAISS retrieval preserves invalidity metadata for filtering. Rebuild older indexes whose validity metadata was discarded; remote-backend validity enforcement still requires separate verification. This does not implement Athena `Maps to`/`Maps to value` relationship traversal or one-to-many row expansion. Those require a separate target compilation contract; see the [OHDSI vocabulary model](https://ohdsi.github.io/CommonDataModel/vocabulary.html).

Explicit concept destination fields, frozen portable bundles, bounded resource controls, a unified report and strict CI comparison remain planned. This slice establishes source ownership and review consistency for that work; it does not yet guarantee that every generated concept column matches its target standard.

Development verification on 2026-09-07: 1,365 tests passed on macOS/Python 3.12, with 30 skipped and 5 model tests deselected. Lint, formatting, type checks and wheel/sdist builds passed. Fresh installed-wheel checks covered core CSV exchange without a dataframe engine, two-source Polars execution without Pandas, and the offline quickstart with `[polars,quality]`. Native Streamlit interaction and the remote OS/backend matrix remain unverified.

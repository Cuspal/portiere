# Mapping Review UI (v0.3.1)

v0.3.1 ships a [Streamlit](https://streamlit.io/)-based human-in-the-loop review UI for AI-generated schema mappings. It reads from a project's existing storage, lets you approve / override / reject each item, and persists decisions to a sibling file. The Python API in [`docs/documentations/18-mapping-review-workflow.md`](documentations/18-mapping-review-workflow.md) remains the authoritative review surface; the UI is a convenience layer on top.

## Install

```bash
pip install "portiere-health[review]"   # adds streamlit>=1.30
```

The `review` extra is opt-in. Without it, the rest of Portiere is unaffected.

## Launch

```bash
portiere review <project-dir>
# opens http://127.0.0.1:8501 in your browser
```

Flags:

| Flag | Default | Notes |
|---|---|---|
| `--host` | `127.0.0.1` | Local-only by default. `--host 0.0.0.0` exposes on the LAN (no auth — see security note below). |
| `--port` | `8501` | Streamlit's default. |

## What you see

- **Sidebar** — filter by mapping status (e.g., show only `needs_review`).
- **Main panel** — one expander per mapping. The `needs_review` rows auto-expand; everything else collapses.
- **Per-row actions:**
  - **Approve** — accept the AI's predicted target.
  - **Reject** — mark this mapping as unmappable.
  - **Override** — provide a `table.column` to use instead. Status flips to `overridden`.
- **Candidate inspector** — the top 5 alternative targets the AI considered are shown beneath each row.

Every click writes `<project_dir>/schema_mappings/schema_mapping_reviewed.json` and re-renders the page. The original `schema_mapping.yaml` is never modified.

## Try it on a fresh fixture

```bash
mkdir -p /tmp/portiere-review-demo/schema_mappings
cat > /tmp/portiere-review-demo/schema_mappings/schema_mapping.yaml <<'YAML'
- source_table: patients
  source_column: patient_id
  target_table: person
  target_column: person_id
  confidence: 0.99
  status: auto_accepted
  candidates: []
- source_table: patients
  source_column: dob
  target_table: person
  target_column: birth_datetime
  confidence: 0.72
  status: needs_review
  candidates:
    - target_table: person
      target_column: year_of_birth
      score: 0.65
YAML

portiere review /tmp/portiere-review-demo
```

Approve the first row, override the second to `person.year_of_birth`, then:

```bash
cat /tmp/portiere-review-demo/schema_mappings/schema_mapping_reviewed.json
```

You'll see the two decisions captured in JSON, with the original YAML untouched.

## Apply reviewed decisions back to the pipeline

The Python API loads the reviewed file when present:

```python
from portiere.review_ui.state import load_schema_mapping

mapping = load_schema_mapping("/tmp/portiere-review-demo")
# load_schema_mapping prefers schema_mapping_reviewed.json over the original YAML
# when both exist, so your downstream ETL run gets the human-reviewed targets.
```

If you want to load the AI's original output instead (e.g., to compare), delete or rename `schema_mapping_reviewed.json`.

## Security notes

- **No authentication.** The UI is intended for single-user local workflows. The default `127.0.0.1` bind prevents accidental LAN exposure.
- **`--host 0.0.0.0` exposes the UI on the LAN without auth.** The CLI prints a warning when this is used. Only enable on trusted networks for demos.
- **Cloud / multi-user reviewer workflows** with authentication are part of Cuspal Cloud, not the open-source SDK.

## Override semantics

When you override:

- For schema mappings: enter a `table.column` string. Both pieces are persisted as `override_target_table` / `override_target_column` on the item. The `effective_target_table` / `effective_target_column` properties resolve to the override.
- The original AI suggestion stays on the item for audit (in `target_table` / `target_column`), so a future code review can see what was overridden and to what.

## Filter and sort

The sidebar lets you filter by mapping status. v0.3.1 ships an ascending sort by confidence for the concept-mapping page (when that ships in v0.3.2) — lowest-confidence first surfaces the items that most need human attention.

## What's not in v0.3.1

- **Concept-mapping page** — coming in v0.3.2.
- **Bulk actions** (approve-all / reject-all on a filtered subset) — v0.3.2.
- **Authentication / multi-user state** — Cloud only.
- **CSV export / re-import from the UI** — the Python API in [`docs/documentations/18-mapping-review-workflow.md`](documentations/18-mapping-review-workflow.md) still handles this.

## Architecture

- `src/portiere/review_ui/state.py` — pure load/save/decision helpers, no Streamlit imports; unit-testable.
- `src/portiere/review_ui/app.py` — Streamlit entrypoint, parses `--project-dir` from argv pass-through.
- `src/portiere/review_ui/pages/schema_review.py` — per-mapping-type page.
- `src/portiere/cli/review.py` — subprocess-launches `streamlit run app.py -- --project-dir <dir>`.

Tests: `tests/test_review_ui_state.py` (helpers) and `tests/test_review_cli.py` (Click-level + monkeypatched `subprocess.Popen`). The UI itself is exercised by manual smoke test — Streamlit's runtime is not started in CI.

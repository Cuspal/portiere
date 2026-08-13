# Review Handoff — Portiere

Run `review-2026-08-13`. Started at branch `main` @ `beabb7e`.

**Update:** the maintainer has since run the commit plan below — the **5 fixes
(F-004/5/7/8/10) + review artifacts are now committed** as `aef3103…a9cddf1` on
top of `beabb7e`. Verified: the cumulative `beabb7e..a9cddf1` src/tests diff is
exactly those five fixes + the PHI corpus, with no foreign edits. The ongoing
review's baseline is now **`a9cddf1`**; only this session's **Mapping-pass doc
updates** (findings/DECISIONS/HANDOFF/state) remain uncommitted. §1 and §3 below
describe the original pre-commit state and are kept for the record.

Partial run by design, extended across sessions. Complete so far: Phase 0
(baseline) + **Privacy** (PV-01…04) + **Core** (CO-01…04) + **Pipeline**
(PL-01…06) + **Mapping** (MP-01…06) + **Knowledge** (KN-01…06) + **Standards**
(ST-01…04). **30 of 47 subsystems** are terminal; the other 17 (QUALITY,
SURFACE, CROSS-CUTTING) are `PENDING` and resumable — `docs/review/state.json`
carries the registry forward; re-invoke `/review` (clean tree first) to continue
at **QR-01** without re-auditing what is done.

**Six safe fixes applied and verified; seven findings parked for your call.**
F-004/5/7/8/10 are committed (`aef3103…a9cddf1`); F-012 was committed this
session too (`59469ea` area). No uncommitted code fixes remain — only this
Standards-pass doc update.

> **⚠️ Standards pass surfaced the review's most serious finding — F-013 (S1):**
> the advertised **FHIR→OMOP** crosswalk is a non-functional stub (its
> `extract_*` transforms are passthrough), silently producing invalid OMOP
> records. Park List (crosswalk/clinical semantics) — see DECISIONS.md, top.
> Recommend documenting FHIR→OMOP inbound mapping as experimental until fixed.

**No git writes were made. Nothing was committed, tagged, or released.**

## 1. State of the tree

`git status --short` should show exactly:

```
 M src/portiere/config.py                 # F-005 (from_yaml guards bad YAML)
 M src/portiere/deid/scrubber.py          # F-004 (warn on auto→regex fallback)
 M src/portiere/runner/etl_runner.py      # F-010 (dropped column → result.warnings)
 M src/portiere/stages/stage4_transform.py# F-008 (escape paths + csv module in codegen)
 M src/portiere/storage/local_backend.py  # F-007 (reject traversal project names)
 M tests/test_config.py                   # F-005 tests (+4, +missing `import pytest`)
 M tests/test_deid_scrubber.py            # F-004 tests (+2)
 M tests/test_etl_runner.py               # F-010 tests (+1)
 M tests/test_integration.py              # F-008 tests (+2)
 M tests/test_storage_backends.py         # F-007 tests (+4)
?? docs/review/                           # review artifacts (findings, plan, patches, state, corpus scorer)
?? tests/fixtures/phi_corpus/             # PHI ratchet corpus + scorer
```

Source/test delta: **10 files changed, 337 insertions, 27 deletions**. Everything
under `docs/review/` and `tests/fixtures/phi_corpus/` is new tooling.

## 2. Verification (baseline → current)

| Check | Baseline | Current | |
|---|---|---|---|
| Tests (default) | 1284 passed, 2 skip | **1299 passed, 2 skip** | +15 (F-004×2, F-005×4, F-007×4, F-008×2, F-010×1, F-012×2) |
| Tests (`-m slow`) | 5 passed, 1 skip | 5 passed, 1 skip | unchanged |
| ruff / format | clean | clean | — |
| mypy (`src/portiere`) | 0 CI / 1 local-faiss | 0 CI / 1 local-faiss | unchanged (env-only) |
| **PHI recall (regex)** | **1.0** | **1.0** | **held** |
| **PHI precision (regex)** | **1.0** | **1.0** | **held** |
| Public API hash | `2243a4b4a63e5415` | unchanged | no API drift |

Nothing moved the wrong way. No public signature changed. Scrubber detection is
byte-identical (ratchet held 1.0/1.0) — no fix touched detection logic. Each fix
passed the full-suite gate on the first attempt (**0 blocked**).

## 3. Commit plan — five independent fixes, no overlaps

Each fix touches its own source file + its own test file; no file is shared
between findings (`overlaps: []` for all five). Commit per-finding or grouped:

```bash
git checkout -b review/core-privacy-pipeline-pass

git add src/portiere/deid/scrubber.py tests/test_deid_scrubber.py
git commit -m "fix(deid/L3): warn when PHIScrubber auto-backend falls back to regex (no names/addresses) [F-004]"

git add src/portiere/config.py tests/test_config.py
git commit -m "fix(config/L1): from_yaml raises ConfigurationError on empty/non-mapping YAML [F-005]"

git add src/portiere/storage/local_backend.py tests/test_storage_backends.py
git commit -m "fix(storage/L1): reject path-traversal project names in LocalStorageBackend [F-007]"

git add src/portiere/stages/stage4_transform.py tests/test_integration.py
git commit -m "fix(stage4/L1): escape paths (repr) + csv module for generated ETL lookup [F-008]"

git add src/portiere/runner/etl_runner.py tests/test_etl_runner.py
git commit -m "fix(runner/L1): surface dropped duplicate-target column in ETLResult.warnings [F-010]"

# Review tooling (optional, separate commit):
git add docs/review/ tests/fixtures/phi_corpus/
git commit -m "chore(review): core+privacy+pipeline-pass artifacts + PHI ratchet corpus"
```

Isolated patches: `docs/review/patches/F-004.patch`, `F-005`, `F-007`, `F-008`,
`F-010` (`git apply` if you prefer).

**Current uncommitted state (after your `aef3103…a9cddf1` commits):** only the
Knowledge-pass fix F-012 + review docs remain. To commit F-012:

```bash
git add src/portiere/local/schema_mapper.py tests/test_local_schema_mapper.py
git commit -m "fix(schema_mapper/L3): re-assert offline gate before building embedding gateway [F-012]"
git add docs/review/
git commit -m "chore(review): knowledge-pass artifacts (F-012 fix + KN notes)"
```
Patch: `docs/review/patches/F-012.patch`. **Review-first**: F-012 is an
egress/privacy path — it only ever *adds* a `ConfigurationError` under
`offline=True` + a remote embedder, never weakens; confirm that matches intent.

## 4. Review-first (read before committing)

- **F-004 → `deid/scrubber.py`.** Touches `deid/` → review-first by rule (though
  logging-only). Confirm the `UserWarning` wording; it fires once per
  `PHIScrubber(backend="auto")` on a base install, and callers under `-W error`
  will see it as an error — the one behaviour change worth a changelog line.
- **F-007 → `storage/local_backend.py`.** Now raises `ValueError` for a project
  `name` that is empty/absolute/contains `/`, `\`, or `..`. Confirm no caller
  relied on slashes in names (none in the suite; spaces still allowed).
- **F-008 → `stages/stage4_transform.py`.** Generated-code output changed:
  `SOURCE_PATH`/`OUTPUT_PATH` now use `repr()` (single-quoted literals), and the
  lookup CSV is written via the `csv` module (values with commas are now quoted
  instead of `,`→`;` substituted). Both make generated artifacts *more* correct;
  confirm no downstream tooling parsed the old semicolon substitution.

## 5. Decisions needed (parked — `docs/review/DECISIONS.md`)

- **⚠️ F-013 (S1) — highest priority** — the advertised FHIR→OMOP crosswalk is a
  non-functional stub (`extract_*` transforms are passthrough), silently
  producing invalid OMOP records (`person_id` a dict not an int, codes lost,
  `concept_id` absent). No value-level test. Fix = implement the extract
  transforms + change crosswalk YAML types + clinical validation. Interim:
  document FHIR→OMOP as experimental.
- **F-001 (S3)** — schema-mapping sends `col_name + sample_values` to a *remote*
  embedding provider unscrubbed. Gated by `offline=True`. Scrub-on-remote vs doc.
- **F-002 (S3)** — concept verifier sends raw `source_term`/`source_context` to a
  *remote* LLM. Gated by `offline=True`. Scrub vs accept.
- **F-003 (S3)** — regex scrubber misses **Malaysian NRIC** (`######-##-####`).
  Recogniser change (Park List) — needs a corpus precision check first.
- **F-006 (S4)** — `config.from_yaml` leaves literal `${VAR}` on unset var,
  silently. Warn/raise vs passthrough. Low priority.
- **F-009 (S3)** — generated ETL join code builds Python *identifiers* from
  column names (`lookup_{safe}`); a column name with a quote/paren yields an
  invalid identifier → `SyntaxError` in the generated script. Needs identifier
  slugification across all three generators (larger codegen change). Rarer than
  F-008 (paths/values are more variable than column names).
- **F-011 (S3)** — `offline=True` does not set `HF_HUB_OFFLINE`/`local_files_only`,
  so loading an uncached cross-encoder/embedder still downloads from
  `huggingface.co` — contradicting the L7 "no non-loopback socket under offline"
  claim on a cold cache. Fix is a maintainer call: process-global HF offline env
  vs threading `local_files_only` through every loader.

## 6. Blocked

None. No fix was restored; all five passed the full-suite gate first try.

## 7. Release statement (for your decision — not a command sequence)

- **Public API:** unchanged (hash identical). F-005 raises the already-public
  `ConfigurationError`; F-007 raises the stdlib `ValueError` the storage methods
  already used; F-010 only populates the existing `ETLResult.warnings` field.
- **Behaviour visible to existing users:**
  - one new `UserWarning` on `PHIScrubber(backend="auto")` base-install fallback (F-004);
  - `config.from_yaml` raises a clear `ConfigurationError` on empty/non-mapping YAML (F-005);
  - `LocalStorageBackend` rejects path-traversal names with `ValueError` (F-007);
  - generated ETL scripts/CSVs are now correctly escaped (F-008);
  - `ETLResult.warnings` now reports dropped duplicate-target columns (F-010).
- **PHI detector:** unchanged in every direction (ratchet 1.0/1.0).
- **Dependencies / extras:** untouched.

The version bump, changelog, tag, and publish are yours.

---

## Notable leads for the resume pass (surfaced incidentally, not findings)

- **Knowledge offline posture = PASS** — `egress_violations` covers all URL-based
  remote backends; ChromaDB is local-only (no `HttpClient`); Athena and
  vocabulary_bridge are local CSV parsers with no network client; rrfusion math
  is correct. The only residual is F-011 (HF cold-cache model download).
- **ST-04 FHIR export / ST-01 crossmaps** — start STANDARDS here; treat crosswalk
  YAML as clinical content (semantic changes are Park List). Check unmapped
  source elements are reported, not silently dropped (L8).
- **MP-03 cross_mapper** — `_set_nested` cannot build FHIR arrays (`coding[0]`);
  fine for shipped crossmaps, latent for custom ones (see findings NOTE).
- **CO-02 (`project.py`)** — focused-scan only; two swallow-without-log spots
  (`project.py:588` drops a concept candidate; `project.py:1049` `except: pass`).
- **CO-04 (`storage`)** — writes are non-atomic (`open(w)`+write, no temp+rename);
  a crash mid-write to `project.yaml` corrupts the project index. L4.
- **PL-01** — `_detect_phi_columns` `"age"` substring-matches `message`/`storage`
  (advisory hint; over-flag is the safe direction, not a defect).
- **PL-05** — `stage5_validate.py:298` inner `except: pass` can silently skip a
  date-range check (validator reporting "clean" without checking).
- **XC-01/L7 "Typed":** mypy runs `disallow_untyped_defs=false` + `implicit_optional`.
- **XC-05 cloud stubs:** `CloudStorageBackend` raises `NotImplementedError` at
  construction; `Client` is exported — verify which cloud methods raise (S2 per L2).

## What was NOT audited (resume scope)

17 subsystems remain `PENDING`: QR-01…04, SF-01…07, XC-01…06.
Re-invoke `/review` (clean tree first) to continue at QR-01.

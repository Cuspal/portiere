# Park List — decisions the maintainer must make (not committable by the review)

## F-001 — scrub sample values before remote embedding (PV-03/L3, S3)
`local/schema_mapper.py:253` sends `col_name + sample_values` to the embedding
gateway; with a remote embedding provider those go off-machine unscrubbed.
**Parked because:** the fix touches the egress boundary and there are two
defensible designs — (a) route through the scrubber when the provider is
remote, or (b) document-only (offline=True already blocks it, and a user who
sets `embedding.provider="openai"` has opted into egress). Pick one; if (a),
it's a real behaviour change on a privacy path and belongs in a review-first
commit.

## F-002 — scrub concept-verifier source terms before remote LLM (PV-03/L3, S3)
`local/llm_verifier.py` interpolates raw `source_term`/`source_context` into the
prompt. **Parked because:** same egress-boundary class as F-001; gated by
`offline=True`; whether to scrub code descriptions (which are usually not PHI)
is a judgement call about over-redacting clinical terms.

## F-003 — add Malaysian NRIC recogniser (PV-01/L3, S3)
Regex misses `######-##-####`. **Parked because:** the Park List explicitly
covers scrubber recogniser changes. A naive `\b\d{6}-\d{2}-\d{4}\b` could
over-match non-PHI 12-digit-dashed strings — verify precision on the corpus
(and add NRIC positives + near-miss negatives) before adopting. Recall-positive,
so ratchet-safe once precision is confirmed.

## F-006 — warn/raise on unset `${VAR}` in config interpolation (CO-01/L6, S4)
`config.from_yaml` leaves the literal `${VAR}` in place when the env var is
unset (`match.group(0)` fallback), silently. **Parked because:** it is a
behaviour choice, not a defect — some workflows may rely on passthrough. Not a
leak; the failure surfaces later as a confusing literal value. Decide: warn,
raise, or keep passthrough. Low priority.

## F-009 — sanitize column names into valid identifiers in generated ETL (PL-04/L1, S3)
The generated join code builds Python identifiers from column names
(`lookup_{safe}`, `{safe}_concept_id`, where `safe = col.replace(" ", "_")`). A
column name containing a quote/paren produces an invalid identifier → the
generated script fails to compile. **Parked because:** the correct fix is a
slugify-to-valid-identifier step with collision handling, applied across all
three generators — a larger codegen change than F-008's path/CSV escaping, and
rarer in practice (column names are more controlled than paths and values).

## F-011 — offline=True does not prevent HuggingFace model downloads (MP-05/L7, S3)
`offline=True` rejects configured remote *providers*, but loading a local
cross-encoder/embedder whose weights are not cached still calls `huggingface.co`
on first use (no `HF_HUB_OFFLINE`/`local_files_only` anywhere). **Parked because:**
the fix is a maintainer call between two intrusive options — set process-global
`HF_HUB_OFFLINE=1`/`TRANSFORMERS_OFFLINE=1` when offline (affects the whole
process, not just Portiere), or thread `local_files_only=True` into every HF
loader (reranker + all embedding providers). Gated by "model not pre-cached";
a correctly provisioned offline deployment pre-downloads weights.

---

## Applied this run (NOT parked): F-004, F-005, F-007, F-008, F-010, F-012
- **F-004** — `deid/scrubber.py` warns when `backend="auto"` silently falls back
  to `regex` (no `phi` extra). Logging-only; detection unchanged; ratchet 1.0/1.0.
- **F-005** — `config.from_yaml` raises `ConfigurationError` (naming the file) on
  empty/comment-only/non-mapping YAML instead of an opaque stdlib `TypeError`.
- **F-007** — `LocalStorageBackend` rejects path-traversal project names that
  previously escaped the storage root (one reached `shutil.rmtree` on an
  arbitrary directory). Validation-only.
- **F-008** — generated ETL: path constants via `repr()` (Windows paths no longer
  corrupt), lookup CSV via the `csv` module (newline/comma-safe).
- **F-010** — `ETLRunner` now surfaces a dropped duplicate-target column in
  `ETLResult.warnings`, not only in the logs.
- **F-012** — `schema_mapper._initialize` now re-asserts the offline gate
  (`assert_no_egress()`) before building the embedding gateway, mirroring
  `concept_mapper`/`Project` — closes a runtime hole where a post-construction
  config mutation to a remote embedder could send schema sample values
  (potential PHI) off-machine under `offline=True`.

F-004/5/7/8/10 are already committed (`aef3103…a9cddf1`); **F-012 is the one
uncommitted code fix** in the tree. See HANDOFF for the commit line.

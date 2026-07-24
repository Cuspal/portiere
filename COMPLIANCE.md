# Compliance Posture

> Portiere is **local-first clinical data mapping**. In its default
> configuration, no clinical data leaves the machine it runs on — and since
> v0.4.0 that claim is **executable**, not just documented:
>
> ```bash
> portiere doctor --assert-no-egress   # exit 0 = no egress possible
> ```

This document states what Portiere does and does not do with respect to
HIPAA, GDPR, and PDPA, and which controls remain the operator's
responsibility. It is engineering documentation, not legal advice — review it
with your compliance officer.

For trust boundaries and the full data-flow diagram, see
[docs/compliance-threat-model.md](docs/compliance-threat-model.md).

---

## 1. Where data can go

Portiere has exactly **one** class of off-machine data path: **BYO-LLM /
remote-provider configuration**. Everything else — profiling, schema mapping,
embedding, reranking, knowledge-layer search, ETL generation, validation —
runs locally by default.

| Component | Default | Can egress? | When |
|---|---|---|---|
| Compute engines (polars/pandas/spark) | local | no | never |
| Embedding (`huggingface` SapBERT) | local | **yes, if reconfigured** | `provider="openai"` or `"bedrock"`, or a remote `endpoint` |
| Reranker (`huggingface` cross-encoder) | local | **yes, if reconfigured** | remote `endpoint` |
| LLM verification | **off** (`provider="none"`) | **yes, if enabled** | `provider="openai" / "azure_openai" / "anthropic" / "bedrock"` |
| Knowledge layer — file backends (BM25s/FAISS/ChromaDB) | local (default `bm25s`) | no | never |
| Knowledge layer — service backends (Elasticsearch/pgvector/MongoDB/Qdrant/Milvus) | not configured | **yes, if pointed at a remote host** | any non-loopback service URL/connection string — checked by `offline=True` and reported by `portiere doctor` |
| Vocabulary data (Athena) | local files | no | never |
| Telemetry / analytics | none exists | no | never — Portiere contains no telemetry |

**What is transmitted when a remote LLM is configured:** source code strings,
their descriptions, and candidate concept names/IDs (the concept-verification
prompt), and column names/sample values during schema mapping. See the threat
model for the exact payloads and stages.

## 2. The enforcement mechanisms (v0.4.0)

1. **Offline mode** — `PortiereConfig(offline=True)` (or `PORTIERE_OFFLINE=true`)
   makes any remote-provider configuration a **hard error** at construction and
   again at runtime (client factories re-check, so post-construction mutation
   cannot bypass it). Covered by `tests/test_offline_mode.py`.
2. **Preflight** — `portiere doctor` prints per-component egress posture
   (`LOCAL`/`REMOTE`); `--assert-no-egress` exits non-zero if any configured
   provider could transmit. Suitable for CI gates and security review.
3. **Value-level PHI scrubbing** — `portiere.deid.PHIScrubber`
   (see [docs/phi-scrubbing.md](docs/phi-scrubbing.md)) scrubs profile
   examples/samples before any LLM-bound payload is constructed. Opt-in in
   v0.4.0 (`scrub_phi=True`), planned default-on in v1.0.

## 3. Regulation-by-regulation

### HIPAA (US)

- **Default posture:** Portiere processes PHI **in place** on infrastructure you
  control; it is not a service and receives nothing. There is no Business
  Associate relationship with Portiere's authors in local mode.
- **Safe Harbor support:** the PHI scrubber detects/removes the identifier
  categories representable in structured columns (names via NER extra, phone,
  email, MRN-style IDs, SSN-shaped IDs, dates). Detection rates are measured and
  published; re-measure on your data.
- **If you enable a remote LLM:** the LLM vendor becomes part of your HIPAA
  scope — you need a BAA with that vendor (Azure OpenAI, AWS Bedrock, and
  Anthropic offer them). Scrub before egress (`scrub_phi=True`) and prefer
  de-identified payloads.
- **Operator's responsibilities:** access control to the machine and project
  directories, audit logging of who ran what, encryption at rest of source
  extracts, minimum-necessary access to outputs.

### GDPR (EU)

- **Roles:** you (the operator) are the controller/processor; Portiere is
  software running under your instructions. No data is transmitted to
  Portiere's authors.
- **Data residency:** local mode keeps processing on-premises — no transfer
  mechanism (SCCs, adequacy) is needed for the mapping step itself.
- **Remote LLM:** configuring a non-EU provider is an international transfer of
  whatever the prompt contains. Use `offline=True` to prohibit it structurally,
  or an EU-region endpoint plus scrubbing if you must verify with an LLM.
- **Art. 30 records / DPIA input:** the data-flow diagram in the threat model
  is written to be pasted into a DPIA. `portiere doctor` output documents the
  configured posture at run time.

### PDPA (Thailand)

- Same structural analysis as GDPR: local mode = no cross-border transfer under
  §28; a remote LLM endpoint outside Thailand is a cross-border transfer of the
  prompt contents and requires an appropriate basis. The offline mode and
  scrubber are the technical controls; consent/notice obligations for the
  underlying processing remain the operator's.

## 4. What Portiere does NOT do

Stating the negative space explicitly, because procurement reviews ask:

- No telemetry, usage analytics, crash reporting, or phone-home of any kind.
- No account, license key, or activation that contacts a server.
- Model downloads (HuggingFace) fetch **model weights in**; they never send
  data out. Pre-download models and run air-gapped if required
  (`portiere models download`, then `offline=True`).
- No background network listeners. The Review UI binds `127.0.0.1` by default.
- Portiere's authors never receive, store, or can access your data in local
  mode. (Portiere Cloud is a separate, opt-in product with its own terms.)

## 5. Known limitations (honest list)

- The PHI scrubber's regex backend cannot detect unlabeled free-text person
  names — install the `[phi]` extra (Presidio NER) for that, and treat any
  detector as probabilistic: measured recall on your own data is the number
  that matters.
- `offline=True` blocks *configured* egress paths; it is not an OS-level
  firewall. For air-gap assurance, combine it with network-level controls
  (the Docker image runs fine with `--network none` after models are cached).
- Compliance of the **outputs** (e.g., whether your OMOP instance is
  de-identified) is a property of your data governance, not of the mapping
  tool.

## 6. Verification commands

```bash
# Prove the configuration cannot egress
portiere doctor --assert-no-egress

# Show full posture (engine, model cache, per-component LOCAL/REMOTE)
portiere doctor

# Scrub check: profile with value-level PHI scrubbing on
python -c "
from portiere.engines import get_engine
from portiere.stages.stage1_ingest import ingest_source
print(ingest_source(get_engine('polars'), 'your.csv', scrub_phi=True))"
```

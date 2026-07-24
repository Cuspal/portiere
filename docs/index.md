# Portiere

**Local-first clinical data mapping with human-in-the-loop review — PHI never
leaves your machine.**

Portiere maps clinical source data to **OMOP CDM**, **FHIR R4**, **HL7 v2**, and
**OpenEHR** through a 5-stage pipeline — profiling, schema mapping, concept
mapping, ETL generation, and validation — running entirely on your own
infrastructure. AI assistance (embeddings, reranking, optional BYO-LLM
verification) is a feature you control, not a cloud dependency.

```bash
pip install "portiere-health[polars,quality]"
portiere quickstart          # offline end-to-end demo
portiere doctor --assert-no-egress   # prove nothing can leave the machine
```

## Why Portiere

- **Data sovereignty, verifiable.** The no-egress guarantee is executable:
  `offline=True` hard-fails any remote-provider configuration, and
  `portiere doctor` reports the egress posture of every component. See the
  [threat model](compliance-threat-model.md) and
  [COMPLIANCE.md](https://github.com/Cuspal/portiere/blob/main/COMPLIANCE.md).
- **Human-in-the-loop by design.** Confidence routing (auto / review / manual)
  plus a [Streamlit review UI](mapping-review-ui.md) — the tool assists your
  terminologists; it doesn't pretend to replace them.
- **Honest numbers.** We publish [benchmarks](benchmarks/athena-icd-snomed.md)
  including configurations where we lose, with reproduction commands.
- **No vendor lock-in.** Generated ETL scripts (Spark/Polars/Pandas) run
  without Portiere installed.
- **Value-level PHI scrubbing.** [Detect and scrub](phi-scrubbing.md) emails,
  phones, MRNs, national IDs, and dates in values — not just column names.

## Start here

| I want to… | Go to |
|---|---|
| Run the demo | [Quickstart](documentations/01-quickstart.md) |
| Set up SNOMED / CPT vocabularies (licensing) | [Vocabulary setup](documentations/15-vocabulary-setup.md) |
| Understand where data can go | [Threat model & data flow](compliance-threat-model.md) |
| Configure a local or BYO-LLM stack | [LLM integration](documentations/06-llm-integration.md) |
| Review mappings with my team | [Mapping Review UI](mapping-review-ui.md) |
| See accuracy numbers | [Benchmarks](benchmarks/athena-icd-snomed.md) |

## Docker

```bash
docker compose run --rm portiere quickstart
docker compose run --rm portiere profile-report /data/your.csv -o /data/report
```

The compose file sets `PORTIERE_OFFLINE=true` — the container is structurally
incapable of sending data out.

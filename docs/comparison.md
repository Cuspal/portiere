# How Portiere Compares

Honest positioning against the tools an OMOP/FHIR conversion team actually
evaluates. Where Portiere is weaker, this page says so.

## Who Portiere is for (ICP)

Hospital data teams, national/registry programmes, and academic research groups
(≈1–50 people) converting clinical source systems to **OMOP CDM or FHIR R4
under data-residency constraints** — environments where sending PHI to a SaaS
is a procurement non-starter, Python is the working language, and a small team
must map hundreds of columns and thousands of codes with an audit trail.

If you are a lone analyst mapping one table, USAGI alone may be enough. If you
are an enterprise wanting a managed service, that is what
[Portiere Cloud](documentations/10-portiere-cloud-guide.md) is for.

## Portiere vs the field

| | **Portiere** | **USAGI** (OHDSI) | **White Rabbit / Rabbit-in-a-Hat** (OHDSI) | **ETL frameworks** (e.g. Delphyne) |
|---|---|---|---|---|
| Scope | profiling → schema map → concept map → executable ETL → validation | concept mapping only | profiling (WR) + schema-mapping *documentation* (RiaH) | ETL execution once mappings are decided |
| Concept mapping | BM25s / SapBERT+FAISS / hybrid + cross-encoder + optional BYO-LLM verification | TF-IDF similarity + manual review UI | — | — |
| Human-in-the-loop | confidence routing (auto / review / manual) + Streamlit review UI | strong manual review UI (the community benchmark for this) | manual by design | — |
| Output | runnable Spark/Polars/Pandas ETL + lookup CSVs, no runtime lock-in | mapping CSV | ETL *specification* document | runnable ETL (you write the logic) |
| Standards | OMOP CDM 5.4, FHIR R4, HL7 v2, OpenEHR, custom YAML | OMOP vocabularies | OMOP-oriented | OMOP (typically) |
| Data egress | none by default; **provably none** with `offline=True` + `portiere doctor` | none (local Java app) | none (local) | none (library) |
| Validation | Great Expectations + Kahn-aligned scores + FHIR profile validation (US Core, mCODE) | — | — | varies |
| Maturity / community | **young (pre-1.0, small community)** | **the community standard, years of use** | mature, widely used | varies |
| Head-to-head accuracy | «RUN-AND-FILL after v0.4.0 measurement — see [benchmark](benchmarks/athena-icd-snomed.md)» | «RUN-AND-FILL» | n/a | n/a |

**Where the others win, honestly:** USAGI has years of community trust and a
review UI refined by thousands of terminologists; OHDSI tooling has an
ecosystem Portiere does not. Portiere's pre-1.0 API can still change. Raw
top-1 retrieval accuracy vs USAGI is unproven until the v0.4.0 measurement
lands — we publish it either way.

**Where Portiere wins:** one pipeline instead of four tools; executable ETL
instead of documentation; multi-standard targets; a verifiable no-egress
guarantee; value-level PHI scrubbing; Python-native automation with
reproducibility manifests.

## The metric that matters: reviewer throughput

Raw top-1 accuracy is the wrong headline for assisted mapping — the operational
question is **how much of the workload never needs a human**. Portiere's
benchmark reports the confidence-routing split (auto / review / manual) on the
public gold set as a throughput proxy:

> «RUN-AND-FILL: auto-accept share at default thresholds (0.95/0.70), from the
> `routing` field of the v0.4.0 benchmark run. Interpretation: auto-share × N
> codes = mappings a reviewer never touches; review-tier items arrive with
> ranked candidates rather than a blank search box.»

This is a *proxy measured on the gold set*, not a claim about your reviewers'
minutes; no real-world timing study exists yet (planned for v1.0 with a case
study).

## Coverage, stated honestly

Portiere covers **19 of ~37 OMOP CDM v5.4 tables and 18 of 145 FHIR R4
resources**. Counting tables overstates the gap: the covered set includes the
core clinical-event tables that carry the overwhelming majority of rows in a
typical conversion — `person`, `visit_occurrence`, `condition_occurrence`,
`drug_exposure`, `measurement`, `observation`, `procedure_occurrence`,
`observation_period` — plus the vocabulary tables. What is *not* covered (e.g.
cost, payer plan, episode, notes-NLP) matters for specific studies; check the
[support matrix](documentations/20-multi-standard-support.md) against your
protocol before committing. We deliberately do not publish a "% of real-world
volume" figure because we have not measured one.

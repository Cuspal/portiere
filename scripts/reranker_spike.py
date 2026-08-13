#!/usr/bin/env python3
"""Domain-reranker spike — evaluate biomedical cross-encoders vs the incumbent.

The v0.4.0 ablation showed the cross-encoder *helps* once the blending defect
was fixed, so retrieval quality is the ceiling. This spike tests whether a
*biomedical* cross-encoder beats the general-purpose incumbent
`cross-encoder/ms-marco-MiniLM-L-6-v2` on the ICD-10-CM → SNOMED benchmark.

Pre-registered decision rule (see specs/2026-07-25-domain-reranker-decision.md):
    adopt a candidate as the default iff its top-1 beats the incumbent by
    > 0.01 AND its MRR is no worse; otherwise keep the incumbent.

Usage (downloads models; needs a FULL Athena export with ICD10CM + 'Maps to'
relationships — the bundled demo vocab is SNOMED-only and yields 0 gold cases):

    python3 scripts/reranker_spike.py \\
        --athena-dir /path/to/full/athena \\
        --candidates ncbi/MedCPT-Cross-Encoder

Numbers are NOT fabricated here — this runs the real benchmark. Without
--candidates it only prints the decision rule and the shortlist.

Adoption caveat (measured 2026-08-13): a candidate that ships a PyTorch
``.bin`` checkpoint (MedCPT does; it has no safetensors) cannot be loaded by
``transformers`` on torch < 2.6 (CVE-2025-32434 guard). Adopting such a model
as the DEFAULT would break base installs on older torch. Weigh this before
changing ``RerankerConfig.model``.
"""

from __future__ import annotations

import argparse

# Licence-checked shortlist of genuine CROSS-ENCODERS (a reranker scores
# (query, candidate) pairs — a bi-encoder like BioLORD-2023 is an *embedding*
# model and cannot serve here). Confirm each model card + safetensors
# availability before publishing a default change.
DEFAULT_CANDIDATES = [
    "ncbi/MedCPT-Cross-Encoder",  # MedCPT — biomedical query/document cross-encoder
]

INCUMBENT = "cross-encoder/ms-marco-MiniLM-L-6-v2"

TOP1_MARGIN = 0.01  # candidate must beat incumbent top-1 by more than this


def decide(incumbent: dict, candidates: list[dict]) -> dict:
    """Apply the pre-registered adoption rule.

    Args:
        incumbent: {"reranker_model": None/str, "top_1": float, "mrr": float}
        candidates: same shape, one per evaluated model.

    Returns:
        {"adopt": <model or None>, "reason": str, "ranking": [...]}
    """
    ranked = sorted(candidates, key=lambda r: r["top_1"], reverse=True)
    for cand in ranked:
        gain = cand["top_1"] - incumbent["top_1"]
        if gain <= TOP1_MARGIN:
            continue
        if cand["mrr"] < incumbent["mrr"]:
            return {
                "adopt": None,
                "reason": (
                    f"{cand['reranker_model']} top-1 +{gain:.3f} but MRR "
                    f"{cand['mrr']:.3f} < incumbent {incumbent['mrr']:.3f} — keep incumbent"
                ),
                "ranking": ranked,
            }
        return {
            "adopt": cand["reranker_model"],
            "reason": (
                f"{cand['reranker_model']} beats incumbent: top-1 "
                f"{cand['top_1']:.3f} (+{gain:.3f}), MRR {cand['mrr']:.3f} "
                f">= {incumbent['mrr']:.3f}"
            ),
            "ranking": ranked,
        }
    return {
        "adopt": None,
        "reason": f"no candidate beats incumbent top-1 by > {TOP1_MARGIN} — keep incumbent",
        "ranking": ranked,
    }


def _run(args: argparse.Namespace) -> None:
    print(f"Incumbent: {INCUMBENT}")
    print(f"Decision rule: adopt iff top-1 gain > {TOP1_MARGIN} and MRR not worse.\n")

    candidates = args.candidates or DEFAULT_CANDIDATES
    if not args.athena_dir:
        print("Shortlist (no --athena-dir given, not running):")
        for c in candidates:
            print(f"  - {c}")
        print("\nRun with --athena-dir <path> to measure and decide.")
        return

    from portiere.benchmarks.athena_icd_snomed.runner import run_benchmark

    backend = args.backend
    inc = run_benchmark(args.athena_dir, backend=backend)
    incumbent = {"reranker_model": None, "top_1": inc.top_1, "mrr": inc.mrr}
    print(f"incumbent ({backend}): top-1={inc.top_1:.3f} MRR={inc.mrr:.3f}")

    rows = []
    for model in candidates:
        r = run_benchmark(args.athena_dir, backend=backend, reranker_model=model)
        rows.append({"reranker_model": model, "top_1": r.top_1, "mrr": r.mrr})
        print(f"  {model}: top-1={r.top_1:.3f} top-5={r.top_5:.3f} MRR={r.mrr:.3f}")

    d = decide(incumbent, rows)
    print("\nDecision:", d["reason"])
    if d["adopt"]:
        print(f"→ change RerankerConfig.model default to {d['adopt']!r} + CHANGELOG note.")
    else:
        print(f"→ keep incumbent default ({INCUMBENT}); publish the negative result.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--athena-dir", default=None)
    ap.add_argument(
        "--backend",
        default="bm25s",
        help="retrieval backend to hold fixed (default bm25s — the v0.4.0 winner)",
    )
    ap.add_argument(
        "--candidates", nargs="*", default=None, help="HF cross-encoder model ids to evaluate"
    )
    ap.set_defaults()
    _run(ap.parse_args())


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""PHI ratchet scorer. Measures recall/precision of a scrubber backend against
the fixed corpus. Recall may never drop below the recorded baseline.

    python3 tests/fixtures/phi_corpus/score.py regex

Recall is measured over positives the backend is *responsible* for (regex is
not responsible for names/addresses — those are presidio-only). Precision is
measured over the whole corpus (any flag on a must_flag=false case is a false
positive)."""

import json
import sys
from pathlib import Path

CORPUS = Path(__file__).parent / "corpus.jsonl"


def score(backend: str) -> dict:
    from portiere.deid import PHIScrubber

    scrubber = PHIScrubber(backend=backend)  # type: ignore[arg-type]
    cases = [json.loads(line) for line in CORPUS.read_text().splitlines() if line.strip()]

    # Which positive categories is this backend responsible for?
    if backend == "regex":
        responsible = {"regex"}  # structural only
    else:
        responsible = {"regex", "regex_gap", "presidio"}

    tp = fn = fp = tn = 0
    misses, false_pos = [], []
    for c in cases:
        flagged = bool(scrubber.detect([c["text"]]))
        if c["must_flag"]:
            if c["backend"] not in responsible:
                continue  # not this backend's job — excluded from recall
            if flagged:
                tp += 1
            else:
                fn += 1
                misses.append(f"{c['category']}: {c['text']!r}")
        else:
            if flagged:
                fp += 1
                false_pos.append(f"{c['category']}: {c['text']!r}")
            else:
                tn += 1

    recall = tp / (tp + fn) if (tp + fn) else 1.0
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    return {
        "backend": backend, "tp": tp, "fn": fn, "fp": fp, "tn": tn,
        "recall": round(recall, 4), "precision": round(precision, 4),
        "misses": misses, "false_positives": false_pos,
    }


if __name__ == "__main__":
    r = score(sys.argv[1] if len(sys.argv) > 1 else "regex")
    print(json.dumps(r, indent=2))

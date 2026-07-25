"""Tests for the domain-reranker decision rule (scripts/reranker_spike.py)."""

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "reranker_spike",
    Path(__file__).resolve().parents[1] / "scripts" / "reranker_spike.py",
)
reranker_spike = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reranker_spike)

decide = reranker_spike.decide


def _row(model, top_1, mrr):
    return {"reranker_model": model, "top_1": top_1, "mrr": mrr}


def test_adopts_clear_winner():
    incumbent = _row(None, 0.303, 0.402)
    candidates = [_row("MedCPT", 0.320, 0.415)]
    d = decide(incumbent, candidates)
    assert d["adopt"] == "MedCPT"
    assert "beats incumbent" in d["reason"]


def test_keeps_incumbent_on_tie():
    incumbent = _row(None, 0.303, 0.402)
    candidates = [_row("BioLORD", 0.308, 0.404)]  # +0.005 < 0.01 threshold
    d = decide(incumbent, candidates)
    assert d["adopt"] is None


def test_rejects_winner_with_worse_mrr():
    incumbent = _row(None, 0.303, 0.402)
    candidates = [_row("X", 0.320, 0.395)]  # top-1 up but MRR down
    d = decide(incumbent, candidates)
    assert d["adopt"] is None
    assert "MRR" in d["reason"]


def test_picks_best_of_several():
    incumbent = _row(None, 0.303, 0.402)
    candidates = [
        _row("A", 0.315, 0.410),
        _row("B", 0.330, 0.420),
        _row("C", 0.290, 0.400),
    ]
    d = decide(incumbent, candidates)
    assert d["adopt"] == "B"

"""
Reciprocal Rank Fusion — Combines results from multiple retrieval backends.

RRF is a simple yet effective fusion method that doesn't require score
normalization across different retrieval systems.

Reference: Cormack, Clarke & Buettcher (2009) "Reciprocal Rank Fusion
outperforms Condorcet and individual Rank Learning Methods"
"""

from __future__ import annotations


def reciprocal_rank_fusion(
    *result_lists: list[dict],
    k: int = 60,
    score_field: str = "score",
    id_field: str = "concept_id",
) -> list[dict]:
    """
    Combine ranked lists using Reciprocal Rank Fusion.

    RRF score = sum over lists: 1 / (k + rank_i)

    Args:
        *result_lists: Variable number of ranked result lists
        k: RRF constant (default 60, prevents high-ranked items from dominating)
        score_field: Field name for the original score
        id_field: Field name for the unique identifier

    Returns:
        Fused and re-ranked result list with 'rrf_score' field added
    """
    # Filter out empty lists
    non_empty = [rl for rl in result_lists if rl]

    if not non_empty:
        return []

    # Single source: pass through original scores to avoid collapse
    if len(non_empty) == 1:
        results = []
        for item in non_empty[0]:
            merged = item.copy()
            merged["rrf_score"] = merged.get(score_field, 0)
            results.append(merged)
        return results

    # Multi-source: compute RRF scores
    scores: dict[int | str, float] = {}
    items: dict[int | str, dict] = {}

    for result_list in non_empty:
        for rank, item in enumerate(result_list):
            item_id = item.get(id_field)
            if item_id is None:
                continue

            rrf_contribution = 1.0 / (k + rank + 1)  # rank is 0-indexed
            scores[item_id] = scores.get(item_id, 0) + rrf_contribution

            # Keep the version with the highest original score
            if item_id not in items or item.get(score_field, 0) > items[item_id].get(
                score_field, 0
            ):
                items[item_id] = item

    # Build fused results sorted by RRF score
    fused = []
    for item_id, rrf_score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        merged = items[item_id].copy()
        merged["rrf_score"] = round(rrf_score, 6)
        fused.append(merged)

    return fused

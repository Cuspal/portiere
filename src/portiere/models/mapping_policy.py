"""Shared execution policy for models, runners and generated artifacts."""

from __future__ import annotations

from enum import Enum
from typing import Any


class ConceptReviewDecision(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    OVERRIDDEN = "overridden"


def _value(item: Any, name: str, default=None):
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def schema_is_executable(item: Any) -> bool:
    return (
        _value(item, "status") in {"auto_accepted", "approved", "overridden"}
        and bool(_value(item, "override_target_table") or _value(item, "target_table"))
        and bool(_value(item, "override_target_column") or _value(item, "target_column"))
    )


def concept_is_approved(item: Any) -> bool:
    decision = _value(item, "review_decision")
    if decision:
        return decision in {"approved", "overridden"}
    # Legacy AUTO / OVERRIDE remain eligible, without inventing a human review.
    return _value(item, "method") in {"auto", "override"}


def concept_is_executable(item: Any) -> bool:
    concept_id = _value(item, "target_concept_id")
    try:
        valid_target = int(concept_id) > 0 and float(concept_id) == int(concept_id)
    except (TypeError, ValueError, OverflowError):
        valid_target = False
    return concept_is_approved(item) and valid_target

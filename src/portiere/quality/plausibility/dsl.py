"""YAML DSL for plausibility rules.

Five rule types are locked for v0.2.0: ``range``, ``regex``, ``enum``,
``temporal_order``, ``fk_exists``. Adding a sixth is a v0.3.0
conversation. Each rule has a unique ``id`` and a ``severity`` of either
``error`` (fails validation) or ``warn`` (reported but does not fail).

Rules originate from the ``plausibility:`` block of a standard's YAML
file and are parsed via :func:`parse_rule` / :func:`parse_rules`.

Example YAML::

    person:
      plausibility:
        - id: gender_concept_in_gender_domain
          type: fk_exists
          column: gender_concept_id
          ref_table: concept
          ref_column: concept_id
          severity: error
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

Severity = Literal["error", "warn"]


class _Base(BaseModel):
    """Common fields and config for all rule types."""

    model_config = ConfigDict(extra="forbid")
    id: str
    severity: Severity = "error"


class RangeRule(_Base):
    """Numeric range constraint on a column. ``min`` and/or ``max`` may be omitted."""

    type: Literal["range"]
    column: str
    min: float | int | None = None
    max: float | int | None = None


class RegexRule(_Base):
    """String values in ``column`` must match ``pattern`` (Python regex)."""

    type: Literal["regex"]
    column: str
    pattern: str


class EnumRule(_Base):
    """Values in ``column`` must be one of ``values``."""

    type: Literal["enum"]
    column: str
    values: list[str | int | float]


class TemporalOrderRule(_Base):
    """Per-row constraint: ``before`` ≤ ``after`` for any row where both are present."""

    type: Literal["temporal_order"]
    before: str
    after: str


class FkExistsRule(_Base):
    """Every non-null value in ``column`` must exist in ``ref_table.ref_column``."""

    type: Literal["fk_exists"]
    column: str
    ref_table: str
    ref_column: str


PlausibilityRule = RangeRule | RegexRule | EnumRule | TemporalOrderRule | FkExistsRule


_TYPE_MAP: dict[str, type[_Base]] = {
    "range": RangeRule,
    "regex": RegexRule,
    "enum": EnumRule,
    "temporal_order": TemporalOrderRule,
    "fk_exists": FkExistsRule,
}


def parse_rule(data: dict) -> PlausibilityRule:
    """Parse a single rule dict into a typed Pydantic model.

    Raises
    ------
    ValueError
        If ``type`` is not one of the 5 locked types, or if any field
        violates the model (unknown field, invalid severity, etc.).
    """
    rule_type = data.get("type")
    if rule_type not in _TYPE_MAP:
        raise ValueError(
            f"unknown plausibility rule type: {rule_type!r}; must be one of {sorted(_TYPE_MAP)}"
        )
    return _TYPE_MAP[rule_type](**data)


def parse_rules(rules: list[dict]) -> list[PlausibilityRule]:
    """Parse a list of rule dicts. Empty input returns ``[]``."""
    return [parse_rule(r) for r in rules]

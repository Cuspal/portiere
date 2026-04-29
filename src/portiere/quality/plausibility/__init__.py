"""Plausibility validation — Kahn-style cross-table and value checks.

Hybrid grammar:

* **YAML DSL** in standards files declares column-level rules drawn from a
  fixed set of 5 rule types (``range``, ``regex``, ``enum``,
  ``temporal_order``, ``fk_exists``). Custom standards can declare rules
  without writing Python.
* **Python rules** in ``omop.py`` / ``fhir.py`` handle multi-table joins,
  aggregates, and other checks the DSL doesn't express.

The :class:`GXValidator` calls into both paths during post-ETL validation
and produces a plausibility score distinct from completeness and
conformance.

The 5 DSL rule types are **locked for v0.2.0** — extending the grammar is
a v0.3.0 conversation.
"""

from portiere.quality.plausibility.dsl import (
    EnumRule,
    FkExistsRule,
    PlausibilityRule,
    RangeRule,
    RegexRule,
    Severity,
    TemporalOrderRule,
    parse_rule,
    parse_rules,
)
from portiere.quality.plausibility.registry import run_python_rules
from portiere.quality.plausibility.runner import (
    RuleResult,
    run_column_rule,
    run_fk_rule,
)

__all__ = [
    "EnumRule",
    "FkExistsRule",
    "PlausibilityRule",
    "RangeRule",
    "RegexRule",
    "RuleResult",
    "Severity",
    "TemporalOrderRule",
    "parse_rule",
    "parse_rules",
    "run_column_rule",
    "run_fk_rule",
    "run_python_rules",
]

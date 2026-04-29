## Summary
<!-- 1–3 bullets describing what changed and why -->

## Affected slice / scope
<!-- e.g., Slice 3 (plausibility), or "tests-only", or "docs". Reference issue # if applicable. -->

## Test plan
<!-- How was this verified? List commands run and outcomes. -->

## Checklist

- [ ] Tests added / updated and passing locally (`pytest`)
- [ ] `ruff check src/ tests/` clean
- [ ] `mypy src/portiere/` clean
- [ ] CHANGELOG entry added (or "no user-facing change" justified)
- [ ] No PHI / credentials in fixtures or examples
- [ ] If touching standards YAML: ran the entity smoke tests (`pytest tests/test_standards.py`)
- [ ] If touching reproducibility manifest: verified API keys still excluded (no secrets in committed manifests)
- [ ] If touching the public API: docstrings updated

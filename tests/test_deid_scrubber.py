"""Tests for value-level PHI detection and scrubbing (portiere.deid)."""

import pytest

from portiere.deid import PHIFinding, PHIScrubber


@pytest.fixture
def scrubber():
    return PHIScrubber(backend="regex")  # deterministic, no optional deps


class TestDetection:
    def test_detects_email(self, scrubber):
        findings = scrubber.detect(["contact: john.doe@hospital.org"])
        assert any(f.entity_type == "EMAIL" for f in findings)

    def test_detects_phone(self, scrubber):
        findings = scrubber.detect(["call 02-123-4567", "+1 (555) 123-4567"])
        assert sum(1 for f in findings if f.entity_type == "PHONE") == 2

    def test_detects_mrn_like_id(self, scrubber):
        findings = scrubber.detect(["MRN: 1234567", "HN 987654321"])
        assert any(f.entity_type == "MRN" for f in findings)

    def test_detects_national_id(self, scrubber):
        # US SSN shape and TH 13-digit national ID shape
        findings = scrubber.detect(["123-45-6789", "1101700203451"])
        assert any(f.entity_type == "NATIONAL_ID" for f in findings)

    def test_detects_date_of_birth(self, scrubber):
        findings = scrubber.detect(["DOB: 1990-05-14", "14/05/1990"])
        assert any(f.entity_type == "DATE" for f in findings)

    def test_clean_clinical_column_zero_findings(self, scrubber):
        """False-positive guard: clinical codes must NOT be flagged."""
        clean = ["E11.9", "I10", "J06.9", "SNOMED", "8480-6", "mg/dL", "250 mg"]
        assert scrubber.detect(clean) == []

    def test_findings_carry_value_index_and_span(self, scrubber):
        findings = scrubber.detect(["ok", "mail me at a@b.co"])
        assert len(findings) == 1
        f = findings[0]
        assert isinstance(f, PHIFinding)
        assert f.value_index == 1
        assert f.start >= 0 and f.end > f.start


class TestScrubbing:
    def test_redact_strategy(self):
        s = PHIScrubber(backend="regex", strategy="redact")
        out = s.scrub_column(["email a@b.co", "clean"])
        assert out[0] == "email [EMAIL]"
        assert out[1] == "clean"

    def test_hash_strategy_deterministic(self):
        s = PHIScrubber(backend="regex", strategy="hash")
        out1 = s.scrub_column(["email a@b.co"])
        out2 = s.scrub_column(["email a@b.co"])
        assert out1 == out2  # same input -> same hash
        assert "a@b.co" not in out1[0]

    def test_surrogate_strategy_preserves_shape(self):
        s = PHIScrubber(backend="regex", strategy="surrogate")
        out = s.scrub_column(["MRN: 1234567"])
        assert "1234567" not in out[0]
        assert "MRN" in out[0]  # label kept, value replaced

    def test_none_values_pass_through(self):
        s = PHIScrubber(backend="regex")
        assert s.scrub_column([None, "a@b.co"])[0] is None


class TestBackendSelection:
    def test_unknown_backend_raises(self):
        with pytest.raises(ValueError, match="backend"):
            PHIScrubber(backend="nope")

    def test_presidio_backend_requires_extra(self):
        pytest.importorskip("presidio_analyzer", reason="phi extra not installed")
        s = PHIScrubber(backend="presidio")
        findings = s.detect(["Patient John Smith visited"])
        assert any(f.entity_type == "PERSON" for f in findings)

    def test_presidio_missing_gives_helpful_error(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name.startswith("presidio"):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(ImportError, match=r"portiere-health\[phi\]"):
            PHIScrubber(backend="presidio")


class TestStage1Wiring:
    def test_ingest_scrubs_profile_value_fields(self, tmp_path):
        import polars as pl

        from portiere.engines import get_engine
        from portiere.stages.stage1_ingest import ingest_source

        src = tmp_path / "patients.csv"
        pl.DataFrame(
            {"contact": ["a@b.co", "c@d.org", "e@f.net"], "code": ["E11.9", "I10", "I10"]}
        ).write_csv(src)

        result = ingest_source(get_engine("polars"), str(src), scrub_phi=True)
        contact = next(c for c in result["columns"] if c["name"] == "contact")
        joined = str(contact)
        assert "a@b.co" not in joined  # example / top_values scrubbed
        assert "[EMAIL]" in joined
        # clinical codes untouched
        code = next(c for c in result["columns"] if c["name"] == "code")
        assert code["example"] in ("E11.9", "I10")

    def test_ingest_default_off_leaves_values(self, tmp_path):
        import polars as pl

        from portiere.engines import get_engine
        from portiere.stages.stage1_ingest import ingest_source

        src = tmp_path / "patients.csv"
        pl.DataFrame({"contact": ["a@b.co", "x@y.io", "z@w.co"]}).write_csv(src)
        result = ingest_source(get_engine("polars"), str(src))
        contact = result["columns"][0]
        assert contact["example"] == "a@b.co"  # default off in v0.4.0


class TestReviewRegressions:
    """Fixes for the v0.4.0 adversarial-review findings."""

    def test_surrogate_not_trivially_reversible(self):
        """Old surrogate was rot-3 — subtracting 3 recovered the PHI."""
        s = PHIScrubber(backend="regex", strategy="surrogate")
        out = s.scrub_column(["MRN: 1234567"])[0]
        digits = "".join(ch for ch in out if ch.isdigit())
        unrot3 = "".join(str((int(d) - 3) % 10) for d in digits)
        assert unrot3 != "1234567"  # rot-3 undo must NOT recover the value
        assert "1234567" not in out

    def test_surrogate_deterministic_within_instance(self):
        s = PHIScrubber(backend="regex", strategy="surrogate")
        assert s.scrub_column(["MRN: 1234567"]) == s.scrub_column(["MRN: 1234567"])

    def test_hash_salted_per_instance(self):
        """Unsalted SHA-256 of low-entropy PHI (DOBs, SSNs) is enumerable."""
        a = PHIScrubber(backend="regex", strategy="hash")
        b = PHIScrubber(backend="regex", strategy="hash")
        va = a.scrub_column(["DOB: 1985-03-02"])[0]
        vb = b.scrub_column(["DOB: 1985-03-02"])[0]
        assert va != vb  # different instances -> different salts
        import hashlib

        plain = hashlib.sha256(b"1985-03-02").hexdigest()[:10]
        assert plain not in va  # not the unsalted digest

    def test_entities_filter_does_not_leak_suppressed_spans(self):
        """A disallowed higher-priority recogniser must not claim (and then
        drop) a span an allowed recogniser would have scrubbed."""
        s = PHIScrubber(backend="regex", strategy="redact", entities=["MRN"])
        out = s.scrub_column(["MRN: 1234567890123"])[0]  # 13-digit value
        assert "1234567890123" not in out

    def test_phone_e164_detected(self):
        s = PHIScrubber(backend="regex")
        findings = s.detect(["+15551234567", "+66812345678"])
        assert sum(1 for f in findings if f.entity_type == "PHONE") == 2

    def test_common_date_shapes_detected(self):
        s = PHIScrubber(backend="regex")
        findings = s.detect(["1/2/1985", "01-15-1985", "2024/01/15"])
        assert sum(1 for f in findings if f.entity_type == "DATE") == 3

    def test_clean_values_still_clean_after_widening(self):
        s = PHIScrubber(backend="regex")
        clean = ["E11.9", "I10", "J06.9", "SNOMED", "8480-6", "mg/dL", "250 mg"]
        assert s.detect(clean) == []

    def test_stage1_scrub_preserves_numeric_types_when_clean(self, tmp_path):
        import polars as pl

        from portiere.engines import get_engine
        from portiere.stages.stage1_ingest import ingest_source

        src = tmp_path / "visits.csv"
        pl.DataFrame({"visits": [3, 3, 5]}).write_csv(src)
        result = ingest_source(get_engine("polars"), str(src), scrub_phi=True)
        col = result["columns"][0]
        tv = (col.get("top_values") or [{}])[0]
        # a clean numeric top value must not be coerced to str
        assert not isinstance(tv.get("visits"), str)

"""
Value-level PHI detection and scrubbing.

Two detection backends:

- ``regex`` — built-in structural recognisers (email, phone, MRN-style IDs,
  national IDs, dates). Zero optional dependencies, deterministic, suitable for
  the base install.
- ``presidio`` — Microsoft Presidio NER (adds PERSON names, locations, ...).
  Requires the ``phi`` extra: ``pip install "portiere-health[phi]"``.

``backend="auto"`` uses Presidio when importable and falls back to regex.

This detects PHI in *values*, unlike Stage 1's ``_detect_phi_columns`` which
only pattern-matches column names (that heuristic remains as a fast pre-filter).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

Strategy = Literal["redact", "hash", "surrogate"]


@dataclass
class PHIFinding:
    """One detected PHI span inside one value of a column."""

    entity_type: str
    value_index: int
    start: int
    end: int
    text: str
    score: float = 1.0


# Built-in recognisers, in priority order (earlier wins on span overlap).
# Each is (entity_type, compiled pattern, value_group) where value_group is the
# match group whose span is reported (0 = whole match; 1 = keep label, flag value).
_RECOGNIZERS: list[tuple[str, re.Pattern[str], int]] = [
    (
        "EMAIL",
        re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
        0,
    ),
    (
        # US SSN shape and 13-digit national-ID shape (e.g. Thai ID).
        "NATIONAL_ID",
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b|\b\d{13}\b"),
        0,
    ),
    (
        # Labeled medical-record / hospital numbers: "MRN: 1234567", "HN 987654321".
        # Only the numeric value (group 1) is the finding, so scrubbing keeps the label.
        "MRN",
        re.compile(r"(?i)\b(?:MRN|HN|PID)\s*[:#]?\s*(\d{5,})\b"),
        1,
    ),
    (
        # DOB-style dates: ISO (Y-M-D), slash ISO (Y/M/D), slash US (M/D/YYYY,
        # unpadded allowed — the most common hand-entered CSV shape), dashed US.
        "DATE",
        re.compile(
            r"\b\d{4}-\d{2}-\d{2}\b|\b\d{4}/\d{2}/\d{2}\b"
            r"|\b\d{1,2}/\d{1,2}/\d{4}\b|\b\d{2}-\d{2}-\d{4}\b"
        ),
        0,
    ),
    (
        # Phone numbers: E.164 (+ and 8-15 digits, no separators needed) or
        # optional country code with separator-structured digit groups. A
        # minimum-total-digits check below rejects short codes like LOINC "8480-6".
        "PHONE",
        re.compile(
            r"(?<!\d)\+\d{8,15}(?!\d)"
            r"|(?<!\d)(?:\+\d{1,3}[\s.-]?)?(?:\(\d{2,4}\)[\s.-]?)?"
            r"\d{2,4}[\s.-]\d{3,4}(?:[\s.-]\d{3,4})?(?!\d)"
        ),
        0,
    ),
]

_PHONE_MIN_DIGITS = 7


def _regex_detect_value(
    value: str, value_index: int, allowed: set[str] | None = None
) -> list[PHIFinding]:
    """Run recognisers over one value with priority overlap suppression.

    ``allowed`` filters recognisers BEFORE suppression — a disallowed
    higher-priority recogniser must not claim (and then drop) a span that an
    allowed lower-priority recogniser would have scrubbed.
    """
    findings: list[PHIFinding] = []
    taken: list[tuple[int, int]] = []

    def overlaps(start: int, end: int) -> bool:
        return any(start < e and end > s for s, e in taken)

    for entity_type, pattern, group in _RECOGNIZERS:
        if allowed is not None and entity_type not in allowed:
            continue
        for m in pattern.finditer(value):
            start, end = m.span(group)
            text = value[start:end]
            if entity_type == "PHONE" and sum(c.isdigit() for c in text) < _PHONE_MIN_DIGITS:
                continue
            if overlaps(start, end):
                continue  # a higher-priority recogniser already claimed this span
            taken.append((start, end))
            findings.append(
                PHIFinding(
                    entity_type=entity_type,
                    value_index=value_index,
                    start=start,
                    end=end,
                    text=text,
                )
            )
    findings.sort(key=lambda f: f.start)
    return findings


def _surrogate(text: str, salt: bytes) -> str:
    """Keyed shape-preserving replacement.

    Per-character substitution offsets are derived from
    ``SHA-256(salt || text)``, so the output preserves the value's shape
    (digits stay digits, letters keep case), is deterministic for the same
    scrubber instance (joins survive), and is NOT invertible without the salt —
    unlike a fixed rotation, which anyone could undo.
    """
    digest = hashlib.sha256(salt + text.encode("utf-8")).digest()
    out = []
    for i, ch in enumerate(text):
        k = digest[i % len(digest)]
        if ch.isdigit():
            out.append(str((int(ch) + k) % 10))
        elif ch.isalpha():
            base = ord("A") if ch.isupper() else ord("a")
            out.append(chr(base + ((ord(ch) - base + k) % 26)))
        else:
            out.append(ch)
    return "".join(out)


class PHIScrubber:
    """Detect and scrub PHI in column values.

    Args:
        backend: ``"regex"`` (built-in, default-safe), ``"presidio"`` (requires
            the ``phi`` extra), or ``"auto"`` (Presidio when available, else regex).
        strategy: What replaces a detected span — ``"redact"`` (``[TYPE]``
            marker), ``"hash"`` (salted SHA-256 token — deterministic within
            this scrubber instance so joins survive; salted so low-entropy PHI
            like DOBs/SSNs cannot be recovered by offline enumeration), or
            ``"surrogate"`` (shape-preserving keyed substitution — not
            invertible without this instance's salt).
        entities: Optional allow-list of entity types to act on (default: all).
        salt: Optional salt for the hash/surrogate strategies. Provide one to
            make tokens stable ACROSS scrubber instances/runs (e.g. incremental
            pipelines); omit for a random per-instance salt (default, safest).
    """

    def __init__(
        self,
        backend: Literal["auto", "regex", "presidio"] = "auto",
        strategy: Strategy = "redact",
        entities: list[str] | None = None,
        salt: bytes | str | None = None,
    ) -> None:
        if backend not in ("auto", "regex", "presidio"):
            raise ValueError(f"Unknown backend: {backend!r} (use auto|regex|presidio)")
        if strategy not in ("redact", "hash", "surrogate"):
            raise ValueError(f"Unknown strategy: {strategy!r}")
        self.strategy: Strategy = strategy
        self.entities = set(entities) if entities else None
        if salt is None:
            import secrets

            self._salt = secrets.token_bytes(16)
        else:
            self._salt = salt.encode("utf-8") if isinstance(salt, str) else bytes(salt)

        if backend == "auto":
            try:
                import presidio_analyzer  # noqa: F401

                backend = "presidio"
            except ImportError:
                backend = "regex"
        if backend == "presidio":
            self._analyzer = self._build_presidio_analyzer()
        else:
            self._analyzer = None
        self.backend = backend

    @staticmethod
    def _build_presidio_analyzer():
        try:
            from presidio_analyzer import AnalyzerEngine
        except ImportError as exc:
            raise ImportError(
                "The presidio backend requires the phi extra. "
                'Install it with: pip install "portiere-health[phi]"'
            ) from exc
        return AnalyzerEngine()

    # Presidio entity names → our compact vocabulary. Identifier-like types are
    # folded into NATIONAL_ID so an ``entities=["NATIONAL_ID", ...]`` allow-list
    # does not silently drop passports/licences/etc.
    _PRESIDIO_MAP = {
        "PERSON": "PERSON",
        "EMAIL_ADDRESS": "EMAIL",
        "PHONE_NUMBER": "PHONE",
        "US_SSN": "NATIONAL_ID",
        "US_PASSPORT": "NATIONAL_ID",
        "US_DRIVER_LICENSE": "NATIONAL_ID",
        "US_ITIN": "NATIONAL_ID",
        "UK_NHS": "NATIONAL_ID",
        "AU_MEDICARE": "NATIONAL_ID",
        "IN_AADHAAR": "NATIONAL_ID",
        "SG_NRIC_FIN": "NATIONAL_ID",
        "DATE_TIME": "DATE",
        "LOCATION": "LOCATION",
        "MEDICAL_LICENSE": "MRN",
        "CREDIT_CARD": "CREDIT_CARD",
        "IBAN_CODE": "FINANCIAL",
        "US_BANK_NUMBER": "FINANCIAL",
        "IP_ADDRESS": "IP_ADDRESS",
        "URL": "URL",
    }

    def detect(self, values: list[str | None]) -> list[PHIFinding]:
        """Detect PHI across a column of values.

        Returns findings ordered by (value_index, start). ``None`` values are
        skipped.
        """
        findings: list[PHIFinding] = []
        for idx, value in enumerate(values):
            if value is None:
                continue
            text = str(value)
            if self.backend == "presidio":
                for r in self._analyzer.analyze(text=text, language="en"):
                    entity = self._PRESIDIO_MAP.get(r.entity_type, r.entity_type)
                    findings.append(
                        PHIFinding(
                            entity_type=entity,
                            value_index=idx,
                            start=r.start,
                            end=r.end,
                            text=text[r.start : r.end],
                            score=float(r.score),
                        )
                    )
            else:
                # entities pre-filters the recognisers themselves so a
                # disallowed higher-priority recogniser cannot claim (and drop)
                # a span an allowed one would have scrubbed.
                findings.extend(_regex_detect_value(text, idx, allowed=self.entities))
        if self.backend == "presidio" and self.entities is not None:
            findings = [f for f in findings if f.entity_type in self.entities]
        return findings

    def _replacement(self, finding: PHIFinding) -> str:
        if self.strategy == "redact":
            return f"[{finding.entity_type}]"
        if self.strategy == "hash":
            digest = hashlib.sha256(self._salt + finding.text.encode("utf-8")).hexdigest()[:10]
            return f"<{finding.entity_type}:{digest}>"
        return _surrogate(finding.text, self._salt)

    def scrub_column(self, values: list[str | None]) -> list[str | None]:
        """Return the column with every detected PHI span replaced.

        ``None`` values pass through unchanged. Replacement is span-exact, so
        surrounding text (e.g. an ``MRN:`` label) is preserved.
        """
        findings = self.detect(values)
        by_index: dict[int, list[PHIFinding]] = {}
        for f in findings:
            by_index.setdefault(f.value_index, []).append(f)

        out: list[str | None] = []
        for idx, value in enumerate(values):
            if value is None or idx not in by_index:
                out.append(value)
                continue
            text = str(value)
            # Replace right-to-left so earlier spans stay valid.
            for f in sorted(by_index[idx], key=lambda f: f.start, reverse=True):
                text = text[: f.start] + self._replacement(f) + text[f.end :]
            out.append(text)
        return out

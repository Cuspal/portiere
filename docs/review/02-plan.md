# Triage — Privacy pass

Rank = (severity × blast radius) ÷ effort. Order: S1 → S2 → S3.

No S1 findings — the `offline=True` guarantee holds (verified: remote LLM +
remote embedding rejected at config construction, re-asserted at runtime).

| Rank | ID | Sev | Decision | Why |
|------|----|-----|----------|-----|
| 1 | F-004 | S2 | **FIX (applied)** | Silent under-scrubbing awareness; logging-only, ratchet-neutral, tiny blast radius. Safe to fix under review. |
| 2 | F-001 | S3 | **PARK** | Egress-boundary behaviour change; two defensible designs; gated by offline. |
| 3 | F-002 | S3 | **PARK** | Same class as F-001; risk of over-redacting clinical terms. |
| 4 | F-003 | S3 | **PARK** | Scrubber recogniser change (explicit Park List item); needs precision proof first. |

Everything below the Privacy subsystem (CO/PL/MP/KN/ST/QR/SF/XC — 42 subsystems)
is **not audited this session**. This is a clean partial run stopped at the
Privacy boundary per the command's context-management guidance; `state.json`
carries the registry forward for a resume.

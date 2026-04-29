# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Portiere, please report it privately. Do **not** open a public GitHub issue.

**Preferred:** email **security@cuspal.co** with a description of the issue, reproduction steps, and any proof-of-concept code.

**Alternative:** GitHub's [private vulnerability reporting](https://github.com/Cuspal/portiere/security/advisories/new) — useful if you want the disclosure tracked alongside the repo's Security tab.

We aim to acknowledge reports within **3 business days** and to provide a remediation timeline within **10 business days**. Please give us a reasonable time window to address the issue before public disclosure.

## Supported Versions

Only the latest minor release of `portiere-health` receives security patches. Older minor versions are not patched; users are encouraged to upgrade promptly.

| Version | Supported          |
|---------|--------------------|
| 0.2.x   | ✅                 |
| 0.1.x   | ❌ (please upgrade) |

## Scope

**In scope:**

- Vulnerabilities in the `portiere-health` Python package
- Insecure handling of credentials, PHI, or vocabulary data
- Reproducibility manifest forgery / tamper paths
- Dependency CVEs that affect Portiere's installed footprint

**Out of scope:**

- Vulnerabilities in third-party LLM providers, vector databases, or other external services
- Issues in user-supplied custom standards or YAML files
- Best-practice deviations that do not constitute a security boundary violation

## What to expect after reporting

1. **Acknowledgement** within 3 business days.
2. **Triage** — we'll assess severity (CVSS-aligned) and confirm reproduction.
3. **Fix development** — coordinated with you on timeline; private patch first, then disclosure.
4. **Release** — fix shipped in a patch release (e.g., 0.2.1). You'll be credited in the release notes unless you prefer anonymity.
5. **CVE assignment** — for high-severity issues, we'll request a CVE via GitHub's CVE Numbering Authority.

Thank you for helping keep Portiere and the health-data community safe.

"""Streamlit-based Mapping Review UI (v0.3.1+).

The UI is launched via ``portiere review <project-dir>``. It reads mappings
from the project's storage layout, lets the user approve / override /
reject each item, and persists decisions to a ``*_reviewed.json`` sibling
file. Originals are never modified.

Module surface:

* :mod:`portiere.review_ui.state` — pure load/save/decision helpers
  (no Streamlit imports). Independently testable.
* :mod:`portiere.review_ui.app` — Streamlit entrypoint.
* :mod:`portiere.review_ui.pages` — per-mapping-type review pages.
"""

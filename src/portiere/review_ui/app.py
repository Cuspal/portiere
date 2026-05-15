"""Streamlit entrypoint for ``portiere review``.

Launched as a subprocess by :mod:`portiere.cli.review`. The project
directory is passed via ``--`` arg pass-through:

    streamlit run app.py -- --project-dir /path/to/project

Streamlit pages live under :mod:`portiere.review_ui.pages`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _parse_streamlit_args() -> Path:
    """Pluck the --project-dir argument from sys.argv after Streamlit's --."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-dir", required=True)
    # Streamlit forwards everything after `--` into sys.argv[1:].
    args, _unknown = parser.parse_known_args(sys.argv[1:])
    return Path(args.project_dir).resolve()


def main() -> None:
    import streamlit as st

    project_dir = _parse_streamlit_args()

    st.set_page_config(page_title="Portiere Mapping Review", layout="wide")
    st.title("Mapping Review")
    st.caption(f"Project: `{project_dir}`")

    page = st.sidebar.radio(
        "Mapping type",
        options=["Schema Mapping", "Concept Mapping"],
        index=0,
    )

    if page == "Schema Mapping":
        from portiere.review_ui.pages.schema_review import render_schema_review

        render_schema_review(project_dir)
    elif page == "Concept Mapping":
        from portiere.review_ui.pages.concept_review import render_concept_review

        render_concept_review(project_dir)


if __name__ == "__main__":
    main()

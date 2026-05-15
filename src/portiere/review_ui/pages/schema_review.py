"""Schema-mapping review page.

Renders the project's schema mappings as a table with per-row approve /
override / reject actions. Decisions are persisted to
``schema_mapping_reviewed.json`` via :mod:`portiere.review_ui.state`.
"""

from __future__ import annotations

from pathlib import Path

from portiere.review_ui.state import (
    apply_user_decision,
    load_schema_mapping,
    save_reviewed_schema_mapping,
)


def render_schema_review(project_dir: Path) -> None:
    import streamlit as st

    mapping = load_schema_mapping(project_dir)
    if not mapping.items:
        st.info(
            "No schema mapping found in this project. "
            "Run `project.map_schema(source)` first, then re-launch review."
        )
        return

    st.subheader(f"{len(mapping.items)} mappings")

    # Filter by status (sidebar)
    statuses = sorted({item.status.value for item in mapping.items})
    selected_statuses = st.sidebar.multiselect(
        "Filter by status", options=statuses, default=statuses
    )

    filtered_indices = [
        i for i, item in enumerate(mapping.items) if item.status.value in selected_statuses
    ]

    for i in filtered_indices:
        item = mapping.items[i]
        with st.expander(
            f"[{item.status.value:<14}] "
            f"{item.source_table}.{item.source_column}  →  "
            f"{item.effective_target_table or '∅'}.{item.effective_target_column or '∅'}  "
            f"(conf={item.confidence:.2f})",
            expanded=item.status.value == "needs_review",
        ):
            cols = st.columns([1, 1, 1, 2])
            if cols[0].button("Approve", key=f"approve_{i}"):
                mapping = apply_user_decision(mapping, index=i, decision="approve")
                save_reviewed_schema_mapping(mapping, project_dir)
                st.rerun()
            if cols[1].button("Reject", key=f"reject_{i}"):
                mapping = apply_user_decision(mapping, index=i, decision="reject")
                save_reviewed_schema_mapping(mapping, project_dir)
                st.rerun()
            override_target = cols[3].text_input(
                "Override target (table.column)",
                value="",
                key=f"override_input_{i}",
                placeholder="person.year_of_birth",
            )
            if cols[2].button("Override", key=f"override_{i}"):
                if "." in override_target:
                    table, column = override_target.split(".", 1)
                    mapping = apply_user_decision(
                        mapping,
                        index=i,
                        decision="override",
                        target_table=table.strip(),
                        target_column=column.strip(),
                    )
                    save_reviewed_schema_mapping(mapping, project_dir)
                    st.rerun()
                else:
                    st.warning("Override target must be `table.column`")

            if item.candidates:
                st.caption("Top candidates:")
                st.table(item.candidates[:5])

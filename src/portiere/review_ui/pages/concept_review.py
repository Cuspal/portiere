"""Concept-mapping review page.

Renders the project's concept mappings sorted by confidence ascending
(lowest-confidence first, so reviewer attention goes where it matters).
Each row offers approve / reject / override-from-candidate /
override-free-form, with an optional reviewer note.

Decisions are persisted to ``concept_mapping_reviewed.json`` via
:mod:`portiere.review_ui.state`. Originals never modified.
"""

from __future__ import annotations

from pathlib import Path

from portiere.review_ui.state import (
    apply_concept_decision,
    load_concept_mapping,
    save_reviewed_concept_mapping,
    sort_by_confidence_ascending,
)


def render_concept_review(project_dir: Path) -> None:
    import streamlit as st

    mapping = load_concept_mapping(project_dir)
    if not mapping.items:
        st.info(
            "No concept mapping found in this project. "
            "Run `project.map_concepts(codes=...)` first, then re-launch review."
        )
        return

    st.subheader(f"{len(mapping.items)} mappings")

    # Status filter (sidebar)
    methods = sorted({item.method.value for item in mapping.items})
    selected_methods = st.sidebar.multiselect("Filter by method", options=methods, default=methods)

    # Sort: default lowest-confidence first
    sort_order = st.sidebar.radio(
        "Sort by",
        options=["Confidence ↑ (lowest first)", "Confidence ↓ (highest first)"],
        index=0,
    )
    indices = sort_by_confidence_ascending(mapping)
    if sort_order.startswith("Confidence ↓"):
        indices = list(reversed(indices))

    filtered = [i for i in indices if mapping.items[i].method.value in selected_methods]

    for i in filtered:
        item = mapping.items[i]
        target_label = (
            f"{item.target_concept_id} ({item.target_concept_name})"
            if item.target_concept_id
            else "∅ unmapped"
        )
        with st.expander(
            f"[{item.method.value:<8}] "
            f"{item.source_code}  →  {target_label}  "
            f"(conf={item.confidence:.2f}, count={item.source_count})",
            expanded=item.method.value == "review",
        ):
            if item.source_description:
                st.caption(f"Source description: {item.source_description}")

            cols = st.columns([1, 1, 2])

            if cols[0].button("Approve", key=f"c_approve_{i}"):
                mapping = apply_concept_decision(mapping, index=i, decision="approve")
                save_reviewed_concept_mapping(mapping, project_dir)
                st.rerun()

            if cols[1].button("Reject (unmapped)", key=f"c_reject_{i}"):
                mapping = apply_concept_decision(mapping, index=i, decision="reject")
                save_reviewed_concept_mapping(mapping, project_dir)
                st.rerun()

            # Override section
            st.markdown("**Override**")
            if item.candidates:
                cand_labels = [
                    f"#{ci}: {c.concept_id} — {c.concept_name} (score={c.score:.2f})"
                    for ci, c in enumerate(item.candidates[:10])
                ]
                pick = st.selectbox(
                    "Pick a candidate",
                    options=["(none)", *cand_labels],
                    key=f"c_pick_{i}",
                )
                note = st.text_input("Reviewer note (optional)", key=f"c_note_{i}")
                if st.button("Override with picked candidate", key=f"c_apply_pick_{i}"):
                    if pick != "(none)":
                        ci = int(pick.split(":", 1)[0].lstrip("#"))
                        mapping = apply_concept_decision(
                            mapping,
                            index=i,
                            decision="override",
                            candidate_index=ci,
                            reviewer_note=note or None,
                        )
                        save_reviewed_concept_mapping(mapping, project_dir)
                        st.rerun()

            free_id = st.text_input(
                "Or enter a concept_id directly", key=f"c_free_id_{i}", placeholder="e.g. 4170143"
            )
            free_name = st.text_input(
                "Concept name (optional)", key=f"c_free_name_{i}", placeholder="Glucose intolerance"
            )
            free_note = st.text_input(
                "Reviewer note (optional)",
                key=f"c_free_note_{i}",
            )
            if st.button("Override with free-form concept_id", key=f"c_apply_free_{i}"):
                if free_id.strip().isdigit():
                    mapping = apply_concept_decision(
                        mapping,
                        index=i,
                        decision="override",
                        target_concept_id=int(free_id),
                        target_concept_name=free_name or None,
                        reviewer_note=free_note or None,
                    )
                    save_reviewed_concept_mapping(mapping, project_dir)
                    st.rerun()
                else:
                    st.warning("concept_id must be an integer")

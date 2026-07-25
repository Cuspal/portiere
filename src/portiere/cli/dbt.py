"""portiere dbt — generate a dbt project from a Portiere mapping."""

from __future__ import annotations

import click


@click.command(name="dbt")
@click.option(
    "--schema-mapping",
    "schema_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="schema_mapping_reviewed.json (or schema_mapping.yaml).",
)
@click.option(
    "--concepts",
    "concepts_path",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Optional concept mapping JSON ({'items': [...]}) → dbt seed.",
)
@click.option("-o", "--out", "out_dir", required=True, help="Output dbt project dir.")
@click.option("--project-name", default="portiere_dbt", show_default=True)
@click.option(
    "--source-schema",
    default="raw",
    show_default=True,
    help="dbt source schema the models read from.",
)
@click.option("--standard", default="omop_cdm_v5.4", show_default=True)
def dbt_command(
    schema_path: str,
    concepts_path: str | None,
    out_dir: str,
    project_name: str,
    source_schema: str,
    standard: str,
) -> None:
    """Generate a runnable dbt project (models + schema.yml + seed) from mappings."""
    from portiere.cli.workbook import _load_items_payload
    from portiere.integrations.dbt import build_dbt_project
    from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem
    from portiere.models.schema_mapping import SchemaMapping, SchemaMappingItem

    schema_mapping = SchemaMapping(
        items=[SchemaMappingItem(**it) for it in _load_items_payload(schema_path)]
    )
    concept_mapping = None
    if concepts_path:
        concept_mapping = ConceptMapping(
            items=[ConceptMappingItem(**it) for it in _load_items_payload(concepts_path)]
        )

    out = build_dbt_project(
        schema_mapping,
        concept_mapping,
        out_dir,
        project_name=project_name,
        source_schema=source_schema,
        standard=standard,
    )
    click.echo(f"wrote dbt project: {out}")
    click.echo("next: cd into it, configure a profile, then `dbt seed && dbt run && dbt test`")

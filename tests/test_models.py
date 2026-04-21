"""
Tests for SDK models.
"""

from portiere.models.concept_mapping import ConceptMapping, ConceptMappingItem, ConceptMappingMethod
from portiere.models.project import Project
from portiere.models.schema_mapping import MappingStatus, SchemaMapping, SchemaMappingItem
from portiere.models.source import Source, SourceProfile


class TestProject:
    """Tests for Project model."""

    def test_project_creation(self):
        """Project can be created with required fields."""
        project = Project(
            id="proj_123",
            name="Test Project",
            target_model="omop_cdm_v5.4",
        )

        assert project.id == "proj_123"
        assert project.name == "Test Project"
        assert project.target_model == "omop_cdm_v5.4"

    def test_project_default_vocabularies(self):
        """Project has default vocabularies."""
        project = Project(
            id="proj_123",
            name="Test Project",
        )

        assert isinstance(project.vocabularies, list)

    def test_project_with_custom_vocabularies(self):
        """Project can have custom vocabularies."""
        project = Project(
            id="proj_123",
            name="Test Project",
            vocabularies=["SNOMED", "LOINC"],
        )

        assert "SNOMED" in project.vocabularies
        assert "LOINC" in project.vocabularies


class TestSource:
    """Tests for Source model."""

    def test_source_creation(self):
        """Source can be created with required fields."""
        source = Source(
            id="src_123",
            name="diagnoses",
            path="/data/diagnoses.csv",
        )

        assert source.id == "src_123"
        assert source.name == "diagnoses"
        assert source.path == "/data/diagnoses.csv"
        assert source.format == "csv"  # Default

    def test_source_with_format(self):
        """Source can specify format."""
        source = Source(
            id="src_123",
            name="diagnoses",
            path="/data/diagnoses.parquet",
            format="parquet",
        )

        assert source.format == "parquet"


class TestSourceProfile:
    """Tests for SourceProfile model."""

    def test_source_profile_creation(self):
        """SourceProfile can be created."""
        profile = SourceProfile(
            row_count=1000,
            column_count=5,
            columns=[
                {"name": "id", "type": "int", "nullable": False},
                {"name": "code", "type": "str", "nullable": True},
            ],
        )

        assert profile.row_count == 1000
        assert profile.column_count == 5
        assert len(profile.columns) == 2


class TestSchemaMapping:
    """Tests for SchemaMapping model."""

    def test_schema_mapping_creation(self):
        """SchemaMapping can be created."""
        mapping = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="patient_id",
                    target_table="person",
                    target_column="person_id",
                    confidence=0.95,
                ),
            ],
        )

        assert len(mapping.items) == 1

    def test_needs_review(self):
        """Needs review returns items with NEEDS_REVIEW status."""
        mapping = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="col1",
                    target_table="t1",
                    target_column="c1",
                    confidence=0.95,
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
                SchemaMappingItem(
                    source_column="col2",
                    target_table="t2",
                    target_column="c2",
                    confidence=0.80,
                    status=MappingStatus.NEEDS_REVIEW,
                ),
            ],
        )

        review_items = mapping.needs_review()
        assert len(review_items) == 1
        assert review_items[0].source_column == "col2"

    def test_summary(self):
        """Summary returns correct counts."""
        mapping = SchemaMapping(
            items=[
                SchemaMappingItem(
                    source_column="col1",
                    target_table="t1",
                    target_column="c1",
                    confidence=0.95,
                    status=MappingStatus.AUTO_ACCEPTED,
                ),
                SchemaMappingItem(
                    source_column="col2",
                    target_table="t2",
                    target_column="c2",
                    confidence=0.80,
                    status=MappingStatus.NEEDS_REVIEW,
                ),
                SchemaMappingItem(
                    source_column="col3",
                    target_table="t3",
                    target_column="c3",
                    confidence=0.50,
                    status=MappingStatus.UNMAPPED,
                ),
            ],
        )

        summary = mapping.summary()
        assert summary["total"] == 3
        assert summary["auto_accepted"] == 1
        assert summary["needs_review"] == 1
        assert summary["unmapped"] == 1


class TestConceptMapping:
    """Tests for ConceptMapping model."""

    def test_concept_mapping_creation(self):
        """ConceptMapping can be created."""
        mapping = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="A01",
                    source_description="Test Code",
                    target_concept_id=12345,
                    target_concept_name="Test Concept",
                    confidence=0.95,
                    method=ConceptMappingMethod.AUTO,
                ),
            ],
        )

        assert len(mapping.items) == 1

    def test_needs_review(self):
        """Needs review returns items with REVIEW method."""
        mapping = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="A01",
                    confidence=0.95,
                    method=ConceptMappingMethod.AUTO,
                ),
                ConceptMappingItem(
                    source_code="B02",
                    confidence=0.80,
                    method=ConceptMappingMethod.REVIEW,
                ),
            ],
        )

        review_items = mapping.needs_review()
        assert len(review_items) == 1
        assert review_items[0].source_code == "B02"

    def test_auto_mapped(self):
        """Auto mapped returns items with AUTO method."""
        mapping = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="A01",
                    confidence=0.95,
                    method=ConceptMappingMethod.AUTO,
                ),
                ConceptMappingItem(
                    source_code="B02",
                    confidence=0.80,
                    method=ConceptMappingMethod.REVIEW,
                ),
            ],
        )

        auto_items = mapping.auto_mapped()
        assert len(auto_items) == 1
        assert auto_items[0].source_code == "A01"

    def test_summary(self):
        """Summary returns correct counts."""
        mapping = ConceptMapping(
            items=[
                ConceptMappingItem(
                    source_code="A01", confidence=0.95, method=ConceptMappingMethod.AUTO
                ),
                ConceptMappingItem(
                    source_code="B02", confidence=0.80, method=ConceptMappingMethod.REVIEW
                ),
                ConceptMappingItem(
                    source_code="C03", confidence=0.50, method=ConceptMappingMethod.MANUAL
                ),
            ],
        )

        summary = mapping.summary()
        assert summary["total"] == 3
        assert summary["auto_mapped"] == 1
        assert summary["needs_review"] == 1
        assert summary["manual_required"] == 1


class TestConceptMappingItem:
    """Tests for ConceptMappingItem actions."""

    def test_approve_item(self):
        """Item can be approved."""
        item = ConceptMappingItem(
            source_code="A01",
            target_concept_id=12345,
            confidence=0.80,
            method=ConceptMappingMethod.REVIEW,
        )

        item.approve()

        assert item.approved is True

    def test_reject_item(self):
        """Item can be rejected."""
        item = ConceptMappingItem(
            source_code="A01",
            target_concept_id=12345,
            confidence=0.80,
            method=ConceptMappingMethod.REVIEW,
        )

        item.reject()

        assert item.rejected is True

    def test_override_item(self):
        """Item can be overridden with different concept."""
        item = ConceptMappingItem(
            source_code="A01",
            target_concept_id=12345,
            target_concept_name="Original Concept",
            confidence=0.80,
            method=ConceptMappingMethod.REVIEW,
        )

        item.override(concept_id=99999, concept_name="Override Concept")

        assert item.target_concept_id == 99999
        assert item.target_concept_name == "Override Concept"
        assert item.method == ConceptMappingMethod.OVERRIDE

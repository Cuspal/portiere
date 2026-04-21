"""
Tests for updated stage2 (schema mapping) and stage3 (concept mapping)
with local mode support.

Tests:
- _should_use_local() helper for both stages
- Local mode paths in map_schema() and map_concepts()
- Cloud mode backward compatibility
"""

from unittest.mock import MagicMock, patch

import pytest

from portiere.config import PortiereConfig

# ── Stage 2: Schema Mapping ─────────────────────────────────────


class TestStage2ShouldUseLocal:
    def test_local_mode_returns_true(self):
        from portiere.stages.stage2_schema import _should_use_local

        config = PortiereConfig(mode="local")
        assert _should_use_local(None, config) is True

    def test_hybrid_mode_no_client_returns_true(self):
        from portiere.stages.stage2_schema import _should_use_local

        config = PortiereConfig(mode="hybrid")
        assert _should_use_local(None, config) is True

    def test_hybrid_mode_with_client_returns_true_in_oss(self):
        """In open-source SDK, effective_pipeline is always 'local'."""
        from portiere.stages.stage2_schema import _should_use_local

        config = PortiereConfig(mode="hybrid", pipeline="cloud")
        mock_client = MagicMock()
        assert _should_use_local(mock_client, config) is True

    def test_cloud_mode_returns_true_in_oss(self):
        """In open-source SDK, effective_pipeline is always 'local'."""
        from portiere.stages.stage2_schema import _should_use_local

        config = PortiereConfig(mode="cloud", pipeline="cloud")
        mock_client = MagicMock()
        assert _should_use_local(mock_client, config) is True

    def test_no_config_no_client_returns_false(self):
        from portiere.stages.stage2_schema import _should_use_local

        assert _should_use_local(None, None) is False

    def test_config_without_mode_but_no_client_returns_true(self):
        from portiere.stages.stage2_schema import _should_use_local

        config = PortiereConfig(mode="cloud")
        # config present, client absent → True
        assert _should_use_local(None, config) is True


class TestStage2MapSchemaLocal:
    @patch("portiere.local.schema_mapper.LocalSchemaMapper")
    def test_local_mode_creates_mapper(self, mock_mapper_cls):
        from portiere.stages.stage2_schema import map_schema

        mock_mapper = MagicMock()
        mock_mapper.suggest.return_value = [
            {
                "source_column": "patient_id",
                "target_table": "person",
                "target_column": "person_id",
                "confidence": 0.95,
            },
        ]
        mock_mapper_cls.return_value = mock_mapper

        config = PortiereConfig(mode="local")
        columns = [{"name": "patient_id", "type": "int", "sample_values": []}]

        result = map_schema(config=config, columns=columns)

        mock_mapper_cls.assert_called_once_with(config)
        mock_mapper.suggest.assert_called_once_with(columns)
        assert "mappings" in result
        assert "stats" in result

    @patch("portiere.local.schema_mapper.LocalSchemaMapper")
    def test_local_mode_extracts_columns_from_source_profile(self, mock_mapper_cls):
        from portiere.stages.stage2_schema import map_schema

        mock_mapper = MagicMock()
        mock_mapper.suggest.return_value = []
        mock_mapper_cls.return_value = mock_mapper

        config = PortiereConfig(mode="local")

        # source_profile with columns attribute
        mock_profile = MagicMock()
        mock_profile.columns = [{"name": "col1"}]

        result = map_schema(source_profile=mock_profile, config=config)
        mock_mapper.suggest.assert_called_once_with([{"name": "col1"}])

    @patch("portiere.local.schema_mapper.LocalSchemaMapper")
    def test_local_mode_extracts_columns_from_dict_profile(self, mock_mapper_cls):
        from portiere.stages.stage2_schema import map_schema

        mock_mapper = MagicMock()
        mock_mapper.suggest.return_value = []
        mock_mapper_cls.return_value = mock_mapper

        config = PortiereConfig(mode="local")
        profile_dict = {"columns": [{"name": "col1"}]}

        result = map_schema(source_profile=profile_dict, config=config)
        mock_mapper.suggest.assert_called_once_with([{"name": "col1"}])

    def test_local_mode_no_columns_raises(self):
        from portiere.stages.stage2_schema import map_schema

        config = PortiereConfig(mode="local")

        with pytest.raises(ValueError, match="Either 'columns' or 'source_profile'"):
            map_schema(config=config)

    @patch("portiere.local.schema_mapper.LocalSchemaMapper")
    def test_stats_computed_correctly(self, mock_mapper_cls):
        from portiere.stages.stage2_schema import map_schema

        mock_mapper = MagicMock()
        mock_mapper.suggest.return_value = [
            {"confidence": 0.95},  # auto
            {"confidence": 0.85},  # review
            {"confidence": 0.50},  # unmapped
        ]
        mock_mapper_cls.return_value = mock_mapper

        config = PortiereConfig(mode="local")
        result = map_schema(config=config, columns=[{"name": "a"}, {"name": "b"}, {"name": "c"}])

        stats = result["stats"]
        assert stats["total"] == 3
        assert stats["auto_accepted"] == 1  # >= 0.90
        assert stats["needs_review"] == 1  # 0.70-0.90
        assert stats["unmapped"] == 1  # < 0.70


class TestStage2MapSchemaCloud:
    def test_cloud_mode_uses_local_in_oss(self):
        """In open-source SDK, even with a client, local pipeline is used."""
        from portiere.stages.stage2_schema import _should_use_local

        config = PortiereConfig()
        mock_client = MagicMock()
        # effective_pipeline is always "local" in OSS
        assert _should_use_local(mock_client, config) is True


class TestStage2BuildResult:
    def test_build_result_partitions(self):
        from portiere.stages.stage2_schema import _build_result

        mappings = [
            {"confidence": 0.95},
            {"confidence": 0.80},
            {"confidence": 0.50},
        ]

        result = _build_result(mappings)

        assert result["stats"]["total"] == 3
        assert result["stats"]["auto_accepted"] == 1
        assert result["stats"]["needs_review"] == 1
        assert result["stats"]["unmapped"] == 1


# ── Stage 3: Concept Mapping ────────────────────────────────────


class TestStage3ShouldUseLocal:
    def test_local_mode_returns_true(self):
        from portiere.stages.stage3_concepts import _should_use_local

        config = PortiereConfig(mode="local")
        assert _should_use_local(None, config) is True

    def test_hybrid_no_client_returns_true(self):
        from portiere.stages.stage3_concepts import _should_use_local

        config = PortiereConfig(mode="hybrid")
        assert _should_use_local(None, config) is True

    def test_cloud_with_client_returns_true_in_oss(self):
        """In open-source SDK, effective_pipeline is always 'local'."""
        from portiere.stages.stage3_concepts import _should_use_local

        config = PortiereConfig(mode="cloud", pipeline="cloud")
        mock_client = MagicMock()
        assert _should_use_local(mock_client, config) is True

    def test_no_config_no_client_returns_false(self):
        from portiere.stages.stage3_concepts import _should_use_local

        assert _should_use_local(None, None) is False


class TestStage3MapConceptsLocal:
    @patch("portiere.local.concept_mapper.LocalConceptMapper")
    def test_local_with_pre_extracted_codes(self, mock_mapper_cls):
        from portiere.stages.stage3_concepts import map_concepts

        mock_mapper = MagicMock()

        # map_batch is async
        async def mock_map_batch(codes, vocabs, domain=None):
            return [{"method": "auto", "confidence": 0.97, "source_code": c["code"]} for c in codes]

        mock_mapper.map_batch = mock_map_batch
        mock_mapper_cls.return_value = mock_mapper

        config = PortiereConfig(mode="local")
        codes = [
            {"code": "E11.9", "description": "Type 2 DM"},
            {"code": "I10", "description": "Hypertension"},
        ]

        result = map_concepts(config=config, codes=codes)

        assert "mappings" in result
        assert "stats" in result
        assert result["stats"]["total_codes"] == 2
        assert result["stats"]["auto_mapped"] == 2

    @patch("portiere.local.concept_mapper.LocalConceptMapper")
    def test_local_with_engine_extraction(self, mock_mapper_cls):
        from portiere.stages.stage3_concepts import map_concepts

        mock_mapper = MagicMock()

        async def mock_map_batch(codes, vocabs, domain=None):
            return [
                {"method": "review", "confidence": 0.80, "source_code": c["code"]} for c in codes
            ]

        mock_mapper.map_batch = mock_map_batch
        mock_mapper_cls.return_value = mock_mapper

        mock_engine = MagicMock()
        mock_engine.read_source.return_value = MagicMock()
        mock_engine.get_distinct_values.return_value = [
            {"value": "E11.9", "count": 100},
            {"value": "I10", "count": 50},
        ]

        config = PortiereConfig(mode="local")

        result = map_concepts(
            config=config,
            engine=mock_engine,
            source_path="data.csv",
            code_columns=["diagnosis"],
            vocabularies=["SNOMED"],
        )

        assert result["stats"]["total_codes"] == 2
        mock_engine.read_source.assert_called_once()
        mock_engine.get_distinct_values.assert_called_once()

    def test_local_no_engine_no_codes_raises(self):
        from portiere.stages.stage3_concepts import map_concepts

        config = PortiereConfig(mode="local")

        with pytest.raises(ValueError, match="Either 'engine' or 'codes'"):
            map_concepts(config=config, code_columns=["col"])


class TestStage3MapConceptsCloud:
    def test_cloud_mode_uses_local_in_oss(self):
        """In open-source SDK, even with a client, local pipeline is used."""
        from portiere.stages.stage3_concepts import _should_use_local

        config = PortiereConfig()
        mock_client = MagicMock()
        # effective_pipeline is always "local" in OSS
        assert _should_use_local(mock_client, config) is True


class TestStage3ComputeStats:
    def test_compute_stats(self):
        from portiere.stages.stage3_concepts import _compute_stats

        items = [
            {"method": "auto"},
            {"method": "verified"},
            {"method": "review"},
            {"method": "manual"},
            {"method": "manual"},
        ]

        stats = _compute_stats(items)

        assert stats["total"] == 5
        assert stats["auto"] == 2  # auto + verified
        assert stats["review"] == 1
        assert stats["manual"] == 2

    def test_compute_stats_empty(self):
        from portiere.stages.stage3_concepts import _compute_stats

        stats = _compute_stats([])

        assert stats["total"] == 0
        assert stats["auto"] == 0
        assert stats["review"] == 0
        assert stats["manual"] == 0


class TestStage3BuildResult:
    def test_build_result_auto_rate(self):
        from portiere.stages.stage3_concepts import _build_result

        all_mappings = {"col1": {"items": [], "stats": {}}}
        total_stats = {"total_codes": 10, "auto_mapped": 7, "needs_review": 2, "manual": 1}

        result = _build_result(all_mappings, total_stats)

        assert result["auto_rate"] == 70.0

    def test_build_result_zero_codes(self):
        from portiere.stages.stage3_concepts import _build_result

        result = _build_result(
            {}, {"total_codes": 0, "auto_mapped": 0, "needs_review": 0, "manual": 0}
        )
        assert result["auto_rate"] == 0

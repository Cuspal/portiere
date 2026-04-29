"""Tests for cloud-stub modules (Slice 8 coverage gap-fill).

The open-source SDK ships placeholder cloud implementations: their
constructors raise ``NotImplementedError`` with a pointer at
https://portiere.io. These tests pin the contract so a future Slice
that fleshes out cloud features can't quietly regress it.
"""

from __future__ import annotations

import pytest


class TestCloudConstructorsRaise:
    def test_client_init_raises(self):
        from portiere.client import Client

        with pytest.raises(NotImplementedError, match=r"portiere\.io"):
            Client(api_key="test-key")

    def test_cloud_storage_backend_init_raises(self):
        from portiere.client import Client  # noqa: F401 — for the type hint
        from portiere.storage.cloud_backend import CloudStorageBackend

        # Client() raises too, so we can't pass a real Client; we'd need
        # to mock one. Either way, the backend's __init__ raises.
        class _FakeClient:
            pass

        with pytest.raises(NotImplementedError, match=r"portiere\.io"):
            CloudStorageBackend(client=_FakeClient())  # type: ignore[arg-type]

    def test_sync_manager_init_raises(self):
        from portiere.sync import SyncManager

        class _FakeClient:
            pass

        class _FakeProject:
            name = "p"

        with pytest.raises(NotImplementedError, match=r"portiere\.io"):
            SyncManager(client=_FakeClient(), local_project=_FakeProject())  # type: ignore[arg-type]


class TestProjectCloudParts:
    """Project's cloud-related methods raise NotImplementedError."""

    @pytest.fixture
    def project(self, tmp_path):
        import portiere
        from portiere.config import EmbeddingConfig, PortiereConfig

        config = PortiereConfig(
            local_project_dir=tmp_path,
            embedding=EmbeddingConfig(provider="none"),
        )
        return portiere.init(name="cloud-stub-test", config=config)

    def test_client_property_raises(self, project):
        with pytest.raises(NotImplementedError, match=r"portiere\.io"):
            _ = project.client

    def test_push_raises(self, project):
        with pytest.raises(NotImplementedError, match=r"portiere\.io"):
            project.push()

    def test_pull_raises(self, project):
        with pytest.raises(NotImplementedError, match=r"portiere\.io"):
            project.pull()

    def test_sync_status_returns_local_only(self, project):
        # sync_status doesn't raise — returns a local-only status dict
        status = project.sync_status()
        assert status["mode"] == "local"
        assert status["synced"] is False

"""
Tests for Portiere Client (open-source SDK).

In the open-source SDK, Client raises NotImplementedError on instantiation.
Cloud features are available in Portiere Cloud.
"""

import pytest


class TestClientNotImplemented:
    """All Client operations raise NotImplementedError in open-source SDK."""

    def test_client_init_raises_not_implemented(self):
        from portiere.client import Client

        with pytest.raises(NotImplementedError, match="Cloud features"):
            Client(api_key="pt_sk_test_12345678901234567890")

    def test_client_init_with_custom_endpoint_raises(self):
        from portiere.client import Client

        with pytest.raises(NotImplementedError, match="Cloud features"):
            Client(
                api_key="pt_sk_test_12345678901234567890",
                endpoint="https://custom.api.endpoint",
            )

    def test_client_class_constants_accessible(self):
        """Class constants should still be accessible without instantiation."""
        from portiere.client import Client

        assert Client.DEFAULT_API_ENDPOINT == "https://api.portiere.io"
        assert Client.API_VERSION == "v1"
        assert "omop_cdm_v5.4" in Client.SUPPORTED_MODELS

    def test_client_importable_from_package(self):
        """Client should be importable from the package (for backward compat)."""
        from portiere import Client

        with pytest.raises(NotImplementedError, match="Cloud features"):
            Client(api_key="pt_sk_test_key")


class TestAuthManagerNotImplemented:
    def test_auth_manager_raises_not_implemented(self):
        from portiere.auth import AuthManager

        with pytest.raises(NotImplementedError, match="Cloud authentication"):
            AuthManager(api_key="pt_sk_test", endpoint="https://api.portiere.io")


class TestSyncManagerNotImplemented:
    def test_sync_manager_raises_not_implemented(self):
        from portiere.sync import SyncManager

        with pytest.raises(NotImplementedError, match="Cloud sync"):
            SyncManager(client=None, local_project=None)

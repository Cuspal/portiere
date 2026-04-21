"""
Tests for Portiere Authentication (open-source SDK).

In the open-source SDK, AuthManager raises NotImplementedError on instantiation.
Cloud authentication is available in Portiere Cloud.
"""

import pytest


class TestAuthManagerNotImplemented:
    """AuthManager is not available in the open-source SDK."""

    def test_init_raises_not_implemented(self):
        from portiere.auth import AuthManager

        with pytest.raises(NotImplementedError, match="Cloud authentication"):
            AuthManager(api_key="pt_sk_test_abc123", endpoint="https://api.portiere.io")

    def test_class_importable(self):
        """AuthManager class should be importable (for backward compat)."""
        from portiere.auth import AuthManager

        assert AuthManager is not None

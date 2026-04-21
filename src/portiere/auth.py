"""
Portiere Authentication — API key to JWT handling (Portiere Cloud only).

Not available in the open-source SDK.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog

logger = structlog.get_logger(__name__)

_CLOUD_MSG = (
    "Cloud authentication is not available in the open-source SDK. "
    "For cloud features, see https://portiere.io"
)


class AuthManager:
    """
    Manages authentication with Portiere Cloud (not available in open-source SDK).
    """

    _api_key: str
    _endpoint: str
    _jwt_token: str | None
    _token_expiry: datetime | None

    def __init__(self, api_key: str, endpoint: str) -> None:
        """
        Initialize auth manager.

        Raises:
            NotImplementedError: Always. Cloud auth requires Portiere Cloud.
        """
        raise NotImplementedError(_CLOUD_MSG)

    def get_headers(self) -> dict[str, str]:
        """
        Get authentication headers for API requests.

        Exchanges API key for JWT on first call. Subsequent calls
        reuse the JWT until it's near expiry, then auto-refresh.

        Returns:
            Headers dict with Authorization Bearer token
        """
        try:
            self.ensure_authenticated()
        except Exception:
            # Fallback: if token exchange fails (e.g. dev mode without API),
            # use API key directly so local testing still works.
            logger.debug("JWT exchange failed, falling back to API key auth")
            return {
                "Authorization": f"Bearer {self._api_key}",
                "X-Portiere-API-Key": self._api_key,
            }

        return {
            "Authorization": f"Bearer {self._jwt_token}",
        }

    def ensure_authenticated(self) -> None:
        """
        Ensure we have a valid authentication token.

        Raises:
            AuthenticationError: If authentication fails
        """
        # Check if token is still valid
        if self._jwt_token and self._token_expiry:
            if datetime.now(tz=timezone.utc) < self._token_expiry - timedelta(minutes=5):
                return  # Token still valid

        # Exchange API key for JWT
        self._exchange_token()

    def _exchange_token(self) -> None:
        """
        Exchange API key for a JWT token.
        """
        import httpx

        from portiere.exceptions import AuthenticationError

        try:
            response = httpx.post(
                f"{self._endpoint}/api/v1/auth/token",
                json={"api_key": self._api_key},
                timeout=10.0,
            )

            if response.status_code == 401:
                raise AuthenticationError("Invalid API key")
            elif response.status_code != 200:
                raise AuthenticationError(f"Auth failed: {response.text}")

            data = response.json()
            self._jwt_token = data["access_token"]
            self._token_expiry = datetime.now(tz=timezone.utc) + timedelta(
                seconds=data.get("expires_in", 3600)
            )

            logger.debug("Token exchanged successfully")

        except httpx.RequestError as e:
            raise AuthenticationError(f"Auth request failed: {e}")

    def invalidate(self) -> None:
        """Invalidate the current token."""
        self._jwt_token = None
        self._token_expiry = None

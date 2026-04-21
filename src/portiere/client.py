"""
Portiere Client — Cloud API client (Portiere Cloud only).

Cloud features are not available in the open-source SDK.
Use the open-source SDK for local-only mapping workflows.
For cloud features, see https://portiere.io
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from portiere.config import LLMConfig, PortiereConfig
    from portiere.models.project import Project

logger = structlog.get_logger(__name__)

_CLOUD_MSG = (
    "Cloud features are not available in the open-source SDK. "
    "For cloud storage, sync, and managed inference, see https://portiere.io"
)


class Client:
    """
    Portiere Cloud Client (not available in open-source SDK).

    Cloud features — managed inference, cloud storage, team collaboration —
    are available in Portiere Cloud. See https://portiere.io
    """

    DEFAULT_API_ENDPOINT = "https://api.portiere.io"
    API_VERSION = "v1"
    SUPPORTED_MODELS = ["omop_cdm_v5.4", "omop_cdm_v5.3", "fhir_r4"]

    _api_key: str
    _llm_config: LLMConfig | None
    _auth: Any
    _http: Any

    def __init__(
        self,
        api_key: str,
        *,
        endpoint: str | None = None,
        llm: LLMConfig | None = None,
        config: PortiereConfig | None = None,
        timeout: float = 30.0,
    ) -> None:
        """
        Initialize the Portiere client.

        .. note:: Not available in the open-source SDK.

        Raises:
            NotImplementedError: Always. Cloud features require Portiere Cloud.
        """
        raise NotImplementedError(_CLOUD_MSG)

    @property
    def api_key(self) -> str:
        """Return the API key (masked)."""
        return f"{self._api_key[:10]}...{self._api_key[-4:]}"

    @property
    def llm_config(self) -> LLMConfig | None:
        """Return the LLM configuration."""
        return self._llm_config

    def create_project(
        self,
        name: str,
        *,
        target_model: str = "omop_cdm_v5.4",
        vocabularies: list[str] | None = None,
        description: str | None = None,
    ) -> Project:
        """
        Create a new mapping project.

        Args:
            name: Human-readable project name
            target_model: Target data model (omop_cdm_v5.4, fhir_r4, etc.)
            vocabularies: Standard vocabularies to use (SNOMED, LOINC, etc.)
            description: Optional project description

        Returns:
            Project instance for further configuration

        Raises:
            ValueError: If target_model is not supported
        """
        from portiere.models.project import Project

        # Validate target model
        if target_model not in self.SUPPORTED_MODELS:
            raise ValueError(
                f"Unsupported target model: {target_model}. "
                f"Supported models: {', '.join(self.SUPPORTED_MODELS)}"
            )

        if vocabularies is None:
            vocabularies = ["SNOMED", "LOINC", "RxNorm", "ICD10CM"]

        logger.info(
            "Creating project",
            name=name,
            target_model=target_model,
            vocabularies=vocabularies,
        )

        # Create project via API
        response = self._request(
            "POST",
            "/projects",
            json={
                "name": name,
                "target_model": target_model,
                "vocabularies": vocabularies,
                "description": description,
            },
        )

        return Project(
            client=self,
            id=response["id"],
            name=name,
            target_model=target_model,
            vocabularies=vocabularies,
        )

    def get_project(self, project_id: str) -> Project:
        """
        Retrieve an existing project by ID.

        Args:
            project_id: The project's unique identifier

        Returns:
            Project instance
        """
        from portiere.models.project import Project

        response = self._request("GET", f"/projects/{project_id}")
        return Project(
            client=self,
            id=response["id"],
            name=response["name"],
            target_model=response["target_model"],
            vocabularies=response["vocabularies"],
        )

    def list_projects(self) -> list[Project]:
        """
        List all projects for this account.

        Returns:
            List of Project instances
        """
        from portiere.models.project import Project

        response = self._request("GET", "/projects")
        return [
            Project(
                client=self,
                id=p["id"],
                name=p["name"],
                target_model=p["target_model"],
                vocabularies=p["vocabularies"],
            )
            for p in response["projects"]
        ]

    def search_concepts(
        self,
        query: str,
        *,
        vocabulary: str | None = None,
        domain: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """
        Search OMOP concepts by text.

        This is a standalone concept search, useful for exploration.
        For batch mapping, use project.map_concepts().

        Args:
            query: Search text (e.g., "paracetamol 500mg")
            vocabulary: Filter by vocabulary (RxNorm, SNOMED, etc.)
            domain: Filter by domain (Drug, Condition, Measurement, etc.)
            limit: Maximum results to return

        Returns:
            List of matching concepts with scores
        """
        response = self._request(
            "POST",
            "/concepts/search",
            json={
                "query": query,
                "vocabulary": vocabulary,
                "domain": domain,
                "limit": limit,
            },
        )
        return response["results"]

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
        timeout: float | None = None,
    ) -> dict:
        """
        Make an authenticated API request.

        Args:
            method: HTTP method
            path: API path (relative to base URL)
            json: JSON body
            params: Query parameters
            timeout: Per-request timeout override (seconds). None uses client default.

        Returns:
            JSON response data
        """
        headers = self._auth.get_headers()

        kwargs: dict = dict(
            headers=headers,
            json=json,
            params=params,
        )
        if timeout is not None:
            kwargs["timeout"] = timeout

        response = self._http.request(method, path, **kwargs)

        # Handle errors
        if response.status_code == 401:
            from portiere.exceptions import AuthenticationError

            raise AuthenticationError("Invalid or expired API key")
        elif response.status_code == 429:
            try:
                data = response.json()
            except Exception:
                data = {}
            if data.get("error_code") == "QUOTA_EXCEEDED":
                from portiere.exceptions import QuotaExceededError

                raise QuotaExceededError(
                    data.get("detail", "Usage limit exceeded"),
                    usage_info=data.get("usage"),
                )
            from portiere.exceptions import RateLimitError

            raise RateLimitError("Rate limit exceeded")
        elif response.status_code >= 400:
            from portiere.exceptions import PortiereError

            raise PortiereError(f"API error: {response.text}")

        return response.json()

    def get_usage(self) -> dict:
        """
        Get current billing period usage summary.

        Returns:
            Dict with plan info, period dates, and per-meter usage.
        """
        return self._request("GET", "/billing/usage")

    def get_plan(self) -> dict:
        """
        Get current subscription plan details.

        Returns:
            Dict with plan name, limits, and subscription status.
        """
        return self._request("GET", "/billing/subscription")

    def close(self) -> None:
        """Close the HTTP client."""
        self._http.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *args) -> None:
        self.close()

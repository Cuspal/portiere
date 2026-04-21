"""
Portiere Exceptions — Error hierarchy for the SDK.
"""

from __future__ import annotations


class PortiereError(Exception):
    """Base exception for all Portiere errors."""

    pass


class AuthenticationError(PortiereError):
    """Raised when authentication fails."""

    pass


class ConfigurationError(PortiereError):
    """Raised when configuration is invalid."""

    pass


class MappingError(PortiereError):
    """Raised when a mapping operation fails."""

    pass


class RateLimitError(PortiereError):
    """Raised when rate limit is exceeded."""

    pass


class QuotaExceededError(PortiereError):
    """Raised when usage quota is exceeded."""

    def __init__(self, message: str, usage_info: dict | None = None):
        super().__init__(message)
        self.usage_info = usage_info or {}


class ValidationError(PortiereError):
    """Raised when validation fails."""

    pass


class EngineError(PortiereError):
    """Raised when compute engine operations fail."""

    pass


class ArtifactError(PortiereError):
    """Raised when artifact operations fail."""

    pass


class ETLExecutionError(PortiereError):
    """Raised when ETL pipeline execution fails."""

    def __init__(self, message: str, result=None):
        super().__init__(message)
        self.result = result

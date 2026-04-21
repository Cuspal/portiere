"""
Portiere Storage Backends — Pluggable artifact storage strategies.

Provides abstract StorageBackend and concrete implementations:
- LocalStorageBackend: Filesystem-based storage (YAML + CSV)
- CloudStorageBackend: Cloud API-based storage (wraps Client)
"""

from portiere.storage.base import StorageBackend
from portiere.storage.cloud_backend import CloudStorageBackend
from portiere.storage.local_backend import LocalStorageBackend

__all__ = ["CloudStorageBackend", "LocalStorageBackend", "StorageBackend"]

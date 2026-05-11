"""FHIR Bundle.transaction JSON serializer."""

from __future__ import annotations

import uuid
from typing import Any


def to_transaction_bundle(resources: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap a list of FHIR resource dicts in a transaction Bundle.

    Each entry gets a fresh ``urn:uuid:`` ``fullUrl`` and a ``POST`` request
    targeting the resource type. Cross-resource references (e.g.,
    ``Observation.subject`` pointing to a Patient) are NOT rewritten in
    v0.3.0 — they should already be in either ``urn:uuid:...`` or
    canonical-URL form upstream.
    """
    entries: list[dict[str, Any]] = []
    for resource in resources:
        rt = resource.get("resourceType")
        if not rt:
            raise ValueError(f"Resource at index {len(entries)} missing required 'resourceType'")
        entries.append(
            {
                "fullUrl": f"urn:uuid:{uuid.uuid4()}",
                "resource": resource,
                "request": {"method": "POST", "url": rt},
            }
        )
    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": entries,
    }

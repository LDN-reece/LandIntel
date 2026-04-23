"""Typed source artifact manifest models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class SourceArtifactRecord:
    """Metadata-only audit trail for downloaded or derived artifacts."""

    source_name: str
    artifact_role: str
    ingest_run_id: str | None = None
    authority_name: str | None = None
    artifact_format: str | None = None
    local_path: str | None = None
    source_url: str | None = None
    source_reference: str | None = None
    storage_backend: str = "none"
    storage_bucket: str | None = None
    storage_path: str | None = None
    content_sha256: str | None = None
    size_bytes: int | None = None
    row_count_estimate: int | None = None
    retention_class: str = "working"
    expires_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

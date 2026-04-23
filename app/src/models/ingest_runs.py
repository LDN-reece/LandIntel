"""Typed ingest run models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class IngestRunRecord:
    """A run record created at the start of each job."""

    run_type: str
    source_name: str
    status: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class IngestRunUpdate:
    """A mutable update payload for ingest run progress."""

    status: str | None = None
    records_fetched: int | None = None
    records_loaded: int | None = None
    records_retained: int | None = None
    error_message: str | None = None
    metadata: dict[str, Any] | None = None
    finished: bool = False


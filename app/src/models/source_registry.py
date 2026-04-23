"""Typed source registry records."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from shapely.geometry.base import BaseGeometry


@dataclass(slots=True)
class SourceRegistryRecord:
    """A source metadata record prepared for persistence."""

    source_name: str
    source_type: str
    publisher: str | None = None
    metadata_uuid: str | None = None
    endpoint_url: str | None = None
    download_url: str | None = None
    record_json: dict[str, Any] = field(default_factory=dict)
    geographic_extent: BaseGeometry | None = None
    last_seen_at: datetime | None = None


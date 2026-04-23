"""Staging records for raw RoS cadastral parcels."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shapely.geometry.base import BaseGeometry


@dataclass(slots=True)
class RosCadastralRawRecord:
    """Raw parcel payload stored in staging."""

    run_id: str
    source_name: str
    source_file: str | None
    source_county: str | None
    ros_inspire_id: str | None
    raw_attributes: dict[str, Any] = field(default_factory=dict)
    geometry: BaseGeometry | None = None


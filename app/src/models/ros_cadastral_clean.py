"""Cleaned and processed RoS parcel records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shapely.geometry.base import BaseGeometry


@dataclass(slots=True)
class RosCadastralCleanRecord:
    """Clean staging record after geometry repair and schema normalisation."""

    run_id: str
    source_name: str
    source_file: str | None
    source_county: str | None
    ros_inspire_id: str | None
    raw_attributes: dict[str, Any] = field(default_factory=dict)
    geometry: BaseGeometry | None = None


@dataclass(slots=True)
class RosProcessedParcelRecord:
    """Production-ready RoS parcel after clipping and classification."""

    ros_inspire_id: str | None
    authority_name: str
    source_county: str | None
    geometry: BaseGeometry
    centroid: BaseGeometry
    area_sqm: float
    area_ha: float
    area_acres: float
    size_bucket: str
    size_bucket_label: str
    source_name: str
    source_file: str | None
    raw_attributes: dict[str, Any] = field(default_factory=dict)


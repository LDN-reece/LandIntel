"""Authority AOI models."""

from __future__ import annotations

from dataclasses import dataclass

from shapely.geometry.base import BaseGeometry


@dataclass(slots=True)
class AuthorityAOIRecord:
    """Authority AOI geometry ready for upsert."""

    authority_name: str
    geometry: BaseGeometry
    geometry_simplified: BaseGeometry | None
    active: bool = True

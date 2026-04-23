"""Geometry validation and repair."""

from __future__ import annotations

import logging

import geopandas as gpd

from src.processors.normalise import to_multipolygon

try:
    from shapely import make_valid
except ImportError:  # pragma: no cover
    from shapely.validation import make_valid  # type: ignore


def repair_invalid_geometries(gdf: gpd.GeoDataFrame, logger: logging.Logger) -> gpd.GeoDataFrame:
    """Repair invalid geometries where possible and drop unusable rows."""

    if gdf.empty:
        return gdf

    repaired = gdf.copy()
    dropped = 0
    fixed = 0

    def repair(geometry):
        nonlocal dropped, fixed
        if geometry is None or geometry.is_empty:
            dropped += 1
            return None
        updated = geometry
        if not updated.is_valid:
            updated = make_valid(updated)
            fixed += 1
        updated = to_multipolygon(updated)
        if updated is None or updated.is_empty:
            dropped += 1
            return None
        return updated

    repaired["geometry"] = repaired.geometry.apply(repair)
    repaired = repaired[repaired.geometry.notna()].copy()

    if fixed or dropped:
        logger.info(
            "geometry_repair_summary",
            extra={"features_fixed": fixed, "features_dropped": dropped},
        )
    return repaired


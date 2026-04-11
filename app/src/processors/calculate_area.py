"""Area calculations in EPSG:27700."""

from __future__ import annotations

import geopandas as gpd


SQM_PER_HECTARE = 10_000.0
SQM_PER_ACRE = 4_046.8564224


def calculate_area_metrics(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Calculate centroid and area metrics for each geometry."""

    if gdf.empty:
        return gdf

    enriched = gdf.copy()
    enriched["centroid"] = enriched.geometry.centroid
    enriched["area_sqm"] = enriched.geometry.area.astype(float)
    enriched["area_ha"] = (enriched["area_sqm"] / SQM_PER_HECTARE).round(6)
    enriched["area_acres"] = (enriched["area_sqm"] / SQM_PER_ACRE).round(6)
    return enriched

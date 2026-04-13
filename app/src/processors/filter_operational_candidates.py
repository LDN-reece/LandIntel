"""Filter processed parcel candidates down to the operational footprint."""

from __future__ import annotations

import geopandas as gpd


def filter_operational_candidates(
    gdf: gpd.GeoDataFrame,
    minimum_area_acres: float,
) -> tuple[gpd.GeoDataFrame, dict[str, float | int]]:
    """Return only rows that meet the minimum operational acreage threshold."""

    if gdf.empty:
        return gdf, {
            "input_rows": 0,
            "retained_rows": 0,
            "filtered_out_rows": 0,
            "filtered_out_area_acres": 0.0,
        }

    filtered = gdf[gdf["area_acres"].astype(float) >= float(minimum_area_acres)].copy()
    filtered_out = gdf[gdf["area_acres"].astype(float) < float(minimum_area_acres)].copy()
    return filtered, {
        "input_rows": int(len(gdf)),
        "retained_rows": int(len(filtered)),
        "filtered_out_rows": int(len(filtered_out)),
        "filtered_out_area_acres": float(filtered_out["area_acres"].sum()) if not filtered_out.empty else 0.0,
    }

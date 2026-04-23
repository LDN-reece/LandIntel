"""Filter parcel candidates down to the operational footprint worth persisting."""

from __future__ import annotations

from typing import Any

import geopandas as gpd


def filter_operational_candidates(
    gdf: gpd.GeoDataFrame,
    *,
    minimum_area_acres: float,
) -> tuple[gpd.GeoDataFrame, dict[str, Any]]:
    """Keep only parcels that are large enough to matter for first-pass sourcing."""

    if gdf.empty:
        return gdf, {
            "minimum_area_acres": minimum_area_acres,
            "input_rows": 0,
            "retained_rows": 0,
            "filtered_out_rows": 0,
            "retained_area_acres": 0.0,
            "filtered_out_area_acres": 0.0,
        }

    filtered = gdf.copy()
    retained_mask = filtered["area_acres"].fillna(0).astype(float) >= float(minimum_area_acres)
    retained = filtered.loc[retained_mask].copy()
    excluded = filtered.loc[~retained_mask].copy()

    summary = {
        "minimum_area_acres": float(minimum_area_acres),
        "input_rows": int(len(filtered)),
        "retained_rows": int(len(retained)),
        "filtered_out_rows": int(len(excluded)),
        "retained_area_acres": round(float(retained["area_acres"].fillna(0).sum()), 3),
        "filtered_out_area_acres": round(float(excluded["area_acres"].fillna(0).sum()), 3),
    }
    return retained, summary

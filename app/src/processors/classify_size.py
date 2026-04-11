"""Size bucketing logic."""

from __future__ import annotations

import geopandas as gpd


def classify_size_buckets(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assign size bucket codes and labels based on acreage."""

    if gdf.empty:
        return gdf

    classified = gdf.copy()
    classified["size_bucket"] = classified["area_acres"].apply(
        lambda value: "bucket_1_under_5_acres" if float(value) < 5 else "bucket_2_5plus_acres"
    )
    classified["size_bucket_label"] = classified["area_acres"].apply(
        lambda value: "Under 5 acres" if float(value) < 5 else "5+ acres"
    )
    return classified

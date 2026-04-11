"""Clip cadastral parcels to the target authority AOIs."""

from __future__ import annotations

import logging

import geopandas as gpd

from src.processors.normalise import to_multipolygon


def clip_parcels_to_authorities(
    parcels_gdf: gpd.GeoDataFrame,
    authority_gdf: gpd.GeoDataFrame,
    logger: logging.Logger,
) -> gpd.GeoDataFrame:
    """Intersect parcels with authority AOIs and preserve only target features."""

    if parcels_gdf.empty:
        return parcels_gdf

    authorities = authority_gdf[["authority_name", "geometry"]].copy()
    if authorities.crs != parcels_gdf.crs:
        authorities = authorities.to_crs(parcels_gdf.crs)

    authority_union = authorities.geometry.unary_union
    candidate_parcels = parcels_gdf[parcels_gdf.geometry.intersects(authority_union)].copy()
    if candidate_parcels.empty:
        return candidate_parcels

    clipped = gpd.overlay(candidate_parcels, authorities, how="intersection", keep_geom_type=True)
    if clipped.empty:
        return clipped

    split_count = 0
    if "ros_inspire_id" in clipped.columns:
        split_summary = clipped.groupby("ros_inspire_id", dropna=False)["authority_name"].nunique()
        split_count = int((split_summary > 1).sum())
    if split_count:
        logger.info("multi_authority_parcels_detected", extra={"split_parcel_count": split_count})

    group_cols = [column for column in clipped.columns if column not in {"geometry", "raw_attributes"}]
    clipped = clipped.dissolve(by=group_cols, as_index=False, aggfunc={"raw_attributes": "first"})
    clipped["geometry"] = clipped.geometry.apply(to_multipolygon)
    clipped = clipped[clipped.geometry.notna()].copy()
    clipped = clipped.set_crs(parcels_gdf.crs)
    return clipped

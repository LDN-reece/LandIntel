"""Normalisation helpers for incoming spatial data."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import geopandas as gpd
import pyogrio
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


def load_preferred_spatial_frame(path: Path) -> gpd.GeoDataFrame:
    """Load a spatial dataset using GeoPandas and pyogrio when possible."""

    layer_name = _default_layer_name(path)
    try:
        if layer_name:
            return gpd.read_file(path, layer=layer_name, engine="pyogrio")
        return gpd.read_file(path, engine="pyogrio")
    except Exception:
        if layer_name:
            return gpd.read_file(path, layer=layer_name)
        return gpd.read_file(path)


def normalise_ros_cadastral_frame(
    frame: gpd.GeoDataFrame,
    *,
    run_id: str,
    source_name: str,
    source_file: str | None,
    source_county: str | None,
) -> gpd.GeoDataFrame:
    """Map the raw RoS frame into a consistent internal schema."""

    if frame.empty:
        return gpd.GeoDataFrame(
            columns=[
                "run_id",
                "source_name",
                "source_file",
                "source_county",
                "ros_inspire_id",
                "raw_attributes",
                "geometry",
            ],
            geometry="geometry",
            crs="EPSG:27700",
        )

    normalised = frame.copy()
    if normalised.crs is None:
        normalised = normalised.set_crs(27700, allow_override=True)
    if normalised.crs.to_epsg() != 27700:
        normalised = normalised.to_crs(27700)

    fields = {column.lower(): column for column in normalised.columns}
    inspire_field = _first_existing(fields, "inspireid", "inspire_id", "inspireid_localid")
    county_field = _first_existing(fields, "county")

    records: list[dict[str, Any]] = []
    for record in normalised.to_dict(orient="records"):
        geometry = to_multipolygon(record.get("geometry"))
        raw_attributes = {
            key: _to_json_safe(value)
            for key, value in record.items()
            if key != "geometry"
        }
        ros_inspire_id = raw_attributes.get(inspire_field) if inspire_field else None
        county_value = raw_attributes.get(county_field) if county_field else source_county

        records.append(
            {
                "run_id": run_id,
                "source_name": source_name,
                "source_file": source_file,
                "source_county": county_value or source_county,
                "ros_inspire_id": str(ros_inspire_id).strip() if ros_inspire_id else None,
                "raw_attributes": raw_attributes,
                "geometry": geometry,
            }
        )

    output = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:27700")
    return output


def to_multipolygon(geometry: BaseGeometry | None) -> BaseGeometry | None:
    """Return a multipolygon-compatible geometry or None."""

    if geometry is None or geometry.is_empty:
        return None
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    if isinstance(geometry, MultiPolygon):
        return geometry
    if isinstance(geometry, GeometryCollection):
        polygons = []
        for item in geometry.geoms:
            converted = to_multipolygon(item)
            if isinstance(converted, MultiPolygon):
                polygons.extend(list(converted.geoms))
        return MultiPolygon(polygons) if polygons else None
    return None


def _default_layer_name(path: Path) -> str | None:
    """Return the first available layer name for container formats."""

    if path.suffix.lower() not in {".gdb", ".gpkg"}:
        return None
    try:
        layers = pyogrio.list_layers(path)
    except Exception:
        return None
    if len(layers) == 0:
        return None
    return str(layers[0][0])


def _first_existing(fields: dict[str, str], *candidates: str) -> str | None:
    """Return the first matching source field name."""

    for candidate in candidates:
        if candidate in fields:
            return fields[candidate]
    return None


def _to_json_safe(value: Any) -> Any:
    """Convert raw attribute values into JSON-safe payloads."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value

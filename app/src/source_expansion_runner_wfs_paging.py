"""Paged WFS entrypoint for Phase One source expansion.

SpatialHub can return service exceptions when large national layers are pulled in
one request. This wrapper preserves the existing source-expansion implementation
but replaces WFS reads with bounded pages so larger sources such as VDL can be
loaded through GitHub Actions without weakening the data model.
"""

from __future__ import annotations

import os
from typing import Any
import traceback
import xml.etree.ElementTree as ET

import geopandas as gpd
import pandas as pd

from config.settings import Settings, get_settings
from src.logging_config import configure_logging
from src.source_expansion_runner import (
    SourceExpansionRunner,
    _dedupe,
    _feature_type_matches,
    _tag_name,
    _workspace_from_url,
    build_parser,
)


class PagedWfsSourceExpansionRunner(SourceExpansionRunner):
    """Source expansion runner with bounded GeoServer WFS paging."""

    def __init__(self, settings: Settings, logger: Any) -> None:
        super().__init__(settings, logger)
        self.logger = logger.getChild("source_expansion_paged_wfs")

    def _empty_geo_frame(self) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)

    def _feature_collection_to_gdf(
        self,
        payload: dict[str, Any],
        source: dict[str, Any],
        layer_name: str,
    ) -> gpd.GeoDataFrame:
        # Empty AOI tiles are expected for constraint layers clipped to live sites.
        if not payload.get("features"):
            return self._empty_geo_frame()
        try:
            return super()._feature_collection_to_gdf(payload, source, layer_name)
        except ValueError as exc:
            if "Unknown column geometry" in str(exc):
                return self._empty_geo_frame()
            raise

    def _wfs_feature_types(self, source: dict[str, Any]) -> list[str]:
        endpoint_url = str(source["endpoint_url"])
        response = self.client.get(
            endpoint_url,
            params={"service": "WFS", "request": "GetCapabilities", **self._auth_params(source)},
        )
        response.raise_for_status()
        root = ET.fromstring(response.text.encode("utf-8"))
        names: list[str] = []
        for node in root.iter():
            if not _tag_name(node.tag).lower().endswith("featuretype"):
                continue
            for child in list(node):
                if _tag_name(child.tag).lower() == "name" and child.text and child.text.strip():
                    names.append(child.text.strip())

        names = _dedupe(names)
        hints = self._layer_hints(source)
        matched = [name for name in names if any(_feature_type_matches(name, hint) for hint in hints)]
        if matched:
            return _dedupe(matched)

        if source.get("source_family") == "vdl" and names:
            # VDL's old static export used pub_vdlPolygon, but the live WFS can
            # rename the advertised type. Capabilities is the authority here.
            return names

        if names and not hints:
            return names

        workspace = _workspace_from_url(endpoint_url)
        return [f"{workspace}:{hint}" if workspace else hint for hint in hints]

    def _fetch_wfs_source_frames(self, source: dict[str, Any]) -> list[gpd.GeoDataFrame]:
        endpoint_url = str(source["endpoint_url"])
        type_names = self._wfs_feature_types(source)
        if not type_names:
            raise RuntimeError(f"No WFS feature types found for {source['source_key']}.")

        frames: list[gpd.GeoDataFrame] = []
        layer_errors: list[str] = []
        page_size = max(1, self.page_size)

        for type_name in type_names:
            fetched = 0
            offset = 0
            while True:
                batch_limit = page_size
                if self.max_features > 0:
                    remaining = self.max_features - fetched
                    if remaining <= 0:
                        break
                    batch_limit = min(batch_limit, remaining)

                params = {
                    "service": "WFS",
                    "version": "1.0.0",
                    "request": "GetFeature",
                    "typeName": type_name,
                    "outputFormat": "application/json",
                    "srsName": "EPSG:27700",
                    "maxFeatures": str(batch_limit),
                }
                if offset > 0:
                    # GeoServer supports startIndex as a vendor parameter for paged WFS reads.
                    params["startIndex"] = str(offset)
                params.update(self._auth_params(source))

                try:
                    response = self.client.get(endpoint_url, params=params)
                    response.raise_for_status()
                    payload = self._json_payload(response, f"WFS GetFeature {source['source_key']} {type_name}")
                    frame = self._feature_collection_to_gdf(payload, source, type_name)
                except Exception as exc:
                    layer_errors.append(f"{type_name} offset {offset}: {exc}")
                    break

                if frame.empty:
                    break

                frames.append(frame)
                batch_count = len(frame)
                fetched += batch_count
                if batch_count < batch_limit:
                    break
                offset += batch_count

        if not frames and layer_errors:
            raise RuntimeError(
                f"No usable WFS features returned for {source['source_key']}. "
                + " | ".join(layer_errors[:5])
            )
        return frames

    def _fetch_arcgis_layer(self, source: dict[str, Any], layer_url: str, layer_name: str) -> gpd.GeoDataFrame:
        if source.get("source_family") != "sepa_flood":
            return super()._fetch_arcgis_layer(source, layer_url, layer_name)

        envelopes = self._canonical_site_envelopes()
        if not envelopes:
            self.logger.warning("sepa_aoi_missing", extra={"layer_name": layer_name})
            return self._empty_geo_frame()

        frames: list[gpd.GeoDataFrame] = []
        seen_feature_ids: set[str] = set()
        page_size = max(1, self.page_size)
        per_layer_cap = int(os.getenv("SOURCE_EXPANSION_ARCGIS_MAX_FEATURES_PER_LAYER", "75000") or "75000")
        fetched = 0

        for envelope in envelopes:
            offset = 0
            while True:
                remaining = per_layer_cap - fetched
                if remaining <= 0:
                    self.logger.warning(
                        "sepa_layer_feature_cap_reached",
                        extra={"layer_name": layer_name, "feature_cap": per_layer_cap},
                    )
                    return self._combine_frames(frames)

                batch_limit = min(page_size, remaining)
                params = {
                    "f": "geojson",
                    "where": "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "outSR": "27700",
                    "resultOffset": str(offset),
                    "resultRecordCount": str(batch_limit),
                    "geometry": f"{envelope['xmin']},{envelope['ymin']},{envelope['xmax']},{envelope['ymax']}",
                    "geometryType": "esriGeometryEnvelope",
                    "inSR": "27700",
                    "spatialRel": "esriSpatialRelIntersects",
                }
                response = self.client.get(f"{layer_url.rstrip('/')}/query", params=params)
                response.raise_for_status()
                payload = self._json_payload(response, f"ArcGIS query {source['source_key']} {layer_name}")
                frame = self._feature_collection_to_gdf(payload, source, layer_name)
                if frame.empty:
                    break

                raw_batch_count = len(frame)
                if "_source_feature_id" in frame.columns:
                    keep_mask = ~frame["_source_feature_id"].astype(str).isin(seen_feature_ids)
                    new_ids = set(frame.loc[keep_mask, "_source_feature_id"].astype(str).tolist())
                    frame = frame.loc[keep_mask].copy()
                    seen_feature_ids.update(new_ids)

                if not frame.empty:
                    frames.append(frame)
                    fetched += len(frame)

                if raw_batch_count < batch_limit:
                    break
                offset += batch_limit

        return self._combine_frames(frames)

    def _canonical_site_envelopes(self) -> list[dict[str, float]]:
        buffer_m = float(os.getenv("SOURCE_EXPANSION_ARCGIS_SITE_BUFFER_M", "250") or "250")
        tile_size_m = float(os.getenv("SOURCE_EXPANSION_ARCGIS_TILE_SIZE_M", "2000") or "2000")
        max_tiles = int(os.getenv("SOURCE_EXPANSION_ARCGIS_MAX_TILES", "1000") or "1000")
        rows = self.database.fetch_all(
            """
            with site_tiles as (
                select
                    floor(st_x(st_centroid(geometry)) / :tile_size_m)::integer as tile_x,
                    floor(st_y(st_centroid(geometry)) / :tile_size_m)::integer as tile_y,
                    st_expand(geometry, :buffer_m) as geometry
                from landintel.canonical_sites
                where geometry is not null
            )
            select
                st_xmin(st_extent(geometry))::double precision as xmin,
                st_ymin(st_extent(geometry))::double precision as ymin,
                st_xmax(st_extent(geometry))::double precision as xmax,
                st_ymax(st_extent(geometry))::double precision as ymax,
                count(*)::integer as site_count
            from site_tiles
            group by tile_x, tile_y
            order by site_count desc, tile_x, tile_y
            limit :max_tiles
            """,
            {"buffer_m": buffer_m, "tile_size_m": tile_size_m, "max_tiles": max_tiles},
        )
        return [
            {"xmin": float(row["xmin"]), "ymin": float(row["ymin"]), "xmax": float(row["xmax"]), "ymax": float(row["ymax"])}
            for row in rows
            if row.get("xmin") is not None and row.get("ymin") is not None and row.get("xmax") is not None and row.get("ymax") is not None
        ]

    def _combine_frames(self, frames: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
        if not frames:
            return self._empty_geo_frame()
        combined = pd.concat(frames, ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry="geometry", crs=frames[0].crs or 27700)


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = PagedWfsSourceExpansionRunner(settings, logger)
    try:
        runner.run_command(args.command)
        logger.info("source_expansion_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception("source_expansion_command_failed", extra={"command": args.command, "traceback": traceback.format_exc()})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

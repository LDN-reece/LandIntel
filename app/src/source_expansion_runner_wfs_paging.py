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

    def _ingest_constraint_family(
        self,
        command: str,
        source_family: str,
        sources: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        self._assert_required_secrets(sources)
        self._clear_constraint_family(source_family)
        raw_rows = 0
        measured_rows = 0
        evidence_rows = 0
        signal_rows = 0
        affected_site_count = 0
        measurement_approved_layers = 0
        measurement_deferred_layers = 0
        layer_results: list[dict[str, Any]] = []

        for source in sources:
            self._upsert_source_estate(source)
            if self._source_uses_arcgis(source):
                loaded_layers = self._load_arcgis_constraint_source(source, run_id)
            else:
                loaded_layers = self._load_wfs_constraint_source(source, run_id)
            for layer in loaded_layers:
                raw_rows += int(layer.get("raw_rows", 0))
                gate = self._constraint_layer_gate(source_family, len(layer_results), layer)
                layer_payload = {**layer, **gate}
                self.logger.info("constraint_layer_gate", extra=layer_payload)

                if gate["measurement_approved"]:
                    try:
                        proof = self.database.fetch_one(
                            "select * from public.refresh_constraint_measurements_for_layer(:layer_key)",
                            {"layer_key": layer["layer_key"]},
                        ) or {}
                    except Exception as exc:
                        if not self._is_statement_timeout(exc):
                            raise
                        measurement_deferred_layers += 1
                        layer_payload.update(
                            self._deferred_measurement_payload(
                                "measurement_statement_timeout",
                                str(exc),
                            )
                        )
                        self.logger.warning(
                            "constraint_measurement_deferred",
                            extra={
                                "layer_key": layer.get("layer_key"),
                                "source_family": source_family,
                                "gate_reason": "measurement_statement_timeout",
                                "error_message": str(exc),
                            },
                        )
                    else:
                        measurement_approved_layers += 1
                        measured_rows += int(proof.get("measurement_count") or 0)
                        evidence_rows += int(proof.get("evidence_count") or 0)
                        signal_rows += int(proof.get("signal_count") or 0)
                        affected_site_count += int(proof.get("affected_site_count") or 0)
                        layer_payload.update(proof)
                else:
                    measurement_deferred_layers += 1
                    layer_payload.update(self._deferred_measurement_payload(gate["gate_reason"]))
                layer_results.append(layer_payload)

        if raw_rows == 0:
            event_status = "empty_source_response"
        elif measurement_approved_layers and affected_site_count == 0:
            event_status = "constraint_loaded_no_site_overlap"
        elif measurement_approved_layers:
            event_status = "constraint_measurements_refreshed"
        else:
            event_status = "constraint_loaded_measurement_deferred"

        result = {
            "command": command,
            "source_family": source_family,
            "source_keys": [source["source_key"] for source in sources],
            "raw_rows": raw_rows,
            "measured_rows": measured_rows,
            "linked_rows": affected_site_count,
            "evidence_rows": evidence_rows,
            "signal_rows": signal_rows,
            "change_event_rows": affected_site_count,
            "measurement_approved_layers": measurement_approved_layers,
            "measurement_deferred_layers": measurement_deferred_layers,
            "gate_status": event_status,
            "layers": layer_results,
        }
        self._record_source_freshness(sources[0], "current" if raw_rows else "empty", "reachable", raw_rows, result)
        self._record_expansion_event(
            command_name=command,
            source_key=sources[0]["source_key"],
            source_family=source_family,
            status=event_status,
            raw_rows=raw_rows,
            linked_rows=affected_site_count,
            measured_rows=measured_rows,
            evidence_rows=evidence_rows,
            signal_rows=signal_rows,
            change_event_rows=affected_site_count,
            summary=f"{source_family} constraints loaded, gate-checked, and measured only where approved by the Phase One budget.",
            metadata=result,
        )
        return result

    def _constraint_layer_gate(
        self,
        source_family: str,
        layer_index: int,
        layer: dict[str, Any],
    ) -> dict[str, Any]:
        mode = self._constraint_measurement_mode(source_family)
        raw_rows = int(layer.get("raw_rows", 0) or 0)
        max_features = self._constraint_env_int(source_family, "MAX_MEASURE_FEATURES", 2500)
        max_layers = self._constraint_env_int(source_family, "MAX_MEASURE_LAYERS", 1)

        if raw_rows <= 0:
            return self._gate_result(False, "empty_layer", mode, raw_rows, max_features, max_layers)
        if mode in {"off", "false", "0", "load_only", "load-only"}:
            return self._gate_result(False, "measurement_disabled_by_config", mode, raw_rows, max_features, max_layers)
        if mode in {"always", "force", "true", "1"}:
            return self._gate_result(True, "measurement_forced_by_config", mode, raw_rows, max_features, max_layers)
        if layer_index >= max_layers:
            return self._gate_result(False, "measurement_layer_budget_exceeded", mode, raw_rows, max_features, max_layers)
        if raw_rows > max_features:
            return self._gate_result(False, "measurement_feature_budget_exceeded", mode, raw_rows, max_features, max_layers)
        return self._gate_result(True, "measurement_budget_passed", mode, raw_rows, max_features, max_layers)

    def _gate_result(
        self,
        approved: bool,
        reason: str,
        mode: str,
        raw_rows: int,
        max_features: int,
        max_layers: int,
    ) -> dict[str, Any]:
        return {
            "measurement_approved": approved,
            "gate_reason": reason,
            "measurement_mode": mode,
            "measurement_feature_budget": max_features,
            "measurement_layer_budget": max_layers,
            "raw_rows_at_gate": raw_rows,
        }

    def _deferred_measurement_payload(self, reason: str, error_message: str | None = None) -> dict[str, Any]:
        payload = {
            "measurement_count": 0,
            "summary_count": 0,
            "friction_fact_count": 0,
            "evidence_count": 0,
            "signal_count": 0,
            "affected_site_count": 0,
            "measurement_deferred_reason": reason,
        }
        if error_message:
            payload["measurement_error_message"] = error_message[:500]
        return payload

    def _is_statement_timeout(self, exc: Exception) -> bool:
        text = f"{type(exc).__name__}: {exc}".lower()
        return "statement timeout" in text or "querycanceled" in text or "query canceled" in text

    def _constraint_measurement_mode(self, source_family: str) -> str:
        family_key = source_family.upper().replace("-", "_")
        configured = os.getenv(f"SOURCE_EXPANSION_{family_key}_MEASURE_MODE") or os.getenv(
            "SOURCE_EXPANSION_CONSTRAINT_MEASURE_MODE"
        )
        if configured and configured.strip():
            return configured.strip().lower()
        if source_family == "sepa_flood":
            return "load_only"
        return "auto"

    def _constraint_env_int(self, source_family: str, suffix: str, default: int) -> int:
        family_key = source_family.upper().replace("-", "_")
        return self._env_int(
            f"SOURCE_EXPANSION_{family_key}_{suffix}",
            self._env_int(f"SOURCE_EXPANSION_{suffix}", default),
        )

    def _env_int(self, name: str, default: int) -> int:
        raw_value = os.getenv(name)
        if raw_value is None or str(raw_value).strip() == "":
            return default
        try:
            return int(str(raw_value))
        except ValueError:
            return default

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
        per_layer_cap = int(os.getenv("SOURCE_EXPANSION_ARCGIS_MAX_FEATURES_PER_LAYER", "10000") or "10000")
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
        max_tiles = int(os.getenv("SOURCE_EXPANSION_ARCGIS_MAX_TILES", "75") or "75")
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

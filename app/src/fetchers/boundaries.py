"""Authority boundary discovery and download helpers."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import geopandas as gpd
import httpx

from config.settings import Settings
from src.models.source_registry import SourceRegistryRecord


BOUNDARY_NAME_MAP = {
    "Edinburgh, City of": "City of Edinburgh",
    "Perth & Kinross": "Perth and Kinross",
}


@dataclass(slots=True)
class BoundaryDownload:
    """Boundary source metadata plus the downloaded local file."""

    source_record: SourceRegistryRecord
    download_url: str
    local_path: Path


class BoundaryFetcher:
    """Discover and download Scottish local authority boundaries."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger.getChild("boundaries")
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self.client.close()

    def discover_source_metadata(self) -> SourceRegistryRecord:
        """Discover the authoritative boundary metadata from Spatial Hub / SSDI."""

        response = self.client.get(self.settings.boundary_package_show_url)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result", {})
        resources = result.get("resources", [])
        resource = next(
            (
                item
                for item in resources
                if "wfs" in str(item.get("format", "")).lower()
                or "wfs" in str(item.get("url", "")).lower()
            ),
            resources[0] if resources else {},
        )

        metadata_uuid = None
        for item in result.get("extras", []):
            if item.get("key") == "ssdi_link":
                match = re.search(r"/metadata/([a-f0-9-]+)", item.get("value", ""))
                if match:
                    metadata_uuid = match.group(1)
                    break

        return SourceRegistryRecord(
            source_name=result.get("title", "Local Authority Areas - Scotland"),
            source_type=resource.get("format", "WFS"),
            publisher=result.get("author") or "Improvement Service",
            metadata_uuid=metadata_uuid or "fallback:local-authority-areas-scotland",
            endpoint_url=resource.get("url"),
            download_url=resource.get("url"),
            record_json=payload,
            geographic_extent=None,
            last_seen_at=None,
        )

    def download_boundaries(self, destination_dir: Path) -> BoundaryDownload:
        """Download the boundary dataset to a local working path."""

        source_record = self.discover_source_metadata()
        download_url = self._build_download_url(source_record.download_url or source_record.endpoint_url or "")
        destination_dir.mkdir(parents=True, exist_ok=True)
        local_path = destination_dir / "local_authority_areas_scotland.geojson"

        response = self.client.get(download_url)
        response.raise_for_status()
        text_preview = response.text[:500]
        if text_preview.lstrip().startswith("<?xml") or "ServiceException" in text_preview or "Access Denied" in text_preview:
            raise RuntimeError(
                "Boundary download failed. If the Spatial Hub endpoint requires an auth key, "
                "set BOUNDARY_AUTHKEY or provide BOUNDARY_GEOJSON_URL."
            )
        local_path.write_bytes(response.content)

        self.logger.info(
            "boundary_downloaded",
            extra={"download_url": download_url, "local_path": str(local_path)},
        )
        return BoundaryDownload(
            source_record=source_record,
            download_url=download_url,
            local_path=local_path,
        )

    def load_target_authorities(self, path: Path, target_authorities: list[str]) -> gpd.GeoDataFrame:
        """Read, standardise, dissolve, and filter the authority boundaries."""

        frame = gpd.read_file(path, engine="pyogrio")
        if frame.empty:
            raise RuntimeError("The downloaded boundary dataset did not contain any features.")

        name_column = self._find_name_column(frame.columns)
        if name_column is None:
            raise RuntimeError("Could not determine the authority name column in the boundary dataset.")

        frame = frame[[name_column, "geometry"]].rename(columns={name_column: "authority_name"}).copy()
        frame["authority_name"] = frame["authority_name"].map(self._canonicalise_authority_name)
        frame = frame[frame["authority_name"].isin(target_authorities)].copy()
        if frame.empty:
            raise RuntimeError("No target authorities were present in the downloaded boundary dataset.")

        if frame.crs is None:
            frame = frame.set_crs(27700, allow_override=True)
        if frame.crs.to_epsg() != 27700:
            frame = frame.to_crs(27700)

        dissolved = frame.dissolve(by="authority_name", as_index=False)
        dissolved["active"] = True
        dissolved["geometry_simplified"] = dissolved.geometry.simplify(
            self.settings.boundary_simplify_tolerance,
            preserve_topology=True,
        )

        missing = sorted(set(target_authorities) - set(dissolved["authority_name"].tolist()))
        if missing:
            raise RuntimeError(f"Boundary dataset is missing target authorities: {', '.join(missing)}")

        return dissolved[["authority_name", "active", "geometry", "geometry_simplified"]]

    def _build_download_url(self, raw_resource_url: str) -> str:
        """Build a GeoJSON-friendly WFS URL from the CKAN resource URL."""

        if self.settings.boundary_geojson_url:
            return self.settings.boundary_geojson_url
        if not raw_resource_url:
            raise RuntimeError("Boundary metadata did not contain a resource URL.")

        parsed = urlparse(raw_resource_url)
        existing = parse_qs(parsed.query)
        type_name = (existing.get("typeName") or existing.get("typeNames") or [None])[0]
        if not type_name:
            raise RuntimeError("Boundary resource URL did not expose a WFS typeName.")

        query = {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": type_name,
            "outputFormat": "application/json",
        }
        if self.settings.boundary_authkey:
            query["authkey"] = self.settings.boundary_authkey

        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query), ""))

    def _find_name_column(self, columns: Any) -> str | None:
        """Return the most likely authority name column."""

        lookup = {str(column).lower(): str(column) for column in columns}
        for candidate in ("local_authority", "authority_name", "name", "lad23nm", "ca_name"):
            if candidate in lookup:
                return lookup[candidate]
        for candidate, original in lookup.items():
            if "name" in candidate:
                return original
        return None

    def _canonicalise_authority_name(self, name: Any) -> str:
        """Map source names onto canonical LandIntel authority names."""

        text = str(name).strip()
        return BOUNDARY_NAME_MAP.get(text, text)

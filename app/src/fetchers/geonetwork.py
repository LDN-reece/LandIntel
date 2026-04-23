"""SpatialData.gov.scot GeoNetwork discovery client."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import httpx
from shapely.geometry import box, shape
from shapely.geometry.base import BaseGeometry

from config.settings import Settings
from src.models.source_registry import SourceRegistryRecord


ROS_FALLBACK_BOUNDS = {
    "west": -9.8018,
    "south": 54.5211,
    "east": 0.2177,
    "north": 61.2008,
}


class GeoNetworkClient:
    """Minimal client for the public GeoNetwork search and record endpoints."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger.getChild("geonetwork")
        self.client = httpx.Client(
            timeout=settings.http_timeout_seconds,
            follow_redirects=True,
            headers={"Accept": "application/json"},
        )
        self._session_bootstrapped = False

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self.client.close()

    def search_records(self, query_text: str, size: int = 10) -> list[dict[str, Any]]:
        """Search published metadata records using the Elasticsearch-backed endpoint."""

        self._bootstrap_session()
        search_url = urljoin(
            f"{self.settings.geonetwork_base_url}/",
            self.settings.geonetwork_search_path.lstrip("/"),
        )
        payload = {
            "from": 0,
            "size": size,
            "sort": [{"changeDate": {"order": "desc", "unmapped_type": "date"}}],
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "default_field": "anytext",
                                "query": query_text,
                            }
                        }
                    ],
                    "filter": [{"term": {"isTemplate": "n"}}],
                }
            },
        }

        response = self.client.post(
            search_url,
            params={"bucket": "metadata", "from": 0, "size": size},
            json=payload,
        )
        if response.status_code >= 400:
            self.logger.warning(
                "geonetwork_search_failed",
                extra={
                    "status_code": response.status_code,
                    "query_text": query_text,
                    "response_text": response.text[:500],
                },
            )
            return []

        try:
            payload = response.json()
        except json.JSONDecodeError:
            self.logger.warning(
                "geonetwork_search_non_json",
                extra={"query_text": query_text, "response_text": response.text[:500]},
            )
            return []
        return payload.get("hits", {}).get("hits", [])

    def get_record_detail(self, metadata_uuid: str) -> dict[str, Any]:
        """Fetch a single metadata record detail."""

        self._bootstrap_session()
        detail_url = urljoin(
            f"{self.settings.geonetwork_base_url}/",
            f"{self.settings.geonetwork_record_path.lstrip('/')}/{metadata_uuid}",
        )
        response = self.client.get(detail_url)
        content_type = response.headers.get("content-type", "")
        if response.status_code >= 400:
            self.logger.warning(
                "geonetwork_record_failed",
                extra={
                    "metadata_uuid": metadata_uuid,
                    "status_code": response.status_code,
                    "response_text": response.text[:500],
                },
            )
            return {}
        if "json" in content_type:
            try:
                return response.json()
            except json.JSONDecodeError:
                return {"raw_text": response.text}
        return {"raw_text": response.text}

    def discover_stage_one_sources(self) -> list[SourceRegistryRecord]:
        """Discover the Stage 1 source catalogue records used by the worker."""

        queries = [
            '"Registers of Scotland" AND cadastral AND parcels',
            '"Local Authority Areas" AND Scotland',
            '"council area" AND Scotland AND boundaries',
        ]

        discovered: list[SourceRegistryRecord] = []
        for query in queries:
            hits = self.search_records(query_text=query, size=8)
            for hit in hits:
                record = self._parse_search_hit(hit)
                if record:
                    discovered.append(record)

        if not any("cadastral" in item.source_name.lower() for item in discovered):
            discovered.append(self.build_ros_fallback_record())

        return self._deduplicate(discovered)

    def build_ros_fallback_record(self) -> SourceRegistryRecord:
        """Return a deterministic fallback record for RoS INSPIRE parcels."""

        extent = box(
            ROS_FALLBACK_BOUNDS["west"],
            ROS_FALLBACK_BOUNDS["south"],
            ROS_FALLBACK_BOUNDS["east"],
            ROS_FALLBACK_BOUNDS["north"],
        )
        return SourceRegistryRecord(
            source_name="RoS INSPIRE Cadastral Parcels",
            source_type="dataset",
            publisher="Registers of Scotland",
            metadata_uuid="fallback:ros-inspire-cadastral-parcels",
            endpoint_url=self.settings.ros_view_service_url,
            download_url=self.settings.ros_download_base_url,
            record_json={
                "title": "RoS INSPIRE Cadastral Parcels",
                "abstract": (
                    "INSPIRE Cadastral Parcels is a Registers of Scotland dataset "
                    "showing indicative ownership polygons at ground level in Scotland."
                ),
                "update_frequency": "quarterly",
                "working_crs": "EPSG:27700",
                "discovery_source": "fallback_seed",
            },
            geographic_extent=extent,
            last_seen_at=datetime.now(timezone.utc),
        )

    def _bootstrap_session(self) -> None:
        """Seed cookies and XSRF token expected by the API."""

        if self._session_bootstrapped:
            return
        response = self.client.get(self.settings.geonetwork_base_url)
        response.raise_for_status()
        token = self.client.cookies.get("XSRF-TOKEN")
        if token:
            self.client.headers["X-XSRF-TOKEN"] = token
        self._session_bootstrapped = True

    def _parse_search_hit(self, hit: dict[str, Any]) -> SourceRegistryRecord | None:
        """Convert a raw search hit into a source registry record."""

        source = hit.get("_source", {})
        metadata_uuid = self._best_text(
            source.get("uuid"),
            source.get("metadataIdentifier"),
            source.get("metadataUuid"),
            hit.get("_id"),
        )
        if not metadata_uuid:
            return None

        detail = self.get_record_detail(metadata_uuid)
        record_json = {"search_hit": source, "record_detail": detail}
        title = self._best_text(
            self._nested(source, "resourceTitleObject.default"),
            source.get("resourceTitle"),
            source.get("defaultTitle"),
            metadata_uuid,
        )
        publisher = self._best_text(
            self._nested(source, "orgName.default"),
            self._nested(source, "contactOrg"),
            source.get("orgName"),
        )
        urls = self._extract_urls(record_json)
        endpoint_url = next((item for item in urls if "wfs" in item.lower() or "wms" in item.lower()), None)
        download_url = next(
            (
                item
                for item in urls
                if "download" in item.lower()
                or item.lower().endswith(".zip")
                or item.lower().endswith(".gdb")
            ),
            endpoint_url,
        )

        extent = self._extract_extent_geometry(source) or self._extract_extent_geometry(detail)
        source_type = self._best_text(source.get("resourceType"), source.get("type"), "dataset")

        return SourceRegistryRecord(
            source_name=title,
            source_type=source_type,
            publisher=publisher,
            metadata_uuid=metadata_uuid,
            endpoint_url=endpoint_url,
            download_url=download_url,
            record_json=record_json,
            geographic_extent=extent,
            last_seen_at=datetime.now(timezone.utc),
        )

    def _extract_extent_geometry(self, payload: Any) -> BaseGeometry | None:
        """Attempt to derive a WGS84 extent geometry from a record payload."""

        if payload is None:
            return None
        if isinstance(payload, dict):
            geom_value = payload.get("geom") or payload.get("geometry") or payload.get("geographicExtent")
            if isinstance(geom_value, dict):
                try:
                    geometry = shape(geom_value)
                    if geometry and not geometry.is_empty:
                        return geometry
                except Exception:
                    pass
            bounds = (
                payload.get("westBoundLongitude"),
                payload.get("southBoundLatitude"),
                payload.get("eastBoundLongitude"),
                payload.get("northBoundLatitude"),
            )
            if all(value is not None for value in bounds):
                return box(*[float(value) for value in bounds])
            for value in payload.values():
                geometry = self._extract_extent_geometry(value)
                if geometry is not None:
                    return geometry
        if isinstance(payload, list):
            for item in payload:
                geometry = self._extract_extent_geometry(item)
                if geometry is not None:
                    return geometry
        if isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                return None
            return self._extract_extent_geometry(decoded)
        return None

    def _extract_urls(self, payload: Any) -> list[str]:
        """Recursively extract HTTP URLs from a nested structure."""

        urls: set[str] = set()

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for nested in value.values():
                    visit(nested)
                return
            if isinstance(value, list):
                for nested in value:
                    visit(nested)
                return
            if isinstance(value, str) and re.match(r"^https?://", value):
                urls.add(value)

        visit(payload)
        return sorted(urls)

    def _nested(self, payload: dict[str, Any], dotted_path: str) -> Any:
        """Read a nested dictionary path safely."""

        current: Any = payload
        for segment in dotted_path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(segment)
        return current

    def _best_text(self, *values: Any) -> str | None:
        """Return the first non-empty textual value."""

        for value in values:
            if value is None:
                continue
            if isinstance(value, dict):
                default_value = value.get("default") or value.get("eng")
                if default_value:
                    return str(default_value)
                continue
            if isinstance(value, list):
                if value:
                    return self._best_text(*value)
                continue
            text_value = str(value).strip()
            if text_value:
                return text_value
        return None

    def _deduplicate(self, records: list[SourceRegistryRecord]) -> list[SourceRegistryRecord]:
        """Deduplicate records by metadata UUID."""

        deduped: dict[str, SourceRegistryRecord] = {}
        for record in records:
            key = record.metadata_uuid or record.source_name
            deduped[key] = record
        return list(deduped.values())


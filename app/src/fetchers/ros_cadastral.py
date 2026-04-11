"""Registers of Scotland INSPIRE cadastral parcel download helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import httpx

from config.settings import Settings
from src.models.source_registry import SourceRegistryRecord


ROS_SOURCE_NAME = "RoS INSPIRE Cadastral Parcels"
ROS_SOURCE_PUBLISHER = "Registers of Scotland"

ROS_COUNTIES: list[tuple[str, str]] = [
    ("ABN", "Aberdeen"),
    ("ANG", "Angus"),
    ("ARG", "Argyll"),
    ("AYR", "Ayr"),
    ("BNF", "Banff"),
    ("BER", "Berwick"),
    ("BUT", "Bute"),
    ("CTH", "Caithness"),
    ("CLK", "Clackmannan"),
    ("DMB", "Dumbarton"),
    ("DMF", "Dumfries"),
    ("ELN", "East Lothian"),
    ("FFE", "Fife"),
    ("GLA", "Glasgow"),
    ("INV", "Inverness"),
    ("KNC", "Kincardine"),
    ("KNR", "Kinross"),
    ("KRK", "Kirkcudbright"),
    ("LAN", "Lanark"),
    ("MID", "Midlothian"),
    ("MOR", "Moray"),
    ("NRN", "Nairn"),
    ("OAZ", "Orkney and Zetland"),
    ("PBL", "Peebles"),
    ("PTH", "Perth"),
    ("REN", "Renfrew"),
    ("ROS", "Ross and Cromarty"),
    ("ROX", "Roxburgh"),
    ("SEL", "Selkirk"),
    ("STG", "Stirling"),
    ("STH", "Sutherland"),
    ("WGN", "Wigtown"),
    ("WLN", "West Lothian"),
]


@dataclass(slots=True)
class RosCountyArchive:
    """A downloaded RoS county ZIP archive."""

    county_code: str
    county_name: str
    source_url: str
    local_path: Path


class RoSCadastralFetcher:
    """Download RoS county parcel ZIPs directly over HTTP."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger.getChild("ros_cadastral")
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)

    def close(self) -> None:
        """Close the HTTP client."""

        self.client.close()

    def build_source_registry_record(self) -> SourceRegistryRecord:
        """Return the RoS source record used by the worker."""

        return SourceRegistryRecord(
            source_name=ROS_SOURCE_NAME,
            source_type="dataset",
            publisher=ROS_SOURCE_PUBLISHER,
            metadata_uuid="fallback:ros-inspire-cadastral-parcels",
            endpoint_url=self.settings.ros_view_service_url,
            download_url=self.settings.ros_download_base_url,
            record_json={
                "county_codes": [{"code": code, "title": title} for code, title in ROS_COUNTIES],
                "download_pattern": f"{self.settings.ros_download_base_url}/{{county_code}}",
                "view_service": self.settings.ros_view_service_url,
            },
            geographic_extent=None,
            last_seen_at=None,
        )

    def download_county_archives(self, destination_dir: Path) -> list[RosCountyArchive]:
        """Download every published county ZIP archive."""

        destination_dir.mkdir(parents=True, exist_ok=True)
        archives: list[RosCountyArchive] = []
        for county_code, county_name in ROS_COUNTIES:
            url = f"{self.settings.ros_download_base_url.rstrip('/')}/{county_code}"
            local_path = destination_dir / f"ros_cadastral_{county_code}.zip"

            response = self.client.get(url)
            response.raise_for_status()
            content = response.content
            preview_text = content[:200].decode("utf-8", errors="ignore").lstrip().lower()
            if not content.startswith(b"PK") and (
                preview_text.startswith("<!doctype html")
                or preview_text.startswith("<html")
                or "serviceexception" in preview_text
            ):
                raise RuntimeError(f"RoS returned HTML instead of a ZIP for county code {county_code}.")

            local_path.write_bytes(content)
            self.logger.info(
                "ros_county_archive_downloaded",
                extra={
                    "county_code": county_code,
                    "county_name": county_name,
                    "source_url": url,
                    "local_path": str(local_path),
                    "bytes_downloaded": len(content),
                },
            )
            archives.append(
                RosCountyArchive(
                    county_code=county_code,
                    county_name=county_name,
                    source_url=url,
                    local_path=local_path,
                )
            )
        return archives

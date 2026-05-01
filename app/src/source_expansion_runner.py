"""Operationalise the missing Phase One source estate.

This runner is intentionally separate from ``source_phase_runner`` so the old
planning/HLA loop remains a support path rather than the product spine.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import traceback
from typing import Any
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET
import zipfile

try:
    import duckdb
except ImportError:  # pragma: no cover - command reports this at runtime.
    duckdb = None
import geopandas as gpd
import httpx
import pandas as pd
from shapely import wkb
from shapely.geometry import Point
import yaml
from shapely import force_2d
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry

from config.settings import Settings, get_settings
from src.db import chunked
from src.loaders.supabase_loader import SupabaseLoader
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.models.source_registry import SourceRegistryRecord
from src.source_phase_runner import SourcePhaseRunner, _geometry_hex, _normalize_ref, _polygonize_geometry

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "config" / "phase_one_source_estate.yaml"
NRS_SETTLEMENT_ARCGIS_LAYER_URL = "https://maps.gov.scot/server/rest/services/NRS/NRS/MapServer/5"
NRS_REQUEST_HEADERS = {
    "Accept": "application/geo+json,application/json,*/*",
    "User-Agent": "LandIntel/1.0 (+https://github.com/LDN-reece/LandIntel)",
}

FUTURE_CONTEXT_FAMILIES = {"ela", "vdl"}
POLICY_PACKAGE_FAMILIES = {"ldp"}
SETTLEMENT_BOUNDARY_FAMILIES = {"settlement"}
CONSTRAINT_FAMILIES = {
    "sepa_flood",
    "coal_authority",
    "hes",
    "naturescot",
    "contaminated_land",
    "tpo",
    "culverts",
    "conservation_areas",
    "greenbelt",
}
OPEN_LOCATION_SPINE_FAMILIES = {
    "os_openmap_local",
    "os_open_roads",
    "os_open_rivers",
    "os_boundary_line",
    "os_open_names",
    "os_open_greenspace",
    "os_open_zoomstack",
    "os_open_toid",
    "os_open_built_up_areas",
    "os_open_uprn",
    "os_open_usrn",
    "osm",
    "naptan",
    "statistics_gov_scot",
    "opentopography_srtm",
}
OPEN_LOCATION_SPINE_BULK_FAMILIES = {
    "os_openmap_local",
    "os_open_roads",
    "os_open_rivers",
    "os_boundary_line",
    "os_open_names",
    "os_open_greenspace",
    "os_open_zoomstack",
    "os_open_toid",
    "os_open_built_up_areas",
    "os_open_uprn",
    "os_open_usrn",
    "osm",
}
PROBE_ONLY_FAMILIES = {"topography", "os_places", "os_features", "os_linked_identifiers"} | OPEN_LOCATION_SPINE_FAMILIES

COMMAND_TO_FAMILIES: dict[str, tuple[str, ...]] = {
    "ingest-ldp": ("ldp",),
    "ingest-settlement-boundaries": ("settlement",),
    "ingest-ela": ("ela",),
    "ingest-vdl": ("vdl",),
    "ingest-sepa-flood": ("sepa_flood",),
    "ingest-coal-authority": ("coal_authority",),
    "ingest-hes-designations": ("hes",),
    "ingest-naturescot": ("naturescot",),
    "ingest-contaminated-land": ("contaminated_land",),
    "ingest-tpo": ("tpo",),
    "ingest-culverts": ("culverts",),
    "ingest-conservation-areas": ("conservation_areas",),
    "ingest-greenbelt": ("greenbelt",),
    "ingest-os-topography": ("topography",),
    "ingest-os-places": ("os_places",),
    "ingest-os-features": ("os_features",),
    "ingest-os-linked-identifiers": ("os_linked_identifiers",),
    "ingest-os-openmap-local": ("os_openmap_local",),
    "ingest-os-open-roads": ("os_open_roads",),
    "ingest-os-open-rivers": ("os_open_rivers",),
    "ingest-os-boundary-line": ("os_boundary_line",),
    "ingest-os-open-names": ("os_open_names",),
    "ingest-os-open-greenspace": ("os_open_greenspace",),
    "ingest-os-open-zoomstack": ("os_open_zoomstack",),
    "ingest-os-open-toid": ("os_open_toid",),
    "ingest-os-open-built-up-areas": ("os_open_built_up_areas",),
    "ingest-os-open-uprn": ("os_open_uprn",),
    "ingest-os-open-usrn": ("os_open_usrn",),
    "ingest-osm-overpass": ("osm",),
    "ingest-naptan": ("naptan",),
    "ingest-statistics-gov-scot": ("statistics_gov_scot",),
    "ingest-opentopography-srtm": ("opentopography_srtm",),
    "ingest-open-location-spine": tuple(sorted(OPEN_LOCATION_SPINE_BULK_FAMILIES)),
    "ingest-bulk-download-universe": tuple(sorted(OPEN_LOCATION_SPINE_BULK_FAMILIES))
    + ("naptan", "statistics_gov_scot", "opentopography_srtm"),
    "probe-open-location-spine": (
        "topography",
        "os_openmap_local",
        "os_open_roads",
        "os_open_rivers",
        "os_boundary_line",
        "os_open_names",
        "os_open_greenspace",
        "os_open_zoomstack",
        "os_open_toid",
        "os_open_built_up_areas",
        "os_open_uprn",
        "os_open_usrn",
        "os_linked_identifiers",
        "osm",
        "naptan",
        "statistics_gov_scot",
        "opentopography_srtm",
    ),
}

CONSTRAINT_GROUPS = {
    "sepa_flood": ("flood", "flood_risk", "intersection", 0),
    "coal_authority": ("mining", "mining_risk", "intersection", 0),
    "hes": ("heritage", "heritage_designation", "intersection_and_distance", 50),
    "naturescot": ("environmental", "environmental_designation", "intersection_and_distance", 50),
    "contaminated_land": ("contamination", "contamination", "intersection", 0),
    "tpo": ("trees", "tree_preservation_order", "intersection_and_distance", 25),
    "culverts": ("drainage", "culvert", "intersection_and_distance", 25),
    "conservation_areas": ("heritage", "conservation_area", "intersection", 0),
    "greenbelt": ("policy_constraint", "greenbelt", "intersection", 0),
}

DEFAULT_LAYER_HINTS = {
    "ela": ("pub_els",),
    "vdl": ("pub_vdlPolygon", "pub_vdl"),
    "contaminated_land": ("pub_cntml",),
    "tpo": ("pub_tpopnt", "pub_tpopol"),
    "culverts": ("pub_clvtlin", "pub_clvtpnt", "pub_clvtos"),
    "conservation_areas": ("pub_coar",),
    "greenbelt": ("pub_grnblt",),
}

def _os_download_source(
    *,
    source_family: str,
    source_key: str,
    source_name: str,
    product_id: str,
    source_group: str,
    target_table: str,
    reconciliation_path: str,
    signal_output: str,
    ranking_impact: str,
    data_age_basis: str,
    download_area: str | None = "GB",
) -> dict[str, Any]:
    return {
        "source_key": source_key,
        "source_family": source_family,
        "source_name": source_name,
        "source_group": source_group,
        "phase_one_role": "context",
        "source_status": "live_opendata_metadata",
        "orchestration_mode": "os_downloads_opendata",
        "endpoint_url": f"https://api.os.uk/downloads/v1/products/{product_id}/downloads",
        "auth_env_vars": [],
        "target_table": target_table,
        "reconciliation_path": reconciliation_path,
        "evidence_path": "OS Downloads API metadata until the controlled bulk download adapter lands rows.",
        "signal_output": signal_output,
        "ranking_impact": ranking_impact,
        "resurfacing_trigger": f"{source_name} product refresh or downstream site-context adapter promotion.",
        "data_age_basis": data_age_basis,
        "ranking_eligible": False,
        "review_output_eligible": True,
        "product_id": product_id,
        "download_area": download_area,
        "licence_basis": "OS OpenData",
    }


def _open_probe_source(
    *,
    source_family: str,
    source_key: str,
    source_name: str,
    endpoint_url: str,
    source_group: str,
    orchestration_mode: str,
    target_table: str,
    reconciliation_path: str,
    signal_output: str,
    ranking_impact: str,
    data_age_basis: str,
    auth_env_vars: list[str] | None = None,
    limitation_notes: str | None = None,
) -> dict[str, Any]:
    return {
        "source_key": source_key,
        "source_family": source_family,
        "source_name": source_name,
        "source_group": source_group,
        "phase_one_role": "context",
        "source_status": "live_probe_target",
        "orchestration_mode": orchestration_mode,
        "endpoint_url": endpoint_url,
        "auth_env_vars": auth_env_vars or [],
        "target_table": target_table,
        "reconciliation_path": reconciliation_path,
        "evidence_path": "Source registry and freshness state until adapter lands site-level rows.",
        "signal_output": signal_output,
        "ranking_impact": ranking_impact,
        "resurfacing_trigger": f"{source_name} freshness change or downstream adapter promotion.",
        "data_age_basis": data_age_basis,
        "ranking_eligible": False,
        "review_output_eligible": True,
        "limitation_notes": limitation_notes,
    }


OS_CATALOGUE_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "source_key": "os_downloads_terrain50",
        "source_family": "topography",
        "source_name": "OS Downloads API - OS Terrain 50 OpenData",
        "source_group": "constraints",
        "phase_one_role": "target_live",
        "source_status": "live_target",
        "orchestration_mode": "os_downloads_api",
        "endpoint_url": "https://api.os.uk/downloads/v1/products/Terrain50/downloads",
        "auth_env_vars": [],
        "target_table": "public.constraint_source_features",
        "reconciliation_path": "OS Downloads product metadata -> terrain download adapter -> indicative_only slope overlay",
        "evidence_path": "OS Downloads API metadata until terrain tile adapter is activated",
        "signal_output": "topography_slope after terrain adapter promotion",
        "ranking_impact": "Geometry/constraints drag only; derived area remains indicative_only.",
        "resurfacing_trigger": "OS Terrain product refresh or derived slope-band change.",
        "data_age_basis": "OS Downloads API product metadata.",
        "ranking_eligible": False,
        "review_output_eligible": True,
        "product_id": "Terrain50",
        "download_area": "GB",
        "licence_basis": "OS OpenData",
    },
    {
        "source_key": "os_places_api",
        "source_family": "os_places",
        "source_name": "OS Places API",
        "source_group": "location",
        "phase_one_role": "context",
        "source_status": "live_api",
        "orchestration_mode": "os_places_api",
        "endpoint_url": "https://api.os.uk/search/places/v1/find",
        "auth_env_vars": ["OS_PROJECT_API"],
        "target_table": "landintel.source_estate_registry",
        "reconciliation_path": "on-demand address/postcode enrichment against canonical sites",
        "evidence_path": "source registry until per-site enrichment is activated",
        "signal_output": "location_context after per-site adapter promotion",
        "ranking_impact": "Review context only in Phase One.",
        "resurfacing_trigger": "OS Places API data refresh or per-site address enrichment change.",
        "data_age_basis": "OS Places API live response.",
        "ranking_eligible": False,
        "review_output_eligible": True,
    },
    {
        "source_key": "os_features_api",
        "source_family": "os_features",
        "source_name": "OS Features API",
        "source_group": "location",
        "phase_one_role": "target_live",
        "source_status": "live_target",
        "orchestration_mode": "os_features_wfs",
        "endpoint_url": "https://api.os.uk/features/v1/wfs",
        "auth_env_vars": ["OS_PROJECT_API"],
        "target_table": "public.constraint_source_features",
        "reconciliation_path": "WFS feature request clipped to canonical site AOI, then evidence overlay",
        "evidence_path": "source registry until layer-specific OS feature adapters are activated",
        "signal_output": "access_strength and location_context after layer adapter promotion",
        "ranking_impact": "Access/location context only; no appraisal or pricing logic.",
        "resurfacing_trigger": "OS Features metadata refresh or layer adapter promotion.",
        "data_age_basis": "OS Features WFS capabilities response.",
        "ranking_eligible": False,
        "review_output_eligible": True,
    },
    {
        "source_key": "os_linked_identifiers_api",
        "source_family": "os_linked_identifiers",
        "source_name": "OS Linked Identifiers API",
        "source_group": "location",
        "phase_one_role": "context",
        "source_status": "live_api",
        "orchestration_mode": "os_linked_identifiers_api",
        "endpoint_url": "https://api.os.uk/search/links/v1",
        "auth_env_vars": ["OS_PROJECT_API"],
        "target_table": "landintel.source_estate_registry",
        "reconciliation_path": "UPRN/USRN/TOID cross-reference lookup after site-address linkage is activated",
        "evidence_path": "source registry until site-to-address and TOID enrichment adapters are activated",
        "signal_output": "address_toid_context after enrichment adapter promotion",
        "ranking_impact": "Review context only; no ownership or developability claim.",
        "resurfacing_trigger": "OS Linked Identifiers API data refresh or UPRN/TOID relationship change.",
        "data_age_basis": "OS Linked Identifiers API live response.",
        "ranking_eligible": False,
        "review_output_eligible": True,
    },
    _os_download_source(
        source_family="os_openmap_local",
        source_key="os_downloads_openmap_local",
        source_name="OS Downloads API - OS OpenMap Local OpenData",
        product_id="OpenMapLocal",
        source_group="base_geometry",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS OpenMap Local download metadata -> controlled bulk vector adapter -> generalised base map context.",
        signal_output="base_map_context after OpenMap Local adapter promotion",
        ranking_impact="Base geometry and visual/context layer only; no ownership or precise access claim.",
        data_age_basis="OS Downloads API OpenMap Local product metadata.",
    ),
    _os_download_source(
        source_family="os_open_roads",
        source_key="os_downloads_open_roads",
        source_name="OS Downloads API - OS Open Roads OpenData",
        product_id="OpenRoads",
        source_group="access_context",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open Roads download metadata -> controlled bulk road-link adapter -> physical access proximity context.",
        signal_output="road_access_context after Open Roads adapter promotion",
        ranking_impact="Physical proximity evidence only; no legal access claim.",
        data_age_basis="OS Downloads API Open Roads product metadata.",
    ),
    _os_download_source(
        source_family="os_open_rivers",
        source_key="os_downloads_open_rivers",
        source_name="OS Downloads API - OS Open Rivers OpenData",
        product_id="OpenRivers",
        source_group="hydrography",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open Rivers download metadata -> controlled bulk watercourse adapter -> hydrography and flood-context linkage.",
        signal_output="watercourse_context after Open Rivers adapter promotion",
        ranking_impact="Hydrography context only; no drainage design conclusion.",
        data_age_basis="OS Downloads API Open Rivers product metadata.",
    ),
    _os_download_source(
        source_family="os_boundary_line",
        source_key="os_downloads_boundary_line",
        source_name="OS Downloads API - Boundary-Line OpenData",
        product_id="BoundaryLine",
        source_group="administrative_geography",
        target_table="landintel.source_estate_registry",
        reconciliation_path="Boundary-Line download metadata -> controlled bulk boundary adapter -> authority and reporting geography.",
        signal_output="administrative_geography_context after Boundary-Line adapter promotion",
        ranking_impact="Reporting and authority context only.",
        data_age_basis="OS Downloads API Boundary-Line product metadata.",
    ),
    _os_download_source(
        source_family="os_open_names",
        source_key="os_downloads_open_names",
        source_name="OS Downloads API - OS Open Names OpenData",
        product_id="OpenNames",
        source_group="location_naming",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open Names download metadata -> controlled bulk gazetteer adapter -> settlement and location labels.",
        signal_output="location_label_context after Open Names adapter promotion",
        ranking_impact="Human-readable location context only.",
        data_age_basis="OS Downloads API Open Names product metadata.",
    ),
    _os_download_source(
        source_family="os_open_greenspace",
        source_key="os_downloads_open_greenspace",
        source_name="OS Downloads API - OS Open Greenspace OpenData",
        product_id="OpenGreenspace",
        source_group="amenities",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open Greenspace download metadata -> controlled bulk greenspace adapter -> amenity/open-space proximity context.",
        signal_output="greenspace_context after Open Greenspace adapter promotion",
        ranking_impact="Amenity/open-space context only.",
        data_age_basis="OS Downloads API Open Greenspace product metadata.",
    ),
    _os_download_source(
        source_family="os_open_zoomstack",
        source_key="os_downloads_open_zoomstack",
        source_name="OS Downloads API - OS Open Zoomstack OpenData",
        product_id="OpenZoomstack",
        source_group="base_geometry",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open Zoomstack metadata -> controlled bulk vector adapter -> contextual base geometry.",
        signal_output="base_geometry_context after Zoomstack adapter promotion",
        ranking_impact="Base map context only; no ownership, access or planning conclusion.",
        data_age_basis="OS Downloads API Open Zoomstack product metadata.",
    ),
    _os_download_source(
        source_family="os_open_toid",
        source_key="os_downloads_open_toid",
        source_name="OS Downloads API - OS Open TOID OpenData",
        product_id="OpenTOID",
        source_group="address_context",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open TOID metadata -> controlled identifier adapter -> TOID linkage support.",
        signal_output="toid_context after Open TOID adapter promotion",
        ranking_impact="Identifier linkage support only; no ownership or legal access claim.",
        data_age_basis="OS Downloads API Open TOID product metadata.",
        download_area=None,
    ),
    _os_download_source(
        source_family="os_open_built_up_areas",
        source_key="os_downloads_open_built_up_areas",
        source_name="OS Downloads API - OS Open Built Up Areas OpenData",
        product_id="BuiltUpAreas",
        source_group="settlement_context",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open Built Up Areas metadata -> controlled bulk polygon adapter -> settlement/service context.",
        signal_output="settlement_service_context after Built Up Areas adapter promotion",
        ranking_impact="Settlement context only; no planning consent conclusion.",
        data_age_basis="OS Downloads API Open Built Up Areas product metadata.",
    ),
    _os_download_source(
        source_family="os_open_uprn",
        source_key="os_downloads_open_uprn",
        source_name="OS Downloads API - OS Open UPRN OpenData",
        product_id="OpenUPRN",
        source_group="address_context",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open UPRN download metadata -> controlled bulk UPRN adapter -> site-address linkage support.",
        signal_output="uprn_context after Open UPRN adapter promotion",
        ranking_impact="Address linkage support only; no ownership claim.",
        data_age_basis="OS Downloads API Open UPRN product metadata.",
    ),
    _os_download_source(
        source_family="os_open_usrn",
        source_key="os_downloads_open_usrn",
        source_name="OS Downloads API - OS Open USRN OpenData",
        product_id="OpenUSRN",
        source_group="access_context",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OS Open USRN download metadata -> controlled bulk street identifier adapter -> road naming/linkage support.",
        signal_output="usrn_context after Open USRN adapter promotion",
        ranking_impact="Street identifier context only; no legal access claim.",
        data_age_basis="OS Downloads API Open USRN product metadata.",
    ),
    _open_probe_source(
        source_family="osm",
        source_key="osm_overpass_context",
        source_name="OpenStreetMap Overpass API",
        endpoint_url="https://overpass-api.de/api/interpreter",
        source_group="open_context",
        orchestration_mode="osm_overpass_interpreter",
        target_table="landintel.source_estate_registry",
        reconciliation_path="Overpass status probe -> controlled Overpass/bulk extract adapter -> road names, amenities, land use and indicative power context.",
        signal_output="osm_context after controlled OSM adapter promotion",
        ranking_impact="Context enrichment only; OS/official geometry remains preferred where available.",
        data_age_basis="Overpass API status response.",
        limitation_notes="OSM is community-maintained and must not override official geometry or evidence legal access.",
    ),
    _open_probe_source(
        source_family="naptan",
        source_key="naptan_api_context",
        source_name="NaPTAN API",
        endpoint_url="https://naptan.api.dft.gov.uk/swagger/v1/swagger.json",
        source_group="amenities",
        orchestration_mode="naptan_api_metadata",
        target_table="landintel.source_estate_registry",
        reconciliation_path="NaPTAN API metadata -> controlled transport stop adapter -> public transport amenity proximity.",
        signal_output="public_transport_context after NaPTAN adapter promotion",
        ranking_impact="Transport proximity context only.",
        data_age_basis="NaPTAN API OpenAPI metadata.",
    ),
    _open_probe_source(
        source_family="statistics_gov_scot",
        source_key="statistics_gov_scot_sparql",
        source_name="statistics.gov.scot SPARQL",
        endpoint_url="https://statistics.gov.scot/sparql",
        source_group="demographics",
        orchestration_mode="statistics_gov_scot_sparql",
        target_table="landintel.source_estate_registry",
        reconciliation_path="SPARQL metadata probe -> controlled datazone/intermediate-zone metric adapter -> demographic context.",
        signal_output="demographic_context after statistics.gov.scot adapter promotion",
        ranking_impact="Area-level demographic context only; no demand certainty.",
        data_age_basis="statistics.gov.scot SPARQL response.",
    ),
    _open_probe_source(
        source_family="opentopography_srtm",
        source_key="opentopography_srtm_global",
        source_name="OpenTopography SRTM Global fallback DEM",
        endpoint_url="https://portal.opentopography.org/API/globaldem",
        source_group="terrain",
        orchestration_mode="opentopography_globaldem",
        target_table="landintel.source_estate_registry",
        reconciliation_path="OpenTopography SRTM API probe -> fallback DEM adapter only where OS Terrain 50 is unavailable or used for QA.",
        signal_output="fallback_terrain_context after SRTM adapter promotion",
        ranking_impact="Fallback terrain context only; no engineering certainty.",
        data_age_basis="OpenTopography SRTM API response and DOI metadata.",
        auth_env_vars=["OPENTOPOGRAPHY_API_KEY"],
        limitation_notes="SRTM is fallback terrain. Licence metadata is not sufficient to mark trusted for review without commercial-use confirmation.",
    ),
)

REFERENCE_FIELDS = (
    "source_record_id",
    "id",
    "fid",
    "objectid",
    "OBJECTID",
    "site_id",
    "siteid",
    "site_ref",
    "site_reference",
    "reference",
    "ref",
    "els_id",
    "vdl_id",
)
NAME_FIELDS = ("site_name", "sitename", "name", "title", "description", "site", "address")
STATUS_FIELDS = ("status", "site_status", "planning_status", "class", "category", "type", "condition")
AUTHORITY_FIELDS = ("local_authority", "local_authority_name", "authority_name", "planning_authority", "council", "la_name")
SEVERITY_FIELDS = ("severity", "risk", "risk_level", "category", "flood_risk", "status", "designation")
LDP_POLICY_FIELDS = ("policy_reference", "policy_ref", "policy", "policy_code", "policy_name", "ldp_policy")
LDP_USE_FIELDS = ("proposed_use", "land_use", "use", "use_class", "allocation_use", "proposal", "category", "type")


class SourceExpansionRunner:
    """Ingest, link, evidence, signal, and prove the missing source estate."""

    def __init__(self, settings: Settings, logger: Any) -> None:
        self.settings = settings
        self.logger = logger.getChild("source_expansion")
        self.phase_runner = SourcePhaseRunner(settings, logger)
        self.database = self.phase_runner.database
        self.loader: SupabaseLoader = self.phase_runner.loader
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)
        self.manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8")) or {}
        self.max_features = int(os.getenv("SOURCE_EXPANSION_MAX_FEATURES", "0") or "0")
        self.page_size = int(os.getenv("SOURCE_EXPANSION_PAGE_SIZE", "2000") or "2000")
        self.ldp_max_resources = self._env_int("SOURCE_EXPANSION_LDP_MAX_RESOURCES", 0)
        self.ldp_max_layers_per_resource = self._env_int("SOURCE_EXPANSION_LDP_MAX_LAYERS_PER_RESOURCE", 0)

    def close(self) -> None:
        self.client.close()
        self.phase_runner.close()

    def run_command(self, command: str) -> dict[str, Any]:
        if command == "audit-source-expansion":
            return self.audit_source_expansion()
        if command == "link-sites-to-ros-parcels":
            return self.link_sites_to_ros_parcels()
        if command == "audit-site-parcel-links":
            return self.audit_site_parcel_links()
        if command == "resolve-title-numbers":
            return self.resolve_title_numbers()
        if command == "audit-title-number-control":
            return self.audit_title_number_control()
        if command == "measure-constraints":
            return self.measure_constraints()
        if command == "measure-constraints-duckdb":
            return self.measure_constraints_duckdb()
        if command == "measure-constraints-debug-all-layers":
            return self.measure_constraints_debug_all_layers()
        if command == "audit-constraint-measurements":
            return self.audit_constraint_measurements()
        if command == "promote-ldp-authority-source":
            return self._record_policy_promotion_placeholder("ldp", command)
        if command not in COMMAND_TO_FAMILIES:
            raise KeyError(f"Unsupported source expansion command: {command}")

        results: list[dict[str, Any]] = []
        for source_family in COMMAND_TO_FAMILIES[command]:
            source_results = self._run_family_command(command, source_family)
            results.append(source_results)
        payload = {"command": command, "results": results}
        self.logger.info("source_expansion_command_completed", extra=payload)
        return payload

    def audit_source_expansion(self) -> dict[str, Any]:
        rows = self.database.fetch_all(
            """
            select *
            from analytics.v_phase_one_source_expansion_readiness
            order by priority_rank, source_role, source_family
            """
        )
        live_wired_statuses = {"live_wired_proven", "control_wired_proven", "core_policy_wired_proven"}
        live_wired = [row for row in rows if row.get("live_proof_status") in live_wired_statuses]
        deferred = [row for row in rows if str(row.get("live_proof_status") or "").startswith("explicitly_deferred")]
        incomplete = [
            row
            for row in rows
            if row.get("live_proof_status") not in live_wired_statuses
            and not str(row.get("live_proof_status") or "").startswith("explicitly_deferred")
        ]
        payload = {
            "source_count": len(rows),
            "live_wired_count": len(live_wired),
            "deferred_count": len(deferred),
            "incomplete_count": len(incomplete),
            "incomplete_sources": [row.get("source_family") for row in incomplete],
            "priority_sources": [
                {
                    "priority_rank": row.get("priority_rank"),
                    "source_family": row.get("source_family"),
                    "live_proof_status": row.get("live_proof_status"),
                    "raw_or_feature_rows": row.get("raw_or_feature_rows"),
                    "linked_or_measured_rows": row.get("linked_or_measured_rows"),
                }
                for row in rows
                if int(row.get("priority_rank") or 999) <= 3
            ],
            "matrix": rows,
        }
        self.logger.info("source_expansion_audit", extra=payload)
        return payload

    def link_sites_to_ros_parcels(self) -> dict[str, Any]:
        sources = self._sources_for_family("title_number")
        for source in sources:
            self._upsert_source_estate(source)

        max_candidates = self._env_int("SITE_PARCEL_LINK_MAX_CANDIDATES_PER_SITE", 10)
        max_distance_m = self._env_float("SITE_PARCEL_LINK_MAX_DISTANCE_M", 250.0)
        min_overlap_sqm = self._env_float("SITE_PARCEL_LINK_MIN_OVERLAP_SQM", 1.0)
        min_operational_area_acres = self._env_float("MIN_OPERATIONAL_AREA_ACRES", 0.0)
        site_batch_size = max(self._env_int("SITE_PARCEL_LINK_SITE_BATCH_SIZE", 250), 1)
        proof_counts = self.database.fetch_one(
            """
            with relation_stats as (
                select
                    (
                        select reltuples
                        from pg_class
                        where oid = to_regclass('public.ros_cadastral_parcels')
                    ) as ros_reltuples,
                    (
                        select reltuples
                        from pg_class
                        where oid = to_regclass('landintel.canonical_sites')
                    ) as canonical_site_reltuples
            )
            select
                case
                    when ros_reltuples is null then 0::bigint
                    else greatest(ros_reltuples::bigint, 1::bigint)
                end as ros_parcel_count,
                case
                    when canonical_site_reltuples is null then 0::bigint
                    else greatest(canonical_site_reltuples::bigint, 1::bigint)
                end as canonical_site_count
            from relation_stats
            """
        ) or {}
        anchor_rows = self.database.fetch_all(
            """
            select site_location_id
            from public.constraints_site_anchor()
            where geometry is not null
              and authority_name is not null
              and (
                  cast(:min_operational_area_acres as numeric) <= 0
                  or coalesce(area_acres, public.calculate_area_acres(st_area(geometry)::numeric)) >= cast(:min_operational_area_acres as numeric)
              )
            order by site_location_id
            """,
            {"min_operational_area_acres": min_operational_area_acres},
        )
        proof: dict[str, Any] = {
            "candidate_rows": 0,
            "candidate_site_count": 0,
            "primary_link_rows": 0,
            "exact_overlap_candidate_rows": 0,
            "nearest_candidate_rows": 0,
            "processed_site_count": 0,
            "ros_parcel_count": int(proof_counts.get("ros_parcel_count") or 0),
            "canonical_site_count": int(proof_counts.get("canonical_site_count") or 0),
            "max_candidates_per_site": max_candidates,
            "max_distance_m": max_distance_m,
            "min_overlap_sqm": min_overlap_sqm,
            "min_operational_area_acres": min_operational_area_acres,
            "site_batch_size": site_batch_size,
        }

        if proof["ros_parcel_count"] and anchor_rows:
            self.database.execute(
                """
                delete from public.site_ros_parcel_link_candidates
                where link_source = 'ros_cadastral_site_parcel_link'
                """
            )

            batches = list(chunked(anchor_rows, site_batch_size))
            for batch_index, batch in enumerate(batches, start=1):
                site_location_ids = [
                    str(row["site_location_id"])
                    for row in batch
                    if row.get("site_location_id") is not None
                ]
                if not site_location_ids:
                    continue
                batch_proof = self.database.fetch_one(
                    """
                    select *
                    from public.refresh_site_ros_parcel_link_candidates_for_sites(
                        cast(:max_candidates_per_site as integer),
                        cast(:max_distance_m as numeric),
                        cast(:min_overlap_sqm as numeric),
                        cast(:site_location_ids as jsonb)
                    )
                    """,
                    {
                        "max_candidates_per_site": max_candidates,
                        "max_distance_m": max_distance_m,
                        "min_overlap_sqm": min_overlap_sqm,
                        "site_location_ids": json.dumps(site_location_ids),
                    },
                ) or {}
                for key in (
                    "candidate_rows",
                    "candidate_site_count",
                    "primary_link_rows",
                    "exact_overlap_candidate_rows",
                    "nearest_candidate_rows",
                ):
                    proof[key] = int(proof.get(key) or 0) + int(batch_proof.get(key) or 0)
                proof["processed_site_count"] = int(proof.get("processed_site_count") or 0) + len(site_location_ids)
                self.logger.info(
                    "site_parcel_link_batch_completed",
                    extra={
                        "batch_index": batch_index,
                        "batch_count": len(batches),
                        "site_count": len(site_location_ids),
                        "candidate_rows": int(batch_proof.get("candidate_rows") or 0),
                        "primary_link_rows": int(batch_proof.get("primary_link_rows") or 0),
                    },
                )

        candidate_rows = int(proof.get("candidate_rows") or 0)
        primary_link_rows = int(proof.get("primary_link_rows") or 0)
        ros_parcel_count = int(proof.get("ros_parcel_count") or 0)
        if ros_parcel_count == 0:
            status = "site_parcel_link_blocked_ros_cadastral_missing"
            summary = "RoS cadastral parcels are not populated. Run ingest-ros-cadastral before site-to-parcel linking."
        elif primary_link_rows > 0:
            status = "site_parcel_links_promoted"
            summary = "RoS parcel linker generated candidates and promoted high-confidence primary parcel links for site intelligence."
        elif candidate_rows > 0:
            status = "site_parcel_candidates_need_review"
            summary = "RoS parcel linker generated candidates but did not promote high-confidence primary links."
        else:
            status = "site_parcel_link_no_candidates"
            summary = "No RoS cadastral parcel candidates were found for the current operational site scope."

        result = {
            "command": "link-sites-to-ros-parcels",
            "source_family": "title_number",
            "status": status,
            "summary": summary,
            **proof,
        }
        if sources:
            self._record_source_freshness(
                sources[0],
                "current" if candidate_rows or primary_link_rows else "empty",
                "reachable" if ros_parcel_count else "blocked_missing_ros_cadastral",
                candidate_rows,
                result,
            )
        self._record_expansion_event(
            command_name="link-sites-to-ros-parcels",
            source_key="ros_cadastral_site_parcel_link",
            source_family="title_number",
            status=status,
            raw_rows=candidate_rows,
            linked_rows=primary_link_rows,
            measured_rows=int(proof.get("exact_overlap_candidate_rows") or 0),
            summary=summary,
            metadata=result,
        )
        self.logger.info("site_parcel_link_bridge", extra=result)
        return result

    def resolve_title_numbers(self) -> dict[str, Any]:
        sources = self._sources_for_family("title_number")
        for source in sources:
            self._upsert_source_estate(source)

        max_candidates = self._env_int("TITLE_RESOLUTION_MAX_CANDIDATES_PER_SITE", 10)
        min_overlap_sqm = self._env_float("TITLE_RESOLUTION_MIN_OVERLAP_SQM", 1.0)
        min_operational_area_acres = self._env_float("MIN_OPERATIONAL_AREA_ACRES", 0.0)
        site_batch_size = max(self._env_int("TITLE_RESOLUTION_SITE_BATCH_SIZE", 10), 1)
        parcel_title_batch_size = max(self._env_int("TITLE_RESOLUTION_PARCEL_TITLE_BATCH_SIZE", 0), 0)
        parcel_title_refresh = (
            self._refresh_ros_parcel_title_numbers(parcel_title_batch_size)
            if parcel_title_batch_size > 0
            else {
                "parcel_title_scanned_rows": 0,
                "parcel_title_updated_rows": 0,
                "parcel_title_populated_rows": 0,
            }
        )
        proof_counts = self.database.fetch_one(
            """
            with relation_stats as (
                select
                    (
                        select reltuples
                        from pg_class
                        where oid = to_regclass('public.ros_cadastral_parcels')
                    ) as ros_reltuples,
                    (
                        select reltuples
                        from pg_class
                        where oid = to_regclass('landintel.canonical_sites')
                    ) as canonical_site_reltuples
            )
            select
                case
                    when ros_reltuples is null then 0::bigint
                    else greatest(ros_reltuples::bigint, 1::bigint)
                end as ros_parcel_count,
                0::bigint as parcel_title_count,
                case
                    when canonical_site_reltuples is null then 0::bigint
                    else greatest(canonical_site_reltuples::bigint, 1::bigint)
                end as canonical_site_count
            from relation_stats
            """
        ) or {}
        anchor_rows = self.database.fetch_all(
            """
            select site_location_id
            from public.constraints_site_anchor()
            where geometry is not null
              and authority_name is not null
              and (
                  cast(:min_operational_area_acres as numeric) <= 0
                  or coalesce(area_acres, public.calculate_area_acres(st_area(geometry)::numeric)) >= cast(:min_operational_area_acres as numeric)
              )
            order by site_location_id
            """,
            {"min_operational_area_acres": min_operational_area_acres},
        )
        proof: dict[str, Any] = {
            "candidate_rows": 0,
            "candidate_site_count": 0,
            "promoted_title_rows": 0,
            "probable_title_rows": 0,
            "licensed_bridge_required_rows": 0,
            "ros_parcel_count": int(proof_counts.get("ros_parcel_count") or 0),
            "parcel_title_count": int(proof_counts.get("parcel_title_count") or 0),
            "canonical_site_count": int(proof_counts.get("canonical_site_count") or 0),
            "processed_site_count": 0,
            "min_operational_area_acres": min_operational_area_acres,
            "site_batch_size": site_batch_size,
            "parcel_title_batch_size": parcel_title_batch_size,
            **parcel_title_refresh,
        }

        if proof["ros_parcel_count"] and anchor_rows:
            self.database.execute(
                """
                delete from public.site_title_validation
                where validation_method = 'spatial_intersection'
                  and title_source = 'ros_cadastral_spatial_intersection'
                """
            )
            self.database.execute(
                """
                delete from public.site_title_resolution_candidates
                where candidate_source = 'ros_cadastral_spatial_intersection'
                """
            )

            batches = list(chunked(anchor_rows, site_batch_size))
            for batch_index, batch in enumerate(batches, start=1):
                site_location_ids = [
                    str(row["site_location_id"])
                    for row in batch
                    if row.get("site_location_id") is not None
                ]
                if not site_location_ids:
                    continue
                batch_proof = self.database.fetch_one(
                    """
                    select *
                    from public.refresh_site_title_resolution_bridge_for_sites(
                        cast(:max_candidates_per_site as integer),
                        cast(:min_overlap_sqm as numeric),
                        cast(:site_location_ids as jsonb)
                    )
                    """,
                    {
                        "max_candidates_per_site": max_candidates,
                        "min_overlap_sqm": min_overlap_sqm,
                        "site_location_ids": json.dumps(site_location_ids),
                    },
                ) or {}
                for key in (
                    "candidate_rows",
                    "candidate_site_count",
                    "promoted_title_rows",
                    "probable_title_rows",
                    "licensed_bridge_required_rows",
                ):
                    proof[key] = int(proof.get(key) or 0) + int(batch_proof.get(key) or 0)
                proof["processed_site_count"] = int(proof.get("processed_site_count") or 0) + len(site_location_ids)
                proof["ros_parcel_count"] = int(batch_proof.get("ros_parcel_count") or proof["ros_parcel_count"] or 0)
                proof["canonical_site_count"] = int(
                    batch_proof.get("canonical_site_count") or proof["canonical_site_count"] or 0
                )
                self.logger.info(
                    "title_resolution_batch_completed",
                    extra={
                        "batch_index": batch_index,
                        "batch_count": len(batches),
                        "site_count": len(site_location_ids),
                        "candidate_rows": int(batch_proof.get("candidate_rows") or 0),
                        "promoted_title_rows": int(batch_proof.get("promoted_title_rows") or 0),
                    },
                )

        candidate_rows = int(proof.get("candidate_rows") or 0)
        candidate_site_count = int(proof.get("candidate_site_count") or 0)
        promoted_title_rows = int(proof.get("promoted_title_rows") or 0)
        licensed_bridge_required_rows = int(proof.get("licensed_bridge_required_rows") or 0)
        ros_parcel_count = int(proof.get("ros_parcel_count") or 0)
        canonical_site_count = int(proof.get("canonical_site_count") or 0)

        if ros_parcel_count == 0:
            status = "title_bridge_blocked_ros_cadastral_missing"
            summary = "RoS cadastral parcels are not populated. Run ingest-ros-cadastral before resolving title numbers."
        elif canonical_site_count == 0:
            status = "title_bridge_blocked_canonical_sites_missing"
            summary = "Canonical site geometry is not populated, so title candidates cannot be spatially resolved."
        elif promoted_title_rows > 0:
            status = "title_bridge_probable_titles_promoted"
            summary = "RoS cadastral spatial bridge generated title candidates and promoted valid title-number-shaped values for control review."
        elif candidate_rows > 0:
            status = "title_bridge_candidates_need_licensed_bridge"
            summary = "RoS cadastral spatial bridge generated candidates, but no candidate exposes a valid ScotLIS title number without the licensed title bridge."
        else:
            status = "title_bridge_no_spatial_matches"
            summary = "No RoS cadastral parcels intersected canonical site geometry."

        result = {
            "command": "resolve-title-numbers",
            "source_family": "title_number",
            "status": status,
            "summary": summary,
            "max_candidates_per_site": max_candidates,
            "min_overlap_sqm": min_overlap_sqm,
            **proof,
        }

        if sources:
            self._record_source_freshness(
                sources[0],
                "current" if candidate_rows or promoted_title_rows else "empty",
                "reachable" if ros_parcel_count else "blocked_missing_ros_cadastral",
                candidate_rows,
                result,
            )
        self._record_expansion_event(
            command_name="resolve-title-numbers",
            source_key=sources[0]["source_key"] if sources else "title_number_control_spine",
            source_family="title_number",
            status=status,
            raw_rows=candidate_rows,
            linked_rows=candidate_site_count,
            measured_rows=promoted_title_rows,
            summary=summary,
            metadata=result,
        )
        self.logger.info("title_number_resolution_bridge", extra=result)
        return result

    def audit_site_parcel_links(self) -> dict[str, Any]:
        summary = self.database.fetch_one(
            """
            with link_candidates as (
                select
                    count(*)::bigint as parcel_candidate_rows,
                    count(*) filter (where link_status = 'primary')::bigint as primary_candidate_rows,
                    count(*) filter (where match_method = 'exact_overlap_candidate')::bigint as exact_overlap_candidate_rows,
                    count(*) filter (where match_method = 'nearest_centroid')::bigint as nearest_candidate_rows,
                    count(distinct site_id)::bigint as candidate_site_count,
                    max(updated_at) as latest_candidate_at
                from public.site_ros_parcel_link_candidates
                where link_source = 'ros_cadastral_site_parcel_link'
            ), primary_sites as (
                select
                    count(*)::bigint as primary_ros_parcel_site_rows
                from landintel.canonical_sites
                where primary_ros_parcel_id is not null
            )
            select *
            from link_candidates
            cross join primary_sites
            """
        ) or {}
        parcel_candidate_rows = int(summary.get("parcel_candidate_rows") or 0)
        primary_candidate_rows = int(summary.get("primary_candidate_rows") or 0)
        primary_ros_parcel_site_rows = int(summary.get("primary_ros_parcel_site_rows") or 0)
        if primary_candidate_rows > 0 and primary_ros_parcel_site_rows > 0:
            status = "site_parcel_links_wired_proven"
        elif parcel_candidate_rows > 0:
            status = "site_parcel_candidates_need_review"
        else:
            status = "site_parcel_links_not_yet_populated"
        result = {
            "command": "audit-site-parcel-links",
            "source_family": "title_number",
            "status": status,
            "summary": "RoS site-to-parcel link spine audited from public.site_ros_parcel_link_candidates and landintel.canonical_sites.primary_ros_parcel_id.",
            **summary,
        }
        self._record_expansion_event(
            command_name="audit-site-parcel-links",
            source_key="ros_cadastral_site_parcel_link",
            source_family="title_number",
            status=status,
            raw_rows=parcel_candidate_rows,
            linked_rows=primary_candidate_rows,
            measured_rows=int(summary.get("exact_overlap_candidate_rows") or 0),
            summary=result["summary"],
            metadata=result,
        )
        self.logger.info("site_parcel_link_audit", extra=result)
        return result

    def _refresh_ros_parcel_title_numbers(self, batch_size: int) -> dict[str, int]:
        total_updated = 0
        total_titles_populated = 0
        total_scanned = 0
        batch_index = 0
        after_id: str | None = None
        while True:
            batch_index += 1
            proof = self.database.fetch_one(
                """
                with candidate_batch as (
                    select
                        parcel.id,
                        parcel.title_number,
                        parcel.normalized_title_number,
                        public.extract_ros_title_number_candidate(parcel.raw_attributes, parcel.ros_inspire_id) as candidate_title_number
                    from public.ros_cadastral_parcels as parcel
                    where parcel.ros_inspire_id is not null
                      and (cast(:after_id as uuid) is null or parcel.id > cast(:after_id as uuid))
                    order by id
                    limit :batch_size
                ), prepared as (
                    select
                        id,
                        candidate_title_number,
                        public.normalize_site_title_number(candidate_title_number) as candidate_normalized_title_number
                    from candidate_batch
                    where title_number is distinct from candidate_title_number
                       or normalized_title_number is distinct from public.normalize_site_title_number(candidate_title_number)
                ), updated as (
                    update public.ros_cadastral_parcels as parcel
                    set title_number = prepared.candidate_title_number,
                        normalized_title_number = prepared.candidate_normalized_title_number,
                        updated_at = now()
                    from prepared
                    where parcel.id = prepared.id
                    returning prepared.candidate_title_number
                )
                select
                    (select count(*)::bigint from candidate_batch) as scanned_rows,
                    (select id::text from candidate_batch order by id desc limit 1) as last_id,
                    (select count(*)::bigint from updated) as updated_rows,
                    (select count(*) filter (where candidate_title_number is not null)::bigint from updated) as title_rows
                """,
                {"batch_size": batch_size, "after_id": after_id},
            ) or {}
            scanned_rows = int(proof.get("scanned_rows") or 0)
            updated_rows = int(proof.get("updated_rows") or 0)
            title_rows = int(proof.get("title_rows") or 0)
            last_id = proof.get("last_id")
            if scanned_rows == 0 or not last_id:
                break
            after_id = str(last_id)
            total_scanned += scanned_rows
            total_updated += updated_rows
            total_titles_populated += title_rows
            self.logger.info(
                "ros_parcel_title_batch_completed",
                extra={
                    "batch_index": batch_index,
                    "batch_size": batch_size,
                    "scanned_rows": scanned_rows,
                    "updated_rows": updated_rows,
                    "title_rows": title_rows,
                },
            )
            if scanned_rows < batch_size:
                break
        return {
            "parcel_title_scanned_rows": total_scanned,
            "parcel_title_updated_rows": total_updated,
            "parcel_title_populated_rows": total_titles_populated,
        }

    def audit_title_number_control(self) -> dict[str, Any]:
        summary = self.database.fetch_one(
            """
            with title_validation as (
                select
                    count(*)::bigint as title_rows,
                    count(*) filter (
                        where validation_status in ('matched', 'probable', 'manual_review')
                    )::bigint as review_ready_rows,
                    count(distinct site_id)::bigint as site_count,
                    count(distinct normalized_title_number)::bigint as normalized_title_count,
                    max(updated_at) as latest_title_validation_at
                from public.site_title_validation
            ), bridge_candidates as (
                select
                    count(*)::bigint as title_candidate_rows,
                    count(*) filter (where resolution_status = 'probable_title')::bigint as probable_title_candidate_rows,
                    count(*) filter (where resolution_status = 'needs_licensed_bridge')::bigint as licensed_bridge_required_rows,
                    count(distinct site_id)::bigint as candidate_site_count,
                    max(updated_at) as latest_title_candidate_at
                from public.site_title_resolution_candidates
            )
            select *
            from title_validation
            cross join bridge_candidates
            """
        ) or {}
        title_rows = int(summary.get("title_rows") or 0)
        review_ready_rows = int(summary.get("review_ready_rows") or 0)
        title_candidate_rows = int(summary.get("title_candidate_rows") or 0)
        licensed_bridge_required_rows = int(summary.get("licensed_bridge_required_rows") or 0)
        if title_rows and review_ready_rows:
            status = "control_wired_proven"
        elif title_candidate_rows:
            status = (
                "title_bridge_candidates_need_licensed_bridge"
                if licensed_bridge_required_rows
                else "control_title_candidates_need_review"
            )
        else:
            status = "control_source_not_yet_populated"
        result = {
            "command": "audit-title-number-control",
            "source_family": "title_number",
            "status": status,
            "summary": "Title number control spine audited from public.site_title_validation and the RoS cadastral bridge.",
            **summary,
        }
        self._record_expansion_event(
            command_name="audit-title-number-control",
            source_key="title_number_control_spine",
            source_family="title_number",
            status=status,
            raw_rows=title_rows + title_candidate_rows,
            linked_rows=max(int(summary.get("site_count") or 0), int(summary.get("candidate_site_count") or 0)),
            measured_rows=review_ready_rows,
            summary=result["summary"],
            metadata=result,
        )
        self.logger.info("title_number_control_audit", extra=result)
        return result

    def measure_constraints(self) -> dict[str, Any]:
        layer_key_filter = (os.getenv("CONSTRAINT_MEASURE_LAYER_KEY") or "").strip() or None
        source_family_filter = (os.getenv("CONSTRAINT_MEASURE_SOURCE_FAMILY") or "").strip() or None
        authority_filter = (os.getenv("CONSTRAINT_MEASURE_AUTHORITY") or "").strip() or None
        batch_size = max(self._env_int("CONSTRAINT_MEASURE_SITE_BATCH_SIZE", 250), 1)
        max_batches = max(self._env_int("CONSTRAINT_MEASURE_MAX_BATCHES", 0), 0)
        max_batches_per_layer = max(
            self._env_int(
                "CONSTRAINT_MEASURE_MAX_BATCHES_PER_LAYER",
                1 if max_batches else 0,
            ),
            0,
        )
        max_layers_per_run = max(self._env_int("CONSTRAINT_MEASURE_MAX_LAYERS_PER_RUN", 1), 0)
        runtime_minutes = max(self._env_int("CONSTRAINT_MEASURE_RUNTIME_MINUTES", 40), 1)
        min_area_acres = self._env_float("CONSTRAINT_MEASURE_MIN_AREA_ACRES", 0.0)
        overlap_delta_pct = self._env_float("CONSTRAINT_MATERIAL_OVERLAP_DELTA_PCT", 1.0)
        distance_delta_m = self._env_float("CONSTRAINT_MATERIAL_DISTANCE_DELTA_M", 25.0)
        refresh_existing = str(os.getenv("CONSTRAINT_MEASURE_REFRESH_EXISTING") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "force",
        }

        for source_family in sorted(CONSTRAINT_FAMILIES):
            for source in self._sources_for_family(source_family):
                self._upsert_source_estate(source)

        layers = self.database.fetch_all(
            """
            with scan_state as (
                select
                    constraint_layer_id,
                    count(*)::integer as scan_state_row_count
                from public.site_constraint_measurement_scan_state
                group by constraint_layer_id
            ), layer_rows as (
                select
                    layer.layer_key,
                    layer.layer_name,
                    layer.source_family,
                    count(feature.id)::integer as source_feature_count,
                    coalesce(scan_state.scan_state_row_count, 0)::integer as scan_state_row_count,
                    case layer.source_family
                        when 'sepa_flood' then 0
                        when 'tpo' then 1
                        when 'conservation_areas' then 2
                        when 'naturescot' then 3
                        when 'hes' then 4
                        when 'greenbelt' then 5
                        when 'contaminated_land' then 6
                        when 'culverts' then 7
                        when 'coal_authority' then 8
                        else 9
                    end as source_family_priority
                from public.constraint_layer_registry as layer
                join public.constraint_source_features as feature
                  on feature.constraint_layer_id = layer.id
                left join scan_state
                  on scan_state.constraint_layer_id = layer.id
                where layer.is_active = true
                  and (
                      cast(:layer_key_filter as text) is null
                      or layer.layer_key = cast(:layer_key_filter as text)
                  )
                  and (
                      cast(:source_family_filter as text) is null
                      or layer.source_family = cast(:source_family_filter as text)
                  )
                group by
                    layer.id,
                    layer.layer_key,
                    layer.layer_name,
                    layer.source_family,
                    scan_state.scan_state_row_count
            ), ranked_layers as (
                select
                    *,
                    row_number() over (
                        partition by source_family
                        order by scan_state_row_count, layer_key
                    ) as source_family_layer_rank
                from layer_rows
            )
            select
                layer.layer_key,
                layer.layer_name,
                layer.source_family,
                layer.source_feature_count,
                layer.scan_state_row_count
            from ranked_layers as layer
            order by
                layer.source_family_layer_rank,
                layer.scan_state_row_count,
                layer.source_family_priority,
                layer.layer_key
            """,
            {"layer_key_filter": layer_key_filter, "source_family_filter": source_family_filter},
        )

        deadline = datetime.now(timezone.utc) + timedelta(minutes=runtime_minutes)
        proof: dict[str, Any] = {
            "command": "measure-constraints",
            "layer_key_filter": layer_key_filter,
            "source_family_filter": source_family_filter,
            "authority_filter": authority_filter,
            "batch_size": batch_size,
            "max_batches": max_batches,
            "max_batches_per_layer": max_batches_per_layer,
            "max_layers_per_run": max_layers_per_run,
            "runtime_minutes": runtime_minutes,
            "min_area_acres": min_area_acres,
            "refresh_existing": refresh_existing,
            "material_overlap_delta_pct": overlap_delta_pct,
            "material_distance_delta_m": distance_delta_m,
            "layer_count": len(layers),
            "source_feature_count": sum(int(layer.get("source_feature_count") or 0) for layer in layers),
            "processed_site_count": 0,
            "processed_batch_count": 0,
            "measurement_count": 0,
            "summary_count": 0,
            "friction_fact_count": 0,
            "evidence_count": 0,
            "signal_count": 0,
            "change_event_count": 0,
            "affected_site_count": 0,
            "material_change_count": 0,
            "failed_batch_count": 0,
            "failed_layers": [],
            "layers": [],
        }

        for layer in layers:
            if datetime.now(timezone.utc) >= deadline:
                break
            if max_batches and proof["processed_batch_count"] >= max_batches:
                break
            if max_layers_per_run and len(proof["layers"]) >= max_layers_per_run:
                break

            layer_key = str(layer["layer_key"])
            layer_payload: dict[str, Any] = {
                "layer_key": layer_key,
                "layer_name": layer.get("layer_name"),
                "source_family": layer.get("source_family"),
                "source_feature_count": int(layer.get("source_feature_count") or 0),
                "starting_scan_state_row_count": int(layer.get("scan_state_row_count") or 0),
                "processed_site_count": 0,
                "processed_batch_count": 0,
                "measurement_count": 0,
                "summary_count": 0,
                "friction_fact_count": 0,
                "evidence_count": 0,
                "signal_count": 0,
                "change_event_count": 0,
                "affected_site_count": 0,
                "material_change_count": 0,
                "failed_batch_count": 0,
                "last_error": None,
            }
            after_site_location_id: str | None = None

            while datetime.now(timezone.utc) < deadline:
                if max_batches and proof["processed_batch_count"] >= max_batches:
                    break
                if max_batches_per_layer and layer_payload["processed_batch_count"] >= max_batches_per_layer:
                    break
                site_rows = self.database.fetch_all(
                    """
                    with layer_row as (
                        select id
                        from public.constraint_layer_registry
                        where layer_key = :layer_key
                    ), anchor as (
                        select *
                        from public.constraints_site_anchor()
                        where geometry is not null
                          and (
                              cast(:authority_filter as text) is null
                              or lower(authority_name) = lower(cast(:authority_filter as text))
                          )
                          and (
                              cast(:after_site_location_id as text) is null
                              or site_location_id > cast(:after_site_location_id as text)
                          )
                          and (
                              cast(:min_area_acres as numeric) <= 0
                              or coalesce(area_acres, public.calculate_area_acres(st_area(geometry)::numeric))
                                  >= cast(:min_area_acres as numeric)
                          )
                    )
                    select anchor.site_location_id
                    from anchor
                    cross join layer_row
                    where (
                        cast(:refresh_existing as boolean)
                        or not exists (
                            select 1
                            from public.site_constraint_measurement_scan_state as scan_state
                            where scan_state.constraint_layer_id = layer_row.id
                              and scan_state.site_location_id = anchor.site_location_id
                              and scan_state.scan_scope = 'canonical_site_geometry'
                        )
                    )
                    order by anchor.site_location_id
                    limit :batch_size
                    """,
                    {
                        "layer_key": layer_key,
                        "authority_filter": authority_filter,
                        "after_site_location_id": after_site_location_id,
                        "min_area_acres": min_area_acres,
                        "refresh_existing": refresh_existing,
                        "batch_size": batch_size,
                    },
                )
                if not site_rows:
                    break

                site_location_ids = [str(row["site_location_id"]) for row in site_rows]
                try:
                    batch_proof = self.database.fetch_one(
                        """
                        select *
                        from public.refresh_constraint_measurements_for_layer_sites(
                            :layer_key,
                            cast(:site_location_ids as text[]),
                            cast(:overlap_delta_pct as numeric),
                            cast(:distance_delta_m as numeric)
                        )
                        """,
                        {
                            "layer_key": layer_key,
                            "site_location_ids": site_location_ids,
                            "overlap_delta_pct": overlap_delta_pct,
                            "distance_delta_m": distance_delta_m,
                        },
                    ) or {}
                except Exception as exc:
                    error_message = str(exc)
                    layer_payload["failed_batch_count"] += 1
                    layer_payload["last_error"] = error_message[:1000]
                    proof["failed_batch_count"] += 1
                    proof["failed_layers"].append(
                        {
                            "layer_key": layer_key,
                            "site_count": len(site_location_ids),
                            "last_site_location_id": site_location_ids[-1],
                            "error": error_message[:1000],
                        }
                    )
                    self.logger.warning(
                        "constraint_measurement_batch_failed",
                        extra={
                            "layer_key": layer_key,
                            "site_batch_count": len(site_location_ids),
                            "last_site_location_id": site_location_ids[-1],
                            "error": error_message,
                        },
                    )
                    break
                after_site_location_id = site_location_ids[-1]
                layer_payload["processed_site_count"] += len(site_location_ids)
                layer_payload["processed_batch_count"] += 1
                proof["processed_site_count"] += len(site_location_ids)
                proof["processed_batch_count"] += 1

                for key in (
                    "measurement_count",
                    "summary_count",
                    "friction_fact_count",
                    "evidence_count",
                    "signal_count",
                    "change_event_count",
                    "affected_site_count",
                    "material_change_count",
                ):
                    increment = int(batch_proof.get(key) or 0)
                    layer_payload[key] += increment
                    proof[key] += increment

                self.logger.info(
                    "constraint_measurement_batch_completed",
                    extra={
                        "layer_key": layer_key,
                        "site_batch_count": len(site_location_ids),
                        "last_site_location_id": after_site_location_id,
                        **batch_proof,
                    },
                )

            proof["layers"].append(layer_payload)

        if proof["failed_batch_count"]:
            status = "constraint_measurements_completed_with_layer_failures"
        elif proof["measurement_count"]:
            status = "constraint_measurements_refreshed"
        elif proof["processed_site_count"]:
            status = "constraint_measurements_scanned_no_material_change"
        else:
            status = "constraint_measurements_no_pending_sites"
        proof["status"] = status

        self._record_expansion_event(
            command_name="measure-constraints",
            source_key=layer_key_filter,
            source_family=source_family_filter or "constraint_measurement",
            status=status,
            raw_rows=int(proof["source_feature_count"]),
            linked_rows=int(proof["affected_site_count"]),
            measured_rows=int(proof["measurement_count"]),
            evidence_rows=int(proof["evidence_count"]),
            signal_rows=int(proof["signal_count"]),
            change_event_rows=int(proof["change_event_count"]),
            summary="Constraint measurements refreshed in site batches; downstream proof rows emitted only for material evidence-state changes.",
            metadata=proof,
        )
        self.logger.info("constraint_measurement_command_completed", extra=proof)
        return proof

    def measure_constraints_duckdb(self) -> dict[str, Any]:
        """Accelerate constraint/flood scans with a local DuckDB spatial prefilter.

        DuckDB is deliberately not the source of truth here. It narrows each
        bounded site batch to likely site/layer relationships, then the existing
        PostGIS finalizer writes measurements, summaries, evidence, signals and
        scan state.
        """

        if duckdb is None:
            raise RuntimeError("duckdb is required for measure-constraints-duckdb. Install app requirements.")

        layer_key_filter = (os.getenv("CONSTRAINT_MEASURE_LAYER_KEY") or "").strip() or None
        source_family_filter = (os.getenv("CONSTRAINT_MEASURE_SOURCE_FAMILY") or "").strip() or None
        authority_filter = (os.getenv("CONSTRAINT_MEASURE_AUTHORITY") or "").strip() or None
        batch_size = max(self._env_int("CONSTRAINT_MEASURE_SITE_BATCH_SIZE", 250), 1)
        max_batches = max(self._env_int("CONSTRAINT_MEASURE_MAX_BATCHES", 0), 0)
        max_batches_per_layer = max(
            self._env_int(
                "CONSTRAINT_MEASURE_MAX_BATCHES_PER_LAYER",
                1 if max_batches else 0,
            ),
            0,
        )
        max_layers_per_run = max(self._env_int("CONSTRAINT_MEASURE_MAX_LAYERS_PER_RUN", 1), 0)
        runtime_minutes = max(self._env_int("CONSTRAINT_MEASURE_RUNTIME_MINUTES", 40), 1)
        min_area_acres = self._env_float("CONSTRAINT_MEASURE_MIN_AREA_ACRES", 0.0)
        overlap_delta_pct = self._env_float("CONSTRAINT_MATERIAL_OVERLAP_DELTA_PCT", 1.0)
        distance_delta_m = self._env_float("CONSTRAINT_MATERIAL_DISTANCE_DELTA_M", 25.0)
        max_features_per_batch = max(self._env_int("CONSTRAINT_DUCKDB_MAX_FEATURES_PER_BATCH", 10000), 1)
        refresh_existing = str(os.getenv("CONSTRAINT_MEASURE_REFRESH_EXISTING") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "force",
        }

        for source_family in sorted(CONSTRAINT_FAMILIES):
            for source in self._sources_for_family(source_family):
                self._upsert_source_estate(source)

        layers = self.database.fetch_all(
            """
            with scan_state as (
                select
                    constraint_layer_id,
                    count(*)::integer as scan_state_row_count
                from public.site_constraint_measurement_scan_state
                group by constraint_layer_id
            ), layer_rows as (
                select
                    layer.id::text as constraint_layer_id,
                    layer.layer_key,
                    layer.layer_name,
                    layer.source_family,
                    layer.buffer_distance_m,
                    count(feature.id)::integer as source_feature_count,
                    coalesce(scan_state.scan_state_row_count, 0)::integer as scan_state_row_count,
                    case layer.source_family
                        when 'sepa_flood' then 0
                        when 'tpo' then 1
                        when 'conservation_areas' then 2
                        when 'naturescot' then 3
                        when 'hes' then 4
                        when 'greenbelt' then 5
                        when 'contaminated_land' then 6
                        when 'culverts' then 7
                        when 'coal_authority' then 8
                        else 9
                    end as source_family_priority
                from public.constraint_layer_registry as layer
                join public.constraint_source_features as feature
                  on feature.constraint_layer_id = layer.id
                left join scan_state
                  on scan_state.constraint_layer_id = layer.id
                where layer.is_active = true
                  and (
                      cast(:layer_key_filter as text) is null
                      or layer.layer_key = cast(:layer_key_filter as text)
                  )
                  and (
                      cast(:source_family_filter as text) is null
                      or layer.source_family = cast(:source_family_filter as text)
                  )
                group by
                    layer.id,
                    layer.layer_key,
                    layer.layer_name,
                    layer.source_family,
                    layer.buffer_distance_m,
                    scan_state.scan_state_row_count
            ), ranked_layers as (
                select
                    *,
                    row_number() over (
                        partition by source_family
                        order by scan_state_row_count, layer_key
                    ) as source_family_layer_rank
                from layer_rows
            )
            select
                layer.constraint_layer_id,
                layer.layer_key,
                layer.layer_name,
                layer.source_family,
                layer.buffer_distance_m,
                layer.source_feature_count,
                layer.scan_state_row_count
            from ranked_layers as layer
            order by
                layer.source_family_layer_rank,
                layer.scan_state_row_count,
                layer.source_family_priority,
                layer.layer_key
            """,
            {"layer_key_filter": layer_key_filter, "source_family_filter": source_family_filter},
        )

        deadline = datetime.now(timezone.utc) + timedelta(minutes=runtime_minutes)
        proof: dict[str, Any] = {
            "command": "measure-constraints-duckdb",
            "backend": "duckdb_spatial_prefilter_postgis_finalizer",
            "layer_key_filter": layer_key_filter,
            "source_family_filter": source_family_filter,
            "authority_filter": authority_filter,
            "batch_size": batch_size,
            "max_batches": max_batches,
            "max_batches_per_layer": max_batches_per_layer,
            "max_layers_per_run": max_layers_per_run,
            "runtime_minutes": runtime_minutes,
            "min_area_acres": min_area_acres,
            "refresh_existing": refresh_existing,
            "max_features_per_batch": max_features_per_batch,
            "layer_count": len(layers),
            "source_feature_count": sum(int(layer.get("source_feature_count") or 0) for layer in layers),
            "processed_site_count": 0,
            "processed_batch_count": 0,
            "duckdb_candidate_site_count": 0,
            "duckdb_no_candidate_site_count": 0,
            "duckdb_prefilter_feature_count": 0,
            "duckdb_spatial_exact_batch_count": 0,
            "duckdb_bbox_fallback_batch_count": 0,
            "feature_truncated_batch_count": 0,
            "no_relationship_scan_state_count": 0,
            "postgis_finalizer_site_count": 0,
            "measurement_count": 0,
            "summary_count": 0,
            "friction_fact_count": 0,
            "evidence_count": 0,
            "signal_count": 0,
            "change_event_count": 0,
            "affected_site_count": 0,
            "material_change_count": 0,
            "failed_batch_count": 0,
            "failed_layers": [],
            "layers": [],
        }

        for layer in layers:
            if datetime.now(timezone.utc) >= deadline:
                break
            if max_batches and proof["processed_batch_count"] >= max_batches:
                break
            if max_layers_per_run and len(proof["layers"]) >= max_layers_per_run:
                break

            layer_key = str(layer["layer_key"])
            buffer_distance_m = float(layer.get("buffer_distance_m") or 0)
            layer_payload: dict[str, Any] = {
                "layer_key": layer_key,
                "layer_name": layer.get("layer_name"),
                "source_family": layer.get("source_family"),
                "source_feature_count": int(layer.get("source_feature_count") or 0),
                "starting_scan_state_row_count": int(layer.get("scan_state_row_count") or 0),
                "buffer_distance_m": buffer_distance_m,
                "processed_site_count": 0,
                "processed_batch_count": 0,
                "duckdb_candidate_site_count": 0,
                "duckdb_no_candidate_site_count": 0,
                "duckdb_prefilter_feature_count": 0,
                "duckdb_spatial_exact_batch_count": 0,
                "duckdb_bbox_fallback_batch_count": 0,
                "feature_truncated_batch_count": 0,
                "no_relationship_scan_state_count": 0,
                "postgis_finalizer_site_count": 0,
                "measurement_count": 0,
                "summary_count": 0,
                "friction_fact_count": 0,
                "evidence_count": 0,
                "signal_count": 0,
                "change_event_count": 0,
                "affected_site_count": 0,
                "material_change_count": 0,
                "failed_batch_count": 0,
                "last_error": None,
            }
            after_site_location_id: str | None = None

            while datetime.now(timezone.utc) < deadline:
                if max_batches and proof["processed_batch_count"] >= max_batches:
                    break
                if max_batches_per_layer and layer_payload["processed_batch_count"] >= max_batches_per_layer:
                    break

                site_rows = self._constraint_measurement_site_rows(
                    layer_key=layer_key,
                    authority_filter=authority_filter,
                    after_site_location_id=after_site_location_id,
                    min_area_acres=min_area_acres,
                    refresh_existing=refresh_existing,
                    batch_size=batch_size,
                )
                if not site_rows:
                    break

                try:
                    prepared_sites = self._prepare_duckdb_geometry_rows(site_rows, id_column="site_location_id")
                    site_location_ids = [str(row["site_location_id"]) for row in site_rows]
                    feature_rows = self._constraint_feature_rows_for_duckdb_batch(
                        layer_key=layer_key,
                        prepared_sites=prepared_sites,
                        buffer_distance_m=buffer_distance_m,
                        max_features_per_batch=max_features_per_batch,
                    )
                    prepared_features = self._prepare_duckdb_geometry_rows(feature_rows, id_column="constraint_feature_id")
                    feature_truncated = len(prepared_features) >= max_features_per_batch
                    candidate_ids, used_spatial_exact, fallback_reason = self._duckdb_candidate_site_location_ids(
                        prepared_sites=prepared_sites,
                        prepared_features=prepared_features,
                        buffer_distance_m=buffer_distance_m,
                    )

                    ordered_candidate_ids = [
                        site_location_id for site_location_id in site_location_ids if site_location_id in candidate_ids
                    ]
                    if feature_truncated or refresh_existing:
                        finalizer_site_ids = site_location_ids
                        no_candidate_ids: list[str] = []
                    else:
                        finalizer_site_ids = ordered_candidate_ids
                        no_candidate_ids = [
                            site_location_id
                            for site_location_id in site_location_ids
                            if site_location_id not in candidate_ids
                        ]

                    batch_proof: dict[str, Any] = {}
                    if finalizer_site_ids:
                        batch_proof = self.database.fetch_one(
                            """
                            select *
                            from public.refresh_constraint_measurements_for_layer_sites(
                                :layer_key,
                                cast(:site_location_ids as text[]),
                                cast(:overlap_delta_pct as numeric),
                                cast(:distance_delta_m as numeric)
                            )
                            """,
                            {
                                "layer_key": layer_key,
                                "site_location_ids": finalizer_site_ids,
                                "overlap_delta_pct": overlap_delta_pct,
                                "distance_delta_m": distance_delta_m,
                            },
                        ) or {}
                    no_relationship_count = self._mark_constraint_duckdb_no_relationship_scan_state(
                        layer_key=layer_key,
                        site_location_ids=no_candidate_ids,
                    )
                except Exception as exc:
                    error_message = str(exc)
                    layer_payload["failed_batch_count"] += 1
                    layer_payload["last_error"] = error_message[:1000]
                    proof["failed_batch_count"] += 1
                    proof["failed_layers"].append(
                        {
                            "layer_key": layer_key,
                            "site_count": len(site_rows),
                            "last_site_location_id": str(site_rows[-1]["site_location_id"]),
                            "error": error_message[:1000],
                        }
                    )
                    self.logger.warning(
                        "constraint_measurement_duckdb_batch_failed",
                        extra={
                            "layer_key": layer_key,
                            "site_batch_count": len(site_rows),
                            "last_site_location_id": str(site_rows[-1]["site_location_id"]),
                            "error": error_message,
                        },
                    )
                    break

                after_site_location_id = str(site_rows[-1]["site_location_id"])
                layer_payload["processed_site_count"] += len(site_rows)
                layer_payload["processed_batch_count"] += 1
                layer_payload["duckdb_candidate_site_count"] += len(ordered_candidate_ids)
                layer_payload["duckdb_no_candidate_site_count"] += len(no_candidate_ids)
                layer_payload["duckdb_prefilter_feature_count"] += len(prepared_features)
                layer_payload["postgis_finalizer_site_count"] += len(finalizer_site_ids)
                layer_payload["no_relationship_scan_state_count"] += no_relationship_count
                proof["processed_site_count"] += len(site_rows)
                proof["processed_batch_count"] += 1
                proof["duckdb_candidate_site_count"] += len(ordered_candidate_ids)
                proof["duckdb_no_candidate_site_count"] += len(no_candidate_ids)
                proof["duckdb_prefilter_feature_count"] += len(prepared_features)
                proof["postgis_finalizer_site_count"] += len(finalizer_site_ids)
                proof["no_relationship_scan_state_count"] += no_relationship_count
                if used_spatial_exact:
                    layer_payload["duckdb_spatial_exact_batch_count"] += 1
                    proof["duckdb_spatial_exact_batch_count"] += 1
                else:
                    layer_payload["duckdb_bbox_fallback_batch_count"] += 1
                    proof["duckdb_bbox_fallback_batch_count"] += 1
                if feature_truncated:
                    layer_payload["feature_truncated_batch_count"] += 1
                    proof["feature_truncated_batch_count"] += 1

                for key in (
                    "measurement_count",
                    "summary_count",
                    "friction_fact_count",
                    "evidence_count",
                    "signal_count",
                    "change_event_count",
                    "affected_site_count",
                    "material_change_count",
                ):
                    increment = int(batch_proof.get(key) or 0)
                    layer_payload[key] += increment
                    proof[key] += increment

                self.logger.info(
                    "constraint_measurement_duckdb_batch_completed",
                    extra={
                        "layer_key": layer_key,
                        "site_batch_count": len(site_rows),
                        "candidate_site_count": len(ordered_candidate_ids),
                        "no_candidate_site_count": len(no_candidate_ids),
                        "postgis_finalizer_site_count": len(finalizer_site_ids),
                        "prefilter_feature_count": len(prepared_features),
                        "duckdb_spatial_exact": used_spatial_exact,
                        "duckdb_fallback_reason": fallback_reason,
                        "feature_truncated": feature_truncated,
                        "last_site_location_id": after_site_location_id,
                        **batch_proof,
                    },
                )

            proof["layers"].append(layer_payload)

        if proof["failed_batch_count"]:
            status = "constraint_measurements_duckdb_completed_with_layer_failures"
        elif proof["measurement_count"]:
            status = "constraint_measurements_duckdb_refreshed"
        elif proof["processed_site_count"]:
            status = "constraint_measurements_duckdb_scanned_no_material_change"
        else:
            status = "constraint_measurements_duckdb_no_pending_sites"
        proof["status"] = status

        self._record_expansion_event(
            command_name="measure-constraints-duckdb",
            source_key=layer_key_filter,
            source_family=source_family_filter or "constraint_measurement",
            status=status,
            raw_rows=int(proof["source_feature_count"]),
            linked_rows=int(proof["affected_site_count"]),
            measured_rows=int(proof["measurement_count"]),
            evidence_rows=int(proof["evidence_count"]),
            signal_rows=int(proof["signal_count"]),
            change_event_rows=int(proof["change_event_count"]),
            summary=(
                "Constraint measurements accelerated with DuckDB spatial prefilter; "
                "Supabase/PostGIS remains the authoritative finalizer and audit store."
            ),
            metadata=proof,
        )
        self.logger.info("constraint_measurement_duckdb_command_completed", extra=proof)
        return proof

    def _constraint_measurement_site_rows(
        self,
        *,
        layer_key: str,
        authority_filter: str | None,
        after_site_location_id: str | None,
        min_area_acres: float,
        refresh_existing: bool,
        batch_size: int,
    ) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            with layer_row as (
                select id
                from public.constraint_layer_registry
                where layer_key = :layer_key
            ), anchor as (
                select *
                from public.constraints_site_anchor()
                where geometry is not null
                  and (
                      cast(:authority_filter as text) is null
                      or lower(authority_name) = lower(cast(:authority_filter as text))
                  )
                  and (
                      cast(:after_site_location_id as text) is null
                      or site_location_id > cast(:after_site_location_id as text)
                  )
                  and (
                      cast(:min_area_acres as numeric) <= 0
                      or coalesce(area_acres, public.calculate_area_acres(st_area(geometry)::numeric))
                          >= cast(:min_area_acres as numeric)
                  )
            )
            select
                anchor.site_id,
                anchor.site_location_id,
                st_asbinary(anchor.geometry) as geometry_wkb
            from anchor
            cross join layer_row
            where (
                cast(:refresh_existing as boolean)
                or not exists (
                    select 1
                    from public.site_constraint_measurement_scan_state as scan_state
                    where scan_state.constraint_layer_id = layer_row.id
                      and scan_state.site_location_id = anchor.site_location_id
                      and scan_state.scan_scope = 'canonical_site_geometry'
                )
            )
            order by anchor.site_location_id
            limit :batch_size
            """,
            {
                "layer_key": layer_key,
                "authority_filter": authority_filter,
                "after_site_location_id": after_site_location_id,
                "min_area_acres": min_area_acres,
                "refresh_existing": refresh_existing,
                "batch_size": batch_size,
            },
        )

    def _prepare_duckdb_geometry_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        id_column: str,
    ) -> list[dict[str, Any]]:
        prepared_rows: list[dict[str, Any]] = []
        for row in rows:
            raw_geometry = row.get("geometry_wkb")
            if raw_geometry is None:
                continue
            geometry_bytes = bytes(raw_geometry)
            geometry = wkb.loads(geometry_bytes)
            if geometry.is_empty:
                continue
            xmin, ymin, xmax, ymax = geometry.bounds
            prepared_rows.append(
                {
                    **row,
                    id_column: str(row[id_column]),
                    "geometry_wkb": geometry_bytes,
                    "xmin": float(xmin),
                    "ymin": float(ymin),
                    "xmax": float(xmax),
                    "ymax": float(ymax),
                }
            )
        return prepared_rows

    def _constraint_feature_rows_for_duckdb_batch(
        self,
        *,
        layer_key: str,
        prepared_sites: list[dict[str, Any]],
        buffer_distance_m: float,
        max_features_per_batch: int,
    ) -> list[dict[str, Any]]:
        if not prepared_sites:
            return []
        xmin = min(float(site["xmin"]) for site in prepared_sites) - buffer_distance_m
        ymin = min(float(site["ymin"]) for site in prepared_sites) - buffer_distance_m
        xmax = max(float(site["xmax"]) for site in prepared_sites) + buffer_distance_m
        ymax = max(float(site["ymax"]) for site in prepared_sites) + buffer_distance_m
        return self.database.fetch_all(
            """
            select
                feature.id::text as constraint_feature_id,
                st_asbinary(feature.geometry) as geometry_wkb
            from public.constraint_source_features as feature
            join public.constraint_layer_registry as layer_row
              on layer_row.id = feature.constraint_layer_id
            where layer_row.layer_key = :layer_key
              and feature.geometry is not null
              and feature.geometry OPERATOR(extensions.&&)
                  st_makeenvelope(:xmin, :ymin, :xmax, :ymax, 27700)
            order by feature.id
            limit :max_features_per_batch
            """,
            {
                "layer_key": layer_key,
                "xmin": xmin,
                "ymin": ymin,
                "xmax": xmax,
                "ymax": ymax,
                "max_features_per_batch": max_features_per_batch,
            },
        )

    def _duckdb_candidate_site_location_ids(
        self,
        *,
        prepared_sites: list[dict[str, Any]],
        prepared_features: list[dict[str, Any]],
        buffer_distance_m: float,
    ) -> tuple[set[str], bool, str | None]:
        if not prepared_sites or not prepared_features:
            return set(), False, None

        site_frame = pd.DataFrame(prepared_sites)
        feature_frame = pd.DataFrame(prepared_features)
        buffer_sql = repr(max(buffer_distance_m, 0.0))
        bbox_where = f"""
            s.xmin <= f.xmax + {buffer_sql}
            and s.xmax >= f.xmin - {buffer_sql}
            and s.ymin <= f.ymax + {buffer_sql}
            and s.ymax >= f.ymin - {buffer_sql}
        """
        bbox_query = f"""
            select distinct s.site_location_id
            from sites as s
            join features as f
              on {bbox_where}
        """
        exact_query = f"""
            select distinct s.site_location_id
            from sites as s
            join features as f
              on {bbox_where}
             and (
                case
                    when {buffer_sql} > 0 then
                        ST_DWithin(ST_GeomFromWKB(s.geometry_wkb), ST_GeomFromWKB(f.geometry_wkb), {buffer_sql})
                    else
                        ST_Intersects(ST_GeomFromWKB(s.geometry_wkb), ST_GeomFromWKB(f.geometry_wkb))
                end
             )
        """

        connection = duckdb.connect(database=":memory:")
        try:
            connection.register("sites", site_frame)
            connection.register("features", feature_frame)
            try:
                connection.execute("install spatial")
                connection.execute("load spatial")
                rows = connection.execute(exact_query).fetchall()
                return {str(row[0]) for row in rows}, True, None
            except Exception as exc:
                rows = connection.execute(bbox_query).fetchall()
                return {str(row[0]) for row in rows}, False, str(exc)[:300]
        finally:
            connection.close()

    def _mark_constraint_duckdb_no_relationship_scan_state(
        self,
        *,
        layer_key: str,
        site_location_ids: list[str],
    ) -> int:
        if not site_location_ids:
            return 0
        result = self.database.fetch_one(
            """
            with layer_row as (
                select
                    id,
                    constraint_group
                from public.constraint_layer_registry
                where layer_key = :layer_key
            ), requested as (
                select distinct input.site_location_id
                from unnest(cast(:site_location_ids as text[])) as input(site_location_id)
            ), inserted as (
                insert into public.site_constraint_measurement_scan_state (
                    site_id,
                    site_location_id,
                    constraint_layer_id,
                    scan_scope,
                    latest_measurement_count,
                    latest_summary_signature,
                    has_constraint_relationship,
                    scanned_at,
                    updated_at,
                    metadata
                )
                select
                    anchor.site_id,
                    anchor.site_location_id,
                    layer_row.id,
                    'canonical_site_geometry',
                    0,
                    null,
                    false,
                    now(),
                    now(),
                    jsonb_build_object(
                        'constraint_layer_key', :layer_key,
                        'constraint_group', layer_row.constraint_group,
                        'source_expansion_constraint', true,
                        'duckdb_prefilter_no_relationship', true,
                        'has_constraint_relationship', false
                    )
                from requested
                join public.constraints_site_anchor() as anchor
                  on anchor.site_location_id = requested.site_location_id
                cross join layer_row
                on conflict (constraint_layer_id, site_location_id, scan_scope) do update set
                    latest_measurement_count = 0,
                    latest_summary_signature = null,
                    has_constraint_relationship = false,
                    scanned_at = now(),
                    updated_at = now(),
                    metadata = excluded.metadata
                returning id
            )
            select count(*)::integer as scan_state_count
            from inserted
            """,
            {"layer_key": layer_key, "site_location_ids": site_location_ids},
        ) or {}
        return int(result.get("scan_state_count") or 0)

    def measure_constraints_debug_all_layers(self) -> dict[str, Any]:
        layer_key_filter = (os.getenv("CONSTRAINT_MEASURE_LAYER_KEY") or "").strip() or None
        source_family_filter = (os.getenv("CONSTRAINT_MEASURE_SOURCE_FAMILY") or "").strip() or None
        authority_filter = (os.getenv("CONSTRAINT_MEASURE_AUTHORITY") or "").strip() or None
        site_count = max(self._env_int("CONSTRAINT_MEASURE_DEBUG_SITE_COUNT", 10), 1)
        feature_probe_limit = max(self._env_int("CONSTRAINT_MEASURE_DEBUG_FEATURE_PROBE_LIMIT", 50), 1)
        runtime_minutes = max(self._env_int("CONSTRAINT_MEASURE_RUNTIME_MINUTES", 10), 1)
        fallback_canonical_sample = str(os.getenv("CONSTRAINT_MEASURE_DEBUG_FALLBACK_CANONICAL") or "").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        deadline = datetime.now(timezone.utc) + timedelta(minutes=runtime_minutes)
        overlap_delta_pct = self._env_float("CONSTRAINT_MATERIAL_OVERLAP_DELTA_PCT", 1.0)
        distance_delta_m = self._env_float("CONSTRAINT_MATERIAL_DISTANCE_DELTA_M", 25.0)

        for source_family in sorted(CONSTRAINT_FAMILIES):
            for source in self._sources_for_family(source_family):
                self._upsert_source_estate(source)

        layers = self.database.fetch_all(
            """
            with layer_rows as (
                select
                    layer.layer_key,
                    layer.layer_name,
                    layer.source_family,
                    count(feature.id)::integer as source_feature_count,
                    case layer.source_family
                        when 'sepa_flood' then 0
                        when 'tpo' then 1
                        when 'conservation_areas' then 2
                        when 'naturescot' then 3
                        when 'hes' then 4
                        when 'greenbelt' then 5
                        when 'contaminated_land' then 6
                        when 'culverts' then 7
                        when 'coal_authority' then 8
                        else 9
                    end as source_family_priority
                from public.constraint_layer_registry as layer
                join public.constraint_source_features as feature
                  on feature.constraint_layer_id = layer.id
                where layer.is_active = true
                  and (
                      cast(:layer_key_filter as text) is null
                      or layer.layer_key = cast(:layer_key_filter as text)
                  )
                  and (
                      cast(:source_family_filter as text) is null
                      or layer.source_family = cast(:source_family_filter as text)
                  )
                group by layer.layer_key, layer.layer_name, layer.source_family
            ), ranked_layers as (
                select
                    *,
                    row_number() over (
                        partition by source_family
                        order by layer_key
                    ) as source_family_layer_rank
                from layer_rows
            )
            select
                layer.layer_key,
                layer.layer_name,
                layer.source_family,
                layer.source_feature_count
            from ranked_layers as layer
            order by
                layer.source_family_layer_rank,
                layer.source_family_priority,
                layer.layer_key
            """,
            {"layer_key_filter": layer_key_filter, "source_family_filter": source_family_filter},
        )
        proof: dict[str, Any] = {
            "command": "measure-constraints-debug-all-layers",
            "layer_key_filter": layer_key_filter,
            "source_family_filter": source_family_filter,
            "authority_filter": authority_filter,
            "debug_site_count_per_layer": site_count,
            "requested_debug_site_count": site_count,
            "feature_probe_limit": feature_probe_limit,
            "fallback_canonical_sample": fallback_canonical_sample,
            "runtime_minutes": runtime_minutes,
            "layer_count": len(layers),
            "source_feature_count": sum(int(layer.get("source_feature_count") or 0) for layer in layers),
            "processed_site_layer_pair_count": 0,
            "measurement_count": 0,
            "summary_count": 0,
            "friction_fact_count": 0,
            "evidence_count": 0,
            "signal_count": 0,
            "change_event_count": 0,
            "affected_site_count": 0,
            "material_change_count": 0,
            "failed_layer_count": 0,
            "failed_layers": [],
            "layers": [],
        }

        for layer in layers:
            if datetime.now(timezone.utc) >= deadline:
                break
            layer_key = str(layer["layer_key"])
            site_rows = self._debug_constraint_site_sample_for_layer(
                layer_key,
                site_count,
                feature_probe_limit,
                authority_filter,
            )
            sample_method = "layer_spatial_probe"
            site_location_ids = [str(row["site_location_id"]) for row in site_rows if row.get("site_location_id")]
            if not site_location_ids and fallback_canonical_sample:
                site_rows = self._fallback_constraint_site_sample(site_count, authority_filter)
                sample_method = "fallback_canonical_site_order"
                site_location_ids = [
                    str(row["site_location_id"]) for row in site_rows if row.get("site_location_id")
                ]
            if not site_location_ids:
                layer_payload = {
                    "layer_key": layer_key,
                    "layer_name": layer.get("layer_name"),
                    "source_family": layer.get("source_family"),
                    "source_feature_count": int(layer.get("source_feature_count") or 0),
                    "debug_site_count": 0,
                    "sample_method": "none",
                    "measurement_count": 0,
                    "summary_count": 0,
                    "friction_fact_count": 0,
                    "evidence_count": 0,
                    "signal_count": 0,
                    "change_event_count": 0,
                    "affected_site_count": 0,
                    "material_change_count": 0,
                }
                proof["layers"].append(layer_payload)
                self.logger.info("constraint_measurement_debug_layer_completed", extra=layer_payload)
                continue

            try:
                batch_proof = self.database.fetch_one(
                    """
                    select *
                    from public.refresh_constraint_measurements_for_layer_sites(
                        :layer_key,
                        cast(:site_location_ids as text[]),
                        cast(:overlap_delta_pct as numeric),
                        cast(:distance_delta_m as numeric)
                    )
                    """,
                    {
                        "layer_key": layer_key,
                        "site_location_ids": site_location_ids,
                        "overlap_delta_pct": overlap_delta_pct,
                        "distance_delta_m": distance_delta_m,
                    },
                ) or {}
            except Exception as exc:
                error_message = str(exc)
                layer_payload = {
                    "layer_key": layer_key,
                    "layer_name": layer.get("layer_name"),
                    "source_family": layer.get("source_family"),
                    "source_feature_count": int(layer.get("source_feature_count") or 0),
                    "debug_site_count": len(site_location_ids),
                    "sample_method": sample_method,
                    "status": "measurement_failed",
                    "last_error": error_message[:1000],
                    "measurement_count": 0,
                    "summary_count": 0,
                    "friction_fact_count": 0,
                    "evidence_count": 0,
                    "signal_count": 0,
                    "change_event_count": 0,
                    "affected_site_count": 0,
                    "material_change_count": 0,
                }
                proof["failed_layer_count"] += 1
                proof["failed_layers"].append(
                    {
                        "layer_key": layer_key,
                        "site_count": len(site_location_ids),
                        "error": error_message[:1000],
                    }
                )
                proof["layers"].append(layer_payload)
                self.logger.warning(
                    "constraint_measurement_debug_layer_failed",
                    extra={"site_count": len(site_location_ids), **layer_payload},
                )
                continue
            layer_payload: dict[str, Any] = {
                "layer_key": layer_key,
                "layer_name": layer.get("layer_name"),
                "source_family": layer.get("source_family"),
                "source_feature_count": int(layer.get("source_feature_count") or 0),
                "debug_site_count": len(site_location_ids),
                "sample_method": sample_method,
                **batch_proof,
            }
            proof["processed_site_layer_pair_count"] += len(site_location_ids)
            for key in (
                "measurement_count",
                "summary_count",
                "friction_fact_count",
                "evidence_count",
                "signal_count",
                "change_event_count",
                "affected_site_count",
                "material_change_count",
            ):
                increment = int(batch_proof.get(key) or 0)
                proof[key] += increment
            proof["layers"].append(layer_payload)
            self.logger.info(
                "constraint_measurement_debug_layer_completed",
                extra={"site_count": len(site_location_ids), **layer_payload},
            )

        family_rows = self.database.fetch_all(
            """
            select
                layer.source_family,
                layer.constraint_group,
                count(measurement.id)::integer as measurement_count,
                count(distinct measurement.site_location_id)::integer as site_count,
                count(*) filter (where measurement.overlap_character is null)::integer as missing_overlap_character_count
            from public.site_constraint_measurements as measurement
            join public.constraint_layer_registry as layer
              on layer.id = measurement.constraint_layer_id
            where (
                cast(:source_family_filter as text) is null
                or layer.source_family = cast(:source_family_filter as text)
            )
              and (
                cast(:layer_key_filter as text) is null
                or layer.layer_key = cast(:layer_key_filter as text)
              )
            group by layer.source_family, layer.constraint_group
            order by measurement_count desc, layer.source_family, layer.constraint_group
            """,
            {"source_family_filter": source_family_filter, "layer_key_filter": layer_key_filter},
        )
        proof["family_coverage"] = family_rows
        proof["measured_source_family_count"] = len({str(row.get("source_family")) for row in family_rows})
        if proof["failed_layer_count"]:
            proof["status"] = "constraint_debug_completed_with_layer_failures"
        elif proof["measured_source_family_count"] > 1 and proof["measurement_count"] > 0:
            proof["status"] = "constraint_debug_multi_family_proven"
        else:
            proof["status"] = "constraint_debug_completed_single_or_no_family"
        self._record_expansion_event(
            command_name="measure-constraints-debug-all-layers",
            source_key=None,
            source_family="constraint_measurement",
            status=str(proof["status"]),
            raw_rows=int(proof["source_feature_count"]),
            linked_rows=int(proof["affected_site_count"]),
            measured_rows=int(proof["measurement_count"]),
            evidence_rows=int(proof["evidence_count"]),
            signal_rows=int(proof["signal_count"]),
            change_event_rows=int(proof["change_event_count"]),
            summary="Debug measurement forced a fixed site sample across every active populated constraint layer.",
            metadata=proof,
        )
        self.logger.info("constraint_measurement_debug_all_layers_completed", extra=proof)
        return proof

    def _debug_constraint_site_sample_for_layer(
        self,
        layer_key: str,
        site_count: int,
        feature_probe_limit: int,
        authority_filter: str | None,
    ) -> list[dict[str, Any]]:
        try:
            return self.database.fetch_all(
                """
                with layer_row as (
                    select id, buffer_distance_m
                    from public.constraint_layer_registry
                    where layer_key = :layer_key
                      and is_active = true
                ), feature_sample as (
                    select feature.geometry
                    from public.constraint_source_features as feature
                    join layer_row
                      on layer_row.id = feature.constraint_layer_id
                    where feature.geometry is not null
                    order by feature.id
                    limit :feature_probe_limit
                )
                select distinct candidate.site_location_id
                from layer_row
                join feature_sample
                  on true
                join lateral (
                    select site.id::text as site_location_id
                    from landintel.canonical_sites as site
                    where site.geometry is not null
                      and site.geometry OPERATOR(extensions.&&)
                            st_expand(feature_sample.geometry, greatest(layer_row.buffer_distance_m, 0)::double precision)
                      and (
                          (
                              layer_row.buffer_distance_m > 0
                              and st_dwithin(site.geometry, feature_sample.geometry, layer_row.buffer_distance_m)
                          )
                          or (
                              layer_row.buffer_distance_m = 0
                              and st_intersects(site.geometry, feature_sample.geometry)
                          )
                      )
                      and (
                          cast(:authority_filter as text) is null
                          or lower(site.authority_name) = lower(cast(:authority_filter as text))
                      )
                    limit :site_count
                ) as candidate on true
                limit :site_count
                """,
                {
                    "layer_key": layer_key,
                    "site_count": site_count,
                    "feature_probe_limit": feature_probe_limit,
                    "authority_filter": authority_filter,
                },
            )
        except Exception as exc:  # pragma: no cover - protects GitHub/Supabase debug runs from probe timeouts.
            self.logger.warning(
                "constraint_measurement_debug_layer_sample_failed",
                extra={"layer_key": layer_key, "error": str(exc)},
            )
            return []

    def _fallback_constraint_site_sample(
        self,
        site_count: int,
        authority_filter: str | None,
    ) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            select site.id::text as site_location_id
            from landintel.canonical_sites as site
            where site.geometry is not null
              and (
                  cast(:authority_filter as text) is null
                  or lower(site.authority_name) = lower(cast(:authority_filter as text))
              )
            order by site.id::text
            limit :site_count
            """,
            {"site_count": site_count, "authority_filter": authority_filter},
        )

    def audit_constraint_measurements(self) -> dict[str, Any]:
        coverage = self.database.fetch_one("select * from analytics.v_constraint_measurement_coverage") or {}
        layer_coverage = self.database.fetch_all(
            """
            select *
            from analytics.v_constraint_measurement_layer_coverage
            order by measured_row_count desc, source_feature_count desc, source_family, layer_key
            """
        )
        top_measurements = self.database.fetch_all(
            """
            select
                measurement.site_id,
                measurement.site_location_id,
                site.site_name_primary as site_name,
                site.authority_name,
                site.area_acres as site_area_acres,
                layer.layer_key,
                layer.layer_name,
                layer.constraint_group,
                layer.constraint_type,
                layer.measurement_mode,
                feature.id as constraint_feature_id,
                feature.source_feature_key,
                feature.feature_name,
                feature.source_reference,
                feature.severity_label,
                measurement.measurement_source,
                measurement.intersects,
                measurement.within_buffer,
                measurement.site_inside_feature,
                measurement.feature_inside_site,
                measurement.overlap_area_sqm,
                measurement.overlap_pct_of_site,
                measurement.overlap_pct_of_feature,
                measurement.nearest_distance_m,
                measurement.measured_at,
                measurement.overlap_character,
                measurement.feature_geometry_dimension
            from public.site_constraint_measurements as measurement
            join public.constraint_layer_registry as layer
              on layer.id = measurement.constraint_layer_id
            left join public.constraint_source_features as feature
              on feature.id = measurement.constraint_feature_id
            left join landintel.canonical_sites as site
              on site.id::text = measurement.site_location_id
            order by measurement.measured_at desc nulls last,
                     measurement.overlap_pct_of_site desc nulls last,
                     measurement.nearest_distance_m nulls last
            limit 20
            """
        )
        multi_group_sites = self.database.fetch_all(
            """
            select
                summary.site_id,
                summary.site_location_id,
                site.site_name_primary as site_name,
                count(distinct summary.constraint_group)::integer as constraint_group_count,
                array_agg(distinct summary.constraint_group order by summary.constraint_group) as constraint_groups,
                max(summary.measured_at) as latest_measured_at
            from public.site_constraint_group_summaries as summary
            left join landintel.canonical_sites as site
              on site.id::text = summary.site_location_id
            group by summary.site_id, summary.site_location_id, site.site_name_primary
            having count(distinct summary.constraint_group) > 1
            order by constraint_group_count desc, latest_measured_at desc nulls last
            limit 10
            """
        )
        family_coverage = self.database.fetch_all(
            """
            with measurement_family as (
                select
                    layer.source_family,
                    layer.constraint_group,
                    count(measurement.id)::integer as measurement_count,
                    count(distinct measurement.site_location_id)::integer as measured_site_count,
                    count(*) filter (where measurement.overlap_character is null)::integer as missing_overlap_character_count,
                    array_agg(distinct measurement.overlap_character order by measurement.overlap_character)
                        filter (where measurement.overlap_character is not null) as overlap_characters
                from public.site_constraint_measurements as measurement
                join public.constraint_layer_registry as layer
                  on layer.id = measurement.constraint_layer_id
                group by layer.source_family, layer.constraint_group
            ),
            fact_family as (
                select
                    layer.source_family,
                    layer.constraint_group,
                    count(fact.id)::integer as commercial_friction_fact_count
                from public.site_commercial_friction_facts as fact
                join public.constraint_layer_registry as layer
                  on layer.id = fact.constraint_layer_id
                group by layer.source_family, layer.constraint_group
            )
            select
                measurement_family.source_family,
                measurement_family.constraint_group,
                measurement_family.measurement_count,
                measurement_family.measured_site_count,
                coalesce(fact_family.commercial_friction_fact_count, 0) as commercial_friction_fact_count,
                measurement_family.missing_overlap_character_count,
                measurement_family.overlap_characters
            from measurement_family
            left join fact_family
              on fact_family.source_family = measurement_family.source_family
             and fact_family.constraint_group = measurement_family.constraint_group
            order by measurement_family.measurement_count desc,
                     measurement_family.source_family,
                     measurement_family.constraint_group
            """
        )
        flood_proof = self.database.fetch_one(
            """
            select
                (select count(*)::integer from landintel.flood_records) as flood_record_count,
                (
                    select count(*)::integer
                    from public.constraint_source_features as feature
                    join public.constraint_layer_registry as layer
                      on layer.id = feature.constraint_layer_id
                    where layer.source_family = 'sepa_flood'
                ) as sepa_flood_source_feature_count,
                (
                    select count(*)::integer
                    from public.site_constraint_measurements as measurement
                    join public.constraint_layer_registry as layer
                      on layer.id = measurement.constraint_layer_id
                    where layer.source_family = 'sepa_flood'
                ) as sepa_flood_measurement_count,
                (
                    select count(distinct measurement.site_location_id)::integer
                    from public.site_constraint_measurements as measurement
                    join public.constraint_layer_registry as layer
                      on layer.id = measurement.constraint_layer_id
                    where layer.source_family = 'sepa_flood'
                ) as sepa_flood_measured_site_count
            """
        ) or {}
        overlap_character_proof = self.database.fetch_all(
            """
            select
                overlap_character,
                count(*)::integer as measurement_count
            from public.site_constraint_measurements
            group by overlap_character
            order by measurement_count desc, overlap_character
            """
        )
        status = (
            "constraint_measurements_live_proven"
            if int(coverage.get("measured_site_constraint_row_count") or 0) > 0
            and int(coverage.get("grouped_summary_row_count") or 0) > 0
            and int(coverage.get("commercial_friction_fact_count") or 0) > 0
            and int(coverage.get("missing_overlap_character_count") or 0) == 0
            else "constraint_measurements_not_yet_proven"
        )
        result = {
            "command": "audit-constraint-measurements",
            "status": status,
            "coverage": coverage,
            "layer_coverage": layer_coverage,
            "family_coverage": family_coverage,
            "flood_proof": flood_proof,
            "overlap_character_proof": overlap_character_proof,
            "top_measurements": top_measurements,
            "multi_group_sites": multi_group_sites,
        }
        self._record_expansion_event(
            command_name="audit-constraint-measurements",
            source_key=None,
            source_family="constraint_measurement",
            status=status,
            raw_rows=int(coverage.get("source_constraint_feature_count") or 0),
            linked_rows=int(coverage.get("canonical_sites_with_measured_constraints") or 0),
            measured_rows=int(coverage.get("measured_site_constraint_row_count") or 0),
            evidence_rows=int(coverage.get("commercial_friction_fact_count") or 0),
            summary="Constraint measurement proof audited from Priority Zero analytics views.",
            metadata=result,
        )
        self.logger.info("constraint_measurement_audit", extra=result)
        return result

    def _run_family_command(self, command: str, source_family: str) -> dict[str, Any]:
        sources = self._sources_for_family(source_family)
        if not sources:
            raise KeyError(f"No registered source config found for {source_family}.")

        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type=command.replace("-", "_"),
                source_name=f"Phase One source expansion: {source_family}",
                status="running",
                metadata={"source_family": source_family, "source_keys": [source["source_key"] for source in sources]},
            )
        )
        try:
            if source_family in FUTURE_CONTEXT_FAMILIES:
                result = self._ingest_future_context_family(command, source_family, sources, run_id)
            elif source_family in POLICY_PACKAGE_FAMILIES:
                result = self._ingest_policy_package_family(command, source_family, sources, run_id)
            elif source_family in SETTLEMENT_BOUNDARY_FAMILIES:
                result = self._ingest_settlement_boundary_family(command, source_family, sources, run_id)
            elif source_family in CONSTRAINT_FAMILIES:
                result = self._ingest_constraint_family(command, source_family, sources, run_id)
            elif command != "probe-open-location-spine" and source_family in OPEN_LOCATION_SPINE_BULK_FAMILIES:
                result = self._ingest_open_location_spine_family(command, source_family, sources, run_id)
            elif source_family in PROBE_ONLY_FAMILIES:
                result = self._probe_catalogue_family(command, source_family, sources, run_id)
            else:
                raise KeyError(f"No source expansion strategy exists for {source_family}.")

            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=int(result.get("raw_rows", 0)),
                    records_loaded=int(
                        result.get("raw_rows", 0)
                        if source_family in POLICY_PACKAGE_FAMILIES | SETTLEMENT_BOUNDARY_FAMILIES
                        else result.get("linked_rows", 0) or result.get("measured_rows", 0)
                    ),
                    records_retained=int(result.get("signal_rows", 0)),
                    metadata=result,
                    finished=True,
                ),
            )
            return result
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="failed",
                    error_message=str(exc),
                    metadata={"traceback": traceback.format_exc(), "source_family": source_family},
                    finished=True,
                ),
            )
            self._record_expansion_event(
                command_name=command,
                source_key=None,
                source_family=source_family,
                status="failed",
                summary=str(exc),
                metadata={"traceback": traceback.format_exc()},
            )
            raise

    def _ingest_future_context_family(
        self,
        command: str,
        source_family: str,
        sources: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        self._assert_required_secrets(sources)
        frames: list[gpd.GeoDataFrame] = []
        for source in sources:
            self._upsert_source_estate(source)
            source_frames = self._fetch_wfs_source_frames(source)
            frames.extend(source_frames)

        if frames:
            frame = pd.concat(frames, ignore_index=True)
            gdf = gpd.GeoDataFrame(frame, geometry="geometry", crs=frames[0].crs or 27700)
        else:
            gdf = gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)

        raw_rows = self._replace_future_context_rows(source_family, sources[0], gdf, run_id)
        self._backfill_future_context_authority(source_family)
        publish_result = self._publish_future_context(source_family, sources[0], run_id)
        result = {
            "command": command,
            "source_family": source_family,
            "source_keys": [source["source_key"] for source in sources],
            "raw_rows": raw_rows,
            **publish_result,
        }
        self._record_source_freshness(sources[0], "current" if raw_rows else "empty", "reachable", raw_rows, result)
        self._record_expansion_event(
            command_name=command,
            source_key=sources[0]["source_key"],
            source_family=source_family,
            status="live_publish_attempted" if raw_rows else "empty_source_response",
            raw_rows=raw_rows,
            linked_rows=int(publish_result.get("linked_rows", 0)),
            evidence_rows=int(publish_result.get("evidence_rows", 0)),
            signal_rows=int(publish_result.get("signal_rows", 0)),
            change_event_rows=int(publish_result.get("change_event_rows", 0)),
            summary=f"{source_family.upper()} ingest completed from live source endpoint.",
            metadata=result,
        )
        return result

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
        layer_results: list[dict[str, Any]] = []

        for source in sources:
            self._upsert_source_estate(source)
            if self._source_uses_arcgis(source):
                loaded_layers = self._load_arcgis_constraint_source(source, run_id)
            else:
                loaded_layers = self._load_wfs_constraint_source(source, run_id)
            for layer in loaded_layers:
                raw_rows += int(layer.get("raw_rows", 0))
                proof = self.database.fetch_one(
                    "select * from public.refresh_constraint_measurements_for_layer(:layer_key)",
                    {"layer_key": layer["layer_key"]},
                ) or {}
                measured_rows += int(proof.get("measurement_count") or 0)
                evidence_rows += int(proof.get("evidence_count") or 0)
                signal_rows += int(proof.get("signal_count") or 0)
                affected_site_count += int(proof.get("affected_site_count") or 0)
                layer_results.append({**layer, **proof})
        flood_record_rows = (
            self._sync_flood_records_from_constraint_features()
            if source_family == "sepa_flood"
            else 0
        )

        result = {
            "command": command,
            "source_family": source_family,
            "source_keys": [source["source_key"] for source in sources],
            "raw_rows": raw_rows,
            "flood_record_rows": flood_record_rows,
            "measured_rows": measured_rows,
            "linked_rows": affected_site_count,
            "evidence_rows": evidence_rows,
            "signal_rows": signal_rows,
            "change_event_rows": affected_site_count,
            "layers": layer_results,
        }
        self._record_source_freshness(sources[0], "current" if raw_rows else "empty", "reachable", raw_rows, result)
        self._record_expansion_event(
            command_name=command,
            source_key=sources[0]["source_key"],
            source_family=source_family,
            status="constraint_measurements_refreshed" if raw_rows else "empty_source_response",
            raw_rows=raw_rows,
            linked_rows=affected_site_count,
            measured_rows=measured_rows,
            evidence_rows=evidence_rows,
            signal_rows=signal_rows,
            change_event_rows=affected_site_count,
            summary=f"{source_family} constraints ingested and measured against canonical sites.",
            metadata=result,
        )
        return result

    def _ingest_policy_package_family(
        self,
        command: str,
        source_family: str,
        sources: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        if source_family != "ldp":
            raise KeyError(f"No policy package strategy exists for {source_family}.")

        raw_rows = 0
        resource_count = 0
        direct_zip_resource_count = 0
        external_link_resource_count = 0
        failed_resources: list[dict[str, str]] = []
        loaded_resources: list[dict[str, Any]] = []

        for source in sources:
            self._upsert_source_estate(source)
            package_payload = self._fetch_json(str(source["endpoint_url"]), {})
            package = dict(package_payload.get("result") or package_payload)
            resources = list(package.get("resources") or [])
            resource_count += len(resources)
            direct_resources = [resource for resource in resources if self._is_direct_zip_resource(resource)]
            external_link_resource_count += len(resources) - len(direct_resources)
            if self.ldp_max_resources > 0:
                direct_resources = direct_resources[: self.ldp_max_resources]
            direct_zip_resource_count += len(direct_resources)
            source_registry_id = self._upsert_ldp_source_registry(source, package)

            with tempfile.TemporaryDirectory(prefix="landintel-ldp-") as tmp_dir:
                for resource in direct_resources:
                    try:
                        frames = self._fetch_ldp_resource_frames(source, resource, tmp_dir)
                    except Exception as exc:
                        failed_resources.append(
                            {
                                "resource_id": str(resource.get("id") or ""),
                                "resource_name": str(resource.get("name") or resource.get("url") or "unknown"),
                                "error": str(exc),
                            }
                        )
                        continue
                    loaded = 0
                    for frame in frames:
                        loaded += self._replace_ldp_rows(source, frame, run_id, source_registry_id)
                    raw_rows += loaded
                    loaded_resources.append(
                        {
                            "resource_id": resource.get("id"),
                            "resource_name": resource.get("name"),
                            "rows_loaded": loaded,
                        }
                    )

        status = (
            "core_policy_storage_partial_licence_gated"
            if raw_rows and failed_resources
            else "core_policy_storage_proven_licence_gated"
            if raw_rows
            else "core_policy_package_reachable_no_features"
        )
        result = {
            "command": command,
            "source_family": source_family,
            "source_keys": [source["source_key"] for source in sources],
            "raw_rows": raw_rows,
            "linked_rows": 0,
            "measured_rows": 0,
            "evidence_rows": 0,
            "signal_rows": 0,
            "change_event_rows": 0,
            "resource_count": resource_count,
            "direct_zip_resource_count": direct_zip_resource_count,
            "external_link_resource_count": external_link_resource_count,
            "loaded_resources": loaded_resources,
            "failed_resources": failed_resources,
            "summary": (
                "SpatialHub LDP package stored in landintel.ldp_site_records. "
                "Ranking remains licence-gated and interpreter-gated."
            ),
        }
        self._record_source_freshness(
            sources[0],
            "current" if raw_rows else "empty",
            "reachable",
            raw_rows,
            result,
        )
        self._record_expansion_event(
            command_name=command,
            source_key=sources[0]["source_key"],
            source_family=source_family,
            status=status,
            raw_rows=raw_rows,
            summary=result["summary"],
            metadata=result,
        )
        return result

    def _ingest_settlement_boundary_family(
        self,
        command: str,
        source_family: str,
        sources: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        if source_family != "settlement":
            raise KeyError(f"No settlement-boundary strategy exists for {source_family}.")

        raw_rows = 0
        loaded_sources: list[dict[str, Any]] = []
        for source in sources:
            self._upsert_source_estate(source)
            source_registry_id = self._upsert_settlement_source_registry(source)
            frame = self._fetch_settlement_boundary_frame(source)
            loaded = self._replace_settlement_boundary_rows(source, frame, run_id, source_registry_id)
            raw_rows += loaded
            loaded_sources.append(
                {
                    "source_key": source["source_key"],
                    "wfs_type_name": source.get("wfs_type_name"),
                    "rows_loaded": loaded,
                }
            )

        status = (
            "core_policy_storage_proven_interpreter_gated"
            if raw_rows
            else "core_policy_nrs_wfs_reachable_no_features"
        )
        result = {
            "command": command,
            "source_family": source_family,
            "source_keys": [source["source_key"] for source in sources],
            "raw_rows": raw_rows,
            "linked_rows": 0,
            "measured_rows": 0,
            "evidence_rows": 0,
            "signal_rows": 0,
            "change_event_rows": 0,
            "loaded_sources": loaded_sources,
            "summary": (
                "NRS settlement boundaries stored in landintel.settlement_boundary_records. "
                "Ranking remains interpreter-gated until canonical settlement-position overlay is promoted."
            ),
        }
        self._record_source_freshness(
            sources[0],
            "current" if raw_rows else "empty",
            "reachable",
            raw_rows,
            result,
        )
        self._record_expansion_event(
            command_name=command,
            source_key=sources[0]["source_key"],
            source_family=source_family,
            status=status,
            raw_rows=raw_rows,
            summary=result["summary"],
            metadata=result,
        )
        return result

    def _probe_catalogue_family(
        self,
        command: str,
        source_family: str,
        sources: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        del run_id
        probed = 0
        blocked = 0
        results: list[dict[str, Any]] = []
        for source in sources:
            self._upsert_source_estate(source)
            missing = [
                str(secret)
                for secret in source.get("auth_env_vars") or []
                if not self._has_required_secret(str(secret))
            ]
            if missing:
                blocked += 1
                status = "blocked_missing_secret"
                summary = "Missing required GitHub Actions secret(s): " + ", ".join(missing)
            else:
                status, summary = self._probe_source(source)
                if status == "reachable":
                    probed += 1
                else:
                    blocked += 1
            self._record_source_freshness(
                source,
                "current" if status == "reachable" else "unknown",
                status,
                0,
                {"summary": summary},
            )
            self._record_expansion_event(
                command_name=command,
                source_key=source["source_key"],
                source_family=source_family,
                status=status,
                summary=summary,
                metadata={"source_key": source["source_key"], "probe_only": True},
            )
            results.append({"source_key": source["source_key"], "status": status, "summary": summary})
        return {
            "command": command,
            "source_family": source_family,
            "raw_rows": 0,
            "linked_rows": 0,
            "measured_rows": 0,
            "evidence_rows": 0,
            "signal_rows": 0,
            "change_event_rows": 0,
            "probed_sources": probed,
            "blocked_sources": blocked,
            "results": results,
        }

    def _ingest_open_location_spine_family(
        self,
        command: str,
        source_family: str,
        sources: list[dict[str, Any]],
        run_id: str,
    ) -> dict[str, Any]:
        raw_rows = 0
        linked_rows = 0
        measured_rows = 0
        evidence_rows = 0
        signal_rows = 0
        results: list[dict[str, Any]] = []
        for source in sources:
            self._upsert_source_estate(source)
            if str(source.get("orchestration_mode") or "") == "os_downloads_opendata":
                source_result = self._ingest_os_download_open_location_source(source, run_id)
            elif source_family == "osm":
                source_result = self._ingest_osm_open_location_source(source, run_id)
            else:
                status, summary = self._probe_source(source)
                source_result = {
                    "source_key": source["source_key"],
                    "source_family": source_family,
                    "status": status,
                    "summary": summary,
                    "raw_rows": 0,
                    "linked_rows": 0,
                    "measured_rows": 0,
                    "evidence_rows": 0,
                    "signal_rows": 0,
                }
            raw_rows += int(source_result.get("raw_rows") or 0)
            linked_rows += int(source_result.get("linked_rows") or 0)
            measured_rows += int(source_result.get("measured_rows") or 0)
            evidence_rows += int(source_result.get("evidence_rows") or 0)
            signal_rows += int(source_result.get("signal_rows") or 0)
            results.append(source_result)

        status = (
            "open_location_spine_landed"
            if raw_rows and linked_rows
            else "open_location_spine_rows_landed_unlinked"
            if raw_rows
            else "open_location_spine_no_rows_landed"
        )
        result = {
            "command": command,
            "source_family": source_family,
            "raw_rows": raw_rows,
            "linked_rows": linked_rows,
            "measured_rows": measured_rows,
            "evidence_rows": evidence_rows,
            "signal_rows": signal_rows,
            "status": status,
            "results": results,
        }
        self._record_expansion_event(
            command_name=command,
            source_key=None,
            source_family=source_family,
            status=status,
            raw_rows=raw_rows,
            linked_rows=linked_rows,
            measured_rows=measured_rows,
            evidence_rows=evidence_rows,
            signal_rows=signal_rows,
            summary="Open location spine bulk landing attempted with download and site-context budgets.",
            metadata=result,
        )
        return result

    def _ingest_os_download_open_location_source(self, source: dict[str, Any], run_id: str) -> dict[str, Any]:
        del run_id
        downloads, download_summary = self._os_download_options(source)
        selected_download = self._select_open_location_download(downloads)
        if selected_download is None:
            result = {
                "source_key": source["source_key"],
                "source_family": source["source_family"],
                "status": "download_size_or_format_budget_skipped",
                "summary": download_summary,
                "raw_rows": 0,
                "linked_rows": 0,
                "measured_rows": 0,
                "evidence_rows": 0,
                "signal_rows": 0,
                "download_options": downloads[:8],
            }
            self._record_source_freshness(source, "empty", "reachable", 0, result)
            self._update_open_location_lifecycle(source, result)
            self._record_expansion_event(
                command_name=f"ingest-{source['source_family'].replace('_', '-')}",
                source_key=source["source_key"],
                source_family=source["source_family"],
                status=str(result["status"]),
                summary=str(result["summary"]),
                metadata=result,
            )
            return result

        max_bytes = max(self._env_int("OPEN_LOCATION_SPINE_MAX_DOWNLOAD_BYTES", 90_000_000), 1)
        max_features = max(self._env_int("OPEN_LOCATION_SPINE_MAX_FEATURES_PER_SOURCE", 2500), 1)
        with tempfile.TemporaryDirectory(prefix="landintel-open-spine-") as tmp_dir:
            archive_path = Path(tmp_dir) / str(selected_download.get("fileName") or "download.zip")
            self._download_budgeted_file(str(selected_download["url"]), archive_path, max_bytes)
            extracted_paths = self._extract_open_location_archive(archive_path, Path(tmp_dir) / "extract")
            rows = self._open_location_rows_from_paths(source, selected_download, extracted_paths, max_features)

        inserted_rows = self._upsert_open_location_spine_rows(rows)
        context = self._refresh_open_location_spine_context(
            [str(source["source_key"])],
            max(self._env_int("OPEN_LOCATION_SPINE_SITE_CONTEXT_BATCH_SIZE", 250), 1),
        )
        result = {
            "source_key": source["source_key"],
            "source_family": source["source_family"],
            "status": "raw_data_landed" if inserted_rows else "reachable_no_supported_features",
            "summary": (
                f"{source['source_name']} landed {inserted_rows} open-location feature row(s) "
                f"from {selected_download.get('format')} {selected_download.get('fileName')}."
            ),
            "raw_rows": inserted_rows,
            "linked_rows": int(context.get("linked_rows") or 0),
            "measured_rows": int(context.get("measured_rows") or 0),
            "evidence_rows": int(context.get("evidence_rows") or 0),
            "signal_rows": int(context.get("signal_rows") or 0),
            "selected_download": {
                "fileName": selected_download.get("fileName"),
                "format": selected_download.get("format"),
                "subformat": selected_download.get("subformat"),
                "size": selected_download.get("size"),
            },
            "context": context,
        }
        self._record_source_freshness(source, "current" if inserted_rows else "empty", "reachable", inserted_rows, result)
        self._update_open_location_lifecycle(source, result)
        self._record_expansion_event(
            command_name=f"ingest-{source['source_family'].replace('_', '-')}",
            source_key=source["source_key"],
            source_family=source["source_family"],
            status=str(result["status"]),
            raw_rows=inserted_rows,
            linked_rows=int(result["linked_rows"]),
            measured_rows=int(result["measured_rows"]),
            evidence_rows=int(result["evidence_rows"]),
            signal_rows=int(result["signal_rows"]),
            summary=str(result["summary"]),
            metadata=result,
        )
        return result

    def _ingest_osm_open_location_source(self, source: dict[str, Any], run_id: str) -> dict[str, Any]:
        del run_id
        site_limit = max(self._env_int("OPEN_LOCATION_SPINE_OSM_SITE_BATCH_SIZE", 10), 1)
        element_limit = max(self._env_int("OPEN_LOCATION_SPINE_OSM_ELEMENT_LIMIT", 2000), 1)
        site_rows = self.database.fetch_all(
            """
            select
                site.id::text as canonical_site_id,
                st_ymin(st_transform(st_expand(site.geometry, 1000), 4326)) as south,
                st_xmin(st_transform(st_expand(site.geometry, 1000), 4326)) as west,
                st_ymax(st_transform(st_expand(site.geometry, 1000), 4326)) as north,
                st_xmax(st_transform(st_expand(site.geometry, 1000), 4326)) as east
            from landintel.canonical_sites as site
            where site.geometry is not null
            order by site.updated_at desc nulls last, site.id
            limit :site_limit
            """,
            {"site_limit": site_limit},
        )
        rows: list[dict[str, Any]] = []
        for site in site_rows:
            rows.extend(self._fetch_osm_open_location_rows(source, site, element_limit))
            if len(rows) >= element_limit:
                rows = rows[:element_limit]
                break

        inserted_rows = self._upsert_open_location_spine_rows(rows)
        context_site_batch_size = min(
            max(self._env_int("OPEN_LOCATION_SPINE_SITE_CONTEXT_BATCH_SIZE", 250), 1),
            max(self._env_int("OPEN_LOCATION_SPINE_OSM_CONTEXT_BATCH_SIZE", 25), 1),
        )
        context = self._refresh_open_location_spine_context(
            [str(source["source_key"])],
            context_site_batch_size,
        )
        result = {
            "source_key": source["source_key"],
            "source_family": source["source_family"],
            "status": "raw_data_landed" if inserted_rows else "reachable_no_supported_features",
            "summary": f"OSM site-bounded open context landed {inserted_rows} feature row(s).",
            "raw_rows": inserted_rows,
            "linked_rows": int(context.get("linked_rows") or 0),
            "measured_rows": int(context.get("measured_rows") or 0),
            "evidence_rows": int(context.get("evidence_rows") or 0),
            "signal_rows": int(context.get("signal_rows") or 0),
            "site_probe_count": len(site_rows),
            "context": context,
        }
        self._record_source_freshness(source, "current" if inserted_rows else "empty", "reachable", inserted_rows, result)
        self._update_open_location_lifecycle(source, result)
        self._record_expansion_event(
            command_name="ingest-osm-overpass",
            source_key=source["source_key"],
            source_family=source["source_family"],
            status=str(result["status"]),
            raw_rows=inserted_rows,
            linked_rows=int(result["linked_rows"]),
            measured_rows=int(result["measured_rows"]),
            evidence_rows=int(result["evidence_rows"]),
            signal_rows=int(result["signal_rows"]),
            summary=str(result["summary"]),
            metadata=result,
        )
        return result

    def _os_download_options(self, source: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
        endpoint = self._os_downloads_product_endpoint(source)
        params: dict[str, str] = {}
        if source.get("download_area"):
            params["area"] = str(source["download_area"])
        response = self.client.get(
            endpoint,
            params=params,
            headers={"User-Agent": "LandIntel/1.0 (+https://github.com/LDN-reece/LandIntel)"},
        )
        response.raise_for_status()
        payload = response.json()
        downloads = [dict(item) for item in payload] if isinstance(payload, list) else []
        return downloads, f"HTTP {response.status_code}: {len(downloads)} OS Downloads option(s)"

    def _select_open_location_download(self, downloads: list[dict[str, Any]]) -> dict[str, Any] | None:
        force_large = str(os.getenv("OPEN_LOCATION_SPINE_FORCE_LARGE_DOWNLOADS") or "").strip().lower() in {
            "1",
            "true",
            "yes",
        }
        max_bytes = max(self._env_int("OPEN_LOCATION_SPINE_MAX_DOWNLOAD_BYTES", 90_000_000), 1)
        priority = ("GeoPackage", "CSV", "ESRI")
        candidates: list[tuple[int, int, dict[str, Any]]] = []
        for download in downloads:
            fmt = str(download.get("format") or "")
            size = int(download.get("size") or 0)
            if not force_large and size > max_bytes:
                continue
            priority_index = next((index for index, token in enumerate(priority) if token.lower() in fmt.lower()), 999)
            if priority_index == 999:
                continue
            candidates.append((priority_index, size, download))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]))
        return dict(candidates[0][2])

    def _download_budgeted_file(self, url: str, target_path: Path, max_bytes: int) -> None:
        total_bytes = 0
        with self.client.stream(
            "GET",
            url,
            headers={"User-Agent": "LandIntel/1.0 (+https://github.com/LDN-reece/LandIntel)"},
        ) as response:
            response.raise_for_status()
            with target_path.open("wb") as handle:
                for chunk in response.iter_bytes():
                    if not chunk:
                        continue
                    total_bytes += len(chunk)
                    if total_bytes > max_bytes:
                        raise RuntimeError("open_location_spine_download_budget_exceeded")
                    handle.write(chunk)

    def _extract_open_location_archive(self, archive_path: Path, extract_dir: Path) -> list[Path]:
        extract_dir.mkdir(parents=True, exist_ok=True)
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as archive:
                archive.extractall(extract_dir)
            return [
                path
                for path in extract_dir.rglob("*")
                if path.suffix.lower() in {".gpkg", ".shp", ".csv"}
            ]
        return [archive_path]

    def _open_location_rows_from_paths(
        self,
        source: dict[str, Any],
        selected_download: dict[str, Any],
        paths: list[Path],
        max_features: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for path in paths:
            if len(rows) >= max_features:
                break
            if path.suffix.lower() == ".csv":
                rows.extend(
                    self._open_location_rows_from_csv(
                        source,
                        selected_download,
                        path,
                        max_features - len(rows),
                    )
                )
            elif path.suffix.lower() in {".gpkg", ".shp"}:
                rows.extend(
                    self._open_location_rows_from_vector(
                        source,
                        selected_download,
                        path,
                        max_features - len(rows),
                    )
                )
        return rows[:max_features]

    def _open_location_rows_from_vector(
        self,
        source: dict[str, Any],
        selected_download: dict[str, Any],
        path: Path,
        max_features: int,
    ) -> list[dict[str, Any]]:
        layer_names = self._vector_layer_names(path)
        rows: list[dict[str, Any]] = []
        for layer_name in layer_names:
            if len(rows) >= max_features:
                break
            try:
                read_kwargs: dict[str, Any] = {"rows": max_features - len(rows)}
                if layer_name is not None:
                    read_kwargs["layer"] = layer_name
                frame = gpd.read_file(path, **read_kwargs)
            except Exception as exc:
                self.logger.warning(
                    "open_location_spine_vector_layer_skipped",
                    extra={"path": str(path), "layer_name": layer_name, "error": str(exc)},
                )
                continue
            if frame.empty or "geometry" not in frame:
                continue
            if frame.crs is None:
                frame = frame.set_crs(27700, allow_override=True)
            if frame.crs.to_epsg() != 27700:
                frame = frame.to_crs(27700)
            frame = frame[frame.geometry.notna()]
            frame = frame[frame.geometry.apply(self._geometry_intersects_scotland_bbox)]
            for index, row in frame.iterrows():
                geometry = force_2d(row.geometry) if row.geometry is not None else None
                if geometry is None or geometry.is_empty:
                    continue
                raw_payload = _raw_payload(dict(row.drop(labels=["geometry"], errors="ignore")))
                rows.append(
                    self._open_location_row_payload(
                        source,
                        selected_download,
                        str(layer_name or path.stem),
                        raw_payload,
                        geometry,
                        index,
                    )
                )
                if len(rows) >= max_features:
                    break
        return rows

    def _open_location_rows_from_csv(
        self,
        source: dict[str, Any],
        selected_download: dict[str, Any],
        path: Path,
        max_features: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        try:
            for chunk in pd.read_csv(path, chunksize=2000, low_memory=False):
                for index, row in chunk.iterrows():
                    geometry = self._point_from_open_location_csv_row(row.to_dict())
                    if geometry is None or not self._geometry_intersects_scotland_bbox(geometry):
                        continue
                    rows.append(
                        self._open_location_row_payload(
                            source,
                            selected_download,
                            path.stem,
                            _raw_payload(row.to_dict()),
                            geometry,
                            index,
                        )
                    )
                    if len(rows) >= max_features:
                        return rows
        except Exception as exc:
            self.logger.warning("open_location_spine_csv_skipped", extra={"path": str(path), "error": str(exc)})
        return rows

    def _vector_layer_names(self, path: Path) -> list[str | None]:
        if path.suffix.lower() != ".gpkg":
            return [None]
        try:
            import pyogrio

            layers = pyogrio.list_layers(path)
            names: list[str | None] = []
            for layer in layers:
                try:
                    names.append(str(layer[0]))
                except Exception:
                    names.append(str(layer))
            return names or [None]
        except Exception:
            return [None]

    def _point_from_open_location_csv_row(self, row: dict[str, Any]) -> Point | None:
        lowered = {str(key).lower(): key for key in row.keys()}
        east_key = next((lowered[key] for key in ("geometry_x", "x", "easting", "eastings") if key in lowered), None)
        north_key = next((lowered[key] for key in ("geometry_y", "y", "northing", "northings") if key in lowered), None)
        lon_key = next((lowered[key] for key in ("longitude", "lon", "long") if key in lowered), None)
        lat_key = next((lowered[key] for key in ("latitude", "lat") if key in lowered), None)
        if east_key and north_key:
            east = _to_float(row.get(east_key))
            north = _to_float(row.get(north_key))
            if east is not None and north is not None:
                return Point(east, north)
        if lon_key and lat_key:
            lon = _to_float(row.get(lon_key))
            lat = _to_float(row.get(lat_key))
            if lon is not None and lat is not None:
                return gpd.GeoSeries([Point(lon, lat)], crs=4326).to_crs(27700).iloc[0]
        return None

    def _open_location_row_payload(
        self,
        source: dict[str, Any],
        selected_download: dict[str, Any],
        layer_name: str,
        raw_payload: dict[str, Any],
        geometry: BaseGeometry,
        row_index: Any,
    ) -> dict[str, Any]:
        reference = (
            _pick_text(raw_payload, REFERENCE_FIELDS)
            or _pick_text(raw_payload, ("toid", "TOID", "id", "identifier", "gml_id"))
            or row_index
        )
        feature_name = _pick_text(raw_payload, NAME_FIELDS)
        feature_type = self._open_location_feature_type(str(source["source_family"]), layer_name, raw_payload)
        signature_payload = _json_dumps(
            {
                "source_key": source["source_key"],
                "layer": layer_name,
                "reference": reference,
                "geometry_hash": self._geometry_hash(geometry),
            }
        )
        return {
            "source_key": source["source_key"],
            "source_family": source["source_family"],
            "source_record_id": f"{source['source_key']}:{_slug(layer_name)}:{_slug(reference)}",
            "feature_type": feature_type,
            "feature_name": feature_name,
            "source_layer": layer_name,
            "source_url": selected_download.get("url"),
            "geometry_wkb": _geometry_hex(geometry),
            "source_record_signature": hashlib.md5(signature_payload.encode("utf-8")).hexdigest(),
            "raw_payload": _json_dumps(
                {
                    **raw_payload,
                    "_download_file_name": selected_download.get("fileName"),
                    "_download_format": selected_download.get("format"),
                    "_licence_basis": source.get("licence_basis") or "open_data",
                }
            ),
        }

    def _open_location_feature_type(
        self,
        source_family: str,
        layer_name: str,
        raw_payload: dict[str, Any],
    ) -> str:
        if source_family == "os_open_roads":
            return "road"
        if source_family == "os_open_rivers":
            return "watercourse"
        if source_family == "os_boundary_line":
            return _slug(_pick_text(raw_payload, ("boundary_type", "type", "theme")) or "administrative_boundary")
        if source_family == "os_open_names":
            return _slug(_pick_text(raw_payload, ("local_type", "type", "classification")) or "place_name")
        if source_family == "os_open_greenspace":
            return _slug(_pick_text(raw_payload, ("function", "function_code", "type")) or "greenspace")
        if source_family == "os_open_built_up_areas":
            return "built_up_area"
        if source_family == "os_open_toid":
            return "toid"
        if source_family == "os_open_uprn":
            return "uprn"
        if source_family == "os_open_usrn":
            return "usrn"
        if source_family == "osm":
            return _slug(
                raw_payload.get("amenity")
                or raw_payload.get("shop")
                or raw_payload.get("power")
                or raw_payload.get("highway")
                or raw_payload.get("landuse")
                or "osm_context"
            )
        return _slug(layer_name or source_family)

    def _geometry_intersects_scotland_bbox(self, geometry: BaseGeometry | None) -> bool:
        if geometry is None or geometry.is_empty:
            return False
        minx, miny, maxx, maxy = geometry.bounds
        return maxx >= -100000 and minx <= 470000 and maxy >= 530000 and miny <= 1220000

    def _fetch_osm_open_location_rows(
        self,
        source: dict[str, Any],
        site: dict[str, Any],
        element_limit: int,
    ) -> list[dict[str, Any]]:
        south = float(site["south"])
        west = float(site["west"])
        north = float(site["north"])
        east = float(site["east"])
        query = f"""
        [out:json][timeout:25];
        (
          node["amenity"]({south},{west},{north},{east});
          node["shop"]({south},{west},{north},{east});
          node["highway"]({south},{west},{north},{east});
          node["power"]({south},{west},{north},{east});
          way["highway"]({south},{west},{north},{east});
          way["power"]({south},{west},{north},{east});
          way["landuse"]({south},{west},{north},{east});
          relation["landuse"]({south},{west},{north},{east});
        );
        out center tags {element_limit};
        """
        response = self.client.post(
            str(source.get("endpoint_url") or "https://overpass-api.de/api/interpreter"),
            data={"data": query},
            headers={
                "Accept": "application/json",
                "User-Agent": "LandIntel/1.0 (+https://github.com/LDN-reece/LandIntel)",
            },
        )
        response.raise_for_status()
        payload = response.json()
        rows: list[dict[str, Any]] = []
        for element in payload.get("elements") or []:
            tags = dict(element.get("tags") or {})
            latitude = element.get("lat") or (element.get("center") or {}).get("lat")
            longitude = element.get("lon") or (element.get("center") or {}).get("lon")
            lat_float = _to_float(latitude)
            lon_float = _to_float(longitude)
            if lat_float is None or lon_float is None:
                continue
            geometry = gpd.GeoSeries([Point(lon_float, lat_float)], crs=4326).to_crs(27700).iloc[0]
            raw_payload = {
                **tags,
                "osm_type": element.get("type"),
                "osm_id": element.get("id"),
                "source_canonical_site_id": site.get("canonical_site_id"),
                "latitude": lat_float,
                "longitude": lon_float,
            }
            rows.append(
                {
                    "source_key": source["source_key"],
                    "source_family": source["source_family"],
                    "source_record_id": f"{source['source_key']}:{element.get('type')}:{element.get('id')}",
                    "feature_type": self._open_location_feature_type("osm", "overpass", raw_payload),
                    "feature_name": tags.get("name"),
                    "source_layer": "overpass_site_bbox",
                    "source_url": source.get("endpoint_url"),
                    "geometry_wkb": _geometry_hex(geometry),
                    "source_record_signature": hashlib.md5(_json_dumps(raw_payload).encode("utf-8")).hexdigest(),
                    "raw_payload": _json_dumps(raw_payload),
                }
            )
        return rows

    def _upsert_open_location_spine_rows(self, rows: list[dict[str, Any]]) -> int:
        valid_rows = [row for row in rows if row.get("geometry_wkb")]
        if not valid_rows:
            return 0
        self.database.execute_many(
            """
            insert into landintel.open_location_spine_features (
                source_key,
                source_family,
                source_record_id,
                feature_type,
                feature_name,
                source_layer,
                source_url,
                geometry,
                source_record_signature,
                raw_payload,
                updated_at
            ) values (
                :source_key,
                :source_family,
                :source_record_id,
                :feature_type,
                :feature_name,
                :source_layer,
                :source_url,
                ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700),
                :source_record_signature,
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_key, source_record_id) do update set
                source_family = excluded.source_family,
                feature_type = excluded.feature_type,
                feature_name = excluded.feature_name,
                source_layer = excluded.source_layer,
                source_url = excluded.source_url,
                geometry = excluded.geometry,
                source_record_signature = excluded.source_record_signature,
                raw_payload = excluded.raw_payload,
                updated_at = now()
            """,
            valid_rows,
        )
        return len(valid_rows)

    def _refresh_open_location_spine_context(
        self,
        source_keys: list[str],
        site_batch_size: int,
    ) -> dict[str, Any]:
        if not source_keys:
            return {"linked_rows": 0, "measured_rows": 0, "evidence_rows": 0, "signal_rows": 0}
        return self.database.fetch_one(
            """
            with selected_sites as (
                select site.id, site.geometry
                from landintel.canonical_sites as site
                where site.geometry is not null
                order by site.updated_at desc nulls last, site.id
                limit :site_batch_size
            ),
            feature_types as (
                select distinct source_key, source_family, feature_type
                from landintel.open_location_spine_features
                where geometry is not null
                  and source_key = any(cast(:source_keys as text[]))
            ),
            nearest_features as (
                select
                    site.id as canonical_site_id,
                    feature_types.source_key,
                    feature_types.source_family,
                    feature_types.feature_type,
                    nearest.id as nearest_feature_id,
                    nearest.feature_name as nearest_feature_name,
                    nearest.nearest_distance_m,
                    nearby.count_within_400m,
                    nearby.count_within_800m,
                    nearby.count_within_1600m
                from selected_sites as site
                join feature_types on true
                join lateral (
                    select
                        feature.id,
                        feature.feature_name,
                        st_distance(site.geometry, feature.geometry) as nearest_distance_m
                    from landintel.open_location_spine_features as feature
                    where feature.geometry is not null
                      and feature.source_key = feature_types.source_key
                      and feature.feature_type = feature_types.feature_type
                    order by site.geometry OPERATOR(extensions.<->) feature.geometry, feature.id
                    limit 1
                ) as nearest on true
                cross join lateral (
                    select
                        count(*) filter (where st_dwithin(site.geometry, feature.geometry, 400))::integer as count_within_400m,
                        count(*) filter (where st_dwithin(site.geometry, feature.geometry, 800))::integer as count_within_800m,
                        count(*) filter (where st_dwithin(site.geometry, feature.geometry, 1600))::integer as count_within_1600m
                    from landintel.open_location_spine_features as feature
                    where feature.geometry is not null
                      and feature.source_key = feature_types.source_key
                      and feature.feature_type = feature_types.feature_type
                      and feature.geometry OPERATOR(extensions.&&) st_expand(site.geometry, 1600)
                ) as nearby
            ),
            upserted_context as (
                insert into landintel.site_open_location_spine_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    feature_type,
                    nearest_feature_id,
                    nearest_feature_name,
                    nearest_distance_m,
                    count_within_400m,
                    count_within_800m,
                    count_within_1600m,
                    source_record_signature,
                    metadata,
                    measured_at,
                    updated_at
                )
                select
                    nearest_features.canonical_site_id,
                    nearest_features.source_key,
                    nearest_features.source_family,
                    nearest_features.feature_type,
                    nearest_features.nearest_feature_id,
                    nearest_features.nearest_feature_name,
                    round(nearest_features.nearest_distance_m::numeric, 2),
                    nearest_features.count_within_400m,
                    nearest_features.count_within_800m,
                    nearest_features.count_within_1600m,
                    md5(concat_ws('|', nearest_features.canonical_site_id::text, nearest_features.source_key, nearest_features.feature_type, nearest_features.nearest_feature_id::text, round(nearest_features.nearest_distance_m::numeric, 2)::text)),
                    jsonb_build_object('source_key', nearest_features.source_key, 'feature_type', nearest_features.feature_type, 'source_limitation', 'open_location_context_not_legal_or_engineering_evidence'),
                    now(),
                    now()
                from nearest_features
                on conflict (canonical_site_id, source_key, feature_type) do update set
                    source_family = excluded.source_family,
                    nearest_feature_id = excluded.nearest_feature_id,
                    nearest_feature_name = excluded.nearest_feature_name,
                    nearest_distance_m = excluded.nearest_distance_m,
                    count_within_400m = excluded.count_within_400m,
                    count_within_800m = excluded.count_within_800m,
                    count_within_1600m = excluded.count_within_1600m,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    measured_at = now(),
                    updated_at = now()
                returning *
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using upserted_context
                where evidence.canonical_site_id = upserted_context.canonical_site_id
                  and evidence.source_family = upserted_context.source_family
                  and evidence.metadata ->> 'source_key' = upserted_context.source_key
                  and evidence.metadata ->> 'feature_type' = upserted_context.feature_type
                returning evidence.id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using upserted_context
                where signal.canonical_site_id = upserted_context.canonical_site_id
                  and signal.source_family = upserted_context.source_family
                  and signal.signal_family = 'location_context'
                  and signal.signal_name = 'open_location_spine_proximity'
                  and signal.metadata ->> 'source_key' = upserted_context.source_key
                  and signal.metadata ->> 'feature_type' = upserted_context.feature_type
                returning signal.id
            ),
            inserted_evidence as (
                insert into landintel.evidence_references (
                    canonical_site_id,
                    source_family,
                    source_dataset,
                    source_record_id,
                    source_reference,
                    confidence,
                    metadata
                )
                select
                    upserted_context.canonical_site_id,
                    upserted_context.source_family,
                    'Open location spine context',
                    upserted_context.id::text,
                    coalesce(upserted_context.nearest_feature_name, upserted_context.feature_type),
                    'medium',
                    jsonb_build_object(
                        'source_key', upserted_context.source_key,
                        'feature_type', upserted_context.feature_type,
                        'nearest_distance_m', upserted_context.nearest_distance_m,
                        'source_limitation', 'open_location_context_not_legal_or_engineering_evidence'
                    )
                from upserted_context
                returning id
            ),
            inserted_signals as (
                insert into landintel.site_signals (
                    canonical_site_id,
                    signal_family,
                    signal_name,
                    signal_value_text,
                    signal_value_numeric,
                    confidence,
                    source_family,
                    source_record_id,
                    fact_label,
                    evidence_metadata,
                    metadata,
                    current_flag
                )
                select
                    upserted_context.canonical_site_id,
                    'location_context',
                    'open_location_spine_proximity',
                    upserted_context.feature_type,
                    upserted_context.nearest_distance_m,
                    0.55,
                    upserted_context.source_family,
                    upserted_context.id::text,
                    'open_location_context_evidence',
                    jsonb_build_object('source_key', upserted_context.source_key, 'feature_type', upserted_context.feature_type),
                    jsonb_build_object('source_key', upserted_context.source_key, 'feature_type', upserted_context.feature_type),
                    true
                from upserted_context
                returning id
            )
            select
                (select count(distinct canonical_site_id)::integer from upserted_context) as linked_rows,
                (select count(distinct canonical_site_id)::integer from upserted_context) as measured_rows,
                (select count(*)::integer from inserted_evidence) as evidence_rows,
                (select count(*)::integer from inserted_signals) as signal_rows
            """,
            {"source_keys": source_keys, "site_batch_size": site_batch_size},
        ) or {"linked_rows": 0, "measured_rows": 0, "evidence_rows": 0, "signal_rows": 0}

    def _update_open_location_lifecycle(self, source: dict[str, Any], result: dict[str, Any]) -> None:
        raw_rows = int(result.get("raw_rows") or 0)
        linked_rows = int(result.get("linked_rows") or 0)
        measured_rows = int(result.get("measured_rows") or 0)
        evidence_rows = int(result.get("evidence_rows") or 0)
        signal_rows = int(result.get("signal_rows") or 0)
        self.database.execute(
            """
            update landintel.source_estate_registry
            set access_status = 'access_confirmed',
                ingest_status = case when :raw_rows > 0 then 'raw_data_landed' else ingest_status end,
                normalisation_status = case when :raw_rows > 0 then 'normalised' else normalisation_status end,
                site_link_status = case when :linked_rows > 0 then 'linked_to_site' else site_link_status end,
                measurement_status = case when :measured_rows > 0 then 'measured' else measurement_status end,
                evidence_status = case when :evidence_rows > 0 then 'evidence_generated' else evidence_status end,
                signal_status = case when :signal_rows > 0 then 'signals_generated' else signal_status end,
                trusted_for_review = false,
                limitation_notes = coalesce(:limitation_notes, limitation_notes),
                next_action = coalesce(:next_action, next_action),
                lifecycle_metadata = lifecycle_metadata || cast(:metadata as jsonb),
                updated_at = now()
            where source_key = :source_key
            """,
            {
                "source_key": source["source_key"],
                "raw_rows": raw_rows,
                "linked_rows": linked_rows,
                "measured_rows": measured_rows,
                "evidence_rows": evidence_rows,
                "signal_rows": signal_rows,
                "limitation_notes": source.get("limitation_notes")
                or "Open location spine context only; does not prove legal access, ownership or engineering capacity.",
                "next_action": source.get("next_action")
                or "Keep measuring site context and promote only after coverage and source limitations are proven.",
                "metadata": _json_dumps(
                    {
                        "latest_bulk_landing": {
                            "raw_rows": raw_rows,
                            "linked_rows": linked_rows,
                            "measured_rows": measured_rows,
                            "evidence_rows": evidence_rows,
                            "signal_rows": signal_rows,
                            "status": result.get("status"),
                        }
                    }
                ),
            },
        )

    def _fetch_wfs_source_frames(self, source: dict[str, Any]) -> list[gpd.GeoDataFrame]:
        endpoint_url = str(source["endpoint_url"])
        type_names = self._wfs_feature_types(source)
        if not type_names:
            raise RuntimeError(f"No WFS feature types found for {source['source_key']}.")
        frames: list[gpd.GeoDataFrame] = []
        for type_name in type_names:
            params = {
                "service": "WFS",
                "version": "1.0.0",
                "request": "GetFeature",
                "typeName": type_name,
                "outputFormat": "application/json",
                "srsName": "EPSG:27700",
            }
            params.update(self._auth_params(source))
            if self.max_features > 0:
                params["maxFeatures"] = str(self.max_features)
            response = self.client.get(endpoint_url, params=params)
            response.raise_for_status()
            payload = self._json_payload(response, f"WFS GetFeature {source['source_key']} {type_name}")
            frame = self._feature_collection_to_gdf(payload, source, type_name)
            if not frame.empty:
                frames.append(frame)
        return frames

    def _load_wfs_constraint_source(self, source: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for frame in self._fetch_wfs_source_frames(source):
            layer_name = str(frame["_source_layer_name"].iloc[0])
            layer_key = self._ensure_constraint_layer(source, layer_name)
            raw_rows = self._insert_constraint_features(source, layer_key, frame, run_id)
            results.append({"layer_key": layer_key, "layer_name": layer_name, "raw_rows": raw_rows})
        return results

    def _load_arcgis_constraint_source(self, source: dict[str, Any], run_id: str) -> list[dict[str, Any]]:
        endpoint_url = str(source["endpoint_url"]).rstrip("/")
        service_payload = self._fetch_json(endpoint_url, {"f": "json"})
        layers = list(service_payload.get("layers") or [])
        if not layers and str(service_payload.get("type") or "").lower().endswith("layer"):
            layers = [{"id": None, "name": service_payload.get("name") or source["source_name"]}]
        if not layers:
            raise RuntimeError(f"ArcGIS service has no layers: {source['source_key']}")

        results: list[dict[str, Any]] = []
        for layer in layers:
            layer_id = layer.get("id")
            layer_name = str(layer.get("name") or layer_id or source["source_name"])
            layer_url = endpoint_url if layer_id is None else f"{endpoint_url}/{layer_id}"
            frame = self._fetch_arcgis_layer(source, layer_url, layer_name)
            if frame.empty:
                continue
            layer_key = self._ensure_constraint_layer(source, layer_name)
            raw_rows = self._insert_constraint_features(source, layer_key, frame, run_id)
            results.append({"layer_key": layer_key, "layer_name": layer_name, "raw_rows": raw_rows})
        return results

    def _fetch_arcgis_layer(self, source: dict[str, Any], layer_url: str, layer_name: str) -> gpd.GeoDataFrame:
        frames: list[gpd.GeoDataFrame] = []
        fetched = 0
        offset = 0
        while True:
            params = {
                "f": "geojson",
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "27700",
                "resultOffset": str(offset),
                "resultRecordCount": str(self.page_size),
            }
            response = self.client.get(f"{layer_url.rstrip('/')}/query", params=params)
            response.raise_for_status()
            payload = self._json_payload(response, f"ArcGIS query {source['source_key']} {layer_name}")
            frame = self._feature_collection_to_gdf(payload, source, layer_name)
            if frame.empty:
                break
            frames.append(frame)
            batch_count = len(frame)
            fetched += batch_count
            if batch_count < self.page_size:
                break
            if self.max_features > 0 and fetched >= self.max_features:
                break
            offset += self.page_size
        if not frames:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)
        combined = pd.concat(frames, ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry="geometry", crs=frames[0].crs or 27700)

    def _feature_collection_to_gdf(
        self,
        payload: dict[str, Any],
        source: dict[str, Any],
        layer_name: str,
    ) -> gpd.GeoDataFrame:
        features = list(payload.get("features") or [])
        rows: list[dict[str, Any]] = []
        for index, feature in enumerate(features):
            geometry_payload = feature.get("geometry")
            if not geometry_payload:
                continue
            try:
                geometry = shape(geometry_payload)
            except Exception:
                continue
            properties = dict(feature.get("properties") or {})
            properties["_source_feature_id"] = str(
                feature.get("id")
                or _pick_text(properties, REFERENCE_FIELDS)
                or f"{source['source_key']}:{_slug(layer_name)}:{index}"
            )
            properties["_source_layer_name"] = layer_name
            properties["_source_key"] = source["source_key"]
            properties["geometry"] = geometry
            rows.append(properties)
        if not rows:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)
        frame = gpd.GeoDataFrame(rows, geometry="geometry", crs=27700)
        return self._normalise_frame_crs(frame)

    def _fetch_settlement_boundary_frame(self, source: dict[str, Any]) -> gpd.GeoDataFrame:
        endpoint_url = str(source.get("endpoint_url") or "")
        if self._settlement_uses_arcgis(source):
            return self._fetch_settlement_boundary_arcgis_frame(source, endpoint_url)
        try:
            return self._fetch_settlement_boundary_wfs_frame(source)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 403:
                raise
            self.logger.warning(
                "nrs_settlement_wfs_forbidden_using_arcgis_rest",
                extra={"source_key": source.get("source_key"), "endpoint_url": endpoint_url},
            )
            return self._fetch_settlement_boundary_arcgis_frame(source)

    def _settlement_uses_arcgis(self, source: dict[str, Any]) -> bool:
        endpoint_url = str(source.get("endpoint_url") or "")
        mode = str(source.get("orchestration_mode") or "")
        return "/rest/services/" in endpoint_url.lower() or "arcgis" in mode.lower()

    def _fetch_settlement_boundary_wfs_frame(self, source: dict[str, Any]) -> gpd.GeoDataFrame:
        endpoint_url = str(source["endpoint_url"])
        type_name = str(source.get("wfs_type_name") or "NRS:SettlementBoundaries")
        frames: list[gpd.GeoDataFrame] = []
        fetched = 0
        offset = 0
        page_size = max(1, self.page_size)

        while True:
            batch_limit = page_size
            if self.max_features > 0:
                remaining = self.max_features - fetched
                if remaining <= 0:
                    break
                batch_limit = min(batch_limit, remaining)

            params: dict[str, Any] = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": type_name,
                "outputFormat": "GEOJSON",
                "srsName": "EPSG:27700",
                "count": str(batch_limit),
            }
            params.update(self._auth_params(source))
            if offset > 0:
                params["startIndex"] = str(offset)

            response = self.client.get(endpoint_url, params=params, headers=NRS_REQUEST_HEADERS)
            response.raise_for_status()
            payload = self._json_payload(response, f"NRS settlement WFS {source['source_key']} {type_name}")
            frame = self._feature_collection_to_gdf(payload, source, type_name)
            if frame.empty:
                break

            frame = frame.copy()
            frame["geometry"] = frame.geometry.apply(
                lambda geometry: force_2d(geometry) if geometry is not None else None
            )
            frames.append(gpd.GeoDataFrame(frame, geometry="geometry", crs=frame.crs or 27700))

            batch_count = len(frame)
            fetched += batch_count
            if batch_count < batch_limit:
                break
            offset += batch_count

        if not frames:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)
        combined = pd.concat(frames, ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry="geometry", crs=frames[0].crs or 27700)

    def _fetch_settlement_boundary_arcgis_frame(
        self,
        source: dict[str, Any],
        layer_url: str | None = None,
    ) -> gpd.GeoDataFrame:
        metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
        endpoint_url = (
            layer_url
            or str(source.get("arcgis_layer_url") or metadata.get("arcgis_layer_url") or "")
            or NRS_SETTLEMENT_ARCGIS_LAYER_URL
        )
        frames: list[gpd.GeoDataFrame] = []
        fetched = 0
        offset = 0
        page_size = max(1, min(self.page_size, 1000))
        layer_name = "SettlementBoundaries"

        while True:
            batch_limit = page_size
            if self.max_features > 0:
                remaining = self.max_features - fetched
                if remaining <= 0:
                    break
                batch_limit = min(batch_limit, remaining)

            params: dict[str, Any] = {
                "f": "geojson",
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "27700",
                "resultOffset": str(offset),
                "resultRecordCount": str(batch_limit),
            }
            params.update(self._auth_params(source))
            response = self.client.get(
                f"{endpoint_url.rstrip('/')}/query",
                params=params,
                headers=NRS_REQUEST_HEADERS,
            )
            response.raise_for_status()
            payload = self._json_payload(response, f"NRS settlement ArcGIS REST {source['source_key']}")
            frame = self._feature_collection_to_gdf(payload, source, layer_name)
            if frame.empty:
                break

            frame = frame.copy()
            frame["geometry"] = frame.geometry.apply(
                lambda geometry: force_2d(geometry) if geometry is not None else None
            )
            frames.append(gpd.GeoDataFrame(frame, geometry="geometry", crs=frame.crs or 27700))

            batch_count = len(frame)
            fetched += batch_count
            if batch_count < batch_limit:
                break
            offset += batch_count

        if not frames:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)
        combined = pd.concat(frames, ignore_index=True)
        return gpd.GeoDataFrame(combined, geometry="geometry", crs=frames[0].crs or 27700)

    def _is_direct_zip_resource(self, resource: dict[str, Any]) -> bool:
        url = str(resource.get("url") or "")
        resource_format = str(resource.get("format") or resource.get("mimetype") or "").lower()
        host = urlparse(url).netloc.lower()
        return "data.spatialhub.scot" in host and (url.lower().endswith(".zip") or "zip" in resource_format)

    def _fetch_ldp_resource_frames(
        self,
        source: dict[str, Any],
        resource: dict[str, Any],
        tmp_dir: str,
    ) -> list[gpd.GeoDataFrame]:
        url = str(resource.get("url") or "")
        if not url:
            return []
        resource_id = str(resource.get("id") or _slug(resource.get("name") or url))
        zip_path = Path(tmp_dir) / f"{_slug(resource_id)}.zip"
        with self.client.stream("GET", url) as response:
            response.raise_for_status()
            with zip_path.open("wb") as file_handle:
                for chunk in response.iter_bytes():
                    if chunk:
                        file_handle.write(chunk)

        dataset_uri = f"zip://{zip_path}"
        layer_names = self._vector_layer_names(dataset_uri)
        if self.ldp_max_layers_per_resource > 0:
            layer_names = layer_names[: self.ldp_max_layers_per_resource]

        frames: list[gpd.GeoDataFrame] = []
        for layer_name in layer_names:
            frame = gpd.read_file(dataset_uri, layer=layer_name) if layer_name else gpd.read_file(dataset_uri)
            normalised = self._normalise_ldp_frame(frame, source, resource, layer_name)
            if not normalised.empty:
                frames.append(normalised)
        return frames

    def _vector_layer_names(self, dataset_uri: str) -> list[str | None]:
        try:
            layers = gpd.list_layers(dataset_uri)
        except Exception:
            return [None]
        if hasattr(layers, "to_dict"):
            records = list(layers.to_dict(orient="records"))
            names = [
                str(record.get("name"))
                for record in records
                if record.get("name") and str(record.get("geometry_type") or "").lower() != "none"
            ]
            return names or [None]
        names = [str(value) for value in layers if value]
        return names or [None]

    def _normalise_ldp_frame(
        self,
        frame: gpd.GeoDataFrame,
        source: dict[str, Any],
        resource: dict[str, Any],
        layer_name: str | None,
    ) -> gpd.GeoDataFrame:
        if frame.empty or "geometry" not in frame.columns:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)
        frame = gpd.GeoDataFrame(frame, geometry="geometry", crs=frame.crs)
        frame = frame[frame.geometry.notna() & ~frame.geometry.is_empty].copy()
        if frame.empty:
            return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=27700)
        if frame.crs:
            frame = frame.to_crs(27700)
        else:
            frame = self._normalise_frame_crs(frame)
        frame["geometry"] = frame.geometry.apply(lambda geometry: force_2d(geometry) if geometry is not None else None)

        resource_name = str(resource.get("name") or resource.get("description") or "Local Development Plan resource")
        resource_id = str(resource.get("id") or _slug(resource_name))
        layer_label = str(layer_name or resource_name)
        authority_name = _authority_from_ldp_resource_name(resource_name) or "Scotland"
        frame["_source_layer_name"] = layer_label
        frame["_source_key"] = source["source_key"]
        frame["_ldp_resource_id"] = resource_id
        frame["_ldp_resource_name"] = resource_name
        frame["_ldp_resource_url"] = resource.get("url")
        frame["_ldp_resource_last_modified"] = resource.get("last_modified") or resource.get("created")
        frame["_ldp_authority_name"] = authority_name
        frame["_ldp_plan_period"] = _plan_period_from_text(resource_name)
        frame["_source_feature_id"] = [
            f"{resource_id}:{_slug(layer_label)}:{_slug(_pick_text(row, REFERENCE_FIELDS) or index)}"
            for index, row in enumerate(frame.to_dict(orient="records"))
        ]
        return gpd.GeoDataFrame(frame, geometry="geometry", crs=27700)

    def _normalise_frame_crs(self, frame: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        if frame.empty:
            return frame.set_crs(27700, allow_override=True)
        bounds = frame.total_bounds
        finite_bounds = [value for value in bounds if value is not None and math.isfinite(float(value))]
        looks_like_lonlat = (
            len(finite_bounds) == 4
            and abs(float(bounds[0])) <= 180
            and abs(float(bounds[2])) <= 180
            and abs(float(bounds[1])) <= 90
            and abs(float(bounds[3])) <= 90
        )
        if looks_like_lonlat:
            frame = frame.set_crs(4326, allow_override=True).to_crs(27700)
        else:
            frame = frame.set_crs(27700, allow_override=True)
        return frame

    def _replace_future_context_rows(
        self,
        source_family: str,
        source: dict[str, Any],
        frame: gpd.GeoDataFrame,
        run_id: str,
    ) -> int:
        table_name = self._future_context_table(source_family)
        self.database.execute(f"delete from {table_name} where source_family = :source_family", {"source_family": source_family})
        if frame.empty:
            return 0
        registry_id = self._source_estate_registry_id(source["source_key"])
        params: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records")):
            geometry = _polygonize_geometry(row.get("geometry"))
            if geometry is None:
                continue
            source_record_id = self._source_record_id(source, row, index)
            raw_payload = _json_dumps(_raw_payload(row))
            params.append(
                {
                    "source_key": source["source_key"],
                    "source_family": source_family,
                    "source_record_id": source_record_id,
                    "authority_name": _pick_text(row, AUTHORITY_FIELDS),
                    "site_reference": _pick_text(row, REFERENCE_FIELDS),
                    "site_name": _pick_text(row, NAME_FIELDS) or source_record_id,
                    "status_text": _pick_text(row, STATUS_FIELDS),
                    "geometry_wkb": _geometry_hex(geometry),
                    "source_estate_registry_id": registry_id,
                    "ingest_run_id": run_id,
                    "source_record_signature": self._signature(raw_payload, geometry),
                    "geometry_hash": self._geometry_hash(geometry),
                    "raw_payload": raw_payload,
                }
            )
        insert_sql = f"""
            insert into {table_name} (
                source_key,
                source_family,
                source_record_id,
                authority_name,
                site_reference,
                site_name,
                status_text,
                geometry,
                source_estate_registry_id,
                ingest_run_id,
                source_record_signature,
                geometry_hash,
                raw_payload,
                updated_at
            ) values (
                :source_key,
                :source_family,
                :source_record_id,
                :authority_name,
                :site_reference,
                :site_name,
                :status_text,
                ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                cast(:source_estate_registry_id as uuid),
                cast(:ingest_run_id as uuid),
                :source_record_signature,
                :geometry_hash,
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_family, source_record_id) do update set
                source_key = excluded.source_key,
                authority_name = excluded.authority_name,
                site_reference = excluded.site_reference,
                site_name = excluded.site_name,
                status_text = excluded.status_text,
                geometry = excluded.geometry,
                source_estate_registry_id = excluded.source_estate_registry_id,
                ingest_run_id = excluded.ingest_run_id,
                source_record_signature = excluded.source_record_signature,
                geometry_hash = excluded.geometry_hash,
                raw_payload = excluded.raw_payload,
                updated_at = now()
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(insert_sql, batch)
        return len(params)

    def _replace_ldp_rows(
        self,
        source: dict[str, Any],
        frame: gpd.GeoDataFrame,
        run_id: str,
        source_registry_id: str | None,
    ) -> int:
        if frame.empty:
            return 0
        params: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records")):
            geometry = row.get("geometry")
            if geometry is None or getattr(geometry, "is_empty", True):
                continue
            raw_payload = _json_dumps(_raw_payload(row))
            source_record_id = self._source_record_id(source, row, index)
            resource_name = str(row.get("_ldp_resource_name") or source["source_name"])
            authority_name = (
                _pick_text(row, AUTHORITY_FIELDS)
                or str(row.get("_ldp_authority_name") or "").strip()
                or "Scotland"
            )
            plan_period = (
                _pick_text(row, ("plan_period", "period", "year", "adopted_year"))
                or str(row.get("_ldp_plan_period") or "").strip()
                or _plan_period_from_text(resource_name)
            )
            params.append(
                {
                    "source_record_id": source_record_id,
                    "authority_name": authority_name,
                    "plan_name": _pick_text(row, ("plan_name", "ldp_name", "document_name")) or resource_name,
                    "plan_period": plan_period,
                    "policy_reference": _pick_text(row, LDP_POLICY_FIELDS),
                    "site_reference": _pick_text(row, REFERENCE_FIELDS),
                    "site_name": _pick_text(row, NAME_FIELDS),
                    "allocation_status": _pick_text(row, STATUS_FIELDS),
                    "proposed_use": _pick_text(row, LDP_USE_FIELDS),
                    "support_level": _pick_text(row, ("support_level", "support", "allocation_type", "status", "category")),
                    "policy_constraints": _policy_constraint_values(row),
                    "geometry_wkb": _geometry_hex(geometry),
                    "source_registry_id": source_registry_id,
                    "ingest_run_id": run_id,
                    "raw_payload": raw_payload,
                }
            )
        insert_sql = """
            insert into landintel.ldp_site_records (
                source_record_id,
                authority_name,
                plan_name,
                plan_period,
                policy_reference,
                site_reference,
                site_name,
                allocation_status,
                proposed_use,
                support_level,
                policy_constraints,
                geometry,
                source_registry_id,
                ingest_run_id,
                raw_payload,
                updated_at
            ) values (
                :source_record_id,
                :authority_name,
                :plan_name,
                :plan_period,
                :policy_reference,
                :site_reference,
                :site_name,
                :allocation_status,
                :proposed_use,
                :support_level,
                :policy_constraints,
                ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700),
                cast(:source_registry_id as uuid),
                cast(:ingest_run_id as uuid),
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_record_id) do update set
                authority_name = excluded.authority_name,
                plan_name = excluded.plan_name,
                plan_period = excluded.plan_period,
                policy_reference = excluded.policy_reference,
                site_reference = excluded.site_reference,
                site_name = excluded.site_name,
                allocation_status = excluded.allocation_status,
                proposed_use = excluded.proposed_use,
                support_level = excluded.support_level,
                policy_constraints = excluded.policy_constraints,
                geometry = excluded.geometry,
                source_registry_id = excluded.source_registry_id,
                ingest_run_id = excluded.ingest_run_id,
                raw_payload = excluded.raw_payload,
                updated_at = now()
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(insert_sql, batch)
        return len(params)

    def _replace_settlement_boundary_rows(
        self,
        source: dict[str, Any],
        frame: gpd.GeoDataFrame,
        run_id: str,
        source_registry_id: str | None,
    ) -> int:
        self.database.execute(
            """
            delete from landintel.settlement_boundary_records
            where raw_payload ->> '_source_key' = :source_key
               or source_registry_id = cast(:source_registry_id as uuid)
            """,
            {"source_key": source["source_key"], "source_registry_id": source_registry_id},
        )
        if frame.empty:
            return 0

        params: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records")):
            geometry = _polygonize_geometry(row.get("geometry"))
            if geometry is None:
                continue
            raw_payload_dict = _raw_payload(row)
            raw_payload_dict.update(
                {
                    "_source_key": source["source_key"],
                    "_metadata_uuid": source.get("metadata_uuid"),
                    "_nrs_identifier": source.get("nrs_identifier"),
                    "_nrs_revision_date": source.get("source_revision_date"),
                    "_license": source.get("license"),
                    "_attribution": source.get("attribution"),
                }
            )
            source_reference = _pick_text(row, ("code", "CODE", "source_record_id", "objectid", "OBJECTID")) or str(index)
            settlement_name = _pick_text(row, ("name", "NAME", "settlement_name")) or f"Settlement {source_reference}"
            params.append(
                {
                    "source_record_id": f"{source['source_key']}:settlement_boundaries:{_slug(source_reference)}",
                    "authority_name": "Scotland",
                    "settlement_name": settlement_name,
                    "boundary_role": "settlement",
                    "boundary_status": "current",
                    "geometry_wkb": _geometry_hex(geometry),
                    "source_registry_id": source_registry_id,
                    "ingest_run_id": run_id,
                    "raw_payload": _json_dumps(raw_payload_dict),
                }
            )

        insert_sql = """
            insert into landintel.settlement_boundary_records (
                source_record_id,
                authority_name,
                settlement_name,
                boundary_role,
                boundary_status,
                geometry,
                source_registry_id,
                ingest_run_id,
                raw_payload,
                updated_at
            ) values (
                :source_record_id,
                :authority_name,
                :settlement_name,
                :boundary_role,
                :boundary_status,
                ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                cast(:source_registry_id as uuid),
                cast(:ingest_run_id as uuid),
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_record_id) do update set
                authority_name = excluded.authority_name,
                settlement_name = excluded.settlement_name,
                boundary_role = excluded.boundary_role,
                boundary_status = excluded.boundary_status,
                geometry = excluded.geometry,
                source_registry_id = excluded.source_registry_id,
                ingest_run_id = excluded.ingest_run_id,
                raw_payload = excluded.raw_payload,
                updated_at = now()
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(insert_sql, batch)
        return len(params)

    def _backfill_future_context_authority(self, source_family: str) -> None:
        table_name = self._future_context_table(source_family)
        self.database.execute(
            f"""
            update {table_name} as source_row
            set authority_name = authority.authority_name,
                updated_at = now()
            from public.authority_aoi as authority
            where source_row.authority_name is null
              and source_row.geometry is not null
              and st_intersects(authority.geometry, source_row.geometry)
            """
        )

    def _publish_future_context(self, source_family: str, source: dict[str, Any], run_id: str) -> dict[str, int]:
        table_name = self._future_context_table(source_family)
        source_dataset = str(source["source_name"])
        self._clear_future_context_publish_rows(source_family, source_dataset)
        rows = self.database.read_geodataframe(
            f"""
            select id, source_record_id, authority_name, site_reference, site_name, status_text,
                   source_estate_registry_id, ingest_run_id, source_record_signature, geometry
            from {table_name}
            where geometry is not null
            order by authority_name nulls last, source_record_id
            """
        )
        if rows.empty:
            return {"linked_rows": 0, "evidence_rows": 0, "signal_rows": 0, "change_event_rows": 0}
        rows = rows.set_crs(27700, allow_override=True)
        site_frames_by_authority = self.phase_runner._load_canonical_site_frames()
        updates: list[dict[str, Any]] = []
        aliases: list[dict[str, Any]] = []
        links: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []
        signals: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        refreshes: list[dict[str, Any]] = []

        for row in rows.itertuples(index=False):
            geometry = _polygonize_geometry(row.geometry)
            if geometry is None or not row.authority_name:
                continue
            site_id = self.phase_runner._find_best_site_in_frame(site_frames_by_authority.get(row.authority_name), geometry)
            if site_id is None:
                prefix = "ELA" if source_family == "ela" else "VDL"
                site_id = self.phase_runner._upsert_canonical_site(
                    site_code=self.phase_runner._canonical_site_code(prefix, row.authority_name, row.site_reference or row.source_record_id),
                    site_name=(row.site_name or row.site_reference or row.source_record_id)[:240],
                    authority_name=row.authority_name,
                    geometry=geometry,
                    surfaced_reason=f"Surfaced from {source_dataset} evidence.",
                    metadata={"seed_source": source_family, "source_record_id": row.source_record_id, "phase_one_source_expansion": True},
                )
                self.phase_runner._add_site_frame_geometry(site_frames_by_authority, row.authority_name, site_id, geometry)
            updates.append({"record_id": str(row.id), "canonical_site_id": site_id})
            raw_reference = row.site_reference or row.source_record_id
            aliases.append(
                {
                    "site_id": site_id,
                    "source_family": source_family,
                    "source_dataset": source_dataset,
                    "authority_name": row.authority_name,
                    "site_name": (row.site_name or row.site_reference or row.source_record_id)[:240],
                    "raw_reference_value": raw_reference,
                    "normalized_reference_value": _normalize_ref(raw_reference),
                    "planning_reference": None,
                    "geometry_hash": _normalize_ref(raw_reference),
                    "status": "matched",
                    "confidence": 0.75,
                    "source_registry_id": None,
                    "ingest_run_id": run_id,
                    "metadata": _json_dumps({"source_expansion_direct_publish": True}),
                }
            )
            links.append(
                {
                    "site_id": site_id,
                    "source_family": source_family,
                    "source_dataset": source_dataset,
                    "source_record_id": row.source_record_id,
                    "link_method": "spatial_overlap_or_seed",
                    "confidence": 0.75,
                    "source_registry_id": None,
                    "ingest_run_id": run_id,
                    "metadata": _json_dumps({"source_expansion_direct_publish": True, "status_text": row.status_text}),
                }
            )
            evidence.append(
                {
                    "site_id": site_id,
                    "source_family": source_family,
                    "source_dataset": source_dataset,
                    "source_record_id": row.source_record_id,
                    "source_reference": raw_reference,
                    "confidence": "medium",
                    "source_registry_id": None,
                    "ingest_run_id": run_id,
                    "metadata": _json_dumps(
                        {
                            "source_expansion_direct_publish": True,
                            "site_name": row.site_name,
                            "status_text": row.status_text,
                        }
                    ),
                }
            )
            for signal_name in self._future_context_signal_names(source_family):
                signals.append(
                    {
                        "site_id": site_id,
                        "signal_family": "future_context",
                        "signal_name": signal_name,
                        "signal_value_text": f"{source_dataset} overlap",
                        "signal_value_numeric": None,
                        "confidence": 0.7,
                        "source_family": source_family,
                        "source_record_id": row.source_record_id,
                        "fact_label": "commercial_inference",
                        "evidence_metadata": _json_dumps({"source_dataset": source_dataset, "status_text": row.status_text}),
                        "metadata": _json_dumps({"source_expansion_direct_publish": True}),
                    }
                )
            events.append(
                {
                    "site_id": site_id,
                    "source_family": source_family,
                    "source_record_id": row.source_record_id,
                    "change_type": f"{source_family}_refresh",
                    "change_summary": f"{source_dataset} refreshed for canonical site.",
                    "current_signature": row.source_record_signature,
                    "metadata": _json_dumps({"source_expansion_direct_publish": True}),
                }
            )
            refreshes.append(
                {
                    "site_id": site_id,
                    "source_family": source_family,
                    "source_record_id": row.source_record_id,
                    "metadata": _json_dumps({"source_expansion_direct_publish": True}),
                }
            )

        self._update_future_context_links(source_family, updates)
        self._insert_reference_aliases(aliases)
        self._insert_source_links(links)
        self._insert_evidence(evidence)
        self._insert_signals(signals)
        self._insert_change_events(events)
        self._enqueue_refreshes(refreshes)
        return {
            "linked_rows": len(updates),
            "evidence_rows": len(evidence),
            "signal_rows": len(signals),
            "change_event_rows": len(events),
        }

    def _insert_constraint_features(
        self,
        source: dict[str, Any],
        layer_key: str,
        frame: gpd.GeoDataFrame,
        run_id: str,
    ) -> int:
        if frame.empty:
            return 0
        layer_id = self.database.scalar(
            "select id from public.constraint_layer_registry where layer_key = :layer_key",
            {"layer_key": layer_key},
        )
        params: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records")):
            geometry = row.get("geometry")
            if geometry is None or getattr(geometry, "is_empty", True):
                continue
            source_feature_key = self._source_record_id(source, row, index)
            raw_payload = _json_dumps(_raw_payload(row))
            params.append(
                {
                    "constraint_layer_id": str(layer_id),
                    "source_feature_key": source_feature_key,
                    "feature_name": _pick_text(row, NAME_FIELDS),
                    "source_reference": _pick_text(row, REFERENCE_FIELDS),
                    "authority_name": _pick_text(row, AUTHORITY_FIELDS),
                    "severity_label": _pick_text(row, SEVERITY_FIELDS),
                    "source_url": source.get("endpoint_url"),
                    "geometry_wkb": _geometry_hex(geometry),
                    "metadata": raw_payload,
                }
            )
        insert_sql = """
            insert into public.constraint_source_features (
                constraint_layer_id,
                source_feature_key,
                feature_name,
                source_reference,
                authority_name,
                severity_label,
                source_url,
                geometry,
                metadata,
                updated_at
            ) values (
                cast(:constraint_layer_id as uuid),
                :source_feature_key,
                :feature_name,
                :source_reference,
                :authority_name,
                :severity_label,
                :source_url,
                ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700),
                cast(:metadata as jsonb),
                now()
            )
            on conflict (constraint_layer_id, source_feature_key) do update set
                feature_name = excluded.feature_name,
                source_reference = excluded.source_reference,
                authority_name = excluded.authority_name,
                severity_label = excluded.severity_label,
                source_url = excluded.source_url,
                geometry = excluded.geometry,
                metadata = excluded.metadata,
                updated_at = now()
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(insert_sql, batch)
        return len(params)

    def _ensure_constraint_layer(self, source: dict[str, Any], layer_name: str) -> str:
        source_family = str(source["source_family"])
        group, constraint_type, measurement_mode, buffer_distance_m = CONSTRAINT_GROUPS[source_family]
        layer_key = f"{source_family}:{_slug(layer_name)}"
        self.database.execute(
            """
            insert into public.constraint_layer_registry (
                layer_key,
                layer_name,
                source_name,
                source_family,
                constraint_group,
                constraint_type,
                geometry_type,
                measurement_mode,
                buffer_distance_m,
                is_active,
                metadata
            ) values (
                :layer_key,
                :layer_name,
                :source_name,
                :source_family,
                :constraint_group,
                :constraint_type,
                'mixed',
                :measurement_mode,
                :buffer_distance_m,
                true,
                cast(:metadata as jsonb)
            )
            on conflict (layer_key) do update set
                layer_name = excluded.layer_name,
                source_name = excluded.source_name,
                source_family = excluded.source_family,
                constraint_group = excluded.constraint_group,
                constraint_type = excluded.constraint_type,
                measurement_mode = excluded.measurement_mode,
                buffer_distance_m = excluded.buffer_distance_m,
                is_active = true,
                metadata = public.constraint_layer_registry.metadata || excluded.metadata,
                updated_at = now()
            """,
            {
                "layer_key": layer_key,
                "layer_name": layer_name,
                "source_name": source["source_name"],
                "source_family": source_family,
                "constraint_group": group,
                "constraint_type": constraint_type,
                "measurement_mode": measurement_mode,
                "buffer_distance_m": buffer_distance_m,
                "metadata": _json_dumps({"source_key": source["source_key"], "phase_one_source_expansion": True}),
            },
        )
        return layer_key

    def _clear_constraint_family(self, source_family: str) -> None:
        params = {"source_family": source_family}
        self.database.execute(
            """
            delete from public.site_commercial_friction_facts as fact
            using public.constraint_layer_registry as layer_row
            where fact.constraint_layer_id = layer_row.id
              and layer_row.source_family = :source_family
            """,
            params,
        )
        self.database.execute(
            """
            delete from public.site_constraint_group_summaries as summary
            using public.constraint_layer_registry as layer_row
            where summary.constraint_layer_id = layer_row.id
              and layer_row.source_family = :source_family
            """,
            params,
        )
        self.database.execute(
            """
            delete from public.site_constraint_measurements as measurement
            using public.constraint_layer_registry as layer_row
            where measurement.constraint_layer_id = layer_row.id
              and layer_row.source_family = :source_family
            """,
            params,
        )
        self.database.execute(
            """
            delete from public.constraint_source_features as feature
            using public.constraint_layer_registry as layer_row
            where feature.constraint_layer_id = layer_row.id
              and layer_row.source_family = :source_family
            """,
            params,
        )
        if source_family == "sepa_flood":
            self.database.execute(
                """
                delete from landintel.flood_records
                where flood_source = 'sepa_flood'
                   or flood_source like 'sepa_flood:%'
                   or raw_payload ->> 'source_expansion_constraint' = 'true'
                """
            )
        self.database.execute(
            "delete from landintel.site_signals where source_family = :source_family",
            params,
        )
        self.database.execute(
            """
            delete from landintel.evidence_references
            where source_family = :source_family
              and metadata ->> 'source_expansion_constraint' = 'true'
            """,
            params,
        )

    def _sync_flood_records_from_constraint_features(self) -> int:
        proof = self.database.fetch_one(
            """
            with deleted_duplicates as (
                delete from landintel.flood_records as duplicate
                using landintel.flood_records as keeper
                where duplicate.source_record_id = keeper.source_record_id
                  and duplicate.flood_source = keeper.flood_source
                  and duplicate.id > keeper.id
                returning duplicate.id
            ), synced as (
                insert into landintel.flood_records (
                    source_record_id,
                    authority_name,
                    flood_source,
                    severity_band,
                    geometry,
                    raw_payload,
                    created_at,
                    updated_at
                )
                select distinct on (feature.source_feature_key, layer.layer_key)
                    feature.source_feature_key,
                    feature.authority_name,
                    layer.layer_key,
                    coalesce(feature.severity_label, layer.layer_name),
                    feature.geometry,
                    coalesce(feature.metadata, '{}'::jsonb)
                        || jsonb_build_object(
                            'constraint_layer_key', layer.layer_key,
                            'constraint_group', layer.constraint_group,
                            'constraint_type', layer.constraint_type,
                            'source_expansion_constraint', true,
                            'flood_constraint_feature_id', feature.id
                        ),
                    now(),
                    now()
                from public.constraint_source_features as feature
                join public.constraint_layer_registry as layer
                  on layer.id = feature.constraint_layer_id
                where layer.source_family = 'sepa_flood'
                  and feature.geometry is not null
                on conflict (source_record_id, flood_source) do update set
                    authority_name = excluded.authority_name,
                    severity_band = excluded.severity_band,
                    geometry = excluded.geometry,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                returning id
            )
            select count(*)::integer as flood_record_rows
            from synced
            """
        ) or {}
        return int(proof.get("flood_record_rows") or 0)

    def _clear_future_context_publish_rows(self, source_family: str, source_dataset: str) -> None:
        params = {"source_family": source_family, "source_dataset": source_dataset}
        self.database.execute(
            """
            delete from landintel.site_source_links
            where source_family = :source_family
              and source_dataset = :source_dataset
              and metadata ->> 'source_expansion_direct_publish' = 'true'
            """,
            params,
        )
        self.database.execute(
            """
            delete from landintel.site_reference_aliases
            where source_family = :source_family
              and source_dataset = :source_dataset
              and metadata ->> 'source_expansion_direct_publish' = 'true'
            """,
            params,
        )
        self.database.execute(
            """
            delete from landintel.evidence_references
            where source_family = :source_family
              and source_dataset = :source_dataset
              and metadata ->> 'source_expansion_direct_publish' = 'true'
            """,
            params,
        )
        self.database.execute(
            """
            delete from landintel.site_signals
            where source_family = :source_family
              and metadata ->> 'source_expansion_direct_publish' = 'true'
            """,
            {"source_family": source_family},
        )

    def _update_future_context_links(self, source_family: str, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        table_name = self._future_context_table(source_family)
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(
                f"""
                update {table_name}
                set canonical_site_id = cast(:canonical_site_id as uuid),
                    updated_at = now()
                where id = cast(:record_id as uuid)
                """,
                batch,
            )

    def _insert_reference_aliases(self, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        sql = """
            insert into landintel.site_reference_aliases (
                canonical_site_id,
                source_family,
                source_dataset,
                authority_name,
                site_name,
                raw_reference_value,
                normalized_reference_value,
                planning_reference,
                geometry_hash,
                status,
                confidence,
                source_registry_id,
                ingest_run_id,
                metadata
            ) values (
                cast(:site_id as uuid),
                :source_family,
                :source_dataset,
                :authority_name,
                :site_name,
                :raw_reference_value,
                :normalized_reference_value,
                :planning_reference,
                :geometry_hash,
                :status,
                :confidence,
                cast(:source_registry_id as uuid),
                cast(:ingest_run_id as uuid),
                cast(:metadata as jsonb)
            )
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(sql, batch)

    def _insert_source_links(self, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        sql = """
            insert into landintel.site_source_links (
                canonical_site_id,
                source_family,
                source_dataset,
                source_record_id,
                link_method,
                confidence,
                source_registry_id,
                ingest_run_id,
                metadata
            ) values (
                cast(:site_id as uuid),
                :source_family,
                :source_dataset,
                :source_record_id,
                :link_method,
                :confidence,
                cast(:source_registry_id as uuid),
                cast(:ingest_run_id as uuid),
                cast(:metadata as jsonb)
            )
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(sql, batch)

    def _insert_evidence(self, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        sql = """
            insert into landintel.evidence_references (
                canonical_site_id,
                source_family,
                source_dataset,
                source_record_id,
                source_reference,
                confidence,
                source_registry_id,
                ingest_run_id,
                metadata
            ) values (
                cast(:site_id as uuid),
                :source_family,
                :source_dataset,
                :source_record_id,
                :source_reference,
                :confidence,
                cast(:source_registry_id as uuid),
                cast(:ingest_run_id as uuid),
                cast(:metadata as jsonb)
            )
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(sql, batch)

    def _insert_signals(self, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        sql = """
            insert into landintel.site_signals (
                canonical_site_id,
                signal_family,
                signal_name,
                signal_value_text,
                signal_value_numeric,
                confidence,
                source_family,
                source_record_id,
                fact_label,
                evidence_metadata,
                metadata,
                current_flag
            ) values (
                cast(:site_id as uuid),
                :signal_family,
                :signal_name,
                :signal_value_text,
                :signal_value_numeric,
                :confidence,
                :source_family,
                :source_record_id,
                :fact_label,
                cast(:evidence_metadata as jsonb),
                cast(:metadata as jsonb),
                true
            )
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(sql, batch)

    def _insert_change_events(self, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        sql = """
            insert into landintel.site_change_events (
                canonical_site_id,
                source_family,
                source_record_id,
                change_type,
                change_summary,
                current_signature,
                triggered_refresh,
                metadata
            ) values (
                cast(:site_id as uuid),
                :source_family,
                :source_record_id,
                :change_type,
                :change_summary,
                :current_signature,
                true,
                cast(:metadata as jsonb)
            )
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(sql, batch)

    def _enqueue_refreshes(self, params: list[dict[str, Any]]) -> None:
        if not params:
            return
        sql = """
            insert into landintel.canonical_site_refresh_queue (
                canonical_site_id,
                refresh_scope,
                trigger_source,
                source_family,
                source_record_id,
                status,
                metadata,
                updated_at
            ) values (
                cast(:site_id as uuid),
                'site_outputs',
                'source_expansion_future_context',
                :source_family,
                :source_record_id,
                'pending',
                cast(:metadata as jsonb),
                now()
            )
            on conflict (canonical_site_id, refresh_scope) do update set
                trigger_source = excluded.trigger_source,
                source_family = excluded.source_family,
                source_record_id = excluded.source_record_id,
                status = 'pending',
                claimed_by = null,
                claimed_at = null,
                lease_expires_at = null,
                attempt_count = 0,
                next_attempt_at = null,
                processed_at = null,
                error_message = null,
                metadata = excluded.metadata,
                updated_at = now()
        """
        for batch in chunked(params, self.settings.batch_size):
            self.database.execute_many(sql, batch)

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
        hints = self._layer_hints(source)
        if not hints:
            return _dedupe(names)
        matched = [name for name in names if any(_feature_type_matches(name, hint) for hint in hints)]
        if matched:
            return _dedupe(matched)
        workspace = _workspace_from_url(endpoint_url)
        return [f"{workspace}:{hint}" if workspace else hint for hint in hints]

    def _layer_hints(self, source: dict[str, Any]) -> tuple[str, ...]:
        hints: list[str] = []
        for asset in source.get("static_assets") or []:
            hints.extend(str(layer) for layer in asset.get("layer_names") or [])
        if hints:
            return tuple(hints)
        return DEFAULT_LAYER_HINTS.get(str(source["source_family"]), ())

    def _source_uses_arcgis(self, source: dict[str, Any]) -> bool:
        endpoint = str(source.get("endpoint_url") or "")
        mode = str(source.get("orchestration_mode") or "")
        return "MapServer" in endpoint or "FeatureServer" in endpoint or "arcgis" in mode

    def _probe_source(self, source: dict[str, Any]) -> tuple[str, str]:
        endpoint = str(source.get("endpoint_url") or "")
        if not endpoint:
            return "not_probeable", "No endpoint URL is registered for this source."
        endpoint = self._os_endpoint(source)
        params: dict[str, str] = {}
        headers: dict[str, str] = {"User-Agent": "LandIntel/1.0 (+https://github.com/LDN-reece/LandIntel)"}
        orchestration_mode = str(source.get("orchestration_mode") or "")
        if source["source_family"] == "topography" or orchestration_mode == "os_downloads_opendata":
            return self._probe_os_download_product(source)
        elif source["source_family"] == "os_places":
            params = {"query": "Glasgow", "maxresults": "1", **self._os_key_params("os_places")}
        elif source["source_family"] == "os_features":
            params = {"service": "WFS", "version": "2.0.0", "request": "GetCapabilities", **self._os_key_params("os_features")}
        elif source["source_family"] == "os_linked_identifiers":
            endpoint = self._os_join_endpoint(endpoint, "/productVersionInfo/BLPU_UPRN_TopographicArea_TOID_5", endpoint)
            params = self._os_key_params("os_linked_identifiers")
        elif source["source_family"] == "osm":
            params = {"data": "[out:json][timeout:5];node(55.85,-4.26,55.86,-4.25);out 1;"}
            headers["Accept"] = "application/json,text/plain,*/*"
        elif source["source_family"] == "statistics_gov_scot":
            endpoint = (
                "https://statistics.gov.scot/sparql"
                "?query=select%20%2A%20where%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D%20limit%201"
                "&format=json"
            )
            headers["Accept"] = "application/sparql-results+json, application/json"
        elif source["source_family"] == "opentopography_srtm":
            params = {
                "demtype": "SRTMGL1",
                "south": "55.85",
                "north": "55.86",
                "west": "-4.26",
                "east": "-4.25",
                "outputFormat": "GTiff",
                "API_Key": os.getenv("OPENTOPOGRAPHY_API_KEY", ""),
            }
        try:
            response = self.client.get(endpoint, params=params, headers=headers)
            status = "reachable" if response.status_code < 400 else "failed"
            return status, f"HTTP {response.status_code} from {source['source_name']}"
        except Exception as exc:
            return "failed", str(exc)

    def _probe_os_download_product(self, source: dict[str, Any]) -> tuple[str, str]:
        endpoint = self._os_downloads_product_endpoint(source)
        params: dict[str, str] = {}
        if source.get("download_area"):
            params["area"] = str(source["download_area"])
        try:
            response = self.client.get(
                endpoint,
                params=params,
                headers={"User-Agent": "LandIntel/1.0 (+https://github.com/LDN-reece/LandIntel)"},
            )
            status = "reachable" if response.status_code < 400 else "failed"
            if status != "reachable":
                return status, f"HTTP {response.status_code} from {source['source_name']}"
            payload = response.json()
            downloads = payload if isinstance(payload, list) else []
            formats = sorted(
                {
                    " ".join(
                        part
                        for part in (
                            str(download.get("format") or "").strip(),
                            str(download.get("subformat") or "").strip(),
                        )
                        if part
                    )
                    for download in downloads
                    if isinstance(download, dict)
                }
            )
            return (
                status,
                (
                    f"HTTP {response.status_code} from {source['source_name']}: "
                    f"{len(downloads)} download option(s)"
                    + (f" ({', '.join(formats)})" if formats else "")
                ),
            )
        except Exception as exc:
            return "failed", str(exc)

    def _auth_params(self, source: dict[str, Any]) -> dict[str, str]:
        auth_vars = [str(value) for value in source.get("auth_env_vars") or []]
        if "IMPROVEMENT_SERVICE_AUTHKEY" in auth_vars:
            authkey = os.getenv("IMPROVEMENT_SERVICE_AUTHKEY")
            return {"authkey": authkey} if authkey else {}
        if "BOUNDARY_AUTHKEY" in auth_vars:
            authkey = os.getenv("BOUNDARY_AUTHKEY")
            return {"authkey": authkey} if authkey else {}
        if "OS_API_KEY" in auth_vars or "OS_PLACES_API_KEY" in auth_vars or "OS_PROJECT_API" in auth_vars:
            return self._os_key_params(str(source.get("source_family") or ""))
        return {}

    def _os_key_params(self, source_family: str = "") -> dict[str, str]:
        key = self._os_key_value(source_family)
        return {"key": key} if key else {}

    def _os_key_headers(self, source_family: str = "") -> dict[str, str]:
        key = self._os_key_value(source_family)
        return {"key": key} if key else {}

    def _os_key_value(self, source_family: str = "") -> str | None:
        if source_family == "os_places":
            env_names = ("OS_PROJECT_API", "OS_PLACES_API_KEY", "OS_API_KEY")
        elif source_family == "os_features":
            env_names = ("OS_PROJECT_API", "OS_FEATURES_API_KEY", "OS_API_KEY")
        elif source_family == "os_linked_identifiers":
            env_names = ("OS_PROJECT_API", "OS_API_KEY")
        else:
            env_names = ("OS_PROJECT_API", "OS_API_KEY")
        for env_name in env_names:
            key = os.getenv(env_name)
            if key and not key.lower().startswith(("http://", "https://")):
                return key
        return None

    def _os_endpoint(self, source: dict[str, Any]) -> str:
        source_family = str(source.get("source_family") or "")
        default_endpoint = str(source.get("endpoint_url") or "")
        if source_family == "topography":
            return self._os_downloads_product_endpoint(source)
        if str(source.get("orchestration_mode") or "") == "os_downloads_opendata":
            return self._os_downloads_product_endpoint(source)
        if source_family == "os_places":
            return self._os_join_endpoint(os.getenv("OS_PLACES_API"), "/find", default_endpoint)
        if source_family == "os_features":
            return os.getenv("OS_FEATURES_API") or default_endpoint
        if source_family == "os_linked_identifiers":
            return os.getenv("OS_LINKED_IDENTIFIERS_API") or default_endpoint
        return default_endpoint

    def _os_downloads_product_endpoint(self, source: dict[str, Any]) -> str:
        product_id = str(source.get("product_id") or "")
        default_endpoint = str(source.get("endpoint_url") or "")
        if not product_id:
            return default_endpoint
        configured_base_url = (os.getenv("OS_DOWNLOADS_API") or "").strip()
        base_url = (
            configured_base_url.split("/downloads/v1", 1)[0] + "/downloads/v1"
            if "/downloads/v1" in configured_base_url
            else "https://api.os.uk/downloads/v1"
        )
        marker = "/products/"
        marker_index = base_url.find(marker)
        if marker_index >= 0:
            base_url = base_url[:marker_index]
        cleaned_base_url = base_url.rstrip("/")
        if cleaned_base_url.endswith("/downloads"):
            cleaned_base_url = cleaned_base_url + "/v1"
        return urljoin(cleaned_base_url.rstrip("/") + "/", f"products/{product_id}/downloads")

    def _os_join_endpoint(self, base_url: str | None, suffix: str, default_endpoint: str) -> str:
        if not base_url:
            return default_endpoint
        cleaned_url = base_url.rstrip("/")
        cleaned_suffix_path = "/" + suffix.strip("/")
        if cleaned_url.endswith(cleaned_suffix_path):
            return cleaned_url
        cleaned_base = base_url.rstrip("/") + "/"
        cleaned_suffix = suffix.lstrip("/")
        return urljoin(cleaned_base, cleaned_suffix)

    def _assert_required_secrets(self, sources: list[dict[str, Any]]) -> None:
        missing = sorted(
            {
                str(secret)
                for source in sources
                for secret in source.get("auth_env_vars") or []
                if not self._has_required_secret(str(secret))
            }
        )
        if missing:
            raise RuntimeError("Missing required GitHub Actions secret(s): " + ", ".join(missing))

    def _has_required_secret(self, secret_name: str) -> bool:
        if secret_name == "OS_PROJECT_API":
            return any(self._secret_value_is_api_key(name) for name in ("OS_PROJECT_API", "OS_API_KEY"))
        if secret_name == "OS_PLACES_API_KEY":
            return any(self._secret_value_is_api_key(name) for name in ("OS_PROJECT_API", "OS_PLACES_API_KEY", "OS_API_KEY"))
        if secret_name == "OS_FEATURES_API_KEY":
            return any(self._secret_value_is_api_key(name) for name in ("OS_PROJECT_API", "OS_FEATURES_API_KEY", "OS_API_KEY"))
        return bool(os.getenv(secret_name))

    def _secret_value_is_api_key(self, env_name: str) -> bool:
        value = os.getenv(env_name)
        return bool(value and not value.lower().startswith(("http://", "https://")))

    def _sources_for_family(self, source_family: str) -> list[dict[str, Any]]:
        manifest_sources = [
            dict(source)
            for source in self.manifest.get("sources") or []
            if source.get("source_family") == source_family
        ]
        os_sources = [dict(source) for source in OS_CATALOGUE_SOURCES if source.get("source_family") == source_family]
        return manifest_sources + os_sources

    def _upsert_source_estate(self, source: dict[str, Any]) -> None:
        self.database.execute(
            """
            insert into landintel.source_estate_registry (
                source_key, source_family, source_name, source_group, phase_one_role,
                source_status, orchestration_mode, endpoint_url, auth_env_vars, target_table,
                reconciliation_path, evidence_path, signal_output, ranking_impact,
                resurfacing_trigger, data_age_basis, drive_folder_url, notes,
                ranking_eligible, review_output_eligible, programme_phase, module_key, geography,
                access_status, limitation_notes, next_action, lifecycle_metadata,
                metadata, last_registered_at, updated_at
            ) values (
                :source_key, :source_family, :source_name, :source_group, :phase_one_role,
                :source_status, :orchestration_mode, :endpoint_url, :auth_env_vars, :target_table,
                :reconciliation_path, :evidence_path, :signal_output, :ranking_impact,
                :resurfacing_trigger, :data_age_basis, :drive_folder_url, :notes,
                :ranking_eligible, :review_output_eligible, :programme_phase, :module_key, :geography,
                :access_status, :limitation_notes, :next_action, cast(:lifecycle_metadata as jsonb),
                cast(:metadata as jsonb), now(), now()
            )
            on conflict (source_key) do update set
                source_family = excluded.source_family,
                source_name = excluded.source_name,
                source_group = excluded.source_group,
                phase_one_role = excluded.phase_one_role,
                source_status = excluded.source_status,
                orchestration_mode = excluded.orchestration_mode,
                endpoint_url = excluded.endpoint_url,
                auth_env_vars = excluded.auth_env_vars,
                target_table = excluded.target_table,
                reconciliation_path = excluded.reconciliation_path,
                evidence_path = excluded.evidence_path,
                signal_output = excluded.signal_output,
                ranking_impact = excluded.ranking_impact,
                resurfacing_trigger = excluded.resurfacing_trigger,
                data_age_basis = excluded.data_age_basis,
                drive_folder_url = excluded.drive_folder_url,
                notes = excluded.notes,
                ranking_eligible = excluded.ranking_eligible,
                review_output_eligible = excluded.review_output_eligible,
                programme_phase = excluded.programme_phase,
                module_key = excluded.module_key,
                geography = excluded.geography,
                access_status = case
                    when landintel.source_estate_registry.access_status = 'source_registered'
                        then excluded.access_status
                    else landintel.source_estate_registry.access_status
                end,
                limitation_notes = coalesce(excluded.limitation_notes, landintel.source_estate_registry.limitation_notes),
                next_action = coalesce(excluded.next_action, landintel.source_estate_registry.next_action),
                lifecycle_metadata = landintel.source_estate_registry.lifecycle_metadata || excluded.lifecycle_metadata,
                metadata = excluded.metadata,
                last_registered_at = now(),
                updated_at = now()
            """,
            {
                "source_key": source["source_key"],
                "source_family": source["source_family"],
                "source_name": source["source_name"],
                "source_group": source.get("source_group") or "unknown",
                "phase_one_role": source.get("phase_one_role") or "context",
                "source_status": source.get("source_status") or "unknown",
                "orchestration_mode": source.get("orchestration_mode") or "unknown",
                "endpoint_url": source.get("endpoint_url"),
                "auth_env_vars": list(source.get("auth_env_vars") or []),
                "target_table": source.get("target_table"),
                "reconciliation_path": source.get("reconciliation_path"),
                "evidence_path": source.get("evidence_path"),
                "signal_output": source.get("signal_output"),
                "ranking_impact": source.get("ranking_impact"),
                "resurfacing_trigger": source.get("resurfacing_trigger"),
                "data_age_basis": source.get("data_age_basis"),
                "drive_folder_url": source.get("drive_folder_url"),
                "notes": source.get("notes"),
                "ranking_eligible": bool(source.get("ranking_eligible", source.get("phase_one_role") in {"critical", "target_live"})),
                "review_output_eligible": bool(source.get("review_output_eligible", True)),
                "programme_phase": source.get("programme_phase")
                or ("phase_two" if source.get("source_family") in OPEN_LOCATION_SPINE_BULK_FAMILIES else "phase_one"),
                "module_key": source.get("module_key")
                or ("open_location_spine" if source.get("source_family") in OPEN_LOCATION_SPINE_BULK_FAMILIES else None),
                "geography": source.get("geography")
                or source.get("download_area")
                or ("Great Britain / Scotland context" if source.get("source_family") in OPEN_LOCATION_SPINE_BULK_FAMILIES else None),
                "access_status": source.get("access_status") or "source_registered",
                "limitation_notes": source.get("limitation_notes"),
                "next_action": source.get("next_action")
                or (
                    "Keep landing and measuring bounded open-location batches; do not mark trusted until coverage and limitations are proven."
                    if source.get("source_family") in OPEN_LOCATION_SPINE_BULK_FAMILIES
                    else None
                ),
                "lifecycle_metadata": _json_dumps(
                    {
                        "source_expansion_registered": True,
                        "open_location_spine": source.get("source_family") in OPEN_LOCATION_SPINE_BULK_FAMILIES,
                    }
                ),
                "metadata": _json_dumps(source),
            },
        )

    def _record_source_freshness(
        self,
        source: dict[str, Any],
        freshness_status: str,
        live_access_status: str,
        records_observed: int,
        metadata: dict[str, Any],
    ) -> None:
        self.database.execute(
            """
            insert into landintel.source_freshness_states (
                source_scope_key, source_family, source_dataset, source_name,
                source_access_mode, source_url, refresh_cadence, max_staleness_days,
                source_observed_at, last_checked_at, last_success_at,
                freshness_status, live_access_status, ranking_eligible,
                review_output_eligible, stale_reason_code, check_summary,
                records_observed, metadata, updated_at
            ) values (
                :scope_key, :source_family, :source_dataset, :source_name,
                :source_access_mode, :source_url, :refresh_cadence, :max_staleness_days,
                now(), now(), :last_success_at,
                :freshness_status, :live_access_status, :ranking_eligible,
                :review_output_eligible, :stale_reason_code, :check_summary,
                :records_observed, cast(:metadata as jsonb), now()
            )
            on conflict (source_scope_key) do update set
                source_family = excluded.source_family,
                source_dataset = excluded.source_dataset,
                source_name = excluded.source_name,
                source_access_mode = excluded.source_access_mode,
                source_url = excluded.source_url,
                last_checked_at = excluded.last_checked_at,
                last_success_at = excluded.last_success_at,
                freshness_status = excluded.freshness_status,
                live_access_status = excluded.live_access_status,
                ranking_eligible = excluded.ranking_eligible,
                review_output_eligible = excluded.review_output_eligible,
                stale_reason_code = excluded.stale_reason_code,
                check_summary = excluded.check_summary,
                records_observed = excluded.records_observed,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            {
                "scope_key": f"source_expansion:{source['source_key']}",
                "source_family": source["source_family"],
                "source_dataset": source["source_name"],
                "source_name": source["source_name"],
                "source_access_mode": source.get("orchestration_mode") or "unknown",
                "source_url": source.get("endpoint_url"),
                "refresh_cadence": "weekly" if source.get("phase_one_role") == "critical" else "monthly",
                "max_staleness_days": 7 if source.get("phase_one_role") == "critical" else 30,
                "last_success_at": datetime.now(timezone.utc) if freshness_status in {"current", "empty"} else None,
                "freshness_status": freshness_status,
                "live_access_status": live_access_status,
                "ranking_eligible": bool(source.get("ranking_eligible", source.get("phase_one_role") in {"critical", "target_live"})),
                "review_output_eligible": bool(source.get("review_output_eligible", True)),
                "stale_reason_code": None if freshness_status in {"current", "empty"} else live_access_status,
                "check_summary": str(metadata.get("summary") or f"{source['source_name']} checked by source expansion runner."),
                "records_observed": records_observed,
                "metadata": _json_dumps(metadata),
            },
        )

    def _record_expansion_event(
        self,
        *,
        command_name: str,
        source_key: str | None,
        source_family: str,
        status: str,
        raw_rows: int = 0,
        linked_rows: int = 0,
        measured_rows: int = 0,
        evidence_rows: int = 0,
        signal_rows: int = 0,
        change_event_rows: int = 0,
        summary: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.database.execute(
            """
            insert into landintel.source_expansion_events (
                command_name, source_key, source_family, status, raw_rows, linked_rows,
                measured_rows, evidence_rows, signal_rows, change_event_rows, summary, metadata
            ) values (
                :command_name, :source_key, :source_family, :status, :raw_rows, :linked_rows,
                :measured_rows, :evidence_rows, :signal_rows, :change_event_rows, :summary, cast(:metadata as jsonb)
            )
            """,
            {
                "command_name": command_name,
                "source_key": source_key,
                "source_family": source_family,
                "status": status,
                "raw_rows": raw_rows,
                "linked_rows": linked_rows,
                "measured_rows": measured_rows,
                "evidence_rows": evidence_rows,
                "signal_rows": signal_rows,
                "change_event_rows": change_event_rows,
                "summary": summary,
                "metadata": _json_dumps(metadata or {}),
            },
        )

    def _record_policy_promotion_placeholder(self, source_family: str, command: str) -> dict[str, Any]:
        if source_family == "ldp":
            status = "core_policy_storage_licence_gated"
            summary = "LDP is now stored through ingest-ldp. Ranking still requires commercial-use clearance and a validated policy interpreter."
        else:
            status = "core_policy_storage_interpreter_gated"
            summary = "Settlement boundaries are now stored through ingest-settlement-boundaries. Ranking still requires the canonical settlement-position overlay."
        result = {
            "command": command,
            "source_family": source_family,
            "status": status,
            "summary": summary,
        }
        self._record_expansion_event(
            command_name=command,
            source_key=None,
            source_family=source_family,
            status=result["status"],
            summary=result["summary"],
            metadata=result,
        )
        return result

    def _future_context_table(self, source_family: str) -> str:
        if source_family == "ela":
            return "landintel.ela_site_records"
        if source_family == "vdl":
            return "landintel.vdl_site_records"
        raise KeyError(f"No future-context table for {source_family}.")

    def _future_context_signal_names(self, source_family: str) -> tuple[str, ...]:
        if source_family == "vdl":
            return ("redevelopment_angle", "stalled_site_angle")
        return ("redevelopment_angle", "planning_context")

    def _source_estate_registry_id(self, source_key: str) -> str | None:
        row = self.database.fetch_one(
            "select id from landintel.source_estate_registry where source_key = :source_key",
            {"source_key": source_key},
        )
        return str(row["id"]) if row else None

    def _upsert_ldp_source_registry(self, source: dict[str, Any], package: dict[str, Any]) -> str | None:
        package_id = str(source.get("spatialhub_package_id") or package.get("name") or "local_development_plans-is")
        metadata_uuid = f"spatialhub:{package_id}"
        self.loader.upsert_source_registry(
            [
                SourceRegistryRecord(
                    source_name=str(package.get("title") or source["source_name"]),
                    source_type="Spatial Hub CKAN package",
                    publisher="Improvement Service",
                    metadata_uuid=metadata_uuid,
                    endpoint_url=source.get("endpoint_url"),
                    download_url=f"https://data.spatialhub.scot/dataset/{package_id}",
                    record_json={
                        "dataset": package,
                        "geonetwork_uuid": source.get("metadata_uuid"),
                        "spatialhub_identifier": source.get("spatialhub_identifier"),
                    },
                    geographic_extent=None,
                    last_seen_at=datetime.now(timezone.utc),
                )
            ]
        )
        return self.phase_runner._resolve_source_registry_id(metadata_uuid)

    def _upsert_settlement_source_registry(self, source: dict[str, Any]) -> str | None:
        metadata_uuid = f"nrs:{source.get('nrs_identifier') or 'NRS_SettlementBdry'}"
        self.loader.upsert_source_registry(
            [
                SourceRegistryRecord(
                    source_name=str(source.get("source_name") or "Settlements - Scotland"),
                    source_type="NRS WFS",
                    publisher="National Records of Scotland",
                    metadata_uuid=metadata_uuid,
                    endpoint_url=source.get("endpoint_url"),
                    download_url=str(source.get("information_url") or "https://www.nrscotland.gov.uk/"),
                    record_json={
                        "metadata_uuid": source.get("metadata_uuid"),
                        "nrs_identifier": source.get("nrs_identifier"),
                        "wfs_type_name": source.get("wfs_type_name"),
                        "source_revision_date": source.get("source_revision_date"),
                        "license": source.get("license"),
                        "attribution": source.get("attribution"),
                    },
                    geographic_extent=None,
                    last_seen_at=datetime.now(timezone.utc),
                )
            ]
        )
        return self.phase_runner._resolve_source_registry_id(metadata_uuid)

    def _source_record_id(self, source: dict[str, Any], row: dict[str, Any], index: int) -> str:
        layer_name = str(row.get("_source_layer_name") or source["source_family"])
        picked = str(row.get("_source_feature_id") or _pick_text(row, REFERENCE_FIELDS) or index)
        return f"{source['source_key']}:{_slug(layer_name)}:{_slug(picked)}"

    def _signature(self, raw_payload: str, geometry: BaseGeometry | None) -> str:
        return hashlib.md5((raw_payload + "|" + str(self._geometry_hash(geometry))).encode("utf-8")).hexdigest()

    def _geometry_hash(self, geometry: BaseGeometry | None) -> str | None:
        geometry_wkb = _geometry_hex(geometry)
        return hashlib.md5(geometry_wkb.encode("utf-8")).hexdigest() if geometry_wkb else None

    def _fetch_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self.client.get(url, params=params or None)
        response.raise_for_status()
        return self._json_payload(response, url)

    def _json_payload(self, response: httpx.Response, context: str) -> dict[str, Any]:
        text = response.text.lstrip()
        if text.startswith("<"):
            raise RuntimeError(f"{context} returned XML/HTML instead of JSON: {_short_snippet(text)}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"{context} returned invalid JSON: {_short_snippet(text)}") from exc
        if isinstance(payload, dict) and payload.get("error"):
            raise RuntimeError(f"{context} returned source error: {payload['error']}")
        if not isinstance(payload, dict):
            raise RuntimeError(f"{context} returned a non-object JSON payload.")
        return payload

    def _env_int(self, name: str, default: int) -> int:
        value = os.getenv(name)
        if value is None or value == "":
            return default
        try:
            return int(value)
        except ValueError:
            return default

    def _env_float(self, name: str, default: float) -> float:
        value = os.getenv(name)
        if value is None or value == "":
            return default
        try:
            return float(value)
        except ValueError:
            return default


def _pick_text(row: dict[str, Any], candidates: tuple[str, ...] | list[str]) -> str | None:
    lowered_lookup = {str(key).lower(): key for key in row.keys()}
    for candidate in candidates:
        key = candidate if candidate in row else lowered_lookup.get(str(candidate).lower())
        if key is None:
            continue
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "null"}:
            return text
    return None


def _raw_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _json_safe(value) for key, value in row.items() if key != "geometry"}


def _json_safe(value: Any) -> Any:
    if isinstance(value, BaseGeometry):
        return value.wkt
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _json_dumps(payload: Any) -> str:
    return json.dumps(_json_safe(payload), ensure_ascii=True, default=str)


def _slug(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "unknown").lower()).strip("_")
    return text[:120] or "unknown"


def _tag_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _feature_type_matches(candidate: str, hint: str) -> bool:
    candidate_tail = candidate.split(":")[-1]
    hint_tail = hint.split(":")[-1]
    return candidate == hint or candidate_tail == hint_tail or _normalize_ref(candidate_tail) == _normalize_ref(hint_tail)


def _workspace_from_url(endpoint_url: str) -> str | None:
    path_parts = [part for part in urlparse(endpoint_url).path.split("/") if part]
    if "geoserver" in path_parts:
        index = path_parts.index("geoserver")
        if len(path_parts) > index + 1:
            return path_parts[index + 1]
    return None


def _dedupe(values: list[str]) -> list[str]:
    output: list[str] = []
    for value in values:
        if value and value not in output:
            output.append(value)
    return output


def _short_snippet(text: str, limit: int = 240) -> str:
    compact = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", text or "")).strip()
    return compact[:limit]


def _authority_from_ldp_resource_name(value: Any) -> str | None:
    text = re.sub(r"\b(proposed|local|development|plan|ldp|layers?|available|zip)\b", " ", str(value or ""), flags=re.I)
    text = re.sub(r"\b(19|20)\d{2}(?:\s*[-/]\s*(?:19|20)\d{2})?\b", " ", text)
    text = re.sub(r"\s+", " ", text.replace("_", " ")).strip(" -")
    return text or None


def _plan_period_from_text(value: Any) -> str | None:
    text = str(value or "")
    match = re.search(r"\b((?:19|20)\d{2}(?:\s*[-/]\s*(?:19|20)\d{2})?)\b", text)
    return match.group(1).replace(" ", "") if match else None


def _policy_constraint_values(row: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key, value in row.items():
        lowered = str(key).lower()
        if lowered.startswith("_") or lowered == "geometry":
            continue
        if "constraint" not in lowered and "policy" not in lowered:
            continue
        text = str(value or "").strip()
        if text and text.lower() not in {"nan", "none", "null"} and text not in values:
            values.append(text[:240])
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run LandIntel Phase One source expansion commands.")
    parser.add_argument(
        "command",
        choices=(
            "audit-source-expansion",
            "link-sites-to-ros-parcels",
            "audit-site-parcel-links",
            "resolve-title-numbers",
            "audit-title-number-control",
            "measure-constraints",
            "measure-constraints-duckdb",
            "measure-constraints-debug-all-layers",
            "audit-constraint-measurements",
            "ingest-ldp",
            "ingest-settlement-boundaries",
            "ingest-ela",
            "ingest-vdl",
            "ingest-sepa-flood",
            "ingest-coal-authority",
            "ingest-hes-designations",
            "ingest-naturescot",
            "ingest-contaminated-land",
            "ingest-tpo",
            "ingest-culverts",
            "ingest-conservation-areas",
            "ingest-greenbelt",
            "ingest-os-topography",
            "ingest-os-places",
            "ingest-os-features",
            "ingest-os-linked-identifiers",
            "ingest-os-openmap-local",
            "ingest-os-open-roads",
            "ingest-os-open-rivers",
            "ingest-os-boundary-line",
            "ingest-os-open-names",
            "ingest-os-open-greenspace",
            "ingest-os-open-zoomstack",
            "ingest-os-open-toid",
            "ingest-os-open-built-up-areas",
            "ingest-os-open-uprn",
            "ingest-os-open-usrn",
            "ingest-osm-overpass",
            "ingest-open-location-spine",
            "ingest-naptan",
            "ingest-statistics-gov-scot",
            "ingest-opentopography-srtm",
            "ingest-bulk-download-universe",
            "probe-open-location-spine",
            "promote-ldp-authority-source",
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = SourceExpansionRunner(settings, logger)
    try:
        runner.run_command(args.command)
        logger.info("source_expansion_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception("source_expansion_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

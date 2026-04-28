"""Register and monitor Phase One policy, topography, and gap sources."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
from typing import Any
import xml.etree.ElementTree as ET

import httpx

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


GEONETWORK_SEARCH_URL = "https://www.spatialdata.gov.scot/geonetwork/srv/api/search/records/_search"
LDP_SPATIALHUB_PACKAGE_URL = "https://data.spatialhub.scot/api/3/action/package_show?id=local_development_plans-is"
NRS_SETTLEMENT_WFS_URL = "https://maps.gov.scot/server/services/NRS/NRS/MapServer/WFSServer"
NRS_SETTLEMENT_TYPE_NAME = "NRS:SettlementBoundaries"
NRS_SETTLEMENT_METADATA_UUID = "e457f123-09df-4d67-ac81-d7bb2e470499"


def _boundary_auth_params() -> dict[str, str]:
    authkey = os.getenv("BOUNDARY_AUTHKEY")
    return {"authkey": authkey} if authkey else {}


POLICY_DISCOVERY: dict[str, dict[str, Any]] = {
    "ldp": {
        "query": "local_development_plans-is",
        "source_name_suffix": "SpatialHub LDP package",
        "signal_output": "planning_context/future_context after licence and policy-interpreter validation",
        "ranking_impact": "Core policy source. Storage is live; ranking stays gated until commercial-use and interpretation checks pass.",
        "resurfacing_trigger": "SpatialHub package revision, direct ZIP resource refresh, allocation change, or policy interpreter promotion.",
    },
    "settlement": {
        "query": NRS_SETTLEMENT_TYPE_NAME,
        "source_name_suffix": "NRS settlement boundaries",
        "signal_output": "settlement_position after canonical overlay validation",
        "ranking_impact": "Core settlement source. Storage is live; no ranking impact until the canonical inside/outside/edge interpreter is promoted.",
        "resurfacing_trigger": "NRS dataset refresh, source revision change, or inside/outside percentage change.",
    },
}

SUPPLEMENTAL_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "source_key": "topography_os_terrain_50",
        "source_family": "topography",
        "source_name": "OS Terrain 50",
        "source_group": "constraints",
        "phase_one_role": "target_live",
        "source_status": "live_target",
        "orchestration_mode": "os_downloads_api",
        "endpoint_url": "https://api.os.uk/downloads/v1",
        "auth_env_vars": ["OS_API_KEY"],
        "target_table": "public.constraint_source_features",
        "reconciliation_path": "terrain tile download -> slope derivation -> canonical site overlay",
        "evidence_path": "evidence_references, site_constraint_measurements, site_constraint_group_summaries",
        "signal_output": "topography_slope and geometry_quality",
        "ranking_impact": "Geometry/constraints drag only; no appraisal or viability conclusion.",
        "resurfacing_trigger": "Terrain release refresh or derived slope-band change.",
        "data_age_basis": "OS Downloads product release metadata.",
        "notes": "Use for national baseline slope/topography. Open broad coverage; derive site-level slope as indicative_only.",
    },
    {
        "source_key": "topography_scottish_lidar",
        "source_family": "topography",
        "source_name": "Scottish Remote Sensing LiDAR",
        "source_group": "constraints",
        "phase_one_role": "target_live",
        "source_status": "live_target",
        "orchestration_mode": "remote_sensing_portal",
        "endpoint_url": "https://remotesensingdata.gov.scot/about",
        "target_table": "public.constraint_source_features",
        "reconciliation_path": "available LiDAR tile discovery -> slope/terrain derivation -> canonical site overlay",
        "evidence_path": "evidence_references, site_constraint_measurements, site_constraint_group_summaries",
        "signal_output": "topography_slope and abnormal-friction facts",
        "ranking_impact": "Constraints/geometry drag only where coverage exists.",
        "resurfacing_trigger": "New LiDAR block release or derived slope-band change.",
        "data_age_basis": "Scottish Remote Sensing Portal dataset metadata.",
        "notes": "Coverage is rolling, not yet national. Use as higher-resolution override where available.",
    },
    {
        "source_key": "school_catchments_spatialhub",
        "source_family": "school_catchments",
        "source_name": "School Catchments - Scotland",
        "source_group": "support",
        "phase_one_role": "context",
        "source_status": "static_snapshot",
        "orchestration_mode": "spatialhub_static_or_wfs",
        "endpoint_url": "https://data.spatialhub.scot/api/3/action/package_search",
        "auth_env_vars": ["IMPROVEMENT_SERVICE_AUTHKEY"],
        "target_table": "public.constraint_source_features",
        "reconciliation_path": "catchment polygon overlay against canonical sites for context only",
        "evidence_path": "evidence_references",
        "signal_output": "location_context",
        "ranking_impact": "Support context only; no Phase One scoring category.",
        "resurfacing_trigger": "Catchment dataset refresh.",
        "data_age_basis": "SpatialHub package metadata and static zip checksum.",
        "notes": "Local static snapshot has 2,396 catchment polygons across primary/secondary denominational/non-denominational layers.",
    },
    {
        "source_key": "adopted_roads_authority_discovery",
        "source_family": "adopted_roads",
        "source_name": "Authority adopted roads discovery",
        "source_group": "access",
        "phase_one_role": "critical",
        "source_status": "explicitly_deferred",
        "orchestration_mode": "authority_discovery",
        "endpoint_url": GEONETWORK_SEARCH_URL,
        "target_table": "landintel.source_estate_registry",
        "reconciliation_path": "deferred until authority adapter validated, then frontage/access overlay",
        "evidence_path": "registry audit only until promoted",
        "signal_output": "none until promoted, then access_strength",
        "ranking_impact": "No live ranking while deferred.",
        "resurfacing_trigger": "Authority adopted-road source discovery, refresh, or adapter promotion.",
        "data_age_basis": "Authority source checked timestamp and metadata modified timestamp.",
        "ranking_eligible": False,
        "review_output_eligible": False,
        "notes": "Access certainty remains a broken chain until adopted roads are authority-wired.",
    },
    {
        "source_key": "utilities_water_electric_discovery",
        "source_family": "utilities",
        "source_name": "Water/electric utility source discovery",
        "source_group": "utilities",
        "phase_one_role": "context",
        "source_status": "explicitly_deferred",
        "orchestration_mode": "manual_and_authority_discovery",
        "target_table": "landintel.source_estate_registry",
        "reconciliation_path": "deferred until validated provider/authority source is available",
        "evidence_path": "registry audit only until promoted",
        "signal_output": "none until promoted, then utilities_burden",
        "ranking_impact": "No live ranking while deferred.",
        "resurfacing_trigger": "Validated utility source added or promoted.",
        "data_age_basis": "Manual/provider source checked timestamp.",
        "ranking_eligible": False,
        "review_output_eligible": False,
        "notes": "SGN gas is registered separately; water/electric/sewer remain incomplete.",
    },
    {
        "source_key": "section75_planning_documents",
        "source_family": "section75",
        "source_name": "Section 75 and infrastructure obligation document extraction",
        "source_group": "planning",
        "phase_one_role": "context",
        "source_status": "explicitly_deferred",
        "orchestration_mode": "planning_document_extraction",
        "target_table": "landintel.site_signals",
        "reconciliation_path": "planning document link -> canonical planning record -> canonical_site_id",
        "evidence_path": "evidence_references with source document URL and extracted clause metadata",
        "signal_output": "commercial_friction_facts",
        "ranking_impact": "Visible warning only; no appraisal, pricing, or viability calculation.",
        "resurfacing_trigger": "New obligation document or planning document update.",
        "data_age_basis": "Planning portal document modified timestamp where available.",
        "ranking_eligible": False,
        "review_output_eligible": True,
        "notes": "Important friction signal, but requires planning document extraction and cannot become appraisal logic.",
    },
)


class SourcePolicyDiscoveryRunner:
    """Push policy/topography discovery state into Supabase through repo-controlled jobs."""

    def __init__(self, settings: Any, logger: Any) -> None:
        self.settings = settings
        self.logger = logger.getChild("source_policy_discovery")
        self.database = Database(settings)
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)

    def close(self) -> None:
        self.client.close()
        self.database.dispose()

    def register_supplemental_sources(self) -> dict[str, Any]:
        for source in SUPPLEMENTAL_SOURCES:
            self._upsert_source(source)
            self._record_freshness(
                source,
                freshness_status=(
                    "core_pending_adapter"
                    if source.get("source_status") == "core_pending_adapter"
                    else "explicitly_deferred"
                    if source.get("source_status") == "explicitly_deferred"
                    else "registered_unproven"
                ),
                live_access_status="registered",
                summary="Registered from Phase One supplemental source discovery.",
            )
        payload = {"registered_sources": len(SUPPLEMENTAL_SOURCES)}
        self.logger.info("supplemental_sources_registered", extra=payload)
        return payload

    def discover_policy_sources(self, source_family: str) -> dict[str, Any]:
        if source_family not in POLICY_DISCOVERY:
            raise KeyError(f"No policy discovery config for {source_family}")
        if source_family == "ldp":
            return self.register_ldp_spatialhub_package()
        if source_family == "settlement":
            return self.register_settlement_boundaries()
        config = POLICY_DISCOVERY[source_family]
        authorities = self.settings.load_target_councils()
        inserted = 0
        discovered = 0
        for authority in authorities:
            records = self._search_geonetwork(authority, str(config["query"]))
            if not records:
                self._upsert_policy_placeholder(source_family, authority, config)
                inserted += 1
                continue
            discovered += len(records)
            for record in records:
                self._upsert_policy_record(source_family, authority, config, record)
                inserted += 1
        payload = {
            "source_family": source_family,
            "authorities_checked": len(authorities),
            "records_discovered": discovered,
            "registry_rows_written": inserted,
        }
        self.logger.info("policy_sources_discovered", extra=payload)
        return payload

    def register_ldp_spatialhub_package(self) -> dict[str, Any]:
        response = self.client.get(LDP_SPATIALHUB_PACKAGE_URL, headers={"Accept": "application/json"})
        response.raise_for_status()
        payload = response.json()
        package = dict(payload.get("result") or payload)
        resources = list(package.get("resources") or [])
        zip_resources = [
            resource
            for resource in resources
            if str(resource.get("url") or "").lower().endswith(".zip")
            or "zip" in str(resource.get("format") or resource.get("mimetype") or "").lower()
        ]
        external_resources = len(resources) - len(zip_resources)
        source = {
            "source_key": "ldp_spatialhub_package",
            "source_family": "ldp",
            "source_name": str(package.get("title") or "Local Development Plans - Scotland"),
            "source_group": "policy",
            "phase_one_role": "critical",
            "source_status": "live_target",
            "orchestration_mode": "spatialhub_ckan_package_zips",
            "endpoint_url": LDP_SPATIALHUB_PACKAGE_URL,
            "target_table": "landintel.ldp_site_records",
            "reconciliation_path": "SpatialHub CKAN package -> direct ZIP resources -> ldp_site_records -> later policy interpreter/canonical overlay",
            "evidence_path": "ldp_site_records raw_payload, source_expansion_events, source_freshness_states",
            "signal_output": POLICY_DISCOVERY["ldp"]["signal_output"],
            "ranking_impact": POLICY_DISCOVERY["ldp"]["ranking_impact"],
            "resurfacing_trigger": POLICY_DISCOVERY["ldp"]["resurfacing_trigger"],
            "data_age_basis": "SpatialHub package revision date and resource last_modified timestamps.",
            "ranking_eligible": False,
            "review_output_eligible": True,
            "notes": "SpatialHub LDP package is reachable. Direct ZIP resources are storage-live; commercial ranking remains licence-gated.",
            "metadata": {
                "package_id": package.get("name"),
                "metadata_uuid": "8e13ad58-41f2-4308-a3a8-5ffe8593e731",
                "spatialhub_identifier": "sh_ldp",
                "resource_count": len(resources),
                "zip_resource_count": len(zip_resources),
                "external_link_resource_count": external_resources,
            },
        }
        self._upsert_source(source)
        self._record_freshness(
            source,
            freshness_status="current",
            live_access_status="reachable",
            summary="SpatialHub LDP package registered; direct ZIP resources available, external links held as metadata.",
            records_observed=len(resources),
        )
        result = {
            "source_family": "ldp",
            "source_key": source["source_key"],
            "resource_count": len(resources),
            "zip_resource_count": len(zip_resources),
            "external_link_resource_count": external_resources,
        }
        self.logger.info("ldp_spatialhub_package_registered", extra=result)
        return result

    def register_settlement_boundaries(self) -> dict[str, Any]:
        capabilities = self.client.get(
            NRS_SETTLEMENT_WFS_URL,
            params={"service": "WFS", "request": "GetCapabilities", **_boundary_auth_params()},
            headers={"Accept": "text/xml,application/xml"},
        )
        capabilities.raise_for_status()
        feature_count = self._settlement_hit_count()
        source = {
            "source_key": "nrs_settlement_boundaries",
            "source_family": "settlement",
            "source_name": "Settlements - Scotland",
            "source_group": "policy",
            "phase_one_role": "critical",
            "source_status": "live_target",
            "orchestration_mode": "nrs_wfs_geojson",
            "endpoint_url": NRS_SETTLEMENT_WFS_URL,
            "auth_env_vars": ["BOUNDARY_AUTHKEY"],
            "target_table": "landintel.settlement_boundary_records",
            "reconciliation_path": "NRS WFS -> settlement_boundary_records -> later canonical settlement-position overlay",
            "evidence_path": "settlement_boundary_records raw_payload, source_expansion_events, source_freshness_states",
            "signal_output": POLICY_DISCOVERY["settlement"]["signal_output"],
            "ranking_impact": POLICY_DISCOVERY["settlement"]["ranking_impact"],
            "resurfacing_trigger": POLICY_DISCOVERY["settlement"]["resurfacing_trigger"],
            "data_age_basis": "NRS metadata revision date and latest successful ingest run.",
            "ranking_eligible": False,
            "review_output_eligible": True,
            "notes": "NRS settlement WFS is reachable and storage-live; ranking waits for canonical settlement-position overlay.",
            "metadata": {
                "metadata_uuid": NRS_SETTLEMENT_METADATA_UUID,
                "nrs_identifier": "NRS_SettlementBdry",
                "wfs_type_name": NRS_SETTLEMENT_TYPE_NAME,
                "source_revision_date": "2023-07-11",
                "license": "Open Government Licence",
                "feature_count": feature_count,
            },
        }
        self._upsert_source(source)
        self._record_freshness(
            source,
            freshness_status="current",
            live_access_status="reachable",
            summary="NRS settlement WFS registered; features are storage-live and interpreter-gated.",
            records_observed=feature_count,
        )
        result = {
            "source_family": "settlement",
            "source_key": source["source_key"],
            "wfs_type_name": NRS_SETTLEMENT_TYPE_NAME,
            "feature_count": feature_count,
        }
        self.logger.info("settlement_boundaries_registered", extra=result)
        return result

    def _settlement_hit_count(self) -> int:
        response = self.client.get(
            NRS_SETTLEMENT_WFS_URL,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": NRS_SETTLEMENT_TYPE_NAME,
                "resultType": "hits",
                **_boundary_auth_params(),
            },
        )
        response.raise_for_status()
        try:
            root = ET.fromstring(response.text.encode("utf-8"))
        except ET.ParseError:
            return 0
        value = root.attrib.get("numberMatched") or root.attrib.get("numberOfFeatures")
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return 0

    def _search_geonetwork(self, authority: str, query: str) -> list[dict[str, Any]]:
        payload = {
            "from": 0,
            "size": 25,
            "query": {"query_string": {"query": f'"{authority}" AND ({query})'}},
        }
        response = self.client.post(
            GEONETWORK_SEARCH_URL,
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        response.raise_for_status()
        hits = list(((response.json().get("hits") or {}).get("hits")) or [])
        return [self._normalise_geonetwork_record(hit, authority) for hit in hits]

    def _normalise_geonetwork_record(self, hit: dict[str, Any], authority: str) -> dict[str, Any]:
        source = hit.get("_source") or {}
        title = _first_text(
            source.get("resourceTitleObject"),
            source.get("resourceTitle"),
            source.get("title"),
            source.get("defaultTitle"),
        )
        record_id = str(hit.get("_id") or source.get("uuid") or title or "unknown")
        links = source.get("link") or source.get("links") or []
        return {
            "record_id": record_id,
            "title": title or record_id,
            "url": _first_url(links) or str(source.get("landingPage") or source.get("linkUrl") or ""),
            "modified_at": _first_text(source.get("dateStamp"), source.get("changeDate"), source.get("revisionDate")),
            "authority": authority,
            "raw": hit,
        }

    def _upsert_policy_placeholder(self, source_family: str, authority: str, config: dict[str, Any]) -> None:
        source = self._policy_source_payload(
            source_family,
            authority,
            config,
            record_id="placeholder",
            title=f"{authority} {config['source_name_suffix']} placeholder",
            url=GEONETWORK_SEARCH_URL,
            metadata={"authority": authority, "placeholder": True},
        )
        self._upsert_source(source)
        self._record_freshness(
            source,
            freshness_status="core_pending_adapter",
            live_access_status="monitored",
            summary="No GeoNetwork record found yet; placeholder kept for authority monitoring.",
        )

    def _upsert_policy_record(
        self,
        source_family: str,
        authority: str,
        config: dict[str, Any],
        record: dict[str, Any],
    ) -> None:
        source = self._policy_source_payload(
            source_family,
            authority,
            config,
            record_id=str(record["record_id"]),
            title=str(record["title"]),
            url=str(record.get("url") or GEONETWORK_SEARCH_URL),
            metadata=record,
        )
        self._upsert_source(source)
        self._record_freshness(
            source,
            freshness_status="core_pending_adapter",
            live_access_status="monitored",
            summary="GeoNetwork record discovered and held out of ranking until authority adapter promotion.",
        )

    def _policy_source_payload(
        self,
        source_family: str,
        authority: str,
        config: dict[str, Any],
        *,
        record_id: str,
        title: str,
        url: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "source_key": f"{source_family}:{_slug(authority)}:{_slug(record_id)}",
            "source_family": source_family,
            "source_name": title,
            "source_group": "policy",
            "phase_one_role": "critical",
            "source_status": "core_pending_adapter",
            "orchestration_mode": "geonetwork_authority_discovery",
            "endpoint_url": url,
            "target_table": (
                "landintel.ldp_site_records"
                if source_family == "ldp"
                else "landintel.settlement_boundary_records"
            ),
            "reconciliation_path": "authority discovery -> validated authority adapter -> canonical site link",
            "evidence_path": "registry rows until promoted; then source records, evidence_references, and site_source_links",
            "signal_output": config["signal_output"],
            "ranking_impact": config["ranking_impact"],
            "resurfacing_trigger": config["resurfacing_trigger"],
            "data_age_basis": "GeoNetwork metadata modified timestamp and authority discovery run timestamp.",
            "ranking_eligible": False,
            "review_output_eligible": False,
            "notes": "Core policy source discovered through GeoNetwork. Promotion must be authority-specific and explicit.",
            "metadata": metadata,
        }

    def _upsert_source(self, source: dict[str, Any]) -> None:
        self.database.execute(
            """
                insert into landintel.source_estate_registry (
                    source_key, source_family, source_name, source_group, phase_one_role,
                    source_status, orchestration_mode, endpoint_url, auth_env_vars, target_table,
                    reconciliation_path, evidence_path, signal_output, ranking_impact,
                    resurfacing_trigger, data_age_basis, notes, ranking_eligible,
                    review_output_eligible, metadata, last_registered_at, updated_at
                )
                values (
                    :source_key, :source_family, :source_name, :source_group, :phase_one_role,
                    :source_status, :orchestration_mode, :endpoint_url,
                    case when :auth_env_vars_csv = '' then '{}'::text[] else string_to_array(:auth_env_vars_csv, ',') end,
                    :target_table, :reconciliation_path, :evidence_path, :signal_output,
                    :ranking_impact, :resurfacing_trigger, :data_age_basis, :notes,
                    :ranking_eligible, :review_output_eligible, cast(:metadata as jsonb),
                    now(), now()
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
                    notes = excluded.notes,
                    ranking_eligible = excluded.ranking_eligible,
                    review_output_eligible = excluded.review_output_eligible,
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
                "auth_env_vars_csv": ",".join(source.get("auth_env_vars") or []),
                "target_table": source.get("target_table"),
                "reconciliation_path": source.get("reconciliation_path"),
                "evidence_path": source.get("evidence_path"),
                "signal_output": source.get("signal_output"),
                "ranking_impact": source.get("ranking_impact"),
                "resurfacing_trigger": source.get("resurfacing_trigger"),
                "data_age_basis": source.get("data_age_basis"),
                "notes": source.get("notes"),
                "ranking_eligible": bool(source.get("ranking_eligible", False)),
                "review_output_eligible": bool(source.get("review_output_eligible", True)),
                "metadata": json.dumps(source.get("metadata") or source, default=str),
            },
        )

    def _record_freshness(
        self,
        source: dict[str, Any],
        *,
        freshness_status: str,
        live_access_status: str,
        summary: str,
        records_observed: int = 0,
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
                )
                values (
                    :scope_key, :source_family, :source_dataset, :source_name,
                    :source_access_mode, :source_url, :refresh_cadence, :max_staleness_days,
                    now(), now(), :last_success_at, :freshness_status, :live_access_status,
                    :ranking_eligible, :review_output_eligible, :stale_reason_code,
                    :check_summary, :records_observed, cast(:metadata as jsonb), now()
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
                "scope_key": f"source_policy:{source['source_key']}",
                "source_family": source["source_family"],
                "source_dataset": source["source_name"],
                "source_name": source["source_name"],
                "source_access_mode": source.get("orchestration_mode") or "unknown",
                "source_url": source.get("endpoint_url"),
                "refresh_cadence": "weekly" if source.get("phase_one_role") == "critical" else "monthly",
                "max_staleness_days": 7 if source.get("phase_one_role") == "critical" else 30,
                "last_success_at": datetime.now(timezone.utc),
                "freshness_status": freshness_status,
                "live_access_status": live_access_status,
                "ranking_eligible": bool(source.get("ranking_eligible", False)),
                "review_output_eligible": bool(source.get("review_output_eligible", True)),
                "stale_reason_code": (
                    "authority_adapter_not_validated"
                    if freshness_status in {"explicitly_deferred", "core_pending_adapter"}
                    else None
                ),
                "check_summary": summary,
                "records_observed": records_observed,
                "metadata": json.dumps({"source_key": source["source_key"]}, default=str),
            },
        )


def _first_text(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for key in ("default", "eng", "en"):
                text = value.get(key)
                if isinstance(text, str) and text.strip():
                    return text.strip()
        if isinstance(value, list):
            for item in value:
                text = _first_text(item)
                if text:
                    return text
    return None


def _first_url(value: Any) -> str | None:
    if isinstance(value, str) and value.startswith(("http://", "https://")):
        return value
    if isinstance(value, dict):
        for key in ("url", "href", "link"):
            found = _first_url(value.get(key))
            if found:
                return found
    if isinstance(value, list):
        for item in value:
            found = _first_url(item)
            if found:
                return found
    return None


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")[:120] or "unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Register and discover Phase One policy/topography sources.")
    parser.add_argument(
        "command",
        choices=(
            "register-supplemental-sources",
            "discover-ldp-geonetwork",
            "discover-settlement-geonetwork",
            "register-settlement-boundaries",
            "discover-policy-geonetwork",
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = SourcePolicyDiscoveryRunner(settings, logger)
    try:
        if args.command == "register-supplemental-sources":
            runner.register_supplemental_sources()
        elif args.command == "discover-ldp-geonetwork":
            runner.discover_policy_sources("ldp")
        elif args.command in {"discover-settlement-geonetwork", "register-settlement-boundaries"}:
            runner.discover_policy_sources("settlement")
        elif args.command == "discover-policy-geonetwork":
            runner.register_supplemental_sources()
            runner.discover_policy_sources("ldp")
            runner.discover_policy_sources("settlement")
        logger.info("source_policy_discovery_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception("source_policy_discovery_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

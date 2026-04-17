"""GitHub Actions runner for the next Scottish source-intelligence phase."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import tempfile
import traceback
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import geopandas as gpd
import httpx
import pandas as pd
import yaml
from shapely import wkb as shapely_wkb
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from config.settings import Settings, get_settings
from src.db import chunked
from src.loaders.supabase_loader import SupabaseLoader
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.models.source_registry import SourceRegistryRecord
from src.url_safety import redact_sensitive_query_params

MANIFEST_PATH = Path(__file__).resolve().parents[1] / "config" / "scotland_core_sources.yaml"

AUTHORITY_NAME_MAP = {
    "Edinburgh, City of": "City of Edinburgh",
    "Perth & Kinross": "Perth and Kinross",
}

REFUSAL_THEME_PATTERNS: dict[str, tuple[str, ...]] = {
    "planning_principle": ("principle", "policy", "countryside", "outwith", "settlement boundary"),
    "access_roads": ("access", "road", "roads", "transport", "visibility", "junction"),
    "drainage_flood": ("drainage", "flood", "flooding", "sewer", "wastewater"),
    "education_infrastructure": ("education", "school", "infrastructure", "capacity"),
    "design_density": ("design", "density", "layout", "placemaking"),
    "ecology_heritage": ("ecology", "heritage", "biodiversity", "archaeology", "conservation"),
    "ground_contamination": ("contamination", "ground", "made ground", "mining"),
}


@dataclass(slots=True)
class SpatialHubSourceConfig:
    source_name: str
    publisher: str
    dataset_id: str
    metadata_uuid: str
    field_mappings: dict[str, list[str]]
    authority_field_candidates: list[str]
    resource_name_contains: str | None = None


@dataclass(slots=True)
class BgsCollectionConfig:
    source_name: str
    metadata_uuid: str
    collection_id: str
    record_type: str
    bbox_buffer_m: int


@dataclass(slots=True)
class SpatialHubResourceHandle:
    source_name: str
    resource_id: str
    resource_page_url: str
    geoserver_root: str
    workspace_name: str
    preview_layer_name: str | None
    alternative_name: str | None
    capabilities_url: str


class SourcePhaseRunner:
    """Populate landintel source tables without disturbing the lean parcel path."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger.getChild("source_phase")
        self.loader = SupabaseLoader(settings, __import__("src.db", fromlist=["Database"]).Database(settings), logger)
        self.database = self.loader.database
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)
        self.target_authorities = settings.load_target_councils()
        self.manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8")) or {}
        self._spatial_hub_package_cache: dict[str, dict[str, Any]] = {}
        self._spatial_hub_handle_cache: dict[str, SpatialHubResourceHandle] = {}
        self._spatial_hub_feature_type_cache: dict[str, list[str]] = {}
        self._spatial_hub_property_name_cache: dict[str, list[str]] = {}
        self._spatial_hub_unfiltered_frame_cache: dict[str, gpd.GeoDataFrame] = {}
        self.spatial_hub_authkey = self._resolve_spatial_hub_authkey()
        self.authority_aoi = self.loader.fetch_active_authorities()

    def close(self) -> None:
        self.client.close()
        self.loader.close()

    def run_migrations(self) -> None:
        self.loader.run_migrations()

    def audit_source_footprint(self) -> dict[str, Any]:
        summary = self.database.fetch_one(
            """
                select
                    (select count(*) from landintel.canonical_sites) as canonical_site_count,
                    (select count(*) from landintel.site_reference_aliases) as alias_count,
                    (select count(*) from landintel.planning_application_records) as planning_record_count,
                    (select count(*) from landintel.hla_site_records) as hla_record_count,
                    (select count(*) from landintel.bgs_records) as bgs_record_count,
                    (select count(*) from landintel.site_source_links) as site_source_link_count,
                    (select count(*) from landintel.evidence_references) as evidence_reference_count
            """
        ) or {}
        planning_by_authority = self.database.fetch_all(
            """
                select authority_name,
                       count(*)::bigint as planning_records,
                       count(*) filter (where canonical_site_id is not null)::bigint as linked_planning_records
                from landintel.planning_application_records
                group by authority_name
                order by planning_records desc, authority_name asc
            """
        )
        payload = {"summary": summary, "planning_by_authority": planning_by_authority}
        self.logger.info("source_phase_audit", extra=payload)
        return payload

    def ingest_planning_history(self) -> dict[str, Any]:
        return self._ingest_spatial_hub_dataset("planning_history")

    def ingest_hla(self) -> dict[str, Any]:
        return self._ingest_spatial_hub_dataset("hla")

    def reconcile_canonical_sites(self) -> dict[str, Any]:
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="reconcile_canonical_sites",
                source_name="landintel.canonical_sites",
                status="running",
                metadata={"target_authorities": self.target_authorities},
            )
        )
        try:
            self._reset_canonical_state()
            hla_registry_id = self._resolve_source_registry_id("spatialhub:housing_land_supply-is")
            planning_registry_id = self._resolve_source_registry_id("spatialhub:planning_applications_official-is")

            created_count = 0
            linked_rows = 0

            hla_rows = self.database.read_geodataframe(
                """
                    select id, source_record_id, authority_name, site_reference, site_name, geometry,
                           effectiveness_status, programming_horizon, constraint_reasons, remaining_capacity
                    from landintel.hla_site_records
                """
            )
            for row in hla_rows.itertuples(index=False):
                site_code = self._canonical_site_code("HLA", row.authority_name, row.site_reference or row.source_record_id)
                site_name = (row.site_name or row.site_reference or row.source_record_id)[:240]
                site_id = self._upsert_canonical_site(
                    site_code=site_code,
                    site_name=site_name,
                    authority_name=row.authority_name,
                    geometry=_polygonize_geometry(row.geometry),
                    surfaced_reason="Surfaced from Housing Land Supply evidence.",
                    metadata={
                        "seed_source": "hla",
                        "effectiveness_status": row.effectiveness_status,
                        "programming_horizon": row.programming_horizon,
                        "constraint_reasons": list(row.constraint_reasons or []),
                        "remaining_capacity": row.remaining_capacity,
                    },
                )
                self.database.execute(
                    "update landintel.hla_site_records set canonical_site_id = cast(:site_id as uuid) where id = cast(:record_id as uuid)",
                    {"site_id": site_id, "record_id": row.id},
                )
                self._record_site_geometry(site_id, "hla", _polygonize_geometry(row.geometry), hla_registry_id, run_id)
                self._record_reference_alias(
                    site_id,
                    source_family="hla",
                    source_dataset="Housing Land Supply - Scotland",
                    authority_name=row.authority_name,
                    raw_reference_value=row.site_reference or row.source_record_id,
                    site_name=site_name,
                    source_registry_id=hla_registry_id,
                    ingest_run_id=run_id,
                    planning_reference=None,
                    status="matched",
                    confidence=1.0,
                )
                self._record_source_link(
                    site_id,
                    source_family="hla",
                    source_dataset="Housing Land Supply - Scotland",
                    source_record_id=row.source_record_id,
                    link_method="direct_reference",
                    confidence=1.0,
                    source_registry_id=hla_registry_id,
                    ingest_run_id=run_id,
                )
                self._record_evidence(
                    site_id,
                    source_family="hla",
                    source_dataset="Housing Land Supply - Scotland",
                    source_record_id=row.source_record_id,
                    source_reference=row.site_reference,
                    confidence="high",
                    source_registry_id=hla_registry_id,
                    ingest_run_id=run_id,
                    metadata={"site_name": site_name},
                )
                self._set_primary_parcel(site_id)
                created_count += 1
                linked_rows += 1

            planning_rows = self.database.read_geodataframe(
                """
                    select id, source_record_id, authority_name, planning_reference, proposal_text, decision, geometry
                    from landintel.planning_application_records
                """
            )
            for row in planning_rows.itertuples(index=False):
                geometry = _polygonize_geometry(row.geometry)
                site_id = self._find_best_site_by_geometry(row.authority_name, geometry) if geometry is not None else None
                if site_id is None:
                    site_code = self._canonical_site_code("PLN", row.authority_name, row.planning_reference or row.source_record_id)
                    site_name = (row.proposal_text or row.planning_reference or row.source_record_id)[:240]
                    site_id = self._upsert_canonical_site(
                        site_code=site_code,
                        site_name=site_name,
                        authority_name=row.authority_name,
                        geometry=geometry,
                        surfaced_reason="Surfaced from planning history evidence.",
                        metadata={"seed_source": "planning", "decision": row.decision},
                    )
                    if geometry is not None:
                        self._record_site_geometry(site_id, "planning", geometry, planning_registry_id, run_id)
                    self._set_primary_parcel(site_id)
                    created_count += 1
                self.database.execute(
                    "update landintel.planning_application_records set canonical_site_id = cast(:site_id as uuid) where id = cast(:record_id as uuid)",
                    {"site_id": site_id, "record_id": row.id},
                )
                self._record_reference_alias(
                    site_id,
                    source_family="planning",
                    source_dataset="Planning Applications: Official - Scotland",
                    authority_name=row.authority_name,
                    raw_reference_value=row.planning_reference or row.source_record_id,
                    site_name=(row.proposal_text or row.planning_reference or row.source_record_id)[:240],
                    source_registry_id=planning_registry_id,
                    ingest_run_id=run_id,
                    planning_reference=row.planning_reference,
                    status="matched",
                    confidence=0.85 if geometry is not None else 0.7,
                )
                self._record_source_link(
                    site_id,
                    source_family="planning",
                    source_dataset="Planning Applications: Official - Scotland",
                    source_record_id=row.source_record_id,
                    link_method="spatial_overlap" if geometry is not None else "planning_reference",
                    confidence=0.85 if geometry is not None else 0.7,
                    source_registry_id=planning_registry_id,
                    ingest_run_id=run_id,
                )
                self._record_evidence(
                    site_id,
                    source_family="planning",
                    source_dataset="Planning Applications: Official - Scotland",
                    source_record_id=row.source_record_id,
                    source_reference=row.planning_reference,
                    confidence="high" if geometry is not None else "medium",
                    source_registry_id=planning_registry_id,
                    ingest_run_id=run_id,
                    metadata={"decision": row.decision, "proposal_text": row.proposal_text},
                )
                linked_rows += 1

            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=len(hla_rows) + len(planning_rows),
                    records_loaded=linked_rows,
                    records_retained=created_count,
                    metadata={"canonical_site_count": created_count, "linked_rows": linked_rows},
                    finished=True,
                ),
            )
            return {"canonical_site_count": created_count, "linked_rows": linked_rows}
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def ingest_bgs(self) -> dict[str, Any]:
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="ingest_bgs",
                source_name="BGS OpenGeoscience API",
                status="running",
                metadata={"target_authorities": self.target_authorities},
            )
        )
        try:
            collections = self._bgs_collections()
            registry_ids = {
                key: self._upsert_and_resolve_source_registry_id(
                    SourceRegistryRecord(
                        source_name=value.source_name,
                        source_type="OGC API Features",
                        publisher="British Geological Survey",
                        metadata_uuid=value.metadata_uuid,
                        endpoint_url=f"{self.manifest['bgs']['base_url']}/collections/{value.collection_id}",
                        download_url=f"{self.manifest['bgs']['base_url']}/collections/{value.collection_id}/items?f=json",
                        record_json={"collection_id": value.collection_id},
                        geographic_extent=None,
                        last_seen_at=datetime.now(timezone.utc),
                    )
                )
                for key, value in collections.items()
            }
            self.database.execute("delete from landintel.bgs_records")
            sites = self.database.read_geodataframe(
                """
                    select id, authority_name, geometry
                    from landintel.canonical_sites
                    where geometry is not null
                """
            )
            inserted = 0
            for row in sites.itertuples(index=False):
                geometry = _polygonize_geometry(row.geometry)
                if geometry is None:
                    continue
                for key, collection in collections.items():
                    items = self._fetch_bgs_items(collection, geometry)
                    if not items:
                        continue
                    sample_ids = [str(item.get("id")) for item in items[:10] if item.get("id") is not None]
                    source_record_id = f"{row.id}:{collection.collection_id}"
                    self.database.execute(
                        """
                            insert into landintel.bgs_records (
                                source_record_id,
                                canonical_site_id,
                                authority_name,
                                record_type,
                                title,
                                severity,
                                geometry,
                                source_registry_id,
                                ingest_run_id,
                                raw_payload
                            )
                            values (
                                :source_record_id,
                                cast(:canonical_site_id as uuid),
                                :authority_name,
                                :record_type,
                                :title,
                                :severity,
                                ST_Centroid(ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700))),
                                cast(:source_registry_id as uuid),
                                cast(:ingest_run_id as uuid),
                                cast(:raw_payload as jsonb)
                            )
                        """,
                        {
                            "source_record_id": source_record_id,
                            "canonical_site_id": row.id,
                            "authority_name": row.authority_name,
                            "record_type": collection.record_type,
                            "title": f"{len(items)} {collection.source_name.lower()} records nearby",
                            "severity": _count_severity(len(items)),
                            "geometry_wkb": _geometry_hex(geometry),
                            "source_registry_id": registry_ids[key],
                            "ingest_run_id": run_id,
                            "raw_payload": json.dumps({"count": len(items), "sample_ids": sample_ids}, default=_json_default),
                        },
                    )
                    self._record_evidence(
                        row.id,
                        source_family="bgs",
                        source_dataset=collection.source_name,
                        source_record_id=source_record_id,
                        source_reference=", ".join(sample_ids[:3]) or collection.collection_id,
                        confidence="high",
                        source_registry_id=registry_ids[key],
                        ingest_run_id=run_id,
                        metadata={"count": len(items), "collection_id": collection.collection_id},
                    )
                    inserted += 1
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=inserted,
                    records_loaded=inserted,
                    records_retained=inserted,
                    metadata={"site_count": len(sites)},
                    finished=True,
                ),
            )
            return {"inserted_records": inserted, "site_count": len(sites)}
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def full_refresh_core_sources(self) -> dict[str, Any]:
        return {
            "planning": self.ingest_planning_history(),
            "hla": self.ingest_hla(),
            "reconciliation": self.reconcile_canonical_sites(),
            "bgs": self.ingest_bgs(),
        }

    def _ingest_spatial_hub_dataset(self, key: str) -> dict[str, Any]:
        config = self._spatial_hub_source(key)
        registry_record = self._build_spatial_hub_source_registry_record(config)
        source_registry_id = self._upsert_and_resolve_source_registry_id(registry_record)
        table_name = "landintel.planning_application_records" if key == "planning_history" else "landintel.hla_site_records"
        run_type = "ingest_planning_history" if key == "planning_history" else "ingest_hla"
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type=run_type,
                source_name=config.source_name,
                status="running",
                metadata={"target_authorities": self.target_authorities},
            )
        )
        fetched = 0
        loaded = 0
        try:
            self.database.execute(
                f"delete from {table_name} where source_registry_id = cast(:source_registry_id as uuid)",
                {"source_registry_id": source_registry_id},
            )
            for authority_name in self.target_authorities:
                frame = self._fetch_spatial_hub_frame(config, authority_name)
                if frame.empty:
                    continue
                raw_rows = self._build_dataset_rows(key, authority_name, frame, config, source_registry_id, run_id)
                fetched += len(raw_rows)
                batch_rows = self._consolidate_dataset_rows(key, raw_rows)
                loaded += len(batch_rows)
                for batch in chunked(batch_rows, self.settings.batch_size):
                    if key == "planning_history":
                        self._insert_planning_batch(batch)
                    else:
                        self._insert_hla_batch(batch)
                self.loader.update_ingest_run(
                    run_id,
                    IngestRunUpdate(
                        status="running",
                        records_fetched=fetched,
                        records_loaded=loaded,
                        records_retained=loaded,
                        metadata={"last_authority": authority_name},
                    ),
                )
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=fetched,
                    records_loaded=loaded,
                    records_retained=loaded,
                    metadata={"source_registry_id": source_registry_id},
                    finished=True,
                ),
            )
            return {"records_fetched": fetched, "records_loaded": loaded, "source_registry_id": source_registry_id}
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def _build_dataset_rows(
        self,
        key: str,
        authority_name: str,
        frame: gpd.GeoDataFrame,
        config: SpatialHubSourceConfig,
        source_registry_id: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records"), start=1):
            source_record_id = _pick_text(row, config.field_mappings["source_record_id"]) or f"{authority_name}:{index}"
            geometry = _polygonize_geometry(row.get("geometry"))
            payload = _row_payload(row)
            if key == "planning_history":
                rows.append(
                    {
                        "source_record_id": source_record_id,
                        "authority_name": authority_name,
                        "planning_reference": _pick_text(row, config.field_mappings["planning_reference"]) or source_record_id,
                        "application_type": _pick_text(row, config.field_mappings["application_type"]),
                        "proposal_text": _pick_text(row, config.field_mappings["proposal_text"]),
                        "application_status": _pick_text(row, config.field_mappings["application_status"]),
                        "decision": _pick_text(row, config.field_mappings["decision"]),
                        "lodged_date": _pick_date(row, config.field_mappings["lodged_date"]),
                        "decision_date": _pick_date(row, config.field_mappings["decision_date"]),
                        "appeal_status": _pick_text(row, config.field_mappings["appeal_status"]),
                        "refusal_themes": _extract_refusal_themes(
                            _pick_text(row, config.field_mappings["refusal_reason"])
                            or _pick_text(row, config.field_mappings["decision"])
                        ),
                        "geometry_wkb": _geometry_hex(geometry),
                        "source_registry_id": source_registry_id,
                        "ingest_run_id": run_id,
                        "raw_payload": json.dumps(payload, default=_json_default),
                    }
                )
            else:
                rows.append(
                    {
                        "source_record_id": source_record_id,
                        "authority_name": authority_name,
                        "site_reference": _pick_text(row, config.field_mappings["site_reference"]) or source_record_id,
                        "site_name": _pick_text(row, config.field_mappings["site_name"]),
                        "effectiveness_status": _pick_text(row, config.field_mappings["effectiveness_status"]),
                        "programming_horizon": _pick_text(row, config.field_mappings["programming_horizon"]),
                        "constraint_reasons": _split_multivalue(_pick_text(row, config.field_mappings["constraint_reason"])),
                        "developer_name": _pick_text(row, config.field_mappings["developer_name"]),
                        "remaining_capacity": _pick_int(row, config.field_mappings["remaining_capacity"]),
                        "completions": _pick_int(row, config.field_mappings["completions"]),
                        "tenure": _pick_text(row, config.field_mappings["tenure"]),
                        "brownfield_indicator": _pick_bool(row, config.field_mappings["brownfield_indicator"]),
                        "geometry_wkb": _geometry_hex(geometry),
                        "source_registry_id": source_registry_id,
                        "ingest_run_id": run_id,
                        "raw_payload": json.dumps(payload, default=_json_default),
                    }
                )
        return rows

    def _consolidate_dataset_rows(
        self,
        key: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if key != "hla":
            return rows
        return self._consolidate_hla_rows(rows)

    def _consolidate_hla_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        consolidated: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            authority_name = str(row.get("authority_name") or "")
            source_record_id = str(row.get("source_record_id") or "")
            key = (authority_name, source_record_id)
            if key not in consolidated:
                seed = dict(row)
                seed["_raw_payloads"] = [row.get("raw_payload")]
                seed["_geometry_wkbs"] = [row.get("geometry_wkb")]
                consolidated[key] = seed
                continue

            existing = consolidated[key]
            for field_name in (
                "site_reference",
                "site_name",
                "effectiveness_status",
                "programming_horizon",
                "developer_name",
                "remaining_capacity",
                "completions",
                "tenure",
            ):
                existing[field_name] = _coalesce_value(existing.get(field_name), row.get(field_name))

            existing["constraint_reasons"] = _merge_text_lists(
                existing.get("constraint_reasons"),
                row.get("constraint_reasons"),
            )
            existing["brownfield_indicator"] = _merge_boolean(
                existing.get("brownfield_indicator"),
                row.get("brownfield_indicator"),
            )
            existing["_raw_payloads"].append(row.get("raw_payload"))
            existing["_geometry_wkbs"].append(row.get("geometry_wkb"))

        output: list[dict[str, Any]] = []
        for merged in consolidated.values():
            geometry_wkbs = merged.pop("_geometry_wkbs", [])
            raw_payloads = merged.pop("_raw_payloads", [])
            merged["geometry_wkb"] = _merge_geometry_wkbs(geometry_wkbs)
            merged["raw_payload"] = _merge_raw_payloads(raw_payloads)
            output.append(merged)
        return output

    def _insert_planning_batch(self, batch: list[dict[str, Any]]) -> None:
        self.database.execute_many(
            """
                insert into landintel.planning_application_records (
                    source_record_id,
                    authority_name,
                    planning_reference,
                    application_type,
                    proposal_text,
                    application_status,
                    decision,
                    lodged_date,
                    decision_date,
                    appeal_status,
                    refusal_themes,
                    geometry,
                    source_registry_id,
                    ingest_run_id,
                    raw_payload
                )
                values (
                    :source_record_id,
                    :authority_name,
                    :planning_reference,
                    :application_type,
                    :proposal_text,
                    :application_status,
                    :decision,
                    :lodged_date,
                    :decision_date,
                    :appeal_status,
                    :refusal_themes,
                    case when cast(:geometry_wkb as text) is null then null::geometry(geometry, 27700)
                         else ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700)
                    end,
                    cast(:source_registry_id as uuid),
                    cast(:ingest_run_id as uuid),
                    cast(:raw_payload as jsonb)
                )
            """,
            batch,
        )

    def _insert_hla_batch(self, batch: list[dict[str, Any]]) -> None:
        self.database.execute_many(
            """
                insert into landintel.hla_site_records (
                    source_record_id,
                    authority_name,
                    site_reference,
                    site_name,
                    effectiveness_status,
                    programming_horizon,
                    constraint_reasons,
                    developer_name,
                    remaining_capacity,
                    completions,
                    tenure,
                    brownfield_indicator,
                    geometry,
                    source_registry_id,
                    ingest_run_id,
                    raw_payload
                )
                values (
                    :source_record_id,
                    :authority_name,
                    :site_reference,
                    :site_name,
                    :effectiveness_status,
                    :programming_horizon,
                    :constraint_reasons,
                    :developer_name,
                    :remaining_capacity,
                    :completions,
                    :tenure,
                    :brownfield_indicator,
                    case when cast(:geometry_wkb as text) is null then null::geometry(geometry, 27700)
                         else ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700)
                    end,
                    cast(:source_registry_id as uuid),
                    cast(:ingest_run_id as uuid),
                    cast(:raw_payload as jsonb)
                )
            """,
            batch,
        )

    def _spatial_hub_source(self, key: str) -> SpatialHubSourceConfig:
        payload = self.manifest.get("spatial_hub", {}).get(key)
        if not payload:
            raise KeyError(f"Missing spatial_hub configuration for {key}.")
        return SpatialHubSourceConfig(
            source_name=payload["source_name"],
            publisher=payload["publisher"],
            dataset_id=payload["dataset_id"],
            metadata_uuid=payload["metadata_uuid"],
            field_mappings=payload["field_mappings"],
            authority_field_candidates=list(payload.get("authority_field_candidates") or []),
            resource_name_contains=payload.get("resource_name_contains"),
        )

    def _bgs_collections(self) -> dict[str, BgsCollectionConfig]:
        payload = self.manifest.get("bgs", {}).get("collections") or {}
        collections: dict[str, BgsCollectionConfig] = {}
        for key, value in payload.items():
            collections[key] = BgsCollectionConfig(
                source_name=value["source_name"],
                metadata_uuid=value["metadata_uuid"],
                collection_id=value["collection_id"],
                record_type=value["record_type"],
                bbox_buffer_m=int(value.get("bbox_buffer_m") or 0),
            )
        return collections

    def _build_spatial_hub_source_registry_record(self, config: SpatialHubSourceConfig) -> SourceRegistryRecord:
        handle = self._resolve_spatial_hub_resource_handle(config)
        package = self._fetch_spatial_hub_package(config.dataset_id)
        dataset_url = f"https://data.spatialhub.scot/dataset/{config.dataset_id}"
        return SourceRegistryRecord(
            source_name=config.source_name,
            source_type="Spatial Hub WFS",
            publisher=config.publisher,
            metadata_uuid=config.metadata_uuid,
            endpoint_url=handle.capabilities_url,
            download_url=handle.resource_page_url,
            record_json={"dataset": package.get("result", {})},
            geographic_extent=None,
            last_seen_at=datetime.now(timezone.utc),
        )

    def _fetch_spatial_hub_package(self, dataset_id: str) -> dict[str, Any]:
        if dataset_id not in self._spatial_hub_package_cache:
            response = self.client.get(
                "https://data.spatialhub.scot/api/3/action/package_show",
                params={"id": dataset_id},
            )
            response.raise_for_status()
            self._spatial_hub_package_cache[dataset_id] = response.json()
        return self._spatial_hub_package_cache[dataset_id]

    def _resolve_spatial_hub_authkey(self) -> str:
        authkey = self.settings.boundary_authkey or os.getenv("BOUNDARY_AUTHKEY")
        if authkey:
            return authkey
        raise RuntimeError("BOUNDARY_AUTHKEY is required for Spatial Hub source ingestion.")

    def _resolve_spatial_hub_resource_handle(self, config: SpatialHubSourceConfig) -> SpatialHubResourceHandle:
        cache_key = config.dataset_id
        if cache_key in self._spatial_hub_handle_cache:
            return self._spatial_hub_handle_cache[cache_key]

        package = self._fetch_spatial_hub_package(config.dataset_id)
        result = package.get("result") or {}
        resources = result.get("resources") or []
        resource_payload = None
        for resource in resources:
            name = str(resource.get("name") or "")
            if config.resource_name_contains and config.resource_name_contains.lower() not in name.lower():
                continue
            format_value = str(resource.get("format") or "")
            url_value = str(resource.get("url") or resource.get("download_url") or "")
            if "wfs" in format_value.lower() or "/wfs" in url_value.lower():
                resource_payload = resource
                break
        if resource_payload is None and resources:
            resource_payload = resources[0]
        if resource_payload is None:
            raise RuntimeError(f"No usable Spatial Hub resource found for {config.source_name}.")

        resource_page_url = f"https://data.spatialhub.scot/dataset/{config.dataset_id}/resource/{resource_payload['id']}"
        response = self.client.get(resource_page_url)
        response.raise_for_status()
        html = response.text

        geoserver_root = _extract_regex_group(html, r"(https://geo\.spatialhub\.scot/geoserver/[A-Za-z0-9_\-]+/wfs)")
        workspace_name = _extract_regex_group(html, r"workspaceName\s*[:=]\s*['\"]([^'\"]+)")
        preview_layer_name = _extract_regex_group(html, r"previewLayerName\s*[:=]\s*['\"]([^'\"]+)")
        alternative_name = _extract_regex_group(html, r"alternativeName\s*[:=]\s*['\"]([^'\"]+)")

        fallback_url = str(resource_payload.get("url") or resource_payload.get("download_url") or "")
        if not geoserver_root and fallback_url:
            geoserver_root = re.sub(r"[?&]authkey=[^&]+", "", fallback_url).split("?")[0]
        if not workspace_name and fallback_url:
            workspace_name = self._workspace_from_url(fallback_url)
        if not preview_layer_name and fallback_url:
            query_params = parse_qs(urlparse(fallback_url).query)
            preview_layer_name = query_params.get("typeName", [None])[0]
        if not geoserver_root or not workspace_name:
            raise RuntimeError(f"Could not resolve Spatial Hub WFS handle for {config.source_name}.")
        if preview_layer_name and ":" in preview_layer_name:
            workspace_name = preview_layer_name.split(":", 1)[0]
        capabilities_url = self._with_authkey(
            geoserver_root,
            {"service": "WFS", "request": "GetCapabilities"},
        )
        handle = SpatialHubResourceHandle(
            source_name=config.source_name,
            resource_id=str(resource_payload["id"]),
            resource_page_url=resource_page_url,
            geoserver_root=geoserver_root,
            workspace_name=workspace_name,
            preview_layer_name=preview_layer_name,
            alternative_name=alternative_name,
            capabilities_url=capabilities_url,
        )
        self._spatial_hub_handle_cache[cache_key] = handle
        return handle

    def _workspace_from_url(self, url: str) -> str | None:
        parts = [part for part in urlparse(url).path.split("/") if part]
        if "geoserver" not in parts:
            return None
        index = parts.index("geoserver")
        if len(parts) <= index + 1:
            return None
        return parts[index + 1]

    def _available_spatial_hub_feature_types(self, handle: SpatialHubResourceHandle) -> list[str]:
        if handle.resource_id in self._spatial_hub_feature_type_cache:
            return self._spatial_hub_feature_type_cache[handle.resource_id]
        response = self.client.get(handle.capabilities_url)
        response.raise_for_status()
        text = response.text
        _raise_for_spatial_hub_error_payload(
            text,
            content_type=response.headers.get("content-type"),
            context=f"{handle.source_name} GetCapabilities",
            allow_xml=True,
        )
        root = ET.fromstring(text)
        names = [
            element.text.strip()
            for element in root.findall(".//{*}FeatureType/{*}Name")
            if element.text and element.text.strip()
        ]
        self._spatial_hub_feature_type_cache[handle.resource_id] = names
        return names

    def _resolve_feature_type_name(self, config: SpatialHubSourceConfig, handle: SpatialHubResourceHandle) -> str:
        candidates = self._available_spatial_hub_feature_types(handle)
        preferred_tokens = [
            handle.preview_layer_name,
            handle.alternative_name,
            self._extract_preferred_layer_name(config),
        ]
        for token in preferred_tokens:
            if not token:
                continue
            for candidate in candidates:
                if _matches_feature_type_name(candidate, token):
                    return candidate
        if candidates:
            return candidates[0]
        raise RuntimeError(f"No Spatial Hub feature types advertised for {config.source_name}.")

    def _extract_preferred_layer_name(self, config: SpatialHubSourceConfig) -> str:
        if config.dataset_id == "planning_applications_official-is":
            return "pub_plnapppol"
        if config.dataset_id == "housing_land_supply-is":
            return "pub_hls"
        return config.dataset_id.replace("-", "_")

    def _describe_feature_type_properties(self, handle: SpatialHubResourceHandle, type_name: str) -> list[str]:
        cache_key = f"{handle.resource_id}:{type_name}"
        if cache_key in self._spatial_hub_property_name_cache:
            return self._spatial_hub_property_name_cache[cache_key]
        response = self.client.get(
            self._with_authkey(
                handle.geoserver_root,
                {
                    "service": "WFS",
                    "version": "1.0.0",
                    "request": "DescribeFeatureType",
                    "typeName": type_name,
                },
            )
        )
        response.raise_for_status()
        text = response.text
        _raise_for_spatial_hub_error_payload(
            text,
            content_type=response.headers.get("content-type"),
            context=f"{handle.source_name} DescribeFeatureType",
            allow_xml=True,
        )
        root = ET.fromstring(text)
        names = [
            element.attrib.get("name")
            for element in root.findall(".//{http://www.w3.org/2001/XMLSchema}element")
            if element.attrib.get("name")
        ]
        self._spatial_hub_property_name_cache[cache_key] = names
        return names

    def _select_authority_filter_field(
        self,
        config: SpatialHubSourceConfig,
        handle: SpatialHubResourceHandle,
        type_name: str,
    ) -> list[str]:
        properties = self._describe_feature_type_properties(handle, type_name)
        valid_fields = [field for field in config.authority_field_candidates if field in properties]
        return valid_fields or config.authority_field_candidates

    def _fetch_spatial_hub_frame(self, config: SpatialHubSourceConfig, authority_name: str) -> gpd.GeoDataFrame:
        handle = self._resolve_spatial_hub_resource_handle(config)
        type_name = self._resolve_feature_type_name(config, handle)
        authority_fields = self._select_authority_filter_field(config, handle, type_name)
        try:
            response = self.client.get(
                self._with_authkey(
                    handle.geoserver_root,
                    {
                        "service": "WFS",
                        "version": "1.0.0",
                        "request": "GetFeature",
                        "typeName": type_name,
                        "outputFormat": "application/json",
                        "cql_filter": self._build_authority_filter(authority_fields, authority_name),
                    },
                )
            )
            response.raise_for_status()
            return self._download_spatial_hub_frame(
                response,
                authority_name,
                authority_fields=authority_fields,
                context=f"{config.source_name} GetFeature for {authority_name}",
            )
        except RuntimeError as error:
            if not _is_spatial_hub_illegal_property_error(error):
                raise
            self.logger.warning(
                "spatial_hub_authority_filter_fallback",
                extra={
                    "source_name": config.source_name,
                    "authority_name": authority_name,
                    "reason": str(error),
                },
            )
            frame = self._fetch_spatial_hub_frame_without_server_filter(config, authority_name, handle, type_name)
            matching_field = next((field for field in authority_fields if field in frame.columns), None)
            if matching_field:
                frame[matching_field] = frame[matching_field].map(_canonicalise_authority_name)
                frame = frame[frame[matching_field] == authority_name].copy()
            if not frame.empty and not self.authority_aoi.empty:
                authority_geom = self.authority_aoi[self.authority_aoi["authority_name"] == authority_name]
                if not authority_geom.empty:
                    frame = gpd.overlay(frame, authority_geom[["geometry"]], how="intersection")
            return frame

    def _fetch_spatial_hub_frame_without_server_filter(
        self,
        config: SpatialHubSourceConfig,
        authority_name: str,
        handle: SpatialHubResourceHandle,
        type_name: str,
    ) -> gpd.GeoDataFrame:
        cache_key = f"{config.dataset_id}:{type_name}"
        if cache_key not in self._spatial_hub_unfiltered_frame_cache:
            response = self.client.get(
                self._with_authkey(
                    handle.geoserver_root,
                    {
                        "service": "WFS",
                        "version": "1.0.0",
                        "request": "GetFeature",
                        "typeName": type_name,
                        "outputFormat": "application/json",
                    },
                )
            )
            response.raise_for_status()
            frame = self._download_spatial_hub_frame(
                response,
                authority_name,
                authority_fields=[],
                context=f"{config.source_name} unfiltered GetFeature",
                standardise=False,
            )
            self._spatial_hub_unfiltered_frame_cache[cache_key] = frame
        return self._spatial_hub_unfiltered_frame_cache[cache_key].copy()

    def _download_spatial_hub_frame(
        self,
        response: httpx.Response,
        authority_name: str,
        *,
        authority_fields: list[str],
        context: str,
        standardise: bool = True,
    ) -> gpd.GeoDataFrame:
        text = response.text
        _raise_for_spatial_hub_error_payload(
            text,
            content_type=response.headers.get("content-type"),
            context=context,
        )
        with tempfile.NamedTemporaryFile(suffix=".geojson") as handle:
            handle.write(response.content)
            handle.flush()
            frame = gpd.read_file(handle.name)
        if standardise:
            frame = self._standardise_frame(frame, authority_name, authority_fields)
        return frame

    def _build_authority_filter(self, authority_fields: list[str], authority_name: str) -> str:
        if not authority_fields:
            return "1=1"
        clauses = [f"{field}='{authority_name}'" for field in authority_fields]
        return " or ".join(clauses)

    def _with_authkey(self, url: str, params: dict[str, Any]) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query, keep_blank_values=True)
        for key, value in params.items():
            query[key] = [str(value)]
        query["authkey"] = [self.spatial_hub_authkey]
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(query, doseq=True), ""))

    def _standardise_frame(
        self,
        frame: gpd.GeoDataFrame,
        authority_name: str,
        authority_fields: list[str],
    ) -> gpd.GeoDataFrame:
        if frame.crs is None:
            frame = frame.set_crs(4326, allow_override=True)
        if frame.crs.to_epsg() != 27700:
            frame = frame.to_crs(27700)
        if "geometry" in frame.columns:
            frame = frame[frame.geometry.notnull()].copy()
        matching_field = next((field for field in authority_fields if field in frame.columns), None)
        if matching_field:
            frame[matching_field] = frame[matching_field].map(_canonicalise_authority_name)
            frame = frame[frame[matching_field] == authority_name].copy()
        if not frame.empty and not self.authority_aoi.empty:
            authority_geom = self.authority_aoi[self.authority_aoi["authority_name"] == authority_name]
            if not authority_geom.empty:
                frame = gpd.overlay(frame, authority_geom[["geometry"]], how="intersection")
        return frame

    def _upsert_and_resolve_source_registry_id(self, record: SourceRegistryRecord) -> str:
        self.loader.upsert_source_registry([record])
        source_registry_id = self._resolve_source_registry_id(record.metadata_uuid)
        if not source_registry_id:
            raise RuntimeError(f"Could not resolve source registry id for {record.source_name}.")
        return source_registry_id

    def _resolve_source_registry_id(self, metadata_uuid: str) -> str | None:
        row = self.database.fetch_one(
            "select id from public.source_registry where metadata_uuid = :metadata_uuid order by updated_at desc limit 1",
            {"metadata_uuid": metadata_uuid},
        )
        return str(row["id"]) if row else None

    def _reset_canonical_state(self) -> None:
        self.database.execute("update landintel.planning_application_records set canonical_site_id = null")
        self.database.execute("update landintel.hla_site_records set canonical_site_id = null")
        self.database.execute("update landintel.bgs_records set canonical_site_id = null")
        self.database.execute("delete from landintel.site_assessments")
        self.database.execute("delete from landintel.site_signals")
        self.database.execute("delete from landintel.evidence_references")
        self.database.execute("delete from landintel.site_source_links")
        self.database.execute("delete from landintel.site_geometry_versions")
        self.database.execute("delete from landintel.site_reference_aliases")
        self.database.execute("delete from landintel.canonical_sites")

    def _canonical_site_code(self, prefix: str, authority_name: str, raw_value: str) -> str:
        return f"{prefix}:{_normalize_ref(authority_name)}:{_normalize_ref(raw_value)}"

    def _upsert_canonical_site(
        self,
        *,
        site_code: str,
        site_name: str,
        authority_name: str,
        geometry: BaseGeometry | None,
        surfaced_reason: str,
        metadata: dict[str, Any],
    ) -> str:
        return str(
            self.database.scalar(
                """
                    insert into landintel.canonical_sites (
                        site_code,
                        site_name_primary,
                        authority_name,
                        geometry,
                        centroid,
                        area_acres,
                        surfaced_reason,
                        metadata
                    )
                    values (
                        :site_code,
                        :site_name_primary,
                        :authority_name,
                        case when cast(:geometry_wkb as text) is null then null::geometry(multipolygon, 27700)
                             else ST_Multi(ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700))
                        end,
                        case when cast(:geometry_wkb as text) is null then null::geometry(point, 27700)
                             else ST_Centroid(ST_Multi(ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700)))
                        end,
                        case when cast(:geometry_wkb as text) is null then null
                             else ST_Area(ST_Multi(ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700))) / 4046.8564224
                        end,
                        :surfaced_reason,
                        cast(:metadata as jsonb)
                    )
                    on conflict (site_code) do update set
                        site_name_primary = excluded.site_name_primary,
                        authority_name = excluded.authority_name,
                        geometry = excluded.geometry,
                        centroid = excluded.centroid,
                        area_acres = excluded.area_acres,
                        surfaced_reason = excluded.surfaced_reason,
                        metadata = excluded.metadata,
                        updated_at = now()
                    returning id
                """,
                {
                    "site_code": site_code,
                    "site_name_primary": site_name,
                    "authority_name": authority_name,
                    "geometry_wkb": _geometry_hex(geometry),
                    "surfaced_reason": surfaced_reason,
                    "metadata": json.dumps(metadata, default=_json_default),
                },
            )
        )

    def _record_site_geometry(
        self,
        site_id: str,
        geometry_source: str,
        geometry: BaseGeometry | None,
        source_registry_id: str | None,
        ingest_run_id: str,
    ) -> None:
        if geometry is None:
            return
        self.database.execute(
            "update landintel.site_geometry_versions set effective_to = now() where canonical_site_id = cast(:site_id as uuid) and geometry_source = :geometry_source and effective_to is null",
            {"site_id": site_id, "geometry_source": geometry_source},
        )
        self.database.execute(
            """
                insert into landintel.site_geometry_versions (
                    canonical_site_id,
                    geometry_source,
                    version_label,
                    geometry,
                    source_registry_id,
                    ingest_run_id,
                    metadata
                )
                values (
                    cast(:site_id as uuid),
                    :geometry_source,
                    :version_label,
                    ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                    cast(:source_registry_id as uuid),
                    cast(:ingest_run_id as uuid),
                    '{}'::jsonb
                )
            """,
            {
                "site_id": site_id,
                "geometry_source": geometry_source,
                "version_label": f"{geometry_source}:{datetime.now(timezone.utc).date().isoformat()}",
                "geometry_wkb": _geometry_hex(geometry),
                "source_registry_id": source_registry_id,
                "ingest_run_id": ingest_run_id,
            },
        )

    def _record_reference_alias(
        self,
        site_id: str,
        *,
        source_family: str,
        source_dataset: str,
        authority_name: str,
        raw_reference_value: str,
        site_name: str,
        source_registry_id: str | None,
        ingest_run_id: str,
        planning_reference: str | None,
        status: str,
        confidence: float,
    ) -> None:
        self.database.execute(
            """
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
                )
                values (
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
                    '{}'::jsonb
                )
            """,
            {
                "site_id": site_id,
                "source_family": source_family,
                "source_dataset": source_dataset,
                "authority_name": authority_name,
                "site_name": site_name,
                "raw_reference_value": raw_reference_value,
                "normalized_reference_value": _normalize_ref(raw_reference_value),
                "planning_reference": planning_reference,
                "geometry_hash": _normalize_ref(raw_reference_value),
                "status": status,
                "confidence": confidence,
                "source_registry_id": source_registry_id,
                "ingest_run_id": ingest_run_id,
            },
        )

    def _record_source_link(
        self,
        site_id: str,
        *,
        source_family: str,
        source_dataset: str,
        source_record_id: str,
        link_method: str,
        confidence: float,
        source_registry_id: str | None,
        ingest_run_id: str,
    ) -> None:
        self.database.execute(
            """
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
                )
                values (
                    cast(:site_id as uuid),
                    :source_family,
                    :source_dataset,
                    :source_record_id,
                    :link_method,
                    :confidence,
                    cast(:source_registry_id as uuid),
                    cast(:ingest_run_id as uuid),
                    '{}'::jsonb
                )
            """,
            {
                "site_id": site_id,
                "source_family": source_family,
                "source_dataset": source_dataset,
                "source_record_id": source_record_id,
                "link_method": link_method,
                "confidence": confidence,
                "source_registry_id": source_registry_id,
                "ingest_run_id": ingest_run_id,
            },
        )

    def _record_evidence(
        self,
        site_id: str,
        *,
        source_family: str,
        source_dataset: str,
        source_record_id: str,
        source_reference: str | None,
        confidence: str,
        source_registry_id: str | None,
        ingest_run_id: str,
        metadata: dict[str, Any],
    ) -> None:
        self.database.execute(
            """
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
                )
                values (
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
            """,
            {
                "site_id": site_id,
                "source_family": source_family,
                "source_dataset": source_dataset,
                "source_record_id": source_record_id,
                "source_reference": source_reference,
                "confidence": confidence,
                "source_registry_id": source_registry_id,
                "ingest_run_id": ingest_run_id,
                "metadata": json.dumps(metadata, default=_json_default),
            },
        )

    def _set_primary_parcel(self, site_id: str) -> None:
        self.database.execute(
            """
                update landintel.canonical_sites as cs
                set primary_ros_parcel_id = (
                        select rp.id
                        from public.ros_cadastral_parcels as rp
                        where rp.authority_name = cs.authority_name
                          and cs.geometry is not null
                          and ST_Intersects(rp.geometry, cs.geometry)
                        order by ST_Area(ST_Intersection(rp.geometry, cs.geometry)) desc nulls last
                        limit 1
                    ),
                    updated_at = now()
                where cs.id = cast(:site_id as uuid)
            """,
            {"site_id": site_id},
        )

    def _find_best_site_by_geometry(self, authority_name: str, geometry: BaseGeometry | None) -> str | None:
        if geometry is None:
            return None
        row = self.database.fetch_one(
            """
                select id
                from landintel.canonical_sites
                where authority_name = :authority_name
                  and geometry is not null
                  and ST_Intersects(geometry, ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)))
                order by ST_Area(ST_Intersection(geometry, ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)))) desc nulls last
                limit 1
            """,
            {"authority_name": authority_name, "geometry_wkb": _geometry_hex(geometry)},
        )
        return str(row["id"]) if row else None

    def _fetch_bgs_items(self, collection: BgsCollectionConfig, geometry: BaseGeometry) -> list[dict[str, Any]]:
        geo_series = gpd.GeoSeries([geometry], crs=27700)
        if collection.bbox_buffer_m > 0:
            geo_series = geo_series.buffer(collection.bbox_buffer_m)
        bounds = geo_series.to_crs(4326).total_bounds
        bbox = ",".join(str(value) for value in bounds)
        response = self.client.get(
            f"{self.manifest['bgs']['base_url']}/collections/{collection.collection_id}/items",
            params={"f": "json", "bbox": bbox, "limit": 200},
        )
        response.raise_for_status()
        payload = response.json()
        return list(payload.get("features", []))


def _extract_regex_group(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text, flags=re.I | re.S)
    if not match:
        return None
    value = match.group(1).strip()
    return value or None


def _matches_feature_type_name(candidate: str, expected: str) -> bool:
    if candidate == expected or candidate.endswith(f":{expected}"):
        return True
    return _normalize_ref(candidate.split(":")[-1]) == _normalize_ref(expected)


def _raise_for_spatial_hub_error_payload(
    text: str,
    *,
    content_type: str | None,
    context: str,
    allow_xml: bool = False,
) -> None:
    preview = (text or "").lstrip()
    lowered_content_type = (content_type or "").lower()
    if not preview:
        raise RuntimeError(f"{context} returned an empty response body.")
    lowered_preview = preview.lower()
    if (
        "serviceexception" in lowered_preview
        or "exceptionreport" in lowered_preview
        or "access denied" in lowered_preview
        or "<html" in lowered_preview
        or "html" in lowered_content_type
    ):
        raise RuntimeError(f"{context} returned a service error instead of features: {_short_error_snippet(preview)}")
    if not allow_xml and ("xml" in lowered_content_type or preview.startswith("<")):
        raise RuntimeError(f"{context} returned a service error instead of features: {_short_error_snippet(preview)}")


def _short_error_snippet(text: str, limit: int = 280) -> str:
    stripped = re.sub(r"<[^>]+>", " ", text or "")
    compact = re.sub(r"\s+", " ", stripped).strip()
    return compact[:limit]


def _is_spatial_hub_illegal_property_error(error: Exception) -> bool:
    message = str(error).lower()
    return "illegal property name" in message or "property name" in message and "instead of features" in message


def _coalesce_value(current: Any, incoming: Any) -> Any:
    if current is None:
        return incoming
    if isinstance(current, str) and not current.strip():
        return incoming
    return current


def _merge_text_lists(current: Any, incoming: Any) -> list[str]:
    merged: list[str] = []
    for values in (current or [], incoming or []):
        if not values:
            continue
        if isinstance(values, str):
            candidates = [values]
        else:
            candidates = list(values)
        for value in candidates:
            text = str(value).strip()
            if text and text not in merged:
                merged.append(text)
    return merged


def _merge_boolean(current: bool | None, incoming: bool | None) -> bool | None:
    if current is True or incoming is True:
        return True
    if current is False or incoming is False:
        return False
    return None


def _merge_geometry_wkbs(geometry_wkbs: list[str | None]) -> str | None:
    geometries: list[BaseGeometry] = []
    for geometry_wkb in geometry_wkbs:
        if not geometry_wkb:
            continue
        geometry = shapely_wkb.loads(bytes.fromhex(str(geometry_wkb)))
        polygonized = _polygonize_geometry(geometry)
        if polygonized is not None:
            geometries.append(polygonized)
    if not geometries:
        return None
    merged = unary_union(geometries)
    polygonized = _polygonize_geometry(merged)
    return _geometry_hex(polygonized)


def _merge_raw_payloads(raw_payloads: list[str | None]) -> str:
    merged_rows: list[Any] = []
    for raw_payload in raw_payloads:
        if not raw_payload:
            continue
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            merged_rows.append({"unparsed_payload": raw_payload})
            continue
        merged_rows.append(payload)
    return json.dumps(
        {
            "source_row_count": len(merged_rows),
            "source_rows": merged_rows,
        },
        default=_json_default,
    )


def _pick_text(row: dict[str, Any], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in row and not pd.isna(row[candidate]):
            text = str(row[candidate]).strip()
            if text and text.lower() != "nan":
                return text
    return None


def _pick_int(row: dict[str, Any], candidates: list[str]) -> int | None:
    text = _pick_text(row, candidates)
    if text is None:
        return None
    digits = re.sub(r"[^0-9-]", "", text)
    return int(digits) if digits else None


def _pick_bool(row: dict[str, Any], candidates: list[str]) -> bool | None:
    text = _pick_text(row, candidates)
    if text is None:
        return None
    return text.lower() in {"yes", "true", "1", "y", "brownfield"}


def _pick_date(row: dict[str, Any], candidates: list[str]) -> date | None:
    text = _pick_text(row, candidates)
    if text is None:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _split_multivalue(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in re.split(r"[|;,/]", value) if part and part.strip()]


def _extract_refusal_themes(text: str | None) -> list[str]:
    if not text:
        return []
    lowered = text.lower()
    return [theme for theme, patterns in REFUSAL_THEME_PATTERNS.items() if any(pattern in lowered for pattern in patterns)]


def _normalize_ref(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower()) or "unknown"


def _canonicalise_authority_name(value: Any) -> str:
    text = str(value).strip()
    return AUTHORITY_NAME_MAP.get(text, text)


def _geometry_hex(geometry: BaseGeometry | None) -> str | None:
    if geometry is None or getattr(geometry, "is_empty", False):
        return None
    return geometry.wkb_hex


def _polygonize_geometry(geometry: BaseGeometry | None) -> BaseGeometry | None:
    if geometry is None or getattr(geometry, "is_empty", False):
        return None
    if isinstance(geometry, MultiPolygon):
        return geometry
    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])
    buffered = geometry.buffer(1)
    if isinstance(buffered, Polygon):
        return MultiPolygon([buffered])
    if isinstance(buffered, MultiPolygon):
        return buffered
    return None


def _count_severity(count: int) -> str:
    if count >= 10:
        return "high"
    if count >= 3:
        return "medium"
    return "low"


def _row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in row.items():
        if key == "geometry":
            continue
        payload[str(key)] = _json_default(value)
    return payload


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, BaseGeometry):
        return value.wkt
    if pd.isna(value):
        return None
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the next Scottish source-intelligence phase.")
    parser.add_argument(
        "command",
        choices=(
            "run-migrations",
            "audit-source-footprint",
            "ingest-planning-history",
            "ingest-hla",
            "reconcile-canonical-sites",
            "ingest-bgs",
            "full-refresh-core-sources",
        ),
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = SourcePhaseRunner(settings, logger)
    try:
        if args.command == "run-migrations":
            runner.run_migrations()
        elif args.command == "audit-source-footprint":
            runner.audit_source_footprint()
        elif args.command == "ingest-planning-history":
            runner.ingest_planning_history()
        elif args.command == "ingest-hla":
            runner.ingest_hla()
        elif args.command == "reconcile-canonical-sites":
            runner.reconcile_canonical_sites()
        elif args.command == "ingest-bgs":
            runner.ingest_bgs()
        elif args.command == "full-refresh-core-sources":
            runner.full_refresh_core_sources()
        logger.info("source_phase_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception("source_phase_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

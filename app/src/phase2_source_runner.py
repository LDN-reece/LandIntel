"""Phase 2 source-estate registration, refresh commands and proof audit."""

from __future__ import annotations

import argparse
import json
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from config.settings import Settings, get_settings
from src.db import Database
from src.logging_config import configure_logging


CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "phase2_source_estate.yaml"

PHASE2_COMMANDS = (
    "discover-phase2-sources",
    "ingest-planning-appeals",
    "ingest-power-infrastructure",
    "ingest-amenities",
    "ingest-demographics",
    "ingest-market-context",
    "ingest-planning-documents",
    "ingest-intelligence-events",
    "refresh-title-readiness",
    "refresh-site-market-context",
    "refresh-site-amenity-context",
    "refresh-site-demographic-context",
    "refresh-site-power-context",
    "refresh-site-abnormal-risk",
    "audit-full-source-estate",
)

INGEST_COMMAND_MODULES = {
    "ingest-planning-appeals": "planning_appeals",
    "ingest-power-infrastructure": "power_infrastructure",
    "ingest-amenities": "amenities",
    "ingest-demographics": "demographics",
    "ingest-market-context": "market_context",
    "ingest-planning-documents": "planning_documents",
    "ingest-intelligence-events": "local_intelligence",
}

REFRESH_COMMAND_FAMILIES = {
    "refresh-title-readiness": "title_control",
    "refresh-site-market-context": "market_context",
    "refresh-site-amenity-context": "amenities",
    "refresh-site-demographic-context": "demographics",
    "refresh-site-power-context": "power_infrastructure",
    "refresh-site-abnormal-risk": "terrain_abnormal",
}

LIFECYCLE_STAGES = (
    "source_registered",
    "access_confirmed",
    "raw_data_landed",
    "normalised",
    "linked_to_site",
    "measured",
    "evidence_generated",
    "signals_generated",
    "assessment_ready",
    "trusted_for_review",
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


class Phase2SourceRunner:
    """Repo-controlled Phase 2 source estate runner."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.database = Database(settings)
        self.logger = configure_logging(settings).getChild("phase2_source_runner")
        self.manifest = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
        self.sources: list[dict[str, Any]] = list(self.manifest.get("sources", []))
        self.source_family_filter = (os.getenv("PHASE2_SOURCE_FAMILY") or "").strip()
        self.authority_filter = (os.getenv("PHASE2_AUTHORITY") or "").strip()
        self.batch_size = max(_env_int("PHASE2_BATCH_SIZE", 250), 1)
        self.runtime_minutes = max(_env_int("PHASE2_RUNTIME_MINUTES", 10), 1)
        self.dry_run = _env_bool("PHASE2_DRY_RUN")
        self.force_refresh = _env_bool("PHASE2_FORCE_REFRESH")
        self.audit_only = _env_bool("PHASE2_AUDIT_ONLY")

    def close(self) -> None:
        self.database.dispose()

    def run_command(self, command: str) -> dict[str, Any]:
        if command == "discover-phase2-sources":
            result = self.discover_phase2_sources()
        elif command in INGEST_COMMAND_MODULES:
            result = self.ingest_registered_module(command, INGEST_COMMAND_MODULES[command])
        elif command == "refresh-title-readiness":
            result = self.refresh_title_readiness()
        elif command == "refresh-site-abnormal-risk":
            result = self.refresh_site_abnormal_risk()
        elif command in REFRESH_COMMAND_FAMILIES:
            result = self.refresh_registered_context(command, REFRESH_COMMAND_FAMILIES[command])
        elif command == "audit-full-source-estate":
            result = self.audit_full_source_estate()
        else:
            raise ValueError(f"Unsupported Phase 2 command: {command}")

        self.logger.info("phase2_source_command_completed", extra={"command": command, **result})
        return result

    def _selected_sources(self, module_key: str | None = None, source_family: str | None = None) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        for source in self.sources:
            if module_key and source.get("module_key") != module_key:
                continue
            if source_family and source.get("source_family") != source_family:
                continue
            if self.source_family_filter:
                filter_value = self.source_family_filter
                if filter_value not in {
                    str(source.get("source_family")),
                    str(source.get("module_key")),
                    str(source.get("source_key")),
                }:
                    continue
            if self.authority_filter:
                geography = str(source.get("geography") or "")
                if self.authority_filter.lower() not in geography.lower():
                    continue
            selected.append(source)
        return selected

    def discover_phase2_sources(self) -> dict[str, Any]:
        selected = self._selected_sources()
        if not self.dry_run and not self.audit_only:
            for source in selected:
                self._upsert_source(source)
                self._record_freshness(source, records_observed=0)
                self._record_expansion_event(
                    command_name="discover-phase2-sources",
                    source_key=source["source_key"],
                    source_family=source["source_family"],
                    status="source_registered",
                    summary=f"{source['source_name']} registered in the Phase 2 source estate.",
                    metadata={"module_key": source.get("module_key"), "dry_run": self.dry_run},
                )
        result = {
            "registered_source_count": len(selected),
            "dry_run": self.dry_run,
            "audit_only": self.audit_only,
        }
        result.update(self.audit_full_source_estate(log_event=False))
        return result

    def ingest_registered_module(self, command_name: str, module_key: str) -> dict[str, Any]:
        selected = self._selected_sources(module_key=module_key)
        if not self.dry_run and not self.audit_only:
            for source in selected:
                self._upsert_source(source)
                self._record_freshness(source, records_observed=0)
                self._record_expansion_event(
                    command_name=command_name,
                    source_key=source["source_key"],
                    source_family=source["source_family"],
                    status=source.get("ingest_status") or "source_registered_no_rows",
                    summary=source.get("limitation_notes") or f"{source['source_name']} registered for Phase 2 ingest.",
                    metadata={
                        "module_key": module_key,
                        "next_action": source.get("next_action"),
                        "access_status": source.get("access_status"),
                    },
                )
        return {
            "module_key": module_key,
            "registered_source_count": len(selected),
            "source_status": "registered_or_gated",
            "dry_run": self.dry_run,
            "audit_only": self.audit_only,
        }

    def refresh_registered_context(self, command_name: str, source_family: str) -> dict[str, Any]:
        selected = self._selected_sources(source_family=source_family)
        if not self.dry_run and not self.audit_only:
            for source in selected:
                self._upsert_source(source)
                self._record_freshness(source, records_observed=0)
                self._record_expansion_event(
                    command_name=command_name,
                    source_key=source["source_key"],
                    source_family=source["source_family"],
                    status="source_registered_no_rows",
                    summary=source.get("limitation_notes") or "Source registered but context adapter has not landed rows yet.",
                    metadata={"next_action": source.get("next_action"), "audit_only": self.audit_only},
                )
        return {
            "source_family": source_family,
            "registered_source_count": len(selected),
            "rows_changed": 0,
            "dry_run": self.dry_run,
            "audit_only": self.audit_only,
        }

    def refresh_title_readiness(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="title_control")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            candidate_count = self.database.scalar(
                """
                select count(*)::bigint
                from landintel.canonical_sites as site
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                """,
                {
                    "authority_name": self.authority_filter,
                    "authority_name_like": f"%{self.authority_filter}%",
                },
            )
            return {
                "source_family": "title_control",
                "candidate_site_count": int(candidate_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select
                    site.id as canonical_site_id,
                    site.id::text as site_location_id,
                    parcel.ros_parcel_id,
                    parcel.ros_inspire_id,
                    title.candidate_title_number,
                    title.normalized_title_number,
                    greatest(coalesce(parcel.confidence, 0), coalesce(title.confidence, 0)) as confidence,
                    existing.source_record_signature as previous_signature,
                    md5(concat_ws(
                        '|',
                        site.id::text,
                        coalesce(parcel.ros_parcel_id::text, ''),
                        coalesce(parcel.ros_inspire_id, ''),
                        coalesce(title.normalized_title_number, ''),
                        round(greatest(coalesce(parcel.confidence, 0), coalesce(title.confidence, 0)), 4)::text
                    )) as current_signature
                from landintel.canonical_sites as site
                left join landintel.title_order_workflow as existing
                  on existing.canonical_site_id = site.id
                left join lateral (
                    select candidate.*
                    from public.site_ros_parcel_link_candidates as candidate
                    where candidate.site_id = site.id::text
                    order by candidate.confidence desc nulls last,
                             candidate.overlap_pct_of_site desc nulls last,
                             candidate.ros_parcel_id
                    limit 1
                ) as parcel on true
                left join lateral (
                    select candidate.*
                    from public.site_title_resolution_candidates as candidate
                    where candidate.site_id = site.id::text
                    order by candidate.confidence desc nulls last,
                             candidate.overlap_pct_of_site desc nulls last,
                             candidate.ros_parcel_id
                    limit 1
                ) as title on true
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by existing.updated_at nulls first, site.id
                limit :batch_size
            ),
            upserted as (
                insert into landintel.title_order_workflow (
                    canonical_site_id,
                    site_location_id,
                    source_key,
                    source_family,
                    title_number,
                    normalized_title_number,
                    parcel_candidate_status,
                    possible_title_reference_status,
                    ownership_status_pre_title,
                    title_required_flag,
                    title_order_status,
                    title_review_status,
                    title_confidence_level,
                    control_signal_summary,
                    next_action,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    selected_sites.canonical_site_id,
                    selected_sites.site_location_id,
                    'title_readiness_internal',
                    'title_control',
                    selected_sites.candidate_title_number,
                    selected_sites.normalized_title_number,
                    case
                        when selected_sites.ros_parcel_id is not null then 'parcel_candidate_identified'
                        else 'title_required'
                    end,
                    case
                        when selected_sites.normalized_title_number is not null then 'possible_title_reference_identified'
                        else 'title_required'
                    end,
                    'ownership_not_confirmed',
                    true,
                    'not_ordered',
                    'not_reviewed',
                    selected_sites.confidence,
                    case
                        when selected_sites.normalized_title_number is not null then 'possible_title_reference_identified'
                        when selected_sites.ros_parcel_id is not null then 'parcel_candidate_identified'
                        else 'evidence_required'
                    end,
                    case
                        when selected_sites.normalized_title_number is not null then 'review_site_before_title_spend'
                        when selected_sites.ros_parcel_id is not null then 'resolve_possible_title_reference'
                        else 'link_ros_parcel_candidate'
                    end,
                    selected_sites.current_signature,
                    jsonb_build_object(
                        'phase2_title_readiness', true,
                        'ros_parcel_id', selected_sites.ros_parcel_id,
                        'ros_inspire_id', selected_sites.ros_inspire_id,
                        'ownership_limitation', 'ownership_not_confirmed_until_title_review'
                    ),
                    now()
                from selected_sites
                on conflict (canonical_site_id) do update set
                    site_location_id = excluded.site_location_id,
                    title_number = excluded.title_number,
                    normalized_title_number = excluded.normalized_title_number,
                    parcel_candidate_status = excluded.parcel_candidate_status,
                    possible_title_reference_status = excluded.possible_title_reference_status,
                    ownership_status_pre_title = excluded.ownership_status_pre_title,
                    title_required_flag = excluded.title_required_flag,
                    title_confidence_level = excluded.title_confidence_level,
                    control_signal_summary = excluded.control_signal_summary,
                    next_action = excluded.next_action,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join selected_sites
                  on selected_sites.canonical_site_id = upserted.canonical_site_id
                where selected_sites.previous_signature is distinct from selected_sites.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'title_control'
                  and evidence.metadata ->> 'phase2_title_readiness' = 'true'
                returning evidence.id
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
                    changed.canonical_site_id,
                    'title_control',
                    'Scottish title readiness workflow',
                    changed.id::text,
                    changed.control_signal_summary,
                    'medium',
                    jsonb_build_object(
                        'source_key', 'title_readiness_internal',
                        'phase2_title_readiness', true,
                        'parcel_candidate_status', changed.parcel_candidate_status,
                        'possible_title_reference_status', changed.possible_title_reference_status,
                        'ownership_status_pre_title', changed.ownership_status_pre_title
                    )
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'title_control'
                  and signal.metadata ->> 'phase2_title_readiness' = 'true'
                returning signal.id
            ),
            inserted_signals as (
                insert into landintel.site_signals (
                    canonical_site_id,
                    signal_key,
                    signal_value,
                    signal_status,
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
                    changed.canonical_site_id,
                    'title_control:title_readiness:' || changed.canonical_site_id::text,
                    jsonb_build_object(
                        'parcel_candidate_status', changed.parcel_candidate_status,
                        'possible_title_reference_status', changed.possible_title_reference_status,
                        'ownership_status_pre_title', changed.ownership_status_pre_title
                    ),
                    'derived',
                    'title_control',
                    'title_readiness',
                    changed.control_signal_summary,
                    changed.title_confidence_level,
                    coalesce(changed.title_confidence_level, 0.5),
                    'title_control',
                    changed.id::text,
                    'title_readiness_evidence',
                    jsonb_build_object('source', 'phase2_title_readiness'),
                    jsonb_build_object('source_key', 'title_readiness_internal', 'phase2_title_readiness', true),
                    true
                from changed
                returning id
            ),
            upserted_control_signals as (
                insert into landintel.ownership_control_signals (
                    canonical_site_id,
                    source_key,
                    source_family,
                    source_record_id,
                    signal_type,
                    signal_label,
                    signal_value_text,
                    confidence,
                    evidence_required,
                    ownership_confirmed,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    changed.canonical_site_id,
                    'title_readiness_internal',
                    'title_control',
                    changed.id::text,
                    'title_readiness',
                    changed.control_signal_summary,
                    changed.ownership_status_pre_title,
                    coalesce(changed.title_confidence_level, 0.5),
                    true,
                    false,
                    changed.source_record_signature,
                    jsonb_build_object('source_key', 'title_readiness_internal', 'phase2_title_readiness', true),
                    now()
                from changed
                on conflict (canonical_site_id, source_family, source_record_id, signal_type) do update set
                    signal_label = excluded.signal_label,
                    signal_value_text = excluded.signal_value_text,
                    confidence = excluded.confidence,
                    evidence_required = excluded.evidence_required,
                    ownership_confirmed = excluded.ownership_confirmed,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning id
            ),
            inserted_events as (
                insert into landintel.site_change_events (
                    canonical_site_id,
                    source_family,
                    source_record_id,
                    change_type,
                    change_summary,
                    previous_signature,
                    current_signature,
                    triggered_refresh,
                    metadata
                )
                select
                    changed.canonical_site_id,
                    'title_control',
                    changed.id::text,
                    'title_readiness_state_changed',
                    'Title readiness evidence changed for canonical site.',
                    selected_sites.previous_signature,
                    selected_sites.current_signature,
                    true,
                    jsonb_build_object('phase2_title_readiness', true)
                from changed
                join selected_sites on selected_sites.canonical_site_id = changed.canonical_site_id
                returning id
            )
            select
                (select count(*)::integer from selected_sites) as selected_site_count,
                (select count(*)::integer from upserted) as upserted_row_count,
                (select count(*)::integer from changed) as changed_row_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count,
                (select count(*)::integer from inserted_events) as change_event_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="title_control",
            source_key="title_readiness_internal",
            row_count=int(proof.get("upserted_row_count") or 0),
            linked_count=int(proof.get("upserted_row_count") or 0),
            measured_count=0,
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="refresh-title-readiness",
            source_key="title_readiness_internal",
            source_family="title_control",
            status="success",
            raw_rows=int(proof.get("upserted_row_count") or 0),
            linked_rows=int(proof.get("upserted_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            change_event_rows=int(proof.get("change_event_count") or 0),
            summary="Title readiness refreshed without confirming legal ownership.",
            metadata={"batch_size": self.batch_size, "authority_filter": self.authority_filter},
        )
        self._record_family_freshness("title_control", "Scottish title readiness workflow", int(proof.get("upserted_row_count") or 0))
        return {"source_family": "title_control", **proof}

    def refresh_site_abnormal_risk(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="terrain_abnormal")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            candidate_count = self.database.scalar(
                """
                select count(distinct summary.site_id)::bigint
                from public.site_constraint_group_summaries as summary
                join landintel.canonical_sites as site on site.id::text = summary.site_id
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                """,
                {
                    "authority_name": self.authority_filter,
                    "authority_name_like": f"%{self.authority_filter}%",
                },
            )
            return {
                "source_family": "terrain_abnormal",
                "candidate_site_count": int(candidate_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select
                    site.id as canonical_site_id,
                    bool_or(layer.source_family = 'coal_authority') as mining_constraint_present,
                    bool_or(layer.source_family = 'sepa_flood') as flood_constraint_present,
                    bool_or(layer.source_family = 'culverts') as culvert_constraint_present,
                    count(summary.id)::integer as measured_constraint_count,
                    existing.source_record_signature as previous_signature,
                    md5(concat_ws(
                        '|',
                        site.id::text,
                        bool_or(layer.source_family = 'coal_authority')::text,
                        bool_or(layer.source_family = 'sepa_flood')::text,
                        bool_or(layer.source_family = 'culverts')::text,
                        count(summary.id)::text
                    )) as current_signature
                from public.site_constraint_group_summaries as summary
                join public.constraint_layer_registry as layer
                  on layer.id = summary.constraint_layer_id
                join landintel.canonical_sites as site
                  on site.id::text = summary.site_id
                left join landintel.site_ground_risk_context as existing
                  on existing.canonical_site_id = site.id
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                group by site.id, existing.source_record_signature, existing.updated_at
                order by existing.updated_at nulls first, site.id
                limit :batch_size
            ),
            upserted as (
                insert into landintel.site_ground_risk_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    boreholes_within_250m,
                    boreholes_within_500m,
                    boreholes_within_1km,
                    mining_constraint_present,
                    flood_constraint_present,
                    culvert_constraint_present,
                    measured_constraint_count,
                    abnormal_review_fact,
                    source_record_signature,
                    metadata,
                    measured_at,
                    updated_at
                )
                select
                    selected_sites.canonical_site_id,
                    'terrain_abnormal_context',
                    'terrain_abnormal',
                    null::integer,
                    null::integer,
                    null::integer,
                    selected_sites.mining_constraint_present,
                    selected_sites.flood_constraint_present,
                    selected_sites.culvert_constraint_present,
                    selected_sites.measured_constraint_count,
                    'abnormal_cost_review_required',
                    selected_sites.current_signature,
                    jsonb_build_object(
                        'phase2_abnormal_context', true,
                        'basis', 'measured_constraints',
                        'bgs_borehole_counts', 'pending_bounded_batch'
                    ),
                    now(),
                    now()
                from selected_sites
                on conflict (canonical_site_id) do update set
                    mining_constraint_present = excluded.mining_constraint_present,
                    flood_constraint_present = excluded.flood_constraint_present,
                    culvert_constraint_present = excluded.culvert_constraint_present,
                    measured_constraint_count = excluded.measured_constraint_count,
                    abnormal_review_fact = excluded.abnormal_review_fact,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    measured_at = now(),
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join selected_sites
                  on selected_sites.canonical_site_id = upserted.canonical_site_id
                where selected_sites.previous_signature is distinct from selected_sites.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'terrain_abnormal'
                  and evidence.metadata ->> 'phase2_abnormal_context' = 'true'
                returning evidence.id
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
                    changed.canonical_site_id,
                    'terrain_abnormal',
                    'Terrain and abnormal-risk derived context',
                    changed.id::text,
                    changed.abnormal_review_fact,
                    'medium',
                    jsonb_build_object(
                        'source_key', 'terrain_abnormal_context',
                        'phase2_abnormal_context', true,
                        'mining_constraint_present', changed.mining_constraint_present,
                        'flood_constraint_present', changed.flood_constraint_present,
                        'culvert_constraint_present', changed.culvert_constraint_present,
                        'measured_constraint_count', changed.measured_constraint_count
                    )
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'terrain_abnormal'
                  and signal.metadata ->> 'phase2_abnormal_context' = 'true'
                returning signal.id
            ),
            inserted_signals as (
                insert into landintel.site_signals (
                    canonical_site_id,
                    signal_key,
                    signal_value,
                    signal_status,
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
                    changed.canonical_site_id,
                    'terrain_abnormal:desktop_context:' || changed.canonical_site_id::text,
                    jsonb_build_object(
                        'mining_constraint_present', changed.mining_constraint_present,
                        'flood_constraint_present', changed.flood_constraint_present,
                        'culvert_constraint_present', changed.culvert_constraint_present,
                        'measured_constraint_count', changed.measured_constraint_count
                    ),
                    'derived',
                    'abnormal_risk',
                    'desktop_abnormal_context',
                    changed.abnormal_review_fact,
                    changed.measured_constraint_count,
                    0.65,
                    'terrain_abnormal',
                    changed.id::text,
                    'abnormal_risk_context',
                    jsonb_build_object('source', 'measured_constraints'),
                    jsonb_build_object('source_key', 'terrain_abnormal_context', 'phase2_abnormal_context', true),
                    true
                from changed
                returning id
            ),
            upserted_flags as (
                insert into landintel.site_abnormal_cost_flags (
                    canonical_site_id,
                    source_key,
                    source_family,
                    flag_key,
                    flag_label,
                    flag_value_text,
                    confidence,
                    evidence_basis,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    changed.canonical_site_id,
                    'terrain_abnormal_context',
                    'terrain_abnormal',
                    'desktop_abnormal_review',
                    'Potential abnormal cost review recommended',
                    changed.abnormal_review_fact,
                    0.65,
                    'measured_constraints',
                    changed.source_record_signature,
                    jsonb_build_object('source_key', 'terrain_abnormal_context', 'phase2_abnormal_context', true),
                    now()
                from changed
                on conflict (canonical_site_id, flag_key) do update set
                    flag_label = excluded.flag_label,
                    flag_value_text = excluded.flag_value_text,
                    confidence = excluded.confidence,
                    evidence_basis = excluded.evidence_basis,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning id
            ),
            inserted_events as (
                insert into landintel.site_change_events (
                    canonical_site_id,
                    source_family,
                    source_record_id,
                    change_type,
                    change_summary,
                    previous_signature,
                    current_signature,
                    triggered_refresh,
                    metadata
                )
                select
                    changed.canonical_site_id,
                    'terrain_abnormal',
                    changed.id::text,
                    'abnormal_risk_context_changed',
                    'Desktop abnormal-risk evidence changed for canonical site.',
                    selected_sites.previous_signature,
                    selected_sites.current_signature,
                    true,
                    jsonb_build_object('phase2_abnormal_context', true)
                from changed
                join selected_sites on selected_sites.canonical_site_id = changed.canonical_site_id
                returning id
            )
            select
                (select count(*)::integer from selected_sites) as selected_site_count,
                (select count(*)::integer from upserted) as upserted_row_count,
                (select count(*)::integer from changed) as changed_row_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count,
                (select count(*)::integer from upserted_flags) as abnormal_flag_count,
                (select count(*)::integer from inserted_events) as change_event_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="terrain_abnormal",
            source_key="terrain_abnormal_context",
            row_count=int(proof.get("upserted_row_count") or 0),
            linked_count=int(proof.get("upserted_row_count") or 0),
            measured_count=int(proof.get("upserted_row_count") or 0),
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="refresh-site-abnormal-risk",
            source_key="terrain_abnormal_context",
            source_family="terrain_abnormal",
            status="success",
            raw_rows=int(proof.get("upserted_row_count") or 0),
            linked_rows=int(proof.get("upserted_row_count") or 0),
            measured_rows=int(proof.get("upserted_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            change_event_rows=int(proof.get("change_event_count") or 0),
            summary="Desktop abnormal-risk context refreshed from measured constraint evidence.",
            metadata={"batch_size": self.batch_size, "authority_filter": self.authority_filter},
        )
        self._record_family_freshness(
            "terrain_abnormal",
            "Terrain and abnormal-risk derived context",
            int(proof.get("upserted_row_count") or 0),
        )
        return {"source_family": "terrain_abnormal", **proof}

    def audit_full_source_estate(self, log_event: bool = True) -> dict[str, Any]:
        matrix_summary = self.database.fetch_one(
            """
            select
                count(*)::integer as source_count,
                count(*) filter (where programme_phase = 'phase_two')::integer as phase2_source_count,
                coalesce(sum(row_count), 0)::bigint as row_count,
                coalesce(sum(linked_site_count), 0)::bigint as linked_site_count,
                coalesce(sum(measured_site_count), 0)::bigint as measured_site_count,
                coalesce(sum(evidence_count), 0)::bigint as evidence_count,
                coalesce(sum(signal_count), 0)::bigint as signal_count,
                count(*) filter (where trusted_for_review)::integer as trusted_source_count
            from analytics.v_landintel_source_estate_matrix
            """
        ) or {}
        lifecycle_counts = self.database.fetch_all(
            """
            select stage_name, source_count
            from analytics.v_landintel_source_lifecycle_stage_counts
            order by array_position(array[
                'source_registered',
                'access_confirmed',
                'raw_data_landed',
                'normalised',
                'linked_to_site',
                'measured',
                'evidence_generated',
                'signals_generated',
                'assessment_ready',
                'trusted_for_review'
            ]::text[], stage_name)
            """
        )
        matrix_rows = self.database.fetch_all(
            """
            select
                source_family,
                source_name,
                current_lifecycle_stage,
                access_status,
                ingest_status,
                row_count,
                linked_site_count,
                measured_site_count,
                evidence_count,
                signal_count,
                trusted_for_review,
                limitation_notes,
                next_action
            from analytics.v_landintel_source_estate_matrix
            where programme_phase = 'phase_two'
            order by source_family, source_name
            limit 50
            """
        )
        result = {
            "source_count": int(matrix_summary.get("source_count") or 0),
            "phase2_source_count": int(matrix_summary.get("phase2_source_count") or 0),
            "row_count": int(matrix_summary.get("row_count") or 0),
            "linked_site_count": int(matrix_summary.get("linked_site_count") or 0),
            "measured_site_count": int(matrix_summary.get("measured_site_count") or 0),
            "evidence_count": int(matrix_summary.get("evidence_count") or 0),
            "signal_count": int(matrix_summary.get("signal_count") or 0),
            "trusted_source_count": int(matrix_summary.get("trusted_source_count") or 0),
            "lifecycle_counts": lifecycle_counts,
            "sample_sources": matrix_rows,
        }
        if log_event and not self.dry_run and not self.audit_only:
            self._record_expansion_event(
                command_name="audit-full-source-estate",
                source_key="phase2_source_estate",
                source_family="source_estate",
                status="success",
                raw_rows=result["row_count"],
                linked_rows=result["linked_site_count"],
                measured_rows=result["measured_site_count"],
                evidence_rows=result["evidence_count"],
                signal_rows=result["signal_count"],
                summary="Full source estate audit completed.",
                metadata={
                    "phase2_source_count": result["phase2_source_count"],
                    "trusted_source_count": result["trusted_source_count"],
                    "lifecycle_counts": lifecycle_counts,
                },
            )
        self.logger.info("full_source_estate_audit", extra=result)
        return result

    def _upsert_source(self, source: dict[str, Any]) -> None:
        self.database.execute(
            """
            insert into landintel.source_estate_registry (
                source_key,
                source_family,
                source_name,
                source_group,
                phase_one_role,
                source_status,
                orchestration_mode,
                endpoint_url,
                auth_env_vars,
                target_table,
                evidence_path,
                signal_output,
                ranking_impact,
                resurfacing_trigger,
                data_age_basis,
                notes,
                ranking_eligible,
                review_output_eligible,
                programme_phase,
                module_key,
                geography,
                access_status,
                ingest_status,
                normalisation_status,
                site_link_status,
                measurement_status,
                evidence_status,
                signal_status,
                assessment_status,
                trusted_for_review,
                limitation_notes,
                next_action,
                lifecycle_metadata,
                metadata,
                last_registered_at,
                updated_at
            ) values (
                :source_key,
                :source_family,
                :source_name,
                :source_group,
                'phase_two_context',
                :source_status,
                :orchestration_mode,
                :endpoint_url,
                :auth_env_vars,
                :target_table,
                :evidence_path,
                :signal_output,
                :ranking_impact,
                :resurfacing_trigger,
                :data_age_basis,
                :notes,
                false,
                true,
                'phase_two',
                :module_key,
                :geography,
                :access_status,
                :ingest_status,
                :normalisation_status,
                :site_link_status,
                :measurement_status,
                :evidence_status,
                :signal_status,
                :assessment_status,
                false,
                :limitation_notes,
                :next_action,
                cast(:lifecycle_metadata as jsonb),
                cast(:metadata as jsonb),
                now(),
                now()
            )
            on conflict (source_key) do update set
                source_family = excluded.source_family,
                source_name = excluded.source_name,
                source_group = excluded.source_group,
                source_status = excluded.source_status,
                orchestration_mode = excluded.orchestration_mode,
                endpoint_url = excluded.endpoint_url,
                auth_env_vars = excluded.auth_env_vars,
                target_table = excluded.target_table,
                evidence_path = excluded.evidence_path,
                signal_output = excluded.signal_output,
                ranking_impact = excluded.ranking_impact,
                resurfacing_trigger = excluded.resurfacing_trigger,
                data_age_basis = excluded.data_age_basis,
                notes = excluded.notes,
                programme_phase = excluded.programme_phase,
                module_key = excluded.module_key,
                geography = excluded.geography,
                access_status = excluded.access_status,
                ingest_status = excluded.ingest_status,
                normalisation_status = excluded.normalisation_status,
                site_link_status = excluded.site_link_status,
                measurement_status = excluded.measurement_status,
                evidence_status = excluded.evidence_status,
                signal_status = excluded.signal_status,
                assessment_status = excluded.assessment_status,
                limitation_notes = excluded.limitation_notes,
                next_action = excluded.next_action,
                lifecycle_metadata = excluded.lifecycle_metadata,
                metadata = excluded.metadata,
                last_registered_at = now(),
                updated_at = now()
            """,
            {
                "source_key": source["source_key"],
                "source_family": source["source_family"],
                "source_name": source["source_name"],
                "source_group": source.get("source_group") or "phase_two",
                "source_status": source.get("ingest_status") or "source_registered",
                "orchestration_mode": source.get("orchestration_mode") or "unknown",
                "endpoint_url": source.get("endpoint_url"),
                "auth_env_vars": source.get("auth_env_vars") or [],
                "target_table": source.get("target_table"),
                "evidence_path": source.get("evidence_path") or "phase2_source_estate_matrix",
                "signal_output": source.get("signal_output") or "site_signals_when_rows_are_proven",
                "ranking_impact": "not_ranked_until_trusted_for_review",
                "resurfacing_trigger": "site_change_events_when_evidence_state_changes",
                "data_age_basis": source.get("refresh_cadence"),
                "notes": source.get("limitation_notes"),
                "module_key": source.get("module_key"),
                "geography": source.get("geography"),
                "access_status": source.get("access_status") or "source_registered",
                "ingest_status": source.get("ingest_status") or "source_registered",
                "normalisation_status": source.get("normalisation_status") or "source_registered",
                "site_link_status": source.get("site_link_status") or "source_registered",
                "measurement_status": source.get("measurement_status") or "source_registered",
                "evidence_status": source.get("evidence_status") or "source_registered",
                "signal_status": source.get("signal_status") or "source_registered",
                "assessment_status": source.get("assessment_status") or "source_registered",
                "limitation_notes": source.get("limitation_notes"),
                "next_action": source.get("next_action"),
                "lifecycle_metadata": _json_dumps(
                    {
                        "register_name": self.manifest.get("register_name"),
                        "lifecycle_stages": self.manifest.get("lifecycle_stages", []),
                    }
                ),
                "metadata": _json_dumps(source),
            },
        )

    def _record_freshness(self, source: dict[str, Any], records_observed: int) -> None:
        freshness_status = source.get("ingest_status") or "source_registered"
        if records_observed > 0:
            freshness_status = "raw_data_landed"
        live_access_status = source.get("access_status") or "source_registered"
        self.database.execute(
            """
            insert into landintel.source_freshness_states (
                source_scope_key,
                source_family,
                source_dataset,
                source_name,
                source_access_mode,
                source_url,
                refresh_cadence,
                max_staleness_days,
                source_observed_at,
                last_checked_at,
                last_success_at,
                next_refresh_due_at,
                freshness_status,
                live_access_status,
                ranking_eligible,
                review_output_eligible,
                stale_reason_code,
                check_summary,
                records_observed,
                metadata,
                updated_at
            ) values (
                :source_scope_key,
                :source_family,
                :source_dataset,
                :source_name,
                :source_access_mode,
                :source_url,
                :refresh_cadence,
                :max_staleness_days,
                now(),
                now(),
                :last_success_at,
                now() + make_interval(days => :max_staleness_days),
                :freshness_status,
                :live_access_status,
                false,
                true,
                :stale_reason_code,
                :check_summary,
                :records_observed,
                cast(:metadata as jsonb),
                now()
            )
            on conflict (source_scope_key) do update set
                source_family = excluded.source_family,
                source_dataset = excluded.source_dataset,
                source_name = excluded.source_name,
                source_access_mode = excluded.source_access_mode,
                source_url = excluded.source_url,
                refresh_cadence = excluded.refresh_cadence,
                max_staleness_days = excluded.max_staleness_days,
                source_observed_at = excluded.source_observed_at,
                last_checked_at = excluded.last_checked_at,
                last_success_at = excluded.last_success_at,
                next_refresh_due_at = excluded.next_refresh_due_at,
                freshness_status = excluded.freshness_status,
                live_access_status = excluded.live_access_status,
                stale_reason_code = excluded.stale_reason_code,
                check_summary = excluded.check_summary,
                records_observed = excluded.records_observed,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            {
                "source_scope_key": f"phase2:{source['source_key']}",
                "source_family": source["source_family"],
                "source_dataset": source["source_name"],
                "source_name": source["source_name"],
                "source_access_mode": source.get("orchestration_mode") or "unknown",
                "source_url": source.get("endpoint_url"),
                "refresh_cadence": source.get("refresh_cadence") or "monthly",
                "max_staleness_days": int(source.get("max_staleness_days") or 30),
                "last_success_at": datetime.now(timezone.utc) if records_observed > 0 else None,
                "freshness_status": freshness_status,
                "live_access_status": live_access_status,
                "stale_reason_code": None if records_observed > 0 else live_access_status,
                "check_summary": source.get("limitation_notes") or source.get("next_action") or "Phase 2 source registered.",
                "records_observed": records_observed,
                "metadata": _json_dumps(
                    {
                        "module_key": source.get("module_key"),
                        "next_action": source.get("next_action"),
                    }
                ),
            },
        )

    def _record_family_freshness(self, source_family: str, source_name: str, records_observed: int) -> None:
        source = {
            "source_key": f"{source_family}_refresh",
            "source_family": source_family,
            "source_name": source_name,
            "orchestration_mode": "phase2_refresh",
            "refresh_cadence": "daily",
            "max_staleness_days": 1,
            "access_status": "access_confirmed",
            "ingest_status": "raw_data_landed" if records_observed > 0 else "source_registered_no_rows",
            "limitation_notes": "Refresh completed from existing LandIntel evidence.",
        }
        self._record_freshness(source, records_observed=records_observed)

    def _update_family_lifecycle(
        self,
        *,
        source_family: str,
        source_key: str | None = None,
        row_count: int,
        linked_count: int,
        measured_count: int,
        evidence_count: int,
        signal_count: int,
    ) -> None:
        self.database.execute(
            """
            update landintel.source_estate_registry
            set ingest_status = case when :row_count > 0 then 'raw_data_landed' else ingest_status end,
                normalisation_status = case when :row_count > 0 then 'normalised' else normalisation_status end,
                site_link_status = case when :linked_count > 0 then 'linked_to_site' else site_link_status end,
                measurement_status = case when :measured_count > 0 then 'measured' else measurement_status end,
                evidence_status = case when :evidence_count > 0 then 'evidence_generated' else evidence_status end,
                signal_status = case when :signal_count > 0 then 'signals_generated' else signal_status end,
                trusted_for_review = false,
                updated_at = now()
            where source_family = :source_family
              and programme_phase = 'phase_two'
              and (:source_key = '' or source_key = :source_key)
            """,
            {
                "source_family": source_family,
                "source_key": source_key or "",
                "row_count": row_count,
                "linked_count": linked_count,
                "measured_count": measured_count,
                "evidence_count": evidence_count,
                "signal_count": signal_count,
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
                command_name,
                source_key,
                source_family,
                status,
                raw_rows,
                linked_rows,
                measured_rows,
                evidence_rows,
                signal_rows,
                change_event_rows,
                summary,
                metadata
            ) values (
                :command_name,
                :source_key,
                :source_family,
                :status,
                :raw_rows,
                :linked_rows,
                :measured_rows,
                :evidence_rows,
                :signal_rows,
                :change_event_rows,
                :summary,
                cast(:metadata as jsonb)
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 2 LandIntel source estate commands.")
    parser.add_argument("command", choices=PHASE2_COMMANDS)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()
    runner = Phase2SourceRunner(settings)
    try:
        runner.run_command(args.command)
        return 0
    except Exception as exc:
        runner.logger.error(
            "phase2_source_command_failed",
            extra={"command": args.command, "exception": str(exc), "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

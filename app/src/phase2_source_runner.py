"""Phase 2 source-estate registration, refresh commands and proof audit."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml

from config.settings import Settings, get_settings
from src.db import Database
from src.logging_config import configure_logging


CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "phase2_source_estate.yaml"
DEFAULT_UK_HPI_AVERAGE_PRICE_URL = (
    "https://publicdata.landregistry.gov.uk/market-trend-data/house-price-index-data/"
    "Average-prices-2026-01.csv"
)
DEFAULT_SIMD_URL = (
    "https://www.opendata.nhs.scot/dataset/78d41fa9-1a62-4f7b-9edb-3e8522a93378/"
    "resource/acade396-8430-4b34-895a-b3e757fa346e/download/simd2020v2_22062020.csv"
)
DEFAULT_NAPTAN_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"
DEFAULT_SPEN_METADATA_URL = (
    "https://spenergynetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "metadata-catalogue/records?limit=100"
)

PHASE2_COMMANDS = (
    "discover-phase2-sources",
    "ingest-planning-appeals",
    "ingest-power-infrastructure",
    "ingest-amenities",
    "ingest-demographics",
    "ingest-market-context",
    "ingest-planning-documents",
    "ingest-intelligence-events",
    "ingest-companies-house",
    "ingest-fca-entities",
    "refresh-title-reviews",
    "refresh-planning-decisions",
    "audit-planning-decisions",
    "refresh-title-readiness",
    "refresh-site-market-context",
    "refresh-site-amenity-context",
    "refresh-site-demographic-context",
    "refresh-site-power-context",
    "refresh-site-abnormal-risk",
    "refresh-site-assessments",
    "refresh-site-prove-it-assessments",
    "audit-site-prove-it-assessments",
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
    "refresh-site-assessments": "site_assessment",
    "refresh-site-prove-it-assessments": "site_conviction",
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


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text_value = str(value).replace(",", "").strip()
    if not text_value:
        return None
    try:
        return float(text_value)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    number_value = _to_float(value)
    if number_value is None:
        return None
    return int(number_value)


def _normalise_area_name(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _is_scotland_lat_lon(latitude: float | None, longitude: float | None) -> bool:
    if latitude is None or longitude is None:
        return False
    return 54.3 <= latitude <= 61.5 and -8.8 <= longitude <= -0.5


def _naptan_amenity_type(stop_type: str | None) -> str:
    stop_type = (stop_type or "").strip().upper()
    if stop_type in {"RLY", "RSE", "MET", "TMU"}:
        return "rail_station"
    if stop_type in {"AIR"}:
        return "airport"
    if stop_type in {"FER", "FBT"}:
        return "ferry_terminal"
    if stop_type in {"BCT", "BCS", "BST", "BCQ"}:
        return "bus_stop"
    return "public_transport_stop"


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
            if command == "ingest-planning-appeals":
                result = self.ingest_planning_appeals()
            elif command == "ingest-power-infrastructure":
                result = self.ingest_power_infrastructure()
            elif command == "ingest-planning-documents":
                result = self.ingest_planning_documents()
            elif command == "ingest-amenities":
                result = self.ingest_amenities()
            elif command == "ingest-demographics":
                result = self.ingest_demographics()
            elif command == "ingest-market-context":
                result = self.ingest_market_context()
            elif command == "ingest-intelligence-events":
                result = self.ingest_intelligence_events()
            else:
                result = self.ingest_registered_module(command, INGEST_COMMAND_MODULES[command])
        elif command == "ingest-companies-house":
            result = self.ingest_companies_house()
        elif command == "ingest-fca-entities":
            result = self.ingest_fca_entities()
        elif command == "refresh-title-reviews":
            result = self.refresh_title_reviews()
        elif command == "refresh-planning-decisions":
            result = self.refresh_planning_decisions()
        elif command == "audit-planning-decisions":
            result = self.audit_planning_decisions()
        elif command == "refresh-title-readiness":
            result = self.refresh_title_readiness()
        elif command == "refresh-site-abnormal-risk":
            result = self.refresh_site_abnormal_risk()
        elif command == "refresh-site-assessments":
            result = self.refresh_site_assessments()
        elif command == "refresh-site-prove-it-assessments":
            result = self.refresh_site_prove_it_assessments()
        elif command == "audit-site-prove-it-assessments":
            result = self.audit_site_prove_it_assessments()
        elif command == "refresh-site-market-context":
            result = self.ingest_market_context()
        elif command == "refresh-site-amenity-context":
            result = self.ingest_amenities()
        elif command == "refresh-site-demographic-context":
            result = self.ingest_demographics()
        elif command == "refresh-site-power-context":
            result = self.ingest_power_infrastructure()
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

    def refresh_planning_decisions(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="planning_decisions")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            candidate_count = self.database.scalar(
                """
                select count(*)::bigint
                from landintel.planning_application_records as planning
                where (:authority_name = '' or planning.authority_name ilike :authority_name_like)
                """,
                {
                    "authority_name": self.authority_filter,
                    "authority_name_like": f"%{self.authority_filter}%",
                },
            )
            return {
                "source_family": "planning_decisions",
                "candidate_record_count": int(candidate_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with selected_records as (
                select
                    planning.id as planning_application_record_id,
                    coalesce(planning.canonical_site_id, state_row.current_canonical_site_id) as canonical_site_id,
                    planning.source_record_id as planning_source_record_id,
                    planning.authority_name,
                    planning.planning_reference,
                    planning.application_status,
                    planning.decision as decision_raw,
                    planning.decision_date,
                    planning.proposal_text,
                    planning.refusal_themes,
                    planning.raw_payload,
                    existing.source_record_signature as previous_signature,
                    md5(concat_ws(
                        '|',
                        planning.id::text,
                        coalesce(planning.application_status, ''),
                        coalesce(planning.decision, ''),
                        coalesce(planning.decision_date::text, ''),
                        coalesce(planning.proposal_text, '')
                    )) as current_signature
                from landintel.planning_application_records as planning
                left join landintel.source_reconcile_state as state_row
                  on state_row.source_family = 'planning'
                 and state_row.authority_name = planning.authority_name
                 and state_row.source_record_id = planning.source_record_id
                 and state_row.active_flag = true
                 and state_row.publish_state in ('published', 'provisional')
                left join landintel.planning_decision_facts as existing
                  on existing.planning_application_record_id = planning.id
                where (:authority_name = '' or planning.authority_name ilike :authority_name_like)
                  and existing.source_record_signature is distinct from md5(concat_ws(
                        '|',
                        planning.id::text,
                        coalesce(planning.application_status, ''),
                        coalesce(planning.decision, ''),
                        coalesce(planning.decision_date::text, ''),
                        coalesce(planning.proposal_text, '')
                  ))
                order by
                    (coalesce(planning.canonical_site_id, state_row.current_canonical_site_id) is not null) desc,
                    (existing.source_record_signature is null) desc,
                    planning.decision_date desc nulls last,
                    planning.id
                limit :batch_size
            ),
            prepared as (
                select
                    *,
                    case
                        when lower(coalesce(decision_raw, application_status, '')) like '%%withdraw%%' then 'withdrawn'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%refus%%' then 'refused'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%reject%%' then 'refused'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%approv%%' then 'approved'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%grant%%' then 'approved'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%permi%%' then 'approved'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%pending%%' then 'live'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%valid%%' then 'live'
                        when lower(coalesce(decision_raw, application_status, '')) like '%%live%%' then 'live'
                        else 'decision_unknown'
                    end as decision_status
                from selected_records
            ),
            upserted_facts as (
                insert into landintel.planning_decision_facts (
                    canonical_site_id,
                    planning_application_record_id,
                    source_key,
                    source_family,
                    source_record_id,
                    authority_name,
                    planning_reference,
                    application_status,
                    decision_raw,
                    decision_status,
                    decision_date,
                    proposal_text,
                    refusal_themes,
                    event_type,
                    source_record_signature,
                    raw_payload,
                    updated_at
                )
                select
                    prepared.canonical_site_id,
                    prepared.planning_application_record_id,
                    'planning_decision_engine',
                    'planning_decisions',
                    'planning_decision:' || prepared.planning_application_record_id::text,
                    prepared.authority_name,
                    prepared.planning_reference,
                    prepared.application_status,
                    prepared.decision_raw,
                    prepared.decision_status,
                    prepared.decision_date,
                    prepared.proposal_text,
                    prepared.refusal_themes,
                    case
                        when prepared.decision_status in ('approved', 'refused', 'withdrawn') then 'planning_decision_recorded'
                        when prepared.decision_status = 'live' then 'planning_application_live'
                        else 'planning_status_recorded'
                    end,
                    prepared.current_signature,
                    prepared.raw_payload,
                    now()
                from prepared
                on conflict (source_key, source_record_id) do update set
                    canonical_site_id = excluded.canonical_site_id,
                    authority_name = excluded.authority_name,
                    planning_reference = excluded.planning_reference,
                    application_status = excluded.application_status,
                    decision_raw = excluded.decision_raw,
                    decision_status = excluded.decision_status,
                    decision_date = excluded.decision_date,
                    proposal_text = excluded.proposal_text,
                    refusal_themes = excluded.refusal_themes,
                    event_type = excluded.event_type,
                    source_record_signature = excluded.source_record_signature,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                returning *
            ),
            changed_facts as (
                select upserted_facts.*
                from upserted_facts
                join prepared
                  on prepared.planning_application_record_id = upserted_facts.planning_application_record_id
                where prepared.previous_signature is distinct from prepared.current_signature
            ),
            context_source_all as (
                select
                    facts.canonical_site_id,
                    (array_agg(facts.planning_reference order by facts.decision_date desc nulls last, facts.updated_at desc))[1] as latest_planning_reference,
                    (array_agg(facts.decision_status order by facts.decision_date desc nulls last, facts.updated_at desc))[1] as latest_decision_status,
                    max(facts.decision_date) as latest_decision_date,
                    count(*) filter (where facts.decision_status = 'approved')::integer as approved_count,
                    count(*) filter (where facts.decision_status = 'refused')::integer as refused_count,
                    count(*) filter (where facts.decision_status = 'withdrawn')::integer as withdrawn_count,
                    count(*) filter (where facts.decision_status = 'live')::integer as live_count,
                    count(*)::integer as decision_record_count,
                    existing.source_record_signature as previous_signature,
                    md5(concat_ws(
                        '|',
                        facts.canonical_site_id::text,
                        count(*)::text,
                        count(*) filter (where facts.decision_status = 'approved')::text,
                        count(*) filter (where facts.decision_status = 'refused')::text,
                        count(*) filter (where facts.decision_status = 'withdrawn')::text,
                        count(*) filter (where facts.decision_status = 'live')::text,
                        coalesce(max(facts.decision_date)::text, '')
                    )) as current_signature
                from landintel.planning_decision_facts as facts
                left join landintel.site_planning_decision_context as existing
                  on existing.canonical_site_id = facts.canonical_site_id
                where facts.canonical_site_id is not null
                group by facts.canonical_site_id, existing.source_record_signature
            ),
            context_source as (
                select *
                from context_source_all
                where previous_signature is distinct from current_signature
                order by latest_decision_date desc nulls last, canonical_site_id
                limit :batch_size
            ),
            upserted_context as (
                insert into landintel.site_planning_decision_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    latest_planning_reference,
                    latest_decision_status,
                    latest_decision_date,
                    approved_count,
                    refused_count,
                    withdrawn_count,
                    live_count,
                    decision_record_count,
                    planning_decision_summary,
                    source_record_signature,
                    metadata,
                    measured_at,
                    updated_at
                )
                select
                    context_source.canonical_site_id,
                    'planning_decision_engine',
                    'planning_decisions',
                    context_source.latest_planning_reference,
                    context_source.latest_decision_status,
                    context_source.latest_decision_date,
                    context_source.approved_count,
                    context_source.refused_count,
                    context_source.withdrawn_count,
                    context_source.live_count,
                    context_source.decision_record_count,
                    'Planning decision evidence recorded',
                    context_source.current_signature,
                    jsonb_build_object('source_key', 'planning_decision_engine'),
                    now(),
                    now()
                from context_source
                on conflict (canonical_site_id) do update set
                    latest_planning_reference = excluded.latest_planning_reference,
                    latest_decision_status = excluded.latest_decision_status,
                    latest_decision_date = excluded.latest_decision_date,
                    approved_count = excluded.approved_count,
                    refused_count = excluded.refused_count,
                    withdrawn_count = excluded.withdrawn_count,
                    live_count = excluded.live_count,
                    decision_record_count = excluded.decision_record_count,
                    planning_decision_summary = excluded.planning_decision_summary,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    measured_at = now(),
                    updated_at = now()
                returning *
            ),
            changed_context as (
                select upserted_context.*
                from upserted_context
                join context_source on context_source.canonical_site_id = upserted_context.canonical_site_id
                where context_source.previous_signature is distinct from context_source.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed_context
                where evidence.canonical_site_id = changed_context.canonical_site_id
                  and evidence.source_family = 'planning_decisions'
                  and evidence.metadata ->> 'source_key' = 'planning_decision_engine'
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
                    changed_context.canonical_site_id,
                    'planning_decisions',
                    'Planning decision and event engine',
                    changed_context.id::text,
                    changed_context.latest_planning_reference,
                    'medium',
                    jsonb_build_object(
                        'source_key', 'planning_decision_engine',
                        'latest_decision_status', changed_context.latest_decision_status,
                        'approved_count', changed_context.approved_count,
                        'refused_count', changed_context.refused_count,
                        'withdrawn_count', changed_context.withdrawn_count,
                        'live_count', changed_context.live_count
                    )
                from changed_context
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed_context
                where signal.canonical_site_id = changed_context.canonical_site_id
                  and signal.source_family = 'planning_decisions'
                  and signal.metadata ->> 'source_key' = 'planning_decision_engine'
                returning signal.id
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
                    changed_context.canonical_site_id,
                    'planning',
                    'planning_decision_context',
                    changed_context.latest_decision_status,
                    changed_context.decision_record_count,
                    0.75,
                    'planning_decisions',
                    changed_context.id::text,
                    'planning_decision_evidence',
                    jsonb_build_object('source', 'planning_decision_engine'),
                    jsonb_build_object('source_key', 'planning_decision_engine'),
                    true
                from changed_context
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
                    changed_context.canonical_site_id,
                    'planning_decisions',
                    changed_context.id::text,
                    'planning_decision_context_changed',
                    'Planning decision evidence changed for canonical site.',
                    context_source.previous_signature,
                    context_source.current_signature,
                    true,
                    jsonb_build_object('source_key', 'planning_decision_engine')
                from changed_context
                join context_source on context_source.canonical_site_id = changed_context.canonical_site_id
                returning id
            )
            select
                (select count(*)::integer from selected_records) as selected_record_count,
                (select count(*)::integer from upserted_facts) as upserted_fact_count,
                (select count(*)::integer from changed_facts) as changed_fact_count,
                (select count(*)::integer from upserted_context) as context_row_count,
                (select count(*)::integer from changed_context) as changed_context_count,
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
            source_family="planning_decisions",
            source_key="planning_decision_engine",
            row_count=int(proof.get("upserted_fact_count") or 0),
            linked_count=int(proof.get("context_row_count") or 0),
            measured_count=int(proof.get("context_row_count") or 0),
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="refresh-planning-decisions",
            source_key="planning_decision_engine",
            source_family="planning_decisions",
            status="success",
            raw_rows=int(proof.get("upserted_fact_count") or 0),
            linked_rows=int(proof.get("context_row_count") or 0),
            measured_rows=int(proof.get("context_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            change_event_rows=int(proof.get("change_event_count") or 0),
            summary="Planning decision facts refreshed from live planning application records.",
            metadata={"batch_size": self.batch_size, "authority_filter": self.authority_filter},
        )
        self._record_family_freshness(
            "planning_decisions",
            "Planning decision and event engine",
            int(proof.get("upserted_fact_count") or 0),
        )
        return {"source_family": "planning_decisions", **proof}

    def audit_planning_decisions(self) -> dict[str, Any]:
        proof = self.database.fetch_one(
            """
            select
                (select count(*)::integer from landintel.planning_application_records) as planning_record_count,
                (
                    select count(*)::integer
                    from landintel.source_reconcile_state as state_row
                    where state_row.source_family = 'planning'
                      and state_row.active_flag = true
                      and state_row.publish_state in ('published', 'provisional')
                      and state_row.current_canonical_site_id is not null
                ) as linked_planning_record_count,
                (select count(*)::integer from landintel.planning_decision_facts) as decision_fact_count,
                (select count(distinct canonical_site_id)::integer from landintel.planning_decision_facts where canonical_site_id is not null) as decision_site_count,
                (select count(*)::integer from landintel.site_planning_decision_context) as decision_context_count,
                (select count(*)::integer from landintel.evidence_references where source_family = 'planning_decisions') as evidence_count,
                (select count(*)::integer from landintel.site_signals where source_family = 'planning_decisions') as signal_count
            """
        ) or {}
        self.logger.info("planning_decision_audit", extra=proof)
        return {"source_family": "planning_decisions", **proof}

    def ingest_planning_appeals(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="planning_appeals")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            candidate_count = self.database.scalar(
                """
                select count(*)::bigint
                from landintel.source_reconcile_state as state_row
                join landintel.planning_application_records as planning
                  on planning.authority_name = state_row.authority_name
                 and planning.source_record_id = state_row.source_record_id
                where state_row.source_family = 'planning'
                  and state_row.active_flag = true
                  and state_row.publish_state in ('published', 'provisional')
                  and state_row.current_canonical_site_id is not null
                  and (
                        nullif(btrim(planning.appeal_status), '') is not null
                        or lower(coalesce(planning.raw_payload::text, '')) like '%%appeal%%'
                  )
                  and (:authority_name = '' or planning.authority_name ilike :authority_name_like)
                """,
                {
                    "authority_name": self.authority_filter,
                    "authority_name_like": f"%{self.authority_filter}%",
                },
            )
            return {
                "source_family": "planning_appeals",
                "candidate_record_count": int(candidate_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with linked_planning as (
                select
                    state_row.authority_name,
                    state_row.source_record_id,
                    state_row.current_canonical_site_id as canonical_site_id
                from landintel.source_reconcile_state as state_row
                where state_row.source_family = 'planning'
                  and state_row.active_flag = true
                  and state_row.publish_state in ('published', 'provisional')
                  and state_row.current_canonical_site_id is not null
                  and (:authority_name = '' or state_row.authority_name ilike :authority_name_like)
                order by state_row.updated_at desc nulls last, state_row.id
                limit :batch_size
            ),
            selected_records as (
                select
                    planning.id as planning_application_record_id,
                    coalesce(planning.canonical_site_id, linked_planning.canonical_site_id) as canonical_site_id,
                    planning.source_record_id,
                    planning.authority_name,
                    planning.planning_reference,
                    planning.proposal_text,
                    planning.appeal_status,
                    planning.decision,
                    planning.decision_date,
                    planning.geometry,
                    planning.raw_payload,
                    md5(concat_ws(
                        '|',
                        planning.id::text,
                        coalesce(planning.appeal_status, ''),
                        coalesce(planning.decision, ''),
                        coalesce(planning.decision_date::text, '')
                    )) as current_signature
                from linked_planning
                join landintel.planning_application_records as planning
                  on planning.authority_name = linked_planning.authority_name
                 and planning.source_record_id = linked_planning.source_record_id
                where coalesce(planning.canonical_site_id, linked_planning.canonical_site_id) is not null
                  and (
                        nullif(btrim(planning.appeal_status), '') is not null
                        or lower(coalesce(planning.raw_payload::text, '')) like '%%appeal%%'
                  )
                  and (:authority_name = '' or planning.authority_name ilike :authority_name_like)
                order by planning.updated_at desc nulls last, planning.id
                limit :batch_size
            ),
            upserted_appeals as (
                insert into landintel.planning_appeal_records (
                    source_key,
                    source_family,
                    source_record_id,
                    appeal_reference,
                    original_application_reference,
                    authority_name,
                    site_address,
                    decision,
                    decision_date,
                    reporter_reasoning,
                    policy_references,
                    geometry,
                    source_url,
                    source_record_signature,
                    raw_payload,
                    updated_at
                )
                select
                    'planning_application_appeal_signals',
                    'planning_appeals',
                    'planning_appeal_signal:' || selected_records.planning_application_record_id::text,
                    coalesce(selected_records.appeal_status, selected_records.planning_reference),
                    selected_records.planning_reference,
                    selected_records.authority_name,
                    selected_records.proposal_text,
                    selected_records.decision,
                    selected_records.decision_date,
                    null::text,
                    '{}'::text[],
                    selected_records.geometry,
                    null::text,
                    selected_records.current_signature,
                    selected_records.raw_payload || jsonb_build_object(
                        'source_limitation', 'appeal_status_signal_from_planning_record',
                        'planning_application_record_id', selected_records.planning_application_record_id
                    ),
                    now()
                from selected_records
                on conflict (source_key, source_record_id) do update set
                    appeal_reference = excluded.appeal_reference,
                    original_application_reference = excluded.original_application_reference,
                    authority_name = excluded.authority_name,
                    site_address = excluded.site_address,
                    decision = excluded.decision,
                    decision_date = excluded.decision_date,
                    geometry = excluded.geometry,
                    source_record_signature = excluded.source_record_signature,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                returning *
            ),
            linked as (
                insert into landintel.site_planning_appeal_links (
                    canonical_site_id,
                    planning_appeal_record_id,
                    source_family,
                    link_method,
                    link_confidence,
                    matched_reference,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    selected_records.canonical_site_id,
                    upserted_appeals.id,
                    'planning_appeals',
                    'planning_reference',
                    0.75,
                    selected_records.planning_reference,
                    selected_records.current_signature,
                    jsonb_build_object('source_key', 'planning_application_appeal_signals'),
                    now()
                from selected_records
                join upserted_appeals
                  on upserted_appeals.source_record_id = 'planning_appeal_signal:' || selected_records.planning_application_record_id::text
                on conflict (canonical_site_id, planning_appeal_record_id) do update set
                    link_method = excluded.link_method,
                    link_confidence = excluded.link_confidence,
                    matched_reference = excluded.matched_reference,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using linked
                where evidence.canonical_site_id = linked.canonical_site_id
                  and evidence.source_family = 'planning_appeals'
                  and evidence.metadata ->> 'source_key' = 'planning_application_appeal_signals'
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
                    linked.canonical_site_id,
                    'planning_appeals',
                    'Planning application appeal status signals',
                    linked.planning_appeal_record_id::text,
                    linked.matched_reference,
                    'medium',
                    jsonb_build_object('source_key', 'planning_application_appeal_signals')
                from linked
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using linked
                where signal.canonical_site_id = linked.canonical_site_id
                  and signal.source_family = 'planning_appeals'
                  and signal.metadata ->> 'source_key' = 'planning_application_appeal_signals'
                returning signal.id
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
                    linked.canonical_site_id,
                    'planning',
                    'appeal_status_signal',
                    'appeal_status_evidence_present',
                    1,
                    0.6,
                    'planning_appeals',
                    linked.planning_appeal_record_id::text,
                    'planning_appeal_signal',
                    jsonb_build_object('source', 'planning_application_records'),
                    jsonb_build_object('source_key', 'planning_application_appeal_signals'),
                    true
                from linked
                returning id
            )
            select
                (select count(*)::integer from selected_records) as selected_record_count,
                (select count(*)::integer from upserted_appeals) as appeal_record_count,
                (select count(*)::integer from linked) as linked_site_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="planning_appeals",
            source_key="planning_application_appeal_signals",
            row_count=int(proof.get("appeal_record_count") or 0),
            linked_count=int(proof.get("linked_site_count") or 0),
            measured_count=0,
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="ingest-planning-appeals",
            source_key="planning_application_appeal_signals",
            source_family="planning_appeals",
            status="success",
            raw_rows=int(proof.get("appeal_record_count") or 0),
            linked_rows=int(proof.get("linked_site_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            summary="Planning appeal status signals extracted from planning records where present.",
        )
        self._record_family_freshness(
            "planning_appeals",
            "Planning application appeal status signals",
            int(proof.get("appeal_record_count") or 0),
        )
        return {"source_family": "planning_appeals", **proof}

    def _fetch_uk_hpi_latest_scottish_authorities(self) -> dict[str, dict[str, Any]]:
        source_url = os.getenv("UK_HPI_AVERAGE_PRICE_URL") or DEFAULT_UK_HPI_AVERAGE_PRICE_URL
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            response = client.get(source_url)
            response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        latest: dict[str, dict[str, Any]] = {}
        for row in reader:
            area_code = str(row.get("Area_Code") or "").strip()
            area_name = str(row.get("Region_Name") or "").strip()
            period = str(row.get("Date") or "").strip()
            if not area_code.startswith("S12") or not area_name or not period:
                continue
            key = _normalise_area_name(area_name)
            if key not in latest or period > str(latest[key].get("Date") or ""):
                latest[key] = {**row, "source_url": source_url}
        return latest

    def ingest_market_context(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="market_context")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        hpi_rows = self._fetch_uk_hpi_latest_scottish_authorities()
        metric_rows: list[dict[str, Any]] = []
        for row in hpi_rows.values():
            period = str(row.get("Date") or "").strip()
            base = {
                "source_key": "uk_hpi_market_context",
                "source_family": "market_context",
                "area_code": row.get("Area_Code"),
                "area_name": row.get("Region_Name"),
                "authority_name": row.get("Region_Name"),
                "period_start": period,
                "period_end": period,
                "confidence": "official_statistic_area_context",
                "source_url": row.get("source_url"),
                "raw_payload": _json_dumps(row),
            }
            for metric_name, column_name, unit in (
                ("average_price", "Average_Price", "gbp"),
                ("monthly_change_pct", "Monthly_Change", "pct"),
                ("annual_change_pct", "Annual_Change", "pct"),
            ):
                metric_value = _to_float(row.get(column_name))
                if metric_value is None:
                    continue
                metric_rows.append(
                    {
                        **base,
                        "metric_name": metric_name,
                        "metric_value": metric_value,
                        "metric_unit": unit,
                        "source_record_signature": _stable_key(
                            f"{base['source_key']}|{base['area_code']}|{metric_name}|{period}|{metric_value}"
                        ),
                    }
                )

        if self.dry_run or self.audit_only:
            return {
                "source_family": "market_context",
                "candidate_area_count": len(hpi_rows),
                "candidate_metric_count": len(metric_rows),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        self.database.execute_many(
            """
            insert into landintel.market_area_metrics (
                source_key,
                source_family,
                area_code,
                area_name,
                authority_name,
                metric_name,
                metric_value,
                metric_unit,
                period_start,
                period_end,
                confidence,
                source_url,
                source_record_signature,
                raw_payload,
                updated_at
            ) values (
                :source_key,
                :source_family,
                :area_code,
                :area_name,
                :authority_name,
                :metric_name,
                :metric_value,
                :metric_unit,
                cast(:period_start as date),
                cast(:period_end as date),
                :confidence,
                :source_url,
                :source_record_signature,
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_key, area_code, metric_name, period_end) do update set
                area_name = excluded.area_name,
                authority_name = excluded.authority_name,
                metric_value = excluded.metric_value,
                metric_unit = excluded.metric_unit,
                confidence = excluded.confidence,
                source_url = excluded.source_url,
                source_record_signature = excluded.source_record_signature,
                raw_payload = excluded.raw_payload,
                updated_at = now()
            """,
            metric_rows,
        )

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select site.id, site.authority_name, existing.source_record_signature as previous_signature
                from landintel.canonical_sites as site
                left join landintel.site_market_context as existing
                  on existing.canonical_site_id = site.id
                where site.authority_name is not null
                  and (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by existing.updated_at nulls first, site.updated_at desc nulls last, site.id
                limit :batch_size
            ),
            latest_market as (
                select distinct on (lower(metric.authority_name))
                    lower(metric.authority_name) as authority_key,
                    metric.authority_name,
                    metric.area_code,
                    metric.area_name,
                    metric.metric_value as average_price,
                    metric.period_end,
                    metric.source_url
                from landintel.market_area_metrics as metric
                where metric.source_key = 'uk_hpi_market_context'
                  and metric.metric_name = 'average_price'
                order by lower(metric.authority_name), metric.period_end desc nulls last
            ),
            prepared as (
                select
                    selected_sites.id as canonical_site_id,
                    selected_sites.authority_name,
                    latest_market.area_code,
                    latest_market.area_name,
                    latest_market.average_price,
                    latest_market.period_end,
                    selected_sites.previous_signature,
                    md5(concat_ws(
                        '|',
                        selected_sites.id::text,
                        latest_market.area_code,
                        latest_market.period_end::text,
                        round(latest_market.average_price, 2)::text
                    )) as current_signature
                from selected_sites
                join latest_market
                  on latest_market.authority_key = lower(selected_sites.authority_name)
            ),
            upserted as (
                insert into landintel.site_market_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    authority_name,
                    market_confidence_tier,
                    evidence_summary,
                    latest_metric_period,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    prepared.canonical_site_id,
                    'uk_hpi_market_context',
                    'market_context',
                    prepared.authority_name,
                    'area_context_only',
                    'UK HPI local authority average price context present',
                    prepared.period_end,
                    prepared.current_signature,
                    jsonb_build_object(
                        'source_key', 'uk_hpi_market_context',
                        'area_code', prepared.area_code,
                        'area_name', prepared.area_name,
                        'average_price', prepared.average_price,
                        'source_limitation', 'area_level_market_context_not_site_value'
                    ),
                    now()
                from prepared
                on conflict (canonical_site_id) do update set
                    source_key = excluded.source_key,
                    source_family = excluded.source_family,
                    authority_name = excluded.authority_name,
                    market_confidence_tier = excluded.market_confidence_tier,
                    evidence_summary = excluded.evidence_summary,
                    latest_metric_period = excluded.latest_metric_period,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join prepared on prepared.canonical_site_id = upserted.canonical_site_id
                where prepared.previous_signature is distinct from prepared.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'market_context'
                  and evidence.metadata ->> 'source_key' = 'uk_hpi_market_context'
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
                    'market_context',
                    'UK House Price Index local market context',
                    changed.id::text,
                    changed.authority_name,
                    'medium',
                    jsonb_build_object('source_key', 'uk_hpi_market_context', 'latest_metric_period', changed.latest_metric_period)
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'market_context'
                  and signal.metadata ->> 'source_key' = 'uk_hpi_market_context'
                returning signal.id
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
                    changed.canonical_site_id,
                    'market_context',
                    'area_market_context',
                    changed.market_confidence_tier,
                    1,
                    0.55,
                    'market_context',
                    changed.id::text,
                    'uk_hpi_area_context',
                    jsonb_build_object('source', 'uk_hpi_market_context'),
                    jsonb_build_object('source_key', 'uk_hpi_market_context'),
                    true
                from changed
                returning id
            )
            select
                (select count(*)::integer from landintel.market_area_metrics where source_key = 'uk_hpi_market_context') as metric_row_count,
                (select count(*)::integer from upserted) as context_row_count,
                (select count(*)::integer from changed) as changed_context_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="market_context",
            source_key="uk_hpi_market_context",
            row_count=int(proof.get("metric_row_count") or 0),
            linked_count=int(proof.get("context_row_count") or 0),
            measured_count=0,
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="ingest-market-context",
            source_key="uk_hpi_market_context",
            source_family="market_context",
            status="success",
            raw_rows=int(proof.get("metric_row_count") or 0),
            linked_rows=int(proof.get("context_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            summary="UK HPI local authority market context refreshed as evidence context only.",
        )
        self._record_family_freshness("market_context", "UK House Price Index local market context", int(proof.get("metric_row_count") or 0))
        return {"source_family": "market_context", **proof}

    def ingest_demographics(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="demographics")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        authority_lookup = {
            str(row.get("Area_Code")): str(row.get("Region_Name"))
            for row in self._fetch_uk_hpi_latest_scottish_authorities().values()
            if row.get("Area_Code") and row.get("Region_Name")
        }
        simd_url = os.getenv("SIMD_2020_URL") or DEFAULT_SIMD_URL
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            response = client.get(simd_url)
            response.raise_for_status()
        aggregates: dict[str, dict[str, Any]] = {}
        for row in csv.DictReader(io.StringIO(response.text.lstrip("\ufeff"))):
            area_code = str(row.get("CA") or "").strip()
            if not area_code.startswith("S12"):
                continue
            aggregate = aggregates.setdefault(
                area_code,
                {
                    "area_code": area_code,
                    "area_name": authority_lookup.get(area_code, area_code),
                    "datazone_count": 0,
                    "rank_total": 0,
                    "most15_count": 0,
                    "least15_count": 0,
                    "decile_total": 0,
                },
            )
            aggregate["datazone_count"] += 1
            aggregate["rank_total"] += _to_int(row.get("SIMD2020V2Rank")) or 0
            aggregate["most15_count"] += _to_int(row.get("SIMD2020V2Most15pc")) or 0
            aggregate["least15_count"] += _to_int(row.get("SIMD2020V2Least15pc")) or 0
            aggregate["decile_total"] += _to_int(row.get("SIMD2020V2CountryDecile")) or 0

        metric_rows: list[dict[str, Any]] = []
        for aggregate in aggregates.values():
            count = max(int(aggregate["datazone_count"]), 1)
            base = {
                "source_key": "simd_demographic_context",
                "source_family": "demographics",
                "area_code": aggregate["area_code"],
                "area_name": aggregate["area_name"],
                "area_type": "council_area",
                "authority_name": aggregate["area_name"],
                "period_start": "2020-01-28",
                "period_end": "2020-06-22",
                "confidence": "official_area_context",
                "source_url": simd_url,
            }
            values = {
                "simd_datazone_count": (count, "count"),
                "simd_average_rank": (round(float(aggregate["rank_total"]) / count, 2), "rank"),
                "simd_average_country_decile": (round(float(aggregate["decile_total"]) / count, 2), "decile"),
                "simd_most15_pct": (round((float(aggregate["most15_count"]) / count) * 100, 2), "pct"),
                "simd_least15_pct": (round((float(aggregate["least15_count"]) / count) * 100, 2), "pct"),
            }
            for metric_name, (metric_value, unit) in values.items():
                metric_rows.append(
                    {
                        **base,
                        "metric_name": metric_name,
                        "metric_value": metric_value,
                        "metric_unit": unit,
                        "source_record_signature": _stable_key(
                            f"{base['source_key']}|{base['area_code']}|{metric_name}|{metric_value}"
                        ),
                        "raw_payload": _json_dumps(aggregate),
                    }
                )

        if self.dry_run or self.audit_only:
            return {
                "source_family": "demographics",
                "candidate_area_count": len(aggregates),
                "candidate_metric_count": len(metric_rows),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        self.database.execute_many(
            """
            insert into landintel.demographic_area_metrics (
                source_key,
                source_family,
                area_code,
                area_name,
                area_type,
                authority_name,
                metric_name,
                metric_value,
                metric_unit,
                period_start,
                period_end,
                confidence,
                source_url,
                source_record_signature,
                raw_payload,
                updated_at
            ) values (
                :source_key,
                :source_family,
                :area_code,
                :area_name,
                :area_type,
                :authority_name,
                :metric_name,
                :metric_value,
                :metric_unit,
                cast(:period_start as date),
                cast(:period_end as date),
                :confidence,
                :source_url,
                :source_record_signature,
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_key, area_code, metric_name, period_end) do update set
                area_name = excluded.area_name,
                area_type = excluded.area_type,
                authority_name = excluded.authority_name,
                metric_value = excluded.metric_value,
                metric_unit = excluded.metric_unit,
                confidence = excluded.confidence,
                source_url = excluded.source_url,
                source_record_signature = excluded.source_record_signature,
                raw_payload = excluded.raw_payload,
                updated_at = now()
            """,
            metric_rows,
        )
        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select site.id, site.authority_name, existing.source_record_signature as previous_signature
                from landintel.canonical_sites as site
                left join landintel.site_demographic_context as existing
                  on existing.canonical_site_id = site.id
                where site.authority_name is not null
                  and (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by existing.updated_at nulls first, site.updated_at desc nulls last, site.id
                limit :batch_size
            ),
            simd_context as (
                select
                    average_rank.authority_name,
                    average_rank.area_code,
                    average_rank.area_name,
                    average_rank.metric_value as average_rank,
                    most15.metric_value as most15_pct,
                    average_rank.period_end
                from landintel.demographic_area_metrics as average_rank
                left join landintel.demographic_area_metrics as most15
                  on most15.source_key = average_rank.source_key
                 and most15.area_code = average_rank.area_code
                 and most15.metric_name = 'simd_most15_pct'
                 and most15.period_end = average_rank.period_end
                where average_rank.source_key = 'simd_demographic_context'
                  and average_rank.metric_name = 'simd_average_rank'
            ),
            prepared as (
                select
                    selected_sites.id as canonical_site_id,
                    simd_context.area_code,
                    simd_context.area_name,
                    'council_area'::text as area_type,
                    'SIMD authority context present'::text as context_summary,
                    'area_context_only'::text as evidence_confidence,
                    selected_sites.previous_signature,
                    md5(concat_ws(
                        '|',
                        selected_sites.id::text,
                        simd_context.area_code,
                        round(simd_context.average_rank, 2)::text,
                        round(coalesce(simd_context.most15_pct, 0), 2)::text
                    )) as current_signature
                from selected_sites
                join simd_context
                  on lower(simd_context.authority_name) = lower(selected_sites.authority_name)
            ),
            upserted as (
                insert into landintel.site_demographic_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    area_code,
                    area_name,
                    area_type,
                    context_summary,
                    evidence_confidence,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    prepared.canonical_site_id,
                    'simd_demographic_context',
                    'demographics',
                    prepared.area_code,
                    prepared.area_name,
                    prepared.area_type,
                    prepared.context_summary,
                    prepared.evidence_confidence,
                    prepared.current_signature,
                    jsonb_build_object(
                        'source_key', 'simd_demographic_context',
                        'source_limitation', 'area_level_context_not_buyer_demand_certainty'
                    ),
                    now()
                from prepared
                on conflict (canonical_site_id) do update set
                    source_key = excluded.source_key,
                    source_family = excluded.source_family,
                    area_code = excluded.area_code,
                    area_name = excluded.area_name,
                    area_type = excluded.area_type,
                    context_summary = excluded.context_summary,
                    evidence_confidence = excluded.evidence_confidence,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join prepared on prepared.canonical_site_id = upserted.canonical_site_id
                where prepared.previous_signature is distinct from prepared.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'demographics'
                  and evidence.metadata ->> 'source_key' = 'simd_demographic_context'
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
                    'demographics',
                    'SIMD local authority context',
                    changed.id::text,
                    changed.area_name,
                    'medium',
                    jsonb_build_object('source_key', 'simd_demographic_context', 'area_code', changed.area_code)
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'demographics'
                  and signal.metadata ->> 'source_key' = 'simd_demographic_context'
                returning signal.id
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
                    changed.canonical_site_id,
                    'demographics',
                    'area_demographic_context',
                    changed.evidence_confidence,
                    1,
                    0.55,
                    'demographics',
                    changed.id::text,
                    'simd_area_context',
                    jsonb_build_object('source', 'simd_demographic_context'),
                    jsonb_build_object('source_key', 'simd_demographic_context'),
                    true
                from changed
                returning id
            )
            select
                (select count(*)::integer from landintel.demographic_area_metrics where source_key = 'simd_demographic_context') as metric_row_count,
                (select count(*)::integer from upserted) as context_row_count,
                (select count(*)::integer from changed) as changed_context_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="demographics",
            source_key="simd_demographic_context",
            row_count=int(proof.get("metric_row_count") or 0),
            linked_count=int(proof.get("context_row_count") or 0),
            measured_count=0,
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="ingest-demographics",
            source_key="simd_demographic_context",
            source_family="demographics",
            status="success",
            raw_rows=int(proof.get("metric_row_count") or 0),
            linked_rows=int(proof.get("context_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            summary="SIMD local authority context refreshed as area evidence only.",
        )
        self._record_family_freshness("demographics", "SIMD local context", int(proof.get("metric_row_count") or 0))
        return {"source_family": "demographics", **proof}

    def ingest_power_infrastructure(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="power_infrastructure")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)
        metadata_url = os.getenv("SPEN_METADATA_CATALOG_URL") or DEFAULT_SPEN_METADATA_URL
        metadata_count = 0
        try:
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                response = client.get(metadata_url)
                response.raise_for_status()
                metadata_count = len((response.json() or {}).get("results") or [])
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("power_metadata_probe_error", extra={"url": metadata_url, "error": str(exc)})

        if not self.dry_run and not self.audit_only:
            self._record_expansion_event(
                command_name="ingest-power-infrastructure",
                source_key="sp_energy_networks_assets",
                source_family="power_infrastructure",
                status="gated",
                raw_rows=0,
                summary="Power metadata catalog probed, but asset geometry endpoint is not proven for review use.",
                metadata={"metadata_catalog_record_count": metadata_count, "metadata_url": metadata_url},
            )
            self._record_family_freshness("power_infrastructure", "SP Energy Networks infrastructure", 0)
        return {
            "source_family": "power_infrastructure",
            "metadata_catalog_record_count": metadata_count,
            "asset_row_count": 0,
            "linked_site_count": 0,
            "evidence_count": 0,
            "signal_count": 0,
            "status": "gated_until_asset_geometry_access_is_proven",
        }

    def ingest_intelligence_events(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="local_intelligence")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)
                self._record_expansion_event(
                    command_name="ingest-intelligence-events",
                    source_key=source["source_key"],
                    source_family=source["source_family"],
                    status=source.get("access_status") or "gated",
                    summary=source.get("limitation_notes") or "Local intelligence source requires approved adapter before ingest.",
                    metadata={"next_action": source.get("next_action")},
                )
                self._record_freshness(source, records_observed=0)
        return {
            "source_family": "local_intelligence",
            "registered_source_count": len(selected_sources),
            "event_row_count": 0,
            "linked_site_count": 0,
            "status": "gated_or_access_required",
            "dry_run": self.dry_run,
            "audit_only": self.audit_only,
        }

    def _ingest_naptan_amenity_assets(self) -> int:
        source_url = os.getenv("NAPTAN_CSV_URL") or DEFAULT_NAPTAN_URL
        candidates: list[dict[str, Any]] = []
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            with client.stream("GET", source_url) as response:
                response.raise_for_status()
                reader = csv.DictReader(response.iter_lines())
                for row in reader:
                    latitude = _to_float(row.get("Latitude"))
                    longitude = _to_float(row.get("Longitude"))
                    if not _is_scotland_lat_lon(latitude, longitude):
                        continue
                    atco_code = str(row.get("ATCOCode") or "").strip()
                    if not atco_code:
                        continue
                    candidates.append(
                        {
                            "source_key": "naptan_public_transport",
                            "source_family": "amenities",
                            "source_record_id": atco_code,
                            "authority_name": None,
                            "amenity_type": _naptan_amenity_type(row.get("StopType")),
                            "amenity_name": row.get("CommonName") or row.get("ShortCommonName") or atco_code,
                            "source_url": source_url,
                            "longitude": longitude,
                            "latitude": latitude,
                            "source_record_signature": _stable_key(
                                f"{atco_code}|{row.get('CommonName')}|{row.get('Status')}|{latitude}|{longitude}"
                            ),
                            "raw_payload": _json_dumps(row),
                        }
                    )
                    if len(candidates) >= self.batch_size:
                        break
        self.database.execute_many(
            """
            insert into landintel.amenity_assets (
                source_key,
                source_family,
                source_record_id,
                authority_name,
                amenity_type,
                amenity_name,
                source_url,
                geometry,
                source_record_signature,
                raw_payload,
                updated_at
            ) values (
                :source_key,
                :source_family,
                :source_record_id,
                :authority_name,
                :amenity_type,
                :amenity_name,
                :source_url,
                st_transform(st_setsrid(st_makepoint(:longitude, :latitude), 4326), 27700),
                :source_record_signature,
                cast(:raw_payload as jsonb),
                now()
            )
            on conflict (source_key, source_record_id) do update set
                authority_name = excluded.authority_name,
                amenity_type = excluded.amenity_type,
                amenity_name = excluded.amenity_name,
                source_url = excluded.source_url,
                geometry = excluded.geometry,
                source_record_signature = excluded.source_record_signature,
                raw_payload = excluded.raw_payload,
                updated_at = now()
            """,
            candidates,
        )
        return len(candidates)

    def ingest_amenities(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="amenities")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            candidate_count = self.database.scalar(
                """
                select count(*)::bigint
                from public.constraint_source_features as feature
                join public.constraint_layer_registry as layer on layer.id = feature.constraint_layer_id
                where layer.source_family in ('os_places', 'os_features')
                """
            )
            return {"source_family": "amenities", "candidate_feature_count": int(candidate_count or 0)}

        naptan_asset_count = self._ingest_naptan_amenity_assets()
        proof = self.database.fetch_one(
            """
            with source_features as (
                select
                    feature.id,
                    feature.source_feature_key,
                    feature.feature_name,
                    feature.source_reference,
                    feature.geometry,
                    jsonb_build_object(
                        'constraint_feature_id', feature.id,
                        'constraint_source_feature_key', feature.source_feature_key,
                        'feature_name', feature.feature_name,
                        'source_reference', feature.source_reference,
                        'source_url', feature.source_url,
                        'authority_name', feature.authority_name,
                        'severity_label', feature.severity_label,
                        'constraint_feature_metadata', feature.metadata
                    ) as raw_payload,
                    layer.source_family,
                    layer.layer_name,
                    md5(concat_ws('|', feature.id::text, coalesce(feature.feature_name, ''), coalesce(feature.source_reference, ''))) as current_signature
                from public.constraint_source_features as feature
                join public.constraint_layer_registry as layer on layer.id = feature.constraint_layer_id
                where layer.source_family in ('os_places', 'os_features')
                order by feature.updated_at desc nulls last, feature.id
                limit :batch_size
            ),
            upserted_assets as (
                insert into landintel.amenity_assets (
                    source_key,
                    source_family,
                    source_record_id,
                    authority_name,
                    amenity_type,
                    amenity_name,
                    source_url,
                    geometry,
                    source_record_signature,
                    raw_payload,
                    updated_at
                )
                select
                    'os_places_amenity_context',
                    'amenities',
                    source_features.source_feature_key,
                    null::text,
                    coalesce(source_features.layer_name, 'place_context'),
                    source_features.feature_name,
                    source_features.source_reference,
                    source_features.geometry,
                    source_features.current_signature,
                    coalesce(source_features.raw_payload, '{}'::jsonb),
                    now()
                from source_features
                on conflict (source_key, source_record_id) do update set
                    amenity_type = excluded.amenity_type,
                    amenity_name = excluded.amenity_name,
                    source_url = excluded.source_url,
                    geometry = excluded.geometry,
                    source_record_signature = excluded.source_record_signature,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                returning *
            ),
            selected_sites as (
                select site.id, site.geometry
                from landintel.canonical_sites as site
                where site.geometry is not null
                  and (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by site.updated_at desc nulls last, site.id
                limit :batch_size
            ),
            amenity_types as (
                select distinct amenity_type
                from landintel.amenity_assets
                where geometry is not null
                  and amenity_type is not null
            ),
            nearest_assets as (
                select
                    site.id as canonical_site_id,
                    nearest.source_key as nearest_source_key,
                    nearest.amenity_type,
                    nearest.id as nearest_asset_id,
                    nearest.amenity_name,
                    nearest.nearest_distance_m,
                    nearby.count_within_400m,
                    nearby.count_within_800m,
                    nearby.count_within_1600m
                from selected_sites as site
                join amenity_types as amenity_type on true
                join lateral (
                    select
                        asset.id,
                        asset.source_key,
                        asset.amenity_type,
                        asset.amenity_name,
                        asset.geometry,
                        st_distance(site.geometry, asset.geometry) as nearest_distance_m
                    from landintel.amenity_assets as asset
                    where asset.geometry is not null
                      and asset.amenity_type = amenity_type.amenity_type
                    order by site.geometry OPERATOR(extensions.<->) asset.geometry, asset.id
                    limit 1
                ) as nearest on true
                cross join lateral (
                    select
                        count(*) filter (where st_dwithin(site.geometry, asset.geometry, 400))::integer as count_within_400m,
                        count(*) filter (where st_dwithin(site.geometry, asset.geometry, 800))::integer as count_within_800m,
                        count(*) filter (where st_dwithin(site.geometry, asset.geometry, 1600))::integer as count_within_1600m
                    from landintel.amenity_assets as asset
                    where asset.geometry is not null
                      and asset.amenity_type = amenity_type.amenity_type
                ) as nearby
            ),
            upserted_context as (
                insert into landintel.site_amenity_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    amenity_type,
                    nearest_amenity_asset_id,
                    nearest_amenity_name,
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
                    nearest_assets.canonical_site_id,
                    nearest_assets.nearest_source_key,
                    'amenities',
                    nearest_assets.amenity_type,
                    nearest_assets.nearest_asset_id,
                    nearest_assets.amenity_name,
                    round(nearest_assets.nearest_distance_m::numeric, 2),
                    nearest_assets.count_within_400m::integer,
                    nearest_assets.count_within_800m::integer,
                    nearest_assets.count_within_1600m::integer,
                    md5(concat_ws('|', nearest_assets.canonical_site_id::text, nearest_assets.nearest_source_key, nearest_assets.amenity_type, nearest_assets.nearest_asset_id::text, round(nearest_assets.nearest_distance_m::numeric, 2)::text)),
                    jsonb_build_object('source_key', nearest_assets.nearest_source_key),
                    now(),
                    now()
                from nearest_assets
                on conflict (canonical_site_id, amenity_type) do update set
                    nearest_amenity_asset_id = excluded.nearest_amenity_asset_id,
                    nearest_amenity_name = excluded.nearest_amenity_name,
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
                  and evidence.source_family = 'amenities'
                  and evidence.metadata ->> 'source_key' = upserted_context.source_key
                  and evidence.metadata ->> 'amenity_type' = upserted_context.amenity_type
                returning evidence.id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using upserted_context
                where signal.canonical_site_id = upserted_context.canonical_site_id
                  and signal.source_family = 'amenities'
                  and signal.signal_family = 'location_strength'
                  and signal.signal_name = 'amenity_proximity'
                  and signal.metadata ->> 'source_key' = upserted_context.source_key
                  and signal.metadata ->> 'amenity_type' = upserted_context.amenity_type
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
                    'amenities',
                    'OS Places and Features amenity context',
                    upserted_context.id::text,
                    upserted_context.nearest_amenity_name,
                    'medium',
                    jsonb_build_object(
                        'source_key', upserted_context.source_key,
                        'amenity_type', upserted_context.amenity_type,
                        'nearest_distance_m', upserted_context.nearest_distance_m
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
                    'location_strength',
                    'amenity_proximity',
                    upserted_context.amenity_type,
                    upserted_context.nearest_distance_m,
                    0.6,
                    'amenities',
                    upserted_context.id::text,
                    'amenity_proximity_evidence',
                    jsonb_build_object('source', upserted_context.source_key),
                    jsonb_build_object(
                        'source_key', upserted_context.source_key,
                        'amenity_type', upserted_context.amenity_type
                    ),
                    true
                from upserted_context
                returning id
            )
            select
                (select count(*)::integer from source_features) as source_feature_count,
                (select count(*)::integer from upserted_assets) as asset_count,
                (select count(*)::integer from upserted_context) as context_row_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        proof["asset_count"] = int(proof.get("asset_count") or 0) + naptan_asset_count
        proof["naptan_asset_count"] = naptan_asset_count
        self._update_family_lifecycle(
            source_family="amenities",
            source_key="os_places_amenity_context",
            row_count=int(proof.get("asset_count") or 0),
            linked_count=int(proof.get("context_row_count") or 0),
            measured_count=int(proof.get("context_row_count") or 0),
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._update_family_lifecycle(
            source_family="amenities",
            source_key="naptan_public_transport",
            row_count=naptan_asset_count,
            linked_count=int(proof.get("context_row_count") or 0),
            measured_count=int(proof.get("context_row_count") or 0),
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="ingest-amenities",
            source_key="os_places_amenity_context",
            source_family="amenities",
            status="success",
            raw_rows=int(proof.get("asset_count") or 0),
            linked_rows=int(proof.get("context_row_count") or 0),
            measured_rows=int(proof.get("context_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            summary="Amenity context refreshed from OS place/features constraint assets where present.",
            metadata={"naptan_asset_count": naptan_asset_count},
        )
        self._record_family_freshness("amenities", "OS Places and Features amenity context", int(proof.get("asset_count") or 0))
        self._record_family_freshness("amenities", "NaPTAN public transport access", naptan_asset_count)
        return {"source_family": "amenities", **proof}

    def ingest_planning_documents(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(module_key="planning_documents")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        rows = self.database.fetch_all(
            """
            select
                planning.id,
                coalesce(planning.canonical_site_id, state_row.current_canonical_site_id) as canonical_site_id,
                planning.authority_name,
                planning.planning_reference,
                planning.raw_payload
            from landintel.source_reconcile_state as state_row
            join landintel.planning_application_records as planning
              on planning.authority_name = state_row.authority_name
             and planning.source_record_id = state_row.source_record_id
            where state_row.source_family = 'planning'
              and state_row.active_flag = true
              and state_row.publish_state in ('published', 'provisional')
              and state_row.current_canonical_site_id is not null
              and (:authority_name = '' or state_row.authority_name ilike :authority_name_like)
            order by planning.updated_at desc nulls last, planning.id
            limit :batch_size
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        )
        candidates: list[dict[str, Any]] = []
        for row in rows:
            for url in _extract_document_urls(row.get("raw_payload")):
                document_type = _classify_document_type(url)
                candidates.append(
                    {
                        "canonical_site_id": row["canonical_site_id"],
                        "source_record_id": f"planning_document:{row['id']}:{_stable_key(url)}",
                        "application_reference": row.get("planning_reference"),
                        "authority_name": row.get("authority_name"),
                        "document_type": document_type,
                        "document_title": document_type.replace("_", " "),
                        "document_url": url,
                        "source_record_signature": _stable_key(f"{row['id']}|{url}|{document_type}"),
                        "raw_payload": _json_dumps({"planning_application_record_id": str(row["id"]), "document_url": url}),
                    }
                )

        if self.dry_run or self.audit_only:
            return {
                "source_family": "planning_documents",
                "candidate_document_count": len(candidates),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        document_count = 0
        link_count = 0
        evidence_count = 0
        signal_count = 0
        section75_count = 0
        for candidate in candidates[: self.batch_size]:
            document = self.database.fetch_one(
                """
                insert into landintel.planning_document_records (
                    source_key,
                    source_family,
                    source_record_id,
                    application_reference,
                    authority_name,
                    document_type,
                    document_title,
                    document_url,
                    source_record_signature,
                    raw_payload,
                    updated_at
                ) values (
                    'council_planning_documents',
                    'planning_documents',
                    :source_record_id,
                    :application_reference,
                    :authority_name,
                    :document_type,
                    :document_title,
                    :document_url,
                    :source_record_signature,
                    cast(:raw_payload as jsonb),
                    now()
                )
                on conflict (source_key, source_record_id) do update set
                    application_reference = excluded.application_reference,
                    authority_name = excluded.authority_name,
                    document_type = excluded.document_type,
                    document_title = excluded.document_title,
                    document_url = excluded.document_url,
                    source_record_signature = excluded.source_record_signature,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                returning id
                """,
                candidate,
            )
            if not document:
                continue
            document_count += 1
            document_id = str(document["id"])
            link = self.database.fetch_one(
                """
                insert into landintel.site_planning_document_links (
                    canonical_site_id,
                    planning_document_record_id,
                    source_family,
                    link_method,
                    link_confidence,
                    matched_reference,
                    source_record_signature,
                    metadata,
                    updated_at
                ) values (
                    cast(:canonical_site_id as uuid),
                    cast(:planning_document_record_id as uuid),
                    'planning_documents',
                    'planning_reference',
                    0.7,
                    :application_reference,
                    :source_record_signature,
                    jsonb_build_object('source_key', 'council_planning_documents'),
                    now()
                )
                on conflict (canonical_site_id, planning_document_record_id) do update set
                    matched_reference = excluded.matched_reference,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning id
                """,
                {**candidate, "planning_document_record_id": document_id},
            )
            if link:
                link_count += 1
            if candidate["document_type"] == "section75":
                self.database.execute(
                    """
                    insert into landintel.section75_obligation_records (
                        source_key,
                        source_family,
                        source_record_id,
                        canonical_site_id,
                        application_reference,
                        authority_name,
                        obligation_type,
                        obligation_summary,
                        source_url,
                        source_record_signature,
                        raw_payload,
                        updated_at
                    ) values (
                        'section75_records',
                        'planning_documents',
                        :source_record_id,
                        cast(:canonical_site_id as uuid),
                        :application_reference,
                        :authority_name,
                        'section75_document_identified',
                        'Section 75 document identified; obligations require human review.',
                        :document_url,
                        :source_record_signature,
                        cast(:raw_payload as jsonb),
                        now()
                    )
                    on conflict (source_key, source_record_id) do update set
                        canonical_site_id = excluded.canonical_site_id,
                        application_reference = excluded.application_reference,
                        authority_name = excluded.authority_name,
                        source_url = excluded.source_url,
                        source_record_signature = excluded.source_record_signature,
                        raw_payload = excluded.raw_payload,
                        updated_at = now()
                    """,
                    candidate,
                )
                section75_count += 1
            self.database.execute(
                """
                delete from landintel.evidence_references
                where canonical_site_id = cast(:canonical_site_id as uuid)
                  and source_family = 'planning_documents'
                  and source_record_id = :planning_document_record_id
                """,
                {**candidate, "planning_document_record_id": document_id},
            )
            self.database.execute(
                """
                insert into landintel.evidence_references (
                    canonical_site_id,
                    source_family,
                    source_dataset,
                    source_record_id,
                    source_reference,
                    source_url,
                    confidence,
                    metadata
                ) values (
                    cast(:canonical_site_id as uuid),
                    'planning_documents',
                    'Council planning document records',
                    :planning_document_record_id,
                    :application_reference,
                    :document_url,
                    'medium',
                    jsonb_build_object('source_key', 'council_planning_documents', 'document_type', :document_type)
                )
                """,
                {**candidate, "planning_document_record_id": document_id},
            )
            evidence_count += 1
            self.database.execute(
                """
                delete from landintel.site_signals
                where canonical_site_id = cast(:canonical_site_id as uuid)
                  and source_family = 'planning_documents'
                  and source_record_id = :planning_document_record_id
                """,
                {**candidate, "planning_document_record_id": document_id},
            )
            self.database.execute(
                """
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
                    cast(:canonical_site_id as uuid),
                    'planning_documents',
                    'planning_document_identified',
                    :document_type,
                    1,
                    0.6,
                    'planning_documents',
                    :planning_document_record_id,
                    'planning_document_evidence',
                    jsonb_build_object('source', 'planning_document_records'),
                    jsonb_build_object('source_key', 'council_planning_documents'),
                    true
                )
                """,
                {**candidate, "planning_document_record_id": document_id},
            )
            signal_count += 1

        self._update_family_lifecycle(
            source_family="planning_documents",
            source_key="council_planning_documents",
            row_count=document_count,
            linked_count=link_count,
            measured_count=0,
            evidence_count=evidence_count,
            signal_count=signal_count,
        )
        self._record_expansion_event(
            command_name="ingest-planning-documents",
            source_key="council_planning_documents",
            source_family="planning_documents",
            status="success",
            raw_rows=document_count,
            linked_rows=link_count,
            evidence_rows=evidence_count,
            signal_rows=signal_count,
            summary="Planning document URLs extracted from planning raw payloads where present.",
            metadata={"section75_document_count": section75_count},
        )
        self._record_family_freshness("planning_documents", "Council planning document records", document_count)
        return {
            "source_family": "planning_documents",
            "document_count": document_count,
            "linked_site_count": link_count,
            "evidence_count": evidence_count,
            "signal_count": signal_count,
            "section75_document_count": section75_count,
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

    def refresh_title_reviews(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="title_control")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            review_count = self.database.scalar(
                """
                select count(*)::bigint
                from landintel.title_review_records as review
                join landintel.canonical_sites as site on site.id = review.canonical_site_id
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                """,
                {
                    "authority_name": self.authority_filter,
                    "authority_name_like": f"%{self.authority_filter}%",
                },
            )
            return {
                "source_family": "title_control",
                "review_record_count": int(review_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with selected_reviews as (
                select
                    review.*,
                    existing.source_record_signature as previous_signal_signature,
                    md5(concat_ws(
                        '|',
                        review.id::text,
                        coalesce(review.normalized_title_number, ''),
                        coalesce(review.registered_proprietor, ''),
                        coalesce(review.proprietor_type, ''),
                        coalesce(review.company_number, ''),
                        coalesce(review.ownership_outcome, ''),
                        coalesce(review.review_date::text, '')
                    )) as current_signature
                from landintel.title_review_records as review
                join landintel.canonical_sites as site on site.id = review.canonical_site_id
                left join landintel.ownership_control_signals as existing
                  on existing.canonical_site_id = review.canonical_site_id
                 and existing.source_family = 'title_control'
                 and existing.source_record_id = review.id::text
                 and existing.signal_type = 'title_review_outcome'
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by review.updated_at desc nulls last, review.id
                limit :batch_size
            ),
            upserted_workflow as (
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
                    control_signal_summary,
                    next_action,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    selected_reviews.canonical_site_id,
                    selected_reviews.canonical_site_id::text,
                    'title_readiness_internal',
                    'title_control',
                    selected_reviews.title_number,
                    selected_reviews.normalized_title_number,
                    'parcel_candidate_identified',
                    'possible_title_reference_identified',
                    'ownership_not_confirmed',
                    false,
                    'reviewed',
                    'reviewed',
                    selected_reviews.ownership_outcome,
                    coalesce(selected_reviews.next_action, 'human_review_required'),
                    selected_reviews.current_signature,
                    jsonb_build_object(
                        'title_review_record_id', selected_reviews.id,
                        'ownership_confirmed_only_from_review', true
                    ),
                    now()
                from selected_reviews
                on conflict (canonical_site_id) do update set
                    title_number = excluded.title_number,
                    normalized_title_number = excluded.normalized_title_number,
                    title_required_flag = excluded.title_required_flag,
                    title_order_status = excluded.title_order_status,
                    title_review_status = excluded.title_review_status,
                    control_signal_summary = excluded.control_signal_summary,
                    next_action = excluded.next_action,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            changed as (
                select selected_reviews.*
                from selected_reviews
                where selected_reviews.previous_signal_signature is distinct from selected_reviews.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'title_control'
                  and evidence.metadata ->> 'source_key' = 'title_review_manual'
                returning evidence.id
            ),
            inserted_evidence as (
                insert into landintel.evidence_references (
                    canonical_site_id,
                    source_family,
                    source_dataset,
                    source_record_id,
                    source_reference,
                    source_url,
                    confidence,
                    metadata
                )
                select
                    changed.canonical_site_id,
                    'title_control',
                    'Manual title review outcomes',
                    changed.id::text,
                    changed.title_number,
                    changed.document_url,
                    'high',
                    jsonb_build_object(
                        'source_key', 'title_review_manual',
                        'registered_proprietor', changed.registered_proprietor,
                        'proprietor_type', changed.proprietor_type,
                        'ownership_outcome', changed.ownership_outcome
                    )
                from changed
                returning id
            ),
            inserted_signals as (
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
                    'title_review_manual',
                    'title_control',
                    changed.id::text,
                    'title_review_outcome',
                    coalesce(changed.ownership_outcome, 'ownership_unclear_after_review'),
                    changed.registered_proprietor,
                    0.95,
                    false,
                    true,
                    changed.current_signature,
                    jsonb_build_object('source_key', 'title_review_manual', 'title_number', changed.title_number),
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
            deleted_site_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'title_control'
                  and signal.metadata ->> 'source_key' = 'title_review_manual'
                returning signal.id
            ),
            inserted_site_signals as (
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
                    changed.canonical_site_id,
                    'title_control',
                    'title_review_outcome',
                    changed.ownership_outcome,
                    1,
                    0.95,
                    'title_control',
                    changed.id::text,
                    'reviewed_title_evidence',
                    jsonb_build_object('source', 'title_review_manual'),
                    jsonb_build_object('source_key', 'title_review_manual'),
                    true
                from changed
                returning id
            )
            select
                (select count(*)::integer from selected_reviews) as selected_review_count,
                (select count(*)::integer from upserted_workflow) as workflow_row_count,
                (select count(*)::integer from changed) as changed_review_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_site_signals) as signal_row_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="title_control",
            source_key="title_review_manual",
            row_count=int(proof.get("selected_review_count") or 0),
            linked_count=int(proof.get("selected_review_count") or 0),
            measured_count=0,
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self._record_expansion_event(
            command_name="refresh-title-reviews",
            source_key="title_review_manual",
            source_family="title_control",
            status="success",
            raw_rows=int(proof.get("selected_review_count") or 0),
            linked_rows=int(proof.get("selected_review_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            summary="Manual title review outcomes refreshed into control signals.",
        )
        self._record_family_freshness("title_control", "Manual title review outcomes", int(proof.get("selected_review_count") or 0))
        return {"source_family": "title_control", **proof}

    def ingest_companies_house(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="corporate_control")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        api_key = os.getenv("COMPANIES_HOUSE_API") or os.getenv("COMPANIES_HOUSE_API_KEY") or ""
        candidates = self.database.fetch_all(
            """
            with corporate_candidates as (
                select
                    review.canonical_site_id,
                    review.registered_proprietor as company_name,
                    'title_review_registered_proprietor'::text as link_basis,
                    0.9::numeric as confidence
                from landintel.title_review_records as review
                where nullif(btrim(review.registered_proprietor), '') is not null
                  and (
                        review.company_number is not null
                        or review.proprietor_type ilike '%%compan%%'
                        or review.registered_proprietor ~* '\\m(ltd|limited|plc|llp|limited liability partnership)\\M'
                  )
                union all
                select
                    hla.canonical_site_id,
                    hla.developer_name as company_name,
                    'hla_developer_signal_not_ownership'::text as link_basis,
                    0.55::numeric as confidence
                from landintel.hla_site_records as hla
                where hla.canonical_site_id is not null
                  and nullif(btrim(hla.developer_name), '') is not null
            )
            select distinct on (lower(company_name), canonical_site_id)
                canonical_site_id,
                company_name,
                link_basis,
                confidence
            from corporate_candidates
            where (:authority_name = '' or exists (
                select 1
                from landintel.canonical_sites as site
                where site.id = corporate_candidates.canonical_site_id
                  and site.authority_name ilike :authority_name_like
            ))
            order by lower(company_name), canonical_site_id, confidence desc
            limit :batch_size
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        )
        if self.dry_run or self.audit_only:
            return {
                "source_family": "corporate_control",
                "candidate_company_count": len(candidates),
                "api_configured": bool(api_key),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }
        if not api_key:
            self._record_expansion_event(
                command_name="ingest-companies-house",
                source_key="companies_house_control_context",
                source_family="corporate_control",
                status="access_required",
                summary="Companies House key missing; no corporate enrichment rows landed.",
            )
            return {"source_family": "corporate_control", "candidate_company_count": len(candidates), "row_count": 0}

        row_count = 0
        linked_count = 0
        evidence_count = 0
        signal_count = 0
        charge_count = 0
        with httpx.Client(timeout=20, follow_redirects=True, auth=(api_key, "")) as client:
            for candidate in candidates:
                company_name = _normalise_company_query(candidate.get("company_name"))
                if not company_name:
                    continue
                try:
                    response = client.get(
                        "https://api.company-information.service.gov.uk/search/companies",
                        params={"q": company_name, "items_per_page": 1},
                    )
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("companies_house_lookup_error", extra={"company_name": company_name, "error": str(exc)})
                    continue
                items = payload.get("items") or []
                if not items:
                    continue
                item = items[0]
                company_number = str(item.get("company_number") or "").strip()
                matched_name = str(item.get("title") or company_name).strip()
                source_record_id = f"companies_house:{company_number or _stable_key(matched_name)}"
                signature = _stable_key(_json_dumps(item))
                inserted = self.database.fetch_one(
                    """
                    insert into landintel.corporate_owner_links (
                        canonical_site_id,
                        source_key,
                        source_family,
                        company_name,
                        company_number,
                        match_method,
                        match_confidence,
                        link_basis,
                        source_url,
                        source_record_signature,
                        raw_payload,
                        updated_at
                    ) values (
                        cast(:canonical_site_id as uuid),
                        'companies_house_control_context',
                        'corporate_control',
                        :company_name,
                        :company_number,
                        'companies_house_search',
                        :match_confidence,
                        :link_basis,
                        :source_url,
                        :source_record_signature,
                        cast(:raw_payload as jsonb),
                        now()
                    )
                    on conflict do nothing
                    returning id
                    """,
                    {
                        "canonical_site_id": candidate.get("canonical_site_id"),
                        "company_name": matched_name,
                        "company_number": company_number or None,
                        "match_confidence": candidate.get("confidence"),
                        "link_basis": candidate.get("link_basis"),
                        "source_url": f"https://find-and-update.company-information.service.gov.uk/company/{company_number}" if company_number else None,
                        "source_record_signature": signature,
                        "raw_payload": _json_dumps(item),
                    },
                )
                if not inserted:
                    continue
                row_count += 1
                linked_count += 1 if candidate.get("canonical_site_id") else 0
                enrichment = self.database.fetch_one(
                    """
                    insert into landintel.corporate_entity_enrichments (
                        canonical_site_id,
                        source_key,
                        source_family,
                        source_record_id,
                        company_name,
                        company_number,
                        entity_status,
                        entity_type,
                        registered_address,
                        source_url,
                        enrichment_basis,
                        source_record_signature,
                        raw_payload,
                        updated_at
                    ) values (
                        cast(:canonical_site_id as uuid),
                        'companies_house_control_context',
                        'corporate_control',
                        :source_record_id,
                        :company_name,
                        :company_number,
                        :entity_status,
                        :entity_type,
                        :registered_address,
                        :source_url,
                        :enrichment_basis,
                        :source_record_signature,
                        cast(:raw_payload as jsonb),
                        now()
                    )
                    on conflict (source_key, source_record_id) do update set
                        canonical_site_id = excluded.canonical_site_id,
                        company_name = excluded.company_name,
                        company_number = excluded.company_number,
                        entity_status = excluded.entity_status,
                        entity_type = excluded.entity_type,
                        registered_address = excluded.registered_address,
                        source_url = excluded.source_url,
                        enrichment_basis = excluded.enrichment_basis,
                        source_record_signature = excluded.source_record_signature,
                        raw_payload = excluded.raw_payload,
                        updated_at = now()
                    returning id
                    """,
                    {
                        "canonical_site_id": candidate.get("canonical_site_id"),
                        "source_record_id": source_record_id,
                        "company_name": matched_name,
                        "company_number": company_number or None,
                        "entity_status": item.get("company_status"),
                        "entity_type": item.get("company_type"),
                        "registered_address": item.get("address_snippet"),
                        "source_url": f"https://find-and-update.company-information.service.gov.uk/company/{company_number}" if company_number else None,
                        "enrichment_basis": candidate.get("link_basis"),
                        "source_record_signature": signature,
                        "raw_payload": _json_dumps(item),
                    },
                )
                if candidate.get("canonical_site_id"):
                    self._replace_evidence_and_signal(
                        canonical_site_id=str(candidate["canonical_site_id"]),
                        source_family="corporate_control",
                        source_key="companies_house_control_context",
                        source_dataset="Companies House corporate control context",
                        source_record_id=source_record_id,
                        source_reference=matched_name,
                        signal_family="title_control",
                        signal_name="corporate_control_context",
                        signal_value_text="corporate_entity_context_identified",
                        confidence_numeric=0.55,
                    )
                    evidence_count += 1
                    signal_count += 1
                if company_number and enrichment:
                    charge_count += self._ingest_companies_house_charges(client, str(enrichment["id"]), candidate, company_number)

        self._update_family_lifecycle(
            source_family="corporate_control",
            source_key="companies_house_control_context",
            row_count=row_count,
            linked_count=linked_count,
            measured_count=0,
            evidence_count=evidence_count,
            signal_count=signal_count,
        )
        self._update_family_lifecycle(
            source_family="corporate_control",
            source_key="companies_house_charges",
            row_count=charge_count,
            linked_count=linked_count if charge_count > 0 else 0,
            measured_count=0,
            evidence_count=0,
            signal_count=0,
        )
        self._record_expansion_event(
            command_name="ingest-companies-house",
            source_key="companies_house_control_context",
            source_family="corporate_control",
            status="success",
            raw_rows=row_count,
            linked_rows=linked_count,
            evidence_rows=evidence_count,
            signal_rows=signal_count,
            summary="Companies House corporate context refreshed without claiming land ownership.",
            metadata={"charge_record_count": charge_count},
        )
        self._record_family_freshness("corporate_control", "Companies House corporate control context", row_count)
        self._record_family_freshness("corporate_control", "Companies House charge and lending context", charge_count)
        return {
            "source_family": "corporate_control",
            "company_row_count": row_count,
            "linked_site_count": linked_count,
            "evidence_count": evidence_count,
            "signal_count": signal_count,
            "charge_record_count": charge_count,
        }

    def ingest_fca_entities(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="corporate_control")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)
        fca_base = os.getenv("FCA_API") or ""
        fca_key = os.getenv("FCA_API_KEY") or ""
        candidates = self.database.fetch_all(
            """
            select canonical_site_id, company_name, company_number
            from landintel.corporate_owner_links
            where nullif(btrim(company_name), '') is not null
            order by updated_at desc nulls last
            limit :batch_size
            """,
            {"batch_size": self.batch_size},
        )
        if self.dry_run or self.audit_only:
            return {"source_family": "corporate_control", "candidate_entity_count": len(candidates), "api_configured": bool(fca_base or fca_key)}
        if not fca_base or not fca_base.startswith("http"):
            self._record_expansion_event(
                command_name="ingest-fca-entities",
                source_key="fca_entity_enrichment",
                source_family="corporate_control",
                status="access_required",
                summary="FCA endpoint not configured; no FCA enrichment rows landed.",
            )
            return {"source_family": "corporate_control", "candidate_entity_count": len(candidates), "row_count": 0}

        row_count = 0
        linked_count = 0
        headers = {"Authorization": f"Bearer {fca_key}"} if fca_key else {}
        with httpx.Client(timeout=20, follow_redirects=True, headers=headers) as client:
            for candidate in candidates:
                try:
                    response = client.get(fca_base, params={"q": candidate["company_name"]})
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("fca_lookup_error", extra={"company_name": candidate.get("company_name"), "error": str(exc)})
                    continue
                signature = _stable_key(_json_dumps(payload))
                self.database.execute(
                    """
                    insert into landintel.corporate_entity_enrichments (
                        canonical_site_id,
                        source_key,
                        source_family,
                        source_record_id,
                        company_name,
                        company_number,
                        entity_status,
                        entity_type,
                        source_url,
                        enrichment_basis,
                        source_record_signature,
                        raw_payload,
                        updated_at
                    ) values (
                        cast(:canonical_site_id as uuid),
                        'fca_entity_enrichment',
                        'corporate_control',
                        :source_record_id,
                        :company_name,
                        :company_number,
                        'fca_context_returned',
                        'fca_entity',
                        :source_url,
                        'fca_lookup_context_not_ownership',
                        :source_record_signature,
                        cast(:raw_payload as jsonb),
                        now()
                    )
                    on conflict (source_key, source_record_id) do update set
                        raw_payload = excluded.raw_payload,
                        source_record_signature = excluded.source_record_signature,
                        updated_at = now()
                    """,
                    {
                        "canonical_site_id": candidate.get("canonical_site_id"),
                        "source_record_id": f"fca:{_stable_key(str(candidate.get('company_number') or candidate.get('company_name')))}",
                        "company_name": candidate.get("company_name"),
                        "company_number": candidate.get("company_number"),
                        "source_url": fca_base,
                        "source_record_signature": signature,
                        "raw_payload": _json_dumps(payload),
                    },
                )
                row_count += 1
                linked_count += 1 if candidate.get("canonical_site_id") else 0
        self._update_family_lifecycle(
            source_family="corporate_control",
            source_key="fca_entity_enrichment",
            row_count=row_count,
            linked_count=linked_count,
            measured_count=0,
            evidence_count=0,
            signal_count=0,
        )
        self._record_expansion_event(
            command_name="ingest-fca-entities",
            source_key="fca_entity_enrichment",
            source_family="corporate_control",
            status="success",
            raw_rows=row_count,
            linked_rows=linked_count,
            summary="FCA entity context refreshed where endpoint access returned data.",
        )
        self._record_family_freshness("corporate_control", "FCA entity enrichment", row_count)
        return {"source_family": "corporate_control", "fca_row_count": row_count, "linked_site_count": linked_count}

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

    def refresh_site_assessments(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="site_assessment")
        if not self.dry_run and not self.audit_only:
            for source in selected_sources:
                self._upsert_source(source)

        if self.dry_run or self.audit_only:
            candidate_count = self.database.scalar(
                """
                select count(*)::bigint
                from landintel.canonical_sites as site
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                """
                ,
                {
                    "authority_name": self.authority_filter,
                    "authority_name_like": f"%{self.authority_filter}%",
                },
            )
            return {
                "source_family": "site_assessment",
                "candidate_site_count": int(candidate_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select
                    site.id as canonical_site_id,
                    site.site_name_primary,
                    site.authority_name,
                    site.surfaced_reason,
                    title.title_required_flag,
                    title.title_review_status,
                    planning.latest_decision_status,
                    planning.approved_count,
                    planning.refused_count,
                    planning.withdrawn_count,
                    planning.live_count,
                    planning.decision_record_count,
                    constraints.constraint_count,
                    constraints.constraint_group_count,
                    ground.mining_constraint_present,
                    ground.flood_constraint_present,
                    ground.culvert_constraint_present,
                    market.id is not null as market_context_present,
                    amenity.amenity_context_count,
                    demographic.id is not null as demographic_context_present,
                    existing.source_record_signature as previous_signature,
                    md5(concat_ws(
                        '|',
                        site.id::text,
                        coalesce(title.title_required_flag::text, ''),
                        coalesce(title.title_review_status, ''),
                        coalesce(planning.latest_decision_status, ''),
                        coalesce(planning.decision_record_count, 0)::text,
                        coalesce(constraints.constraint_count, 0)::text,
                        coalesce(ground.mining_constraint_present::text, ''),
                        coalesce(ground.flood_constraint_present::text, ''),
                        coalesce(market.id::text, ''),
                        coalesce(amenity.amenity_context_count, 0)::text,
                        coalesce(demographic.id::text, '')
                    )) as current_signature
                from landintel.canonical_sites as site
                left join landintel.title_order_workflow as title on title.canonical_site_id = site.id
                left join landintel.site_planning_decision_context as planning on planning.canonical_site_id = site.id
                left join lateral (
                    select
                        count(*)::integer as constraint_count,
                        count(distinct constraint_group)::integer as constraint_group_count
                    from public.site_constraint_group_summaries as summary
                    where summary.site_id = site.id::text
                ) as constraints on true
                left join landintel.site_ground_risk_context as ground on ground.canonical_site_id = site.id
                left join landintel.site_market_context as market on market.canonical_site_id = site.id
                left join lateral (
                    select count(*)::integer as amenity_context_count
                    from landintel.site_amenity_context as amenity_row
                    where amenity_row.canonical_site_id = site.id
                ) as amenity on true
                left join landintel.site_demographic_context as demographic on demographic.canonical_site_id = site.id
                left join landintel.site_assessments as existing
                  on existing.canonical_site_id = site.id
                 and existing.source_key = 'site_assessment_refresh'
                 and existing.assessment_version = 1
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by existing.updated_at nulls first, site.updated_at desc nulls last, site.id
                limit :batch_size
            ),
            prepared as (
                select
                    selected_sites.*,
                    array_remove(array[
                        case when coalesce(approved_count, 0) > 0 then 'Planning approval evidence present' end,
                        case when coalesce(live_count, 0) > 0 then 'Live planning activity present' end,
                        case when title_review_status = 'reviewed' then 'Title review recorded' end,
                        case when coalesce(amenity_context_count, 0) > 0 then 'Amenity proximity context present' end,
                        case when market_context_present then 'Market context present' end
                    ]::text[], null) as top_positives,
                    array_remove(array[
                        case when coalesce(title_required_flag, true) then 'Title required before ownership can be confirmed' end,
                        case when coalesce(refused_count, 0) > 0 then 'Planning refusal evidence present' end,
                        case when coalesce(withdrawn_count, 0) > 0 then 'Planning withdrawal evidence present' end,
                        case when coalesce(constraint_count, 0) > 0 then 'Measured constraints present' end,
                        case when coalesce(flood_constraint_present, false) then 'Flood constraint evidence present' end,
                        case when coalesce(mining_constraint_present, false) then 'Mining or coal constraint evidence present' end
                    ]::text[], null) as top_warnings,
                    array_remove(array[
                        case when title_review_status is distinct from 'reviewed' then 'reviewed_title' end,
                        case when coalesce(decision_record_count, 0) = 0 then 'planning_decision_context' end,
                        case when coalesce(constraint_count, 0) = 0 then 'measured_constraints' end,
                        case when not market_context_present then 'market_context' end,
                        case when coalesce(amenity_context_count, 0) = 0 then 'amenity_context' end,
                        case when not demographic_context_present then 'demographic_context' end
                    ]::text[], null) as missing_critical_evidence
                from selected_sites
            ),
            upserted as (
                insert into landintel.site_assessments (
                    canonical_site_id,
                    assessment_version,
                    source_key,
                    source_family,
                    review_tier,
                    site_review_status,
                    why_site_surfaced,
                    top_positives,
                    top_warnings,
                    missing_critical_evidence,
                    title_required_flag,
                    review_next_action,
                    evidence_completeness_tier,
                    source_limitation_notes,
                    human_review_required,
                    explanation_text,
                    scores,
                    score_confidence,
                    metadata,
                    source_record_signature,
                    updated_at
                )
                select
                    prepared.canonical_site_id,
                    1,
                    'site_assessment_refresh',
                    'site_assessment',
                    case
                        when cardinality(prepared.missing_critical_evidence) <= 2 and cardinality(prepared.top_positives) >= 2 then 'strong_candidate'
                        when cardinality(prepared.missing_critical_evidence) <= 4 then 'queued_for_review'
                        else 'needs_more_evidence'
                    end,
                    case
                        when coalesce(prepared.title_required_flag, true) then 'title_required'
                        when cardinality(prepared.missing_critical_evidence) > 3 then 'needs_more_evidence'
                        else 'queued_for_review'
                    end,
                    coalesce(prepared.surfaced_reason, 'Surfaced from LandIntel evidence.'),
                    prepared.top_positives,
                    prepared.top_warnings,
                    prepared.missing_critical_evidence,
                    coalesce(prepared.title_required_flag, true),
                    case
                        when coalesce(prepared.title_required_flag, true) then 'review_site_before_title_spend'
                        when cardinality(prepared.missing_critical_evidence) > 0 then 'fill_missing_evidence'
                        else 'human_review_required'
                    end,
                    case
                        when cardinality(prepared.missing_critical_evidence) = 0 then 'broad_evidence_present'
                        when cardinality(prepared.missing_critical_evidence) <= 3 then 'partial_evidence_present'
                        else 'early_evidence_only'
                    end,
                    array_remove(array[
                        case when coalesce(prepared.title_required_flag, true) then 'ownership_not_confirmed_until_title_review' end,
                        case when not prepared.market_context_present then 'market_context_missing' end,
                        case when not prepared.demographic_context_present then 'demographic_context_missing' end
                    ]::text[], null),
                    true,
                    'Evidence-led assessment prepared for human review.',
                    jsonb_build_object(
                        'positive_count', cardinality(prepared.top_positives),
                        'warning_count', cardinality(prepared.top_warnings),
                        'missing_evidence_count', cardinality(prepared.missing_critical_evidence)
                    ),
                    jsonb_build_object('assessment_confidence', 'derived_from_current_source_coverage'),
                    jsonb_build_object(
                        'source_key', 'site_assessment_refresh',
                        'planning_decision_status', prepared.latest_decision_status,
                        'constraint_count', coalesce(prepared.constraint_count, 0),
                        'constraint_group_count', coalesce(prepared.constraint_group_count, 0)
                    ),
                    prepared.current_signature,
                    now()
                from prepared
                on conflict (canonical_site_id, source_key, assessment_version) do update set
                    review_tier = excluded.review_tier,
                    site_review_status = excluded.site_review_status,
                    why_site_surfaced = excluded.why_site_surfaced,
                    top_positives = excluded.top_positives,
                    top_warnings = excluded.top_warnings,
                    missing_critical_evidence = excluded.missing_critical_evidence,
                    title_required_flag = excluded.title_required_flag,
                    review_next_action = excluded.review_next_action,
                    evidence_completeness_tier = excluded.evidence_completeness_tier,
                    source_limitation_notes = excluded.source_limitation_notes,
                    human_review_required = excluded.human_review_required,
                    explanation_text = excluded.explanation_text,
                    scores = excluded.scores,
                    score_confidence = excluded.score_confidence,
                    metadata = excluded.metadata,
                    source_record_signature = excluded.source_record_signature,
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join prepared on prepared.canonical_site_id = upserted.canonical_site_id
                where prepared.previous_signature is distinct from prepared.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'site_assessment'
                  and evidence.metadata ->> 'source_key' = 'site_assessment_refresh'
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
                    'site_assessment',
                    'Site assessment refresh engine',
                    changed.id::text,
                    changed.review_next_action,
                    'medium',
                    jsonb_build_object(
                        'source_key', 'site_assessment_refresh',
                        'review_tier', changed.review_tier,
                        'site_review_status', changed.site_review_status,
                        'missing_critical_evidence', changed.missing_critical_evidence
                    )
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'site_assessment'
                  and signal.metadata ->> 'source_key' = 'site_assessment_refresh'
                returning signal.id
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
                    changed.canonical_site_id,
                    'assessment',
                    'site_review_next_action',
                    changed.review_next_action,
                    cardinality(changed.missing_critical_evidence),
                    0.7,
                    'site_assessment',
                    changed.id::text,
                    'site_assessment_output',
                    jsonb_build_object('source', 'site_assessment_refresh'),
                    jsonb_build_object('source_key', 'site_assessment_refresh'),
                    true
                from changed
                returning id
            )
            select
                (select count(*)::integer from selected_sites) as selected_site_count,
                (select count(*)::integer from upserted) as assessment_row_count,
                (select count(*)::integer from changed) as changed_assessment_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}
        self._update_family_lifecycle(
            source_family="site_assessment",
            source_key="site_assessment_refresh",
            row_count=int(proof.get("assessment_row_count") or 0),
            linked_count=int(proof.get("assessment_row_count") or 0),
            measured_count=0,
            evidence_count=int(proof.get("evidence_row_count") or 0),
            signal_count=int(proof.get("signal_row_count") or 0),
        )
        self.database.execute(
            """
            update landintel.source_estate_registry
            set assessment_status = case when :assessment_count > 0 then 'assessment_ready' else assessment_status end,
                updated_at = now()
            where source_key = 'site_assessment_refresh'
              and source_family = 'site_assessment'
            """,
            {"assessment_count": int(proof.get("assessment_row_count") or 0)},
        )
        self._record_expansion_event(
            command_name="refresh-site-assessments",
            source_key="site_assessment_refresh",
            source_family="site_assessment",
            status="success",
            raw_rows=int(proof.get("assessment_row_count") or 0),
            linked_rows=int(proof.get("assessment_row_count") or 0),
            evidence_rows=int(proof.get("evidence_row_count") or 0),
            signal_rows=int(proof.get("signal_row_count") or 0),
            summary="Site assessment outputs refreshed from proven site evidence.",
        )
        self._record_family_freshness("site_assessment", "Site assessment refresh engine", int(proof.get("assessment_row_count") or 0))
        return {"source_family": "site_assessment", **proof}

    def refresh_site_prove_it_assessments(self) -> dict[str, Any]:
        selected_sources = self._selected_sources(source_family="site_conviction")
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
                "source_family": "site_conviction",
                "candidate_site_count": int(candidate_count or 0),
                "dry_run": self.dry_run,
                "audit_only": self.audit_only,
            }

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select
                    site.id as canonical_site_id,
                    site.site_name_primary,
                    site.authority_name,
                    site.surfaced_reason,
                    title.title_required_flag,
                    title.title_review_status,
                    title.title_order_status,
                    title.control_signal_summary,
                    title.planning_applicant_signal,
                    review_row.ownership_outcome,
                    planning.latest_decision_status,
                    planning.approved_count,
                    planning.refused_count,
                    planning.withdrawn_count,
                    planning.live_count,
                    planning.decision_record_count,
                    hla.hla_count,
                    hla.remaining_capacity,
                    ldp.ldp_count,
                    ela.ela_count,
                    vdl.vdl_count,
                    constraints.constraint_count,
                    constraints.constraint_group_count,
                    constraints.max_overlap_pct_of_site,
                    constraints.constraint_character,
                    constraints.flood_constraint_present,
                    constraints.mining_constraint_present,
                    constraints.central_constraint_present,
                    ground.boreholes_within_500m,
                    ground.flood_constraint_present as ground_flood_present,
                    ground.mining_constraint_present as ground_mining_present,
                    market.id is not null as market_context_present,
                    market.market_confidence_tier,
                    amenity.amenity_context_count,
                    open_location.open_location_context_count,
                    demographic.id is not null as demographic_context_present,
                    control.ownership_control_signal_count,
                    control.builder_control_signal_present,
                    control.local_control_signal_present,
                    known_control.known_control_count,
                    existing.source_record_signature as previous_signature
                from landintel.canonical_sites as site
                left join landintel.title_order_workflow as title on title.canonical_site_id = site.id
                left join lateral (
                    select *
                    from landintel.title_review_records as title_review
                    where title_review.canonical_site_id = site.id
                    order by title_review.review_date desc nulls last, title_review.updated_at desc
                    limit 1
                ) as review_row on true
                left join landintel.site_planning_decision_context as planning on planning.canonical_site_id = site.id
                left join lateral (
                    select
                        count(*)::integer as hla_count,
                        coalesce(sum(remaining_capacity), 0)::integer as remaining_capacity
                    from landintel.hla_site_records as record
                    where record.canonical_site_id = site.id
                ) as hla on true
                left join lateral (
                    select count(*)::integer as ldp_count
                    from landintel.ldp_site_records as record
                    where record.canonical_site_id = site.id
                ) as ldp on true
                left join lateral (
                    select count(*)::integer as ela_count
                    from landintel.ela_site_records as record
                    where record.canonical_site_id = site.id
                ) as ela on true
                left join lateral (
                    select count(*)::integer as vdl_count
                    from landintel.vdl_site_records as record
                    where record.canonical_site_id = site.id
                ) as vdl on true
                left join lateral (
                    select
                        count(*)::integer as constraint_count,
                        count(distinct constraint_group)::integer as constraint_group_count,
                        coalesce(max(max_overlap_pct_of_site), 0) as max_overlap_pct_of_site,
                        (array_agg(constraint_character order by max_overlap_pct_of_site desc nulls last))[1] as constraint_character,
                        bool_or(constraint_group ilike '%flood%') as flood_constraint_present,
                        bool_or(constraint_group ilike '%coal%' or constraint_group ilike '%mining%') as mining_constraint_present,
                        bool_or(
                            coalesce(max_overlap_pct_of_site, 0) >= 25
                            and constraint_character = any(array['central', 'core-based']::text[])
                        ) as central_constraint_present
                    from public.site_constraint_group_summaries as summary
                    where summary.site_id = site.id::text
                ) as constraints on true
                left join landintel.site_ground_risk_context as ground on ground.canonical_site_id = site.id
                left join landintel.site_market_context as market on market.canonical_site_id = site.id
                left join lateral (
                    select count(*)::integer as amenity_context_count
                    from landintel.site_amenity_context as amenity_row
                    where amenity_row.canonical_site_id = site.id
                ) as amenity on true
                left join lateral (
                    select count(*)::integer as open_location_context_count
                    from landintel.site_open_location_spine_context as context
                    where context.canonical_site_id = site.id
                ) as open_location on true
                left join landintel.site_demographic_context as demographic on demographic.canonical_site_id = site.id
                left join lateral (
                    select
                        count(*)::integer as ownership_control_signal_count,
                        bool_or(
                            coalesce(signal_label, '') ilike any(array['%miller%', '%persimmon%', '%barratt%', '%bellway%', '%cala%', '%taylor wimpey%', '%housebuilder%', '%promoter%'])
                            or coalesce(signal_value_text, '') ilike any(array['%miller%', '%persimmon%', '%barratt%', '%bellway%', '%cala%', '%taylor wimpey%', '%housebuilder%', '%promoter%'])
                        ) as builder_control_signal_present,
                        bool_or(
                            coalesce(signal_label, '') ilike any(array['%local%', '%private company%', '%trading company%'])
                            or coalesce(signal_value_text, '') ilike any(array['%local%', '%private company%', '%trading company%'])
                        ) as local_control_signal_present
                    from landintel.ownership_control_signals as signal
                    where signal.canonical_site_id = site.id
                ) as control on true
                left join lateral (
                    select count(*)::integer as known_control_count
                    from landintel.known_controlled_sites as controlled
                    where controlled.canonical_site_id = site.id
                ) as known_control on true
                left join landintel.site_prove_it_assessments as existing
                  on existing.canonical_site_id = site.id
                 and existing.source_key = 'prove_it_conviction_layer'
                 and existing.assessment_version = 1
                where (:authority_name = '' or site.authority_name ilike :authority_name_like)
                order by existing.updated_at nulls first, site.updated_at desc nulls last, site.id
                limit :batch_size
            ),
            classified as (
                select
                    selected_sites.*,
                    case
                        when coalesce(hla_count, 0) > 0 or coalesce(ldp_count, 0) > 0 then 'allocated_or_recognised'
                        when coalesce(approved_count, 0) > 0 then 'adjacent_precedent'
                        when coalesce(refused_count, 0) > 0 then 'refusal_repair'
                        when coalesce(vdl_count, 0) > 0 then 'brownfield_regeneration'
                        when coalesce(live_count, 0) > 0 then 'policy_momentum'
                        else 'no_clear_journey'
                    end as planning_journey_type,
                    case
                        when coalesce(constraint_count, 0) = 0 then 'unknown'
                        when coalesce(max_overlap_pct_of_site, 0) >= 95
                         and constraint_character = any(array['central', 'core-based']::text[]) then 'terminal'
                        when coalesce(central_constraint_present, false)
                          or coalesce(flood_constraint_present, false)
                          or coalesce(mining_constraint_present, false)
                          or coalesce(ground_flood_present, false)
                          or coalesce(ground_mining_present, false) then 'major_review'
                        when constraint_character = any(array['edge-based', 'linear', 'fragmented']::text[]) then 'priceable_design_led'
                        else 'context_only'
                    end as constraint_position,
                    case
                        when market_context_present and coalesce(market_confidence_tier, '') ilike '%high%' then 'strong'
                        when market_context_present
                          or coalesce(approved_count, 0) > 0
                          or coalesce(hla_count, 0) > 0 then 'credible'
                        when coalesce(approved_count, 0) = 0
                         and coalesce(live_count, 0) = 0
                         and not market_context_present then 'weak'
                        else 'unproven'
                    end as market_position,
                    case
                        when coalesce(known_control_count, 0) > 0
                          or coalesce(ownership_outcome, '') ilike '%controlled%' then 'known_blocked'
                        when coalesce(builder_control_signal_present, false) then 'likely_controlled_by_housebuilder_promoter'
                        when title_review_status = 'reviewed' then 'known_and_attractive'
                        when coalesce(local_control_signal_present, false) then 'likely_local_trading_company'
                        when coalesce(hla_count, 0) > 0
                          or coalesce(ldp_count, 0) > 0
                          or coalesce(approved_count, 0) > 0
                          or market_context_present then 'unknown_but_worth_title_spend'
                        when coalesce(title_required_flag, true) then 'unknown_not_worth_title_spend'
                        else 'ownership_not_confirmed'
                    end as control_position
                from selected_sites
            ),
            drivered as (
                select
                    classified.*,
                    array_remove(array[
                        case when planning_journey_type is distinct from 'no_clear_journey' then 'planning_angle' end,
                        case when coalesce(vdl_count, 0) > 0 or coalesce(ela_count, 0) > 0 then 'mispricing_or_overlooked_angle' end,
                        case when control_position = any(array['likely_local_trading_company', 'likely_controlled_by_small_private_company', 'unknown_but_worth_title_spend']::text[]) then 'control_opportunity' end,
                        case when market_position = any(array['strong', 'credible']::text[]) then 'buyer_angle' end,
                        case when coalesce(live_count, 0) > 0 or coalesce(approved_count, 0) > 0 then 'timing_angle' end
                    ]::text[], null) as prove_it_drivers
                from classified
            ),
            proofed as (
                select
                    drivered.*,
                    coalesce((
                        select jsonb_agg(point order by ord)
                        from (
                            values
                                (1, case when coalesce(hla_count, 0) > 0 then jsonb_build_object('fact', 'Site has HLA evidence', 'source', 'HLA', 'confidence', 'high', 'evidence_type', 'direct') end),
                                (2, case when coalesce(ldp_count, 0) > 0 then jsonb_build_object('fact', 'Site has LDP allocation or policy evidence', 'source', 'LDP', 'confidence', 'high', 'evidence_type', 'direct') end),
                                (3, case when coalesce(vdl_count, 0) > 0 then jsonb_build_object('fact', 'Vacant or derelict land evidence exists', 'source', 'VDL', 'confidence', 'medium', 'evidence_type', 'direct') end),
                                (4, case when coalesce(ela_count, 0) > 0 then jsonb_build_object('fact', 'Employment land evidence exists', 'source', 'ELA', 'confidence', 'medium', 'evidence_type', 'direct') end),
                                (5, case when coalesce(approved_count, 0) > 0 then jsonb_build_object('fact', coalesce(approved_count, 0)::text || ' approval record(s) linked to this site context', 'source', 'Planning records', 'confidence', 'medium', 'evidence_type', 'direct') end),
                                (6, case when coalesce(refused_count, 0) > 0 then jsonb_build_object('fact', coalesce(refused_count, 0)::text || ' refusal record(s) linked to this site context', 'source', 'Planning records', 'confidence', 'medium', 'evidence_type', 'direct') end),
                                (7, case when coalesce(live_count, 0) > 0 then jsonb_build_object('fact', coalesce(live_count, 0)::text || ' live planning record(s) linked to this site context', 'source', 'Planning records', 'confidence', 'medium', 'evidence_type', 'direct') end),
                                (8, case when coalesce(constraint_count, 0) > 0 then jsonb_build_object('fact', coalesce(constraint_count, 0)::text || ' measured constraint summary row(s)', 'source', 'Constraint measurements', 'confidence', 'high', 'evidence_type', 'direct') end),
                                (9, case when coalesce(flood_constraint_present, false) or coalesce(ground_flood_present, false) then jsonb_build_object('fact', 'Flood evidence is present in measured constraints or abnormal-risk context', 'source', 'SEPA / constraint measurement', 'confidence', 'high', 'evidence_type', 'direct') end),
                                (10, case when coalesce(mining_constraint_present, false) or coalesce(ground_mining_present, false) then jsonb_build_object('fact', 'Mining or coal evidence is present', 'source', 'Coal Authority / constraint measurement', 'confidence', 'high', 'evidence_type', 'direct') end),
                                (11, case when market_context_present then jsonb_build_object('fact', 'Market context exists for this site', 'source', 'Market context engine', 'confidence', 'medium', 'evidence_type', 'contextual') end),
                                (12, case when coalesce(amenity_context_count, 0) + coalesce(open_location_context_count, 0) > 0 then jsonb_build_object('fact', 'Location and amenity context exists', 'source', 'Amenities / open location spine', 'confidence', 'medium', 'evidence_type', 'contextual') end),
                                (13, case when demographic_context_present then jsonb_build_object('fact', 'Demographic context exists', 'source', 'Demographics engine', 'confidence', 'medium', 'evidence_type', 'contextual') end),
                                (14, case when coalesce(title_required_flag, true) then jsonb_build_object('fact', 'Ownership is not confirmed before title review', 'source', 'Title readiness workflow', 'confidence', 'high', 'evidence_type', 'direct') end),
                                (15, case when coalesce(ownership_control_signal_count, 0) > 0 then jsonb_build_object('fact', coalesce(ownership_control_signal_count, 0)::text || ' ownership/control signal(s) exist', 'source', 'Title/control workflow', 'confidence', 'medium', 'evidence_type', 'inferred') end)
                        ) as proof(ord, point)
                        where point is not null
                    ), '[]'::jsonb) as proof_points
                from drivered
            ),
            narrated as (
                select
                    proofed.*,
                    case
                        when jsonb_array_length(proof_points) >= 6
                         and planning_journey_type is distinct from 'no_clear_journey'
                         and market_position = any(array['strong', 'credible']::text[])
                         and constraint_position is distinct from 'terminal' then 'high'
                        when cardinality(prove_it_drivers) >= 2
                         and jsonb_array_length(proof_points) >= 4
                         and planning_journey_type is distinct from 'no_clear_journey' then 'medium'
                        when jsonb_array_length(proof_points) >= 2 then 'low'
                        when jsonb_array_length(proof_points) > 0 then 'low'
                        else 'insufficient'
                    end as evidence_confidence,
                    case
                        when constraint_position = 'terminal' then array['Whole-site or central constraint evidence requires review before any spend']
                        else '{}'::text[]
                    end
                    || array_remove(array[
                        case when coalesce(title_required_flag, true) then 'Ownership not confirmed' end,
                        case when control_position = 'likely_controlled_by_housebuilder_promoter' then 'Likely housebuilder/promoter control signal' end,
                        case when constraint_position = 'major_review' then 'Major constraint review required' end,
                        case when planning_journey_type = 'no_clear_journey' then 'Planning journey is not clear' end,
                        case when market_position = any(array['weak', 'unproven']::text[]) then 'Buyer or market evidence is not proven' end
                    ]::text[], null) as warning_candidates,
                    array_remove(array[
                        case when coalesce(title_review_status, 'not_reviewed') is distinct from 'reviewed' then 'Title not reviewed' end,
                        case when coalesce(decision_record_count, 0) = 0 then 'Planning decision evidence not reviewed' end,
                        case when coalesce(constraint_count, 0) = 0 then 'Measured constraints missing' end,
                        case when not market_context_present then 'Buyer or market appetite evidence missing' end,
                        case when not demographic_context_present then 'Demographic context missing' end
                    ]::text[], null) as gap_candidates,
                    array_remove(array[
                        case when planning_journey_type = 'allocated_or_recognised' then 'Planning journey exists through allocation or HLA/LDP recognition' end,
                        case when planning_journey_type = 'adjacent_precedent' then 'Residential precedent or planning approval evidence exists' end,
                        case when planning_journey_type = 'brownfield_regeneration' then 'Brownfield or vacant-land angle exists' end,
                        case when market_position = any(array['strong', 'credible']::text[]) then 'Buyer or market context exists' end,
                        case when control_position = any(array['likely_local_trading_company', 'unknown_but_worth_title_spend']::text[]) then 'Control route may justify title spend' end,
                        case when constraint_position = 'priceable_design_led' then 'Measured constraint appears design-led rather than central' end
                    ]::text[], null) as positive_candidates
                from proofed
            ),
            finalised as (
                select
                    narrated.*,
                    case
                        when cardinality(positive_candidates) = 0 then '{}'::text[]
                        else positive_candidates[1:3]
                    end as top_positives,
                    case
                        when cardinality(warning_candidates) = 0 then array['No major warning identified yet']
                        else warning_candidates[1:3]
                    end as top_warnings,
                    case
                        when cardinality(gap_candidates) = 0 then array['No decision-critical evidence gap identified yet']
                        else gap_candidates[1:3]
                    end as missing_critical_evidence,
                    case
                        when constraint_position = 'terminal'
                          or cardinality(prove_it_drivers) = 0
                          or jsonb_array_length(proof_points) = 0 then 'ignore'
                        when planning_journey_type is distinct from 'no_clear_journey'
                         and market_position = any(array['strong', 'credible']::text[])
                         and control_position <> all(array['known_blocked', 'likely_controlled_by_housebuilder_promoter']::text[])
                         and constraint_position <> all(array['terminal', 'major_review']::text[])
                         and evidence_confidence = any(array['high', 'medium']::text[]) then 'pursue'
                        when planning_journey_type is distinct from 'no_clear_journey'
                          or market_position = 'credible'
                          or control_position = 'unknown_but_worth_title_spend' then 'review'
                        else 'monitor'
                    end as verdict
                from narrated
            ),
            actioned as (
                select
                    finalised.*,
                    case
                        when verdict = 'ignore'
                          or constraint_position = 'terminal'
                          or (planning_journey_type = 'no_clear_journey' and market_position = 'weak') then 'do_not_order'
                        when verdict = 'pursue'
                         and coalesce(title_review_status, 'not_reviewed') is distinct from 'reviewed'
                         and control_position = 'unknown_but_worth_title_spend' then 'order_title_urgently'
                        when verdict = any(array['review', 'pursue']::text[])
                         and coalesce(title_review_status, 'not_reviewed') is distinct from 'reviewed'
                         and evidence_confidence = any(array['high', 'medium']::text[])
                         and constraint_position <> 'major_review' then 'order_title'
                        else 'manual_review_before_order'
                    end as title_spend_recommendation,
                    case
                        when verdict = 'ignore' then 'Do not spend title money until a planning, control, timing or buyer angle appears.'
                        when constraint_position = 'major_review' then 'Constraint interpretation should happen before title spend.'
                        when coalesce(title_review_status, 'not_reviewed') = 'reviewed' then 'Title has already been reviewed; use reviewed ownership evidence.'
                        when control_position = 'unknown_but_worth_title_spend' then 'Ownership is unknown and would materially affect the next commercial decision.'
                        else 'Manual review should confirm whether title spend is justified.'
                    end as title_spend_reason
                from finalised
            ),
            next_actioned as (
                select
                    actioned.*,
                    case
                        when verdict = 'pursue' and title_spend_recommendation = 'order_title_urgently' then 'Order title urgently.'
                        when title_spend_recommendation = 'order_title' then 'Order title.'
                        when constraint_position = 'major_review' then 'Run constraint review before title spend.'
                        when planning_journey_type is distinct from 'no_clear_journey' then 'Review planning documents before title spend.'
                        when verdict = 'monitor' then 'Monitor only. Do not spend time or title money yet.'
                        else 'Ignore until new evidence appears.'
                    end as review_next_action
                from actioned
            ),
            completed as (
                select
                    next_actioned.*,
                    case
                        when verdict = 'ignore' then
                            'This is not currently an opportunity because current evidence does not justify LDN spending the next pound or hour.'
                        when verdict = 'monitor' then
                            'This site is worth monitoring because it has weak or incomplete evidence, but not enough to justify active LDN time yet.'
                        when verdict = 'review' then
                            'This site is worth LDN review because it has a credible evidence thread, but key commercial gaps still need testing.'
                        else
                            'This site is worth LDN attention because planning, market and control evidence may justify active pursuit.'
                    end as claim_statement,
                    concat_ws(
                        ' ',
                        case
                            when planning_journey_type = 'allocated_or_recognised' then 'Planning evidence suggests the site is already recognised in policy or supply data.'
                            when planning_journey_type = 'adjacent_precedent' then 'Planning evidence suggests residential development is not alien to this location.'
                            when planning_journey_type = 'refusal_repair' then 'Planning evidence is mixed; refusal reasons need review before spend.'
                            when planning_journey_type = 'brownfield_regeneration' then 'The site may be an overlooked regeneration lead rather than a standard greenfield search result.'
                            when planning_journey_type = 'policy_momentum' then 'Live planning activity creates a timing reason to keep the site under review.'
                            else 'No clear planning journey is proven yet.'
                        end,
                        case
                            when constraint_position = 'terminal' then 'Constraint evidence may prevent a commercial route unless specialist review changes that view.'
                            when constraint_position = 'major_review' then 'Constraints need interpretation before LDN spends on ownership.'
                            when constraint_position = 'priceable_design_led' then 'Measured constraints appear more likely to affect layout or pricing than remove the whole opportunity.'
                            when constraint_position = 'context_only' then 'Constraint evidence is present but currently looks contextual.'
                            else 'Constraint evidence is not yet strong enough to interpret.'
                        end,
                        case
                            when control_position = 'likely_controlled_by_housebuilder_promoter' then 'Control evidence weakens the opportunity because a housebuilder or promoter signal is present.'
                            when control_position = 'unknown_but_worth_title_spend' then 'Ownership is unknown, but current evidence may justify targeted title spend.'
                            when control_position = 'likely_local_trading_company' then 'A local company signal may support a direct-to-owner control route.'
                            else 'Ownership remains a controlled evidence gap.'
                        end,
                        case
                            when market_position = any(array['strong', 'credible']::text[]) then 'Market context gives a possible exit argument, but buyer demand is not treated as certain.'
                            else 'Buyer demand is not yet proven.'
                        end
                    ) as interpretation_text,
                    (
                        verdict = any(array['review', 'pursue']::text[])
                        and cardinality(prove_it_drivers) > 0
                        and jsonb_array_length(proof_points) > 0
                        and title_spend_recommendation is not null
                        and review_next_action is not null
                    ) as review_ready_flag,
                    md5(concat_ws(
                        '|',
                        canonical_site_id::text,
                        verdict,
                        title_spend_recommendation,
                        planning_journey_type,
                        constraint_position,
                        market_position,
                        control_position,
                        evidence_confidence,
                        prove_it_drivers::text,
                        proof_points::text,
                        top_warnings::text,
                        missing_critical_evidence::text,
                        review_next_action
                    )) as current_signature
                from next_actioned
            ),
            upserted as (
                insert into landintel.site_prove_it_assessments (
                    canonical_site_id,
                    assessment_version,
                    source_key,
                    source_family,
                    claim_statement,
                    prove_it_drivers,
                    proof_points,
                    interpretation_text,
                    top_positives,
                    top_warnings,
                    missing_critical_evidence,
                    title_spend_recommendation,
                    title_spend_reason,
                    constraint_position,
                    planning_journey_type,
                    market_position,
                    control_position,
                    evidence_confidence,
                    verdict,
                    review_next_action,
                    review_ready_flag,
                    source_record_signature,
                    metadata,
                    updated_at
                )
                select
                    canonical_site_id,
                    1,
                    'prove_it_conviction_layer',
                    'site_conviction',
                    claim_statement,
                    prove_it_drivers,
                    proof_points,
                    interpretation_text,
                    top_positives,
                    top_warnings,
                    missing_critical_evidence,
                    title_spend_recommendation,
                    title_spend_reason,
                    constraint_position,
                    planning_journey_type,
                    market_position,
                    control_position,
                    evidence_confidence,
                    verdict,
                    review_next_action,
                    review_ready_flag,
                    current_signature,
                    jsonb_build_object(
                        'source_key', 'prove_it_conviction_layer',
                        'evidence_standard', 'next_pound_or_hour',
                        'title_ownership_limitation', 'ownership_not_confirmed_until_title_review',
                        'site_name', site_name_primary,
                        'authority_name', authority_name
                    ),
                    now()
                from completed
                on conflict (canonical_site_id, source_key, assessment_version) do update set
                    claim_statement = excluded.claim_statement,
                    prove_it_drivers = excluded.prove_it_drivers,
                    proof_points = excluded.proof_points,
                    interpretation_text = excluded.interpretation_text,
                    top_positives = excluded.top_positives,
                    top_warnings = excluded.top_warnings,
                    missing_critical_evidence = excluded.missing_critical_evidence,
                    title_spend_recommendation = excluded.title_spend_recommendation,
                    title_spend_reason = excluded.title_spend_reason,
                    constraint_position = excluded.constraint_position,
                    planning_journey_type = excluded.planning_journey_type,
                    market_position = excluded.market_position,
                    control_position = excluded.control_position,
                    evidence_confidence = excluded.evidence_confidence,
                    verdict = excluded.verdict,
                    review_next_action = excluded.review_next_action,
                    review_ready_flag = excluded.review_ready_flag,
                    source_record_signature = excluded.source_record_signature,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*, completed.previous_signature
                from upserted
                join completed on completed.canonical_site_id = upserted.canonical_site_id
                where completed.previous_signature is distinct from completed.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = 'site_conviction'
                  and evidence.metadata ->> 'source_key' = 'prove_it_conviction_layer'
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
                    'site_conviction',
                    'LandIntel Prove It conviction layer',
                    changed.id::text,
                    changed.verdict,
                    case
                        when changed.evidence_confidence = 'high' then 'high'
                        when changed.evidence_confidence = any(array['medium', 'mixed']::text[]) then 'medium'
                        else 'low'
                    end,
                    jsonb_build_object(
                        'source_key', 'prove_it_conviction_layer',
                        'verdict', changed.verdict,
                        'title_spend_recommendation', changed.title_spend_recommendation,
                        'prove_it_drivers', changed.prove_it_drivers,
                        'proof_point_count', jsonb_array_length(changed.proof_points)
                    )
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = 'site_conviction'
                  and signal.metadata ->> 'source_key' = 'prove_it_conviction_layer'
                returning signal.id
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
                    changed.canonical_site_id,
                    'conviction',
                    'prove_it_verdict',
                    changed.verdict,
                    jsonb_array_length(changed.proof_points),
                    case
                        when changed.evidence_confidence = 'high' then 0.85
                        when changed.evidence_confidence = any(array['medium', 'mixed']::text[]) then 0.65
                        else 0.35
                    end,
                    'site_conviction',
                    changed.id::text,
                    'prove_it_conviction_output',
                    jsonb_build_object('source_key', 'prove_it_conviction_layer'),
                    jsonb_build_object(
                        'source_key', 'prove_it_conviction_layer',
                        'title_spend_recommendation', changed.title_spend_recommendation,
                        'review_ready_flag', changed.review_ready_flag
                    ),
                    true
                from changed
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
                    'site_conviction',
                    changed.id::text,
                    'prove_it_assessment_changed',
                    'LandIntel Prove It verdict or next action changed.',
                    changed.previous_signature,
                    changed.source_record_signature,
                    false,
                    jsonb_build_object(
                        'source_key', 'prove_it_conviction_layer',
                        'verdict', changed.verdict,
                        'review_next_action', changed.review_next_action
                    )
                from changed
                returning id
            )
            select
                (select count(*)::integer from selected_sites) as selected_site_count,
                (select count(*)::integer from upserted) as prove_it_assessment_count,
                (select count(*)::integer from changed) as changed_assessment_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count,
                (select count(*)::integer from inserted_events) as change_event_count,
                (select count(*)::integer from upserted where review_ready_flag) as review_ready_count,
                (select count(*)::integer from upserted where verdict = 'ignore') as ignore_count,
                (select count(*)::integer from upserted where verdict = 'monitor') as monitor_count,
                (select count(*)::integer from upserted where verdict = 'review') as review_count,
                (select count(*)::integer from upserted where verdict = 'pursue') as pursue_count
            """,
            {
                "batch_size": self.batch_size,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
            },
        ) or {}

        assessment_count = int(proof.get("prove_it_assessment_count") or 0)
        evidence_count = int(proof.get("evidence_row_count") or 0)
        signal_count = int(proof.get("signal_row_count") or 0)
        change_event_count = int(proof.get("change_event_count") or 0)
        self._update_family_lifecycle(
            source_family="site_conviction",
            source_key="prove_it_conviction_layer",
            row_count=assessment_count,
            linked_count=assessment_count,
            measured_count=0,
            evidence_count=evidence_count,
            signal_count=signal_count,
        )
        self.database.execute(
            """
            update landintel.source_estate_registry
            set assessment_status = case when :assessment_count > 0 then 'assessment_ready' else assessment_status end,
                updated_at = now()
            where source_key = 'prove_it_conviction_layer'
              and source_family = 'site_conviction'
            """,
            {"assessment_count": assessment_count},
        )
        self._record_expansion_event(
            command_name="refresh-site-prove-it-assessments",
            source_key="prove_it_conviction_layer",
            source_family="site_conviction",
            status="success",
            raw_rows=assessment_count,
            linked_rows=assessment_count,
            evidence_rows=evidence_count,
            signal_rows=signal_count,
            change_event_rows=change_event_count,
            summary="Prove It conviction assessments refreshed from current site evidence.",
            metadata=proof,
        )
        self._record_family_freshness(
            "site_conviction",
            "LandIntel Prove It conviction layer",
            assessment_count,
        )
        audit = self.audit_site_prove_it_assessments(log_event=False)
        return {"source_family": "site_conviction", **proof, "coverage": audit.get("coverage", {})}

    def audit_site_prove_it_assessments(self, log_event: bool = True) -> dict[str, Any]:
        coverage = self.database.fetch_one("select * from analytics.v_site_prove_it_coverage") or {}
        sample_rows = self.database.fetch_all(
            """
            select
                canonical_site_id::text,
                site_name_primary,
                authority_name,
                verdict,
                title_spend_recommendation,
                evidence_confidence,
                planning_journey_type,
                constraint_position,
                market_position,
                control_position,
                review_ready_flag,
                review_next_action
            from analytics.v_site_prove_it_assessments
            order by updated_at desc nulls last, site_name_primary
            limit 20
            """
        )
        result = {"coverage": coverage, "sample_assessments": sample_rows}
        if log_event and not self.dry_run and not self.audit_only:
            self._record_expansion_event(
                command_name="audit-site-prove-it-assessments",
                source_key="prove_it_conviction_layer",
                source_family="site_conviction",
                status="success",
                raw_rows=int(coverage.get("prove_it_assessment_count") or 0),
                linked_rows=int(coverage.get("assessed_site_count") or 0),
                evidence_rows=int(coverage.get("review_ready_site_count") or 0),
                summary="Prove It conviction layer audited.",
                metadata=result,
            )
        self.logger.info("site_prove_it_assessment_audit", extra=result)
        return result

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
        prove_it_coverage = self.database.fetch_one(
            """
            select *
            from analytics.v_site_prove_it_coverage
            """
        ) or {}
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
            "prove_it_coverage": prove_it_coverage,
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
                    "prove_it_coverage": prove_it_coverage,
                },
            )
        self.logger.info("full_source_estate_audit", extra=result)
        return result

    def _replace_evidence_and_signal(
        self,
        *,
        canonical_site_id: str,
        source_family: str,
        source_key: str,
        source_dataset: str,
        source_record_id: str,
        source_reference: str,
        signal_family: str,
        signal_name: str,
        signal_value_text: str,
        confidence_numeric: float,
    ) -> None:
        self.database.execute(
            """
            delete from landintel.evidence_references
            where canonical_site_id = cast(:canonical_site_id as uuid)
              and source_family = :source_family
              and source_record_id = :source_record_id
            """,
            {
                "canonical_site_id": canonical_site_id,
                "source_family": source_family,
                "source_record_id": source_record_id,
            },
        )
        self.database.execute(
            """
            insert into landintel.evidence_references (
                canonical_site_id,
                source_family,
                source_dataset,
                source_record_id,
                source_reference,
                confidence,
                metadata
            ) values (
                cast(:canonical_site_id as uuid),
                :source_family,
                :source_dataset,
                :source_record_id,
                :source_reference,
                'medium',
                jsonb_build_object('source_key', :source_key)
            )
            """,
            {
                "canonical_site_id": canonical_site_id,
                "source_family": source_family,
                "source_dataset": source_dataset,
                "source_record_id": source_record_id,
                "source_reference": source_reference,
                "source_key": source_key,
            },
        )
        self.database.execute(
            """
            delete from landintel.site_signals
            where canonical_site_id = cast(:canonical_site_id as uuid)
              and source_family = :source_family
              and source_record_id = :source_record_id
            """,
            {
                "canonical_site_id": canonical_site_id,
                "source_family": source_family,
                "source_record_id": source_record_id,
            },
        )
        self.database.execute(
            """
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
                cast(:canonical_site_id as uuid),
                :signal_family,
                :signal_name,
                :signal_value_text,
                :confidence_numeric,
                :confidence_numeric,
                :source_family,
                :source_record_id,
                'source_evidence_signal',
                jsonb_build_object('source', :source_key),
                jsonb_build_object('source_key', :source_key),
                true
            )
            """,
            {
                "canonical_site_id": canonical_site_id,
                "signal_family": signal_family,
                "signal_name": signal_name,
                "signal_value_text": signal_value_text,
                "confidence_numeric": confidence_numeric,
                "source_family": source_family,
                "source_record_id": source_record_id,
                "source_key": source_key,
            },
        )

    def _ingest_companies_house_charges(
        self,
        client: httpx.Client,
        enrichment_id: str,
        candidate: dict[str, Any],
        company_number: str,
    ) -> int:
        try:
            response = client.get(
                f"https://api.company-information.service.gov.uk/company/{company_number}/charges",
                params={"items_per_page": 25},
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("companies_house_charges_error", extra={"company_number": company_number, "error": str(exc)})
            return 0
        charge_count = 0
        for item in payload.get("items") or []:
            charge_code = str(item.get("charge_code") or item.get("id") or _stable_key(_json_dumps(item)))
            persons_entitled = item.get("persons_entitled") or []
            lender_name = None
            if persons_entitled and isinstance(persons_entitled[0], dict):
                lender_name = persons_entitled[0].get("name")
            self.database.execute(
                """
                insert into landintel.corporate_charge_records (
                    canonical_site_id,
                    corporate_entity_enrichment_id,
                    source_key,
                    source_family,
                    company_number,
                    charge_code,
                    charge_status,
                    lender_name,
                    created_on,
                    delivered_on,
                    source_url,
                    source_record_signature,
                    raw_payload,
                    updated_at
                ) values (
                    cast(:canonical_site_id as uuid),
                    cast(:corporate_entity_enrichment_id as uuid),
                    'companies_house_charges',
                    'corporate_control',
                    :company_number,
                    :charge_code,
                    :charge_status,
                    :lender_name,
                    cast(:created_on as date),
                    cast(:delivered_on as date),
                    :source_url,
                    :source_record_signature,
                    cast(:raw_payload as jsonb),
                    now()
                )
                on conflict (source_key, company_number, charge_code) do update set
                    charge_status = excluded.charge_status,
                    lender_name = excluded.lender_name,
                    created_on = excluded.created_on,
                    delivered_on = excluded.delivered_on,
                    source_url = excluded.source_url,
                    source_record_signature = excluded.source_record_signature,
                    raw_payload = excluded.raw_payload,
                    updated_at = now()
                """,
                {
                    "canonical_site_id": candidate.get("canonical_site_id"),
                    "corporate_entity_enrichment_id": enrichment_id,
                    "company_number": company_number,
                    "charge_code": charge_code,
                    "charge_status": item.get("status"),
                    "lender_name": lender_name,
                    "created_on": item.get("created_on"),
                    "delivered_on": item.get("delivered_on"),
                    "source_url": f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/charges/{charge_code}",
                    "source_record_signature": _stable_key(_json_dumps(item)),
                    "raw_payload": _json_dumps(item),
                },
            )
            charge_count += 1
        return charge_count

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
        source_key = f"{source_family}_refresh"
        for source_row in self.sources:
            if source_row.get("source_family") == source_family and source_row.get("source_name") == source_name:
                source_key = str(source_row.get("source_key"))
                break
        source = {
            "source_key": source_key,
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


def _stable_key(value: str) -> str:
    import hashlib

    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _normalise_company_query(value: Any) -> str:
    text_value = re.sub(r"\s+", " ", str(value or "")).strip()
    text_value = re.sub(r"\b(the|group|homes|developments?)\b", "", text_value, flags=re.I).strip()
    return text_value[:160]


def _extract_document_urls(payload: Any) -> list[str]:
    urls: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key).lower()
                if isinstance(child, str) and child.startswith(("http://", "https://")):
                    child_lower = child.lower()
                    if any(token in key_text or token in child_lower for token in ("document", "notice", "report", "committee", "section75", "section-75", "s75", ".pdf")):
                        urls.append(child)
                else:
                    walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, str) and value.startswith(("http://", "https://")):
            if any(token in value.lower() for token in ("document", "notice", "report", "committee", "section75", "section-75", "s75", ".pdf")):
                urls.append(value)

    walk(payload or {})
    return sorted(set(urls))


def _classify_document_type(url: str) -> str:
    lowered = url.lower()
    if "section75" in lowered or "section-75" in lowered or "s75" in lowered:
        return "section75"
    if "committee" in lowered:
        return "committee_report"
    if "officer" in lowered:
        return "officer_report"
    if "decision" in lowered or "notice" in lowered:
        return "decision_notice"
    if "flood" in lowered:
        return "flood_report"
    if "transport" in lowered:
        return "transport_report"
    return "planning_document"


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
        print(traceback.format_exc(), file=sys.stderr)
        try:
            runner.logger.error(
                "phase2_source_command_failed",
                extra={"command": args.command, "exception": str(exc), "traceback": traceback.format_exc()},
            )
        except Exception:
            print(f"phase2_source_command_failed: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            runner.close()
        except Exception as exc:
            print(f"phase2_source_runner_close_failed: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())

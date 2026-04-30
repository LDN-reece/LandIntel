"""Phase 2 source-estate registration, refresh commands and proof audit."""

from __future__ import annotations

import argparse
import json
import os
import re
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
            if command == "ingest-planning-appeals":
                result = self.ingest_planning_appeals()
            elif command == "ingest-planning-documents":
                result = self.ingest_planning_documents()
            elif command == "ingest-amenities":
                result = self.ingest_amenities()
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

        proof = self.database.fetch_one(
            """
            with source_features as (
                select
                    feature.id,
                    feature.source_feature_key,
                    feature.feature_name,
                    feature.source_reference,
                    feature.geometry,
                    feature.raw_payload,
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
            nearest_assets as (
                select distinct on (site.id, asset.amenity_type)
                    site.id as canonical_site_id,
                    asset.amenity_type,
                    asset.id as nearest_asset_id,
                    asset.amenity_name,
                    st_distance(site.geometry, asset.geometry) as nearest_distance_m,
                    count(asset.id) filter (where st_dwithin(site.geometry, asset.geometry, 400)) over (partition by site.id, asset.amenity_type) as count_within_400m,
                    count(asset.id) filter (where st_dwithin(site.geometry, asset.geometry, 800)) over (partition by site.id, asset.amenity_type) as count_within_800m,
                    count(asset.id) filter (where st_dwithin(site.geometry, asset.geometry, 1600)) over (partition by site.id, asset.amenity_type) as count_within_1600m
                from selected_sites as site
                join landintel.amenity_assets as asset
                  on asset.geometry is not null
                 and st_dwithin(site.geometry, asset.geometry, 1600)
                order by site.id, asset.amenity_type, st_distance(site.geometry, asset.geometry), asset.id
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
                    'os_places_amenity_context',
                    'amenities',
                    nearest_assets.amenity_type,
                    nearest_assets.nearest_asset_id,
                    nearest_assets.amenity_name,
                    round(nearest_assets.nearest_distance_m::numeric, 2),
                    nearest_assets.count_within_400m::integer,
                    nearest_assets.count_within_800m::integer,
                    nearest_assets.count_within_1600m::integer,
                    md5(concat_ws('|', nearest_assets.canonical_site_id::text, nearest_assets.amenity_type, nearest_assets.nearest_asset_id::text, round(nearest_assets.nearest_distance_m::numeric, 2)::text)),
                    jsonb_build_object('source_key', 'os_places_amenity_context'),
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
                        'source_key', 'os_places_amenity_context',
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
                    jsonb_build_object('source', 'os_places_amenity_context'),
                    jsonb_build_object('source_key', 'os_places_amenity_context'),
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
        self._update_family_lifecycle(
            source_family="amenities",
            source_key="os_places_amenity_context",
            row_count=int(proof.get("asset_count") or 0),
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
        )
        self._record_family_freshness("amenities", "OS Places and Features amenity context", int(proof.get("asset_count") or 0))
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
        runner.logger.error(
            "phase2_source_command_failed",
            extra={"command": args.command, "exception": str(exc), "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

"""Incremental reconcile worker for planning and HLA source records."""

from __future__ import annotations

import argparse
import json
import os
import socket
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from shapely import wkb as shapely_wkb
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from sqlalchemy import text

from config.settings import Settings, get_settings
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.source_phase_runner import (
    SourcePhaseRunner,
    _geometry_hex,
    _json_default,
    _normalize_ref,
    _polygonize_geometry,
)

PLANNING_DATASET = "Planning Applications: Official - Scotland"
HLA_DATASET = "Housing Land Supply - Scotland"
PROVISIONAL_LABEL = "Provisional link — analyst review required"
BLOCKED_STRUCTURAL_LABEL = "Blocked — structural review required"
REVIEW_REASON_CODES = {
    "reference_conflict",
    "trusted_alias_conflict",
    "spatial_ambiguous",
    "weak_spatial_overlap",
    "geometry_missing_for_new_record",
    "new_site_below_area_floor",
    "near_existing_site_conflict",
    "reassignment_conflict",
    "possible_merge",
    "possible_split",
    "retirement_orphan_risk",
    "data_integrity_anomaly",
}
STRUCTURAL_REVIEW_REASON_CODES = {"possible_merge", "possible_split", "reassignment_conflict"}


@dataclass(slots=True)
class MatchOutcome:
    status: str
    publish_state: str
    canonical_site_id: str | None = None
    previous_canonical_site_id: str | None = None
    match_method: str | None = None
    match_confidence: float | None = None
    link_method: str | None = None
    review_reason_code: str | None = None
    candidate_site_ids: list[str] | None = None
    affected_site_ids: list[str] | None = None
    created_new_site: bool = False


class IncrementalReconcileRunner(SourcePhaseRunner):
    """Process only changed planning and HLA source records into canonical sites."""

    def __init__(self, settings: Settings, logger) -> None:
        super().__init__(settings, logger)
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self.max_attempts = settings.reconcile_max_attempts

    def process_reconcile_queue(self, *, limit: int | None = None, runtime_minutes: int | None = None) -> dict[str, int]:
        effective_limit = limit or self.settings.reconcile_queue_batch_limit
        effective_runtime_minutes = runtime_minutes or self.settings.reconcile_runtime_minutes
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="process_reconcile_queue",
                source_name="landintel.source_reconcile_queue",
                status="running",
                metadata={"worker_id": self.worker_id, "limit": effective_limit, "runtime_minutes": effective_runtime_minutes},
            )
        )
        started = monotonic()
        stats = {
            "claimed": 0,
            "completed": 0,
            "review_required": 0,
            "blocked": 0,
            "superseded": 0,
            "retryable_failed": 0,
            "dead_letter": 0,
            "refresh_enqueued": 0,
        }
        try:
            while not self._runtime_limit_reached(started, effective_runtime_minutes):
                remaining = max(effective_limit - stats["claimed"], 0)
                if remaining == 0:
                    break
                stats["superseded"] += self._supersede_stale_reconcile_items(
                    max(min(remaining, self.settings.reconcile_queue_batch_limit) * 5, 1)
                )
                if self._runtime_limit_reached(started, effective_runtime_minutes):
                    break
                batch = self._claim_reconcile_items(min(remaining, self.settings.reconcile_queue_batch_limit))
                if not batch:
                    break
                stats["claimed"] += len(batch)
                for item in batch:
                    if self._runtime_limit_reached(started, effective_runtime_minutes):
                        break
                    try:
                        processed = self._process_reconcile_item(item, run_id)
                    except Exception as exc:  # pragma: no cover - defensive runtime protection
                        outcome = self._handle_processing_error(item, exc)
                        stats[outcome] += 1
                        continue
                    stats[processed] += 1
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=stats["claimed"],
                    records_loaded=stats["completed"],
                    records_retained=stats["review_required"] + stats["blocked"],
                    metadata=stats,
                    finished=True,
                ),
            )
            self.logger.info("incremental_reconcile_queue_processed", extra=stats)
            return stats
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def reconcile_catchup_scan(self, *, source_family: str | None = None) -> dict[str, int]:
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="reconcile_catchup_scan",
                source_name="landintel.source_reconcile_state",
                status="running",
                metadata={"source_family": source_family},
            )
        )
        try:
            result = {"planning": 0, "hla": 0}
            if source_family in (None, "planning"):
                latest_planning = self.database.fetch_one(
                    """
                    select id
                    from public.ingest_runs
                    where run_type = 'ingest_planning_history'
                      and status = 'success'
                    order by finished_at desc nulls last, created_at desc nulls last
                    limit 1
                    """
                )
                if latest_planning:
                    result["planning"] = int(
                        self.database.scalar(
                            "select landintel.queue_planning_reconcile_from_ingest(cast(:run_id as uuid))",
                            {"run_id": latest_planning["id"]},
                        )
                    )
            if source_family in (None, "hla"):
                latest_hla = self.database.fetch_one(
                    """
                    select id
                    from public.ingest_runs
                    where run_type = 'ingest_hla'
                      and status = 'success'
                    order by finished_at desc nulls last, created_at desc nulls last
                    limit 1
                    """
                )
                if latest_hla:
                    result["hla"] = int(
                        self.database.scalar(
                            "select landintel.queue_hla_reconcile_from_ingest(cast(:run_id as uuid))",
                            {"run_id": latest_hla["id"]},
                        )
                    )
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=result["planning"] + result["hla"],
                    records_loaded=result["planning"] + result["hla"],
                    records_retained=result["planning"] + result["hla"],
                    metadata=result,
                    finished=True,
                ),
            )
            self.logger.info("incremental_reconcile_catchup_completed", extra=result)
            return result
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def refresh_affected_sites(self, *, limit: int | None = None, runtime_minutes: int | None = None) -> dict[str, int]:
        effective_limit = limit or self.settings.reconcile_refresh_batch_limit
        effective_runtime_minutes = runtime_minutes or self.settings.reconcile_runtime_minutes
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="refresh_affected_sites",
                source_name="landintel.canonical_site_refresh_queue",
                status="running",
                metadata={"worker_id": self.worker_id, "limit": effective_limit, "runtime_minutes": effective_runtime_minutes},
            )
        )
        started = monotonic()
        stats = {"claimed": 0, "completed": 0, "retryable_failed": 0, "dead_letter": 0, "superseded": 0}
        try:
            while not self._runtime_limit_reached(started, effective_runtime_minutes):
                remaining = max(effective_limit - stats["claimed"], 0)
                if remaining == 0:
                    break
                batch = self._claim_refresh_items(min(remaining, self.settings.reconcile_refresh_batch_limit))
                if not batch:
                    break
                stats["claimed"] += len(batch)
                for item in batch:
                    if self._runtime_limit_reached(started, effective_runtime_minutes):
                        break
                    try:
                        processed = self._process_refresh_item(item, run_id)
                    except Exception as exc:  # pragma: no cover - defensive runtime protection
                        processed = self._handle_refresh_error(item, exc)
                    stats[processed] += 1
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=stats["claimed"],
                    records_loaded=stats["completed"],
                    records_retained=stats["claimed"] - stats["completed"],
                    metadata=stats,
                    finished=True,
                ),
            )
            self.logger.info("incremental_site_refresh_completed", extra=stats)
            return stats
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def weekly_reconcile_maintenance(self) -> dict[str, int]:
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="weekly_reconcile_maintenance",
                source_name="landintel.source_reconcile_queue",
                status="running",
                metadata={"worker_id": self.worker_id},
            )
        )
        try:
            source_queue_reclaimed = self.database.fetch_one(
                """
                with reclaimed as (
                    update landintel.source_reconcile_queue
                    set status = case when attempt_count >= :max_attempts then 'dead_letter' else 'retryable_failed' end,
                        claimed_by = null,
                        claimed_at = null,
                        lease_expires_at = null,
                        next_attempt_at = case
                            when attempt_count >= :max_attempts then null
                            else now() + interval '5 minutes'
                        end,
                        error_code = coalesce(error_code, 'stale_lease'),
                        error_message = coalesce(error_message, 'Lease expired before processing completed.'),
                        updated_at = now()
                    where status in ('claimed', 'processing')
                      and lease_expires_at is not null
                      and lease_expires_at < now()
                    returning status
                )
                select
                    count(*)::int as reclaimed_count,
                    count(*) filter (where status = 'dead_letter')::int as dead_letter_count
                from reclaimed
                """,
                {"max_attempts": self.max_attempts},
            ) or {"reclaimed_count": 0, "dead_letter_count": 0}
            refresh_queue_reclaimed = self.database.fetch_one(
                """
                with reclaimed as (
                    update landintel.canonical_site_refresh_queue
                    set status = case when attempt_count >= :max_attempts then 'dead_letter' else 'retryable_failed' end,
                        claimed_by = null,
                        claimed_at = null,
                        lease_expires_at = null,
                        next_attempt_at = case
                            when attempt_count >= :max_attempts then null
                            else now() + interval '5 minutes'
                        end,
                        error_message = coalesce(error_message, 'Lease expired before refresh completed.'),
                        updated_at = now()
                    where status = 'processing'
                      and lease_expires_at is not null
                      and lease_expires_at < now()
                    returning status
                )
                select
                    count(*)::int as reclaimed_count,
                    count(*) filter (where status = 'dead_letter')::int as dead_letter_count
                from reclaimed
                """,
                {"max_attempts": self.max_attempts},
            ) or {"reclaimed_count": 0, "dead_letter_count": 0}
            drift = self.database.fetch_one("select * from analytics.v_reconcile_drift_summary") or {}
            result = {
                "source_queue_reclaimed": int(source_queue_reclaimed.get("reclaimed_count", 0)),
                "source_queue_dead_letters": int(source_queue_reclaimed.get("dead_letter_count", 0)),
                "refresh_queue_reclaimed": int(refresh_queue_reclaimed.get("reclaimed_count", 0)),
                "refresh_queue_dead_letters": int(refresh_queue_reclaimed.get("dead_letter_count", 0)),
                "stale_claim_count": int(drift.get("stale_claim_count", 0)),
                "planning_records_without_state": int(drift.get("planning_records_without_state", 0)),
                "hla_records_without_state": int(drift.get("hla_records_without_state", 0)),
            }
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=sum(result.values()),
                    records_loaded=result["source_queue_reclaimed"] + result["refresh_queue_reclaimed"],
                    records_retained=result["source_queue_dead_letters"] + result["refresh_queue_dead_letters"],
                    metadata=result,
                    finished=True,
                ),
            )
            self.logger.info("incremental_reconcile_maintenance_completed", extra=result)
            return result
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def _process_reconcile_item(self, item: dict[str, Any], ingest_run_id: str) -> str:
        state = self._fetch_state(item["state_id"])
        source_row = self._fetch_source_row(item["source_family"], item["authority_name"], item["source_record_id"])
        if item["work_type"] == "upsert" and source_row is not None and self._source_signature_drifted(item, state, source_row):
            self._refresh_reconcile_item_to_current_source(item, state, source_row)
        if self._queue_item_is_outdated(item, state, source_row):
            self._mark_reconcile_queue_status(item, "superseded")
            return "superseded"
        if item["work_type"] == "retire":
            outcome = self._resolve_retirement(state, source_row)
        else:
            if source_row is None:
                outcome = MatchOutcome(status="review_required", publish_state="blocked", review_reason_code="data_integrity_anomaly")
            else:
                outcome = self._resolve_upsert(state, source_row)
        if outcome.status == "published":
            self._apply_published_outcome(item, state, source_row, outcome)
            return "completed"
        if outcome.status == "retired":
            self._apply_retirement_outcome(item, state, outcome)
            return "completed"
        self._apply_review_outcome(item, state, source_row, outcome)
        return "blocked" if outcome.publish_state == "blocked" else "review_required"

    def _resolve_upsert(self, state: dict[str, Any], source_row: dict[str, Any]) -> MatchOutcome:
        geometry = source_row.get("geometry")
        current_site_id = state.get("current_canonical_site_id")
        candidates = self._fetch_candidate_sites(source_row["authority_name"], geometry)
        candidate_ids = [candidate["site_id"] for candidate in candidates]

        if geometry is not None and current_site_id:
            continuity_candidate = next((candidate for candidate in candidates if candidate["site_id"] == current_site_id), None)
            if continuity_candidate and self._passes_continuity_rule(continuity_candidate, candidates):
                return MatchOutcome(
                    status="published",
                    publish_state="published",
                    canonical_site_id=current_site_id,
                    previous_canonical_site_id=state.get("previous_canonical_site_id"),
                    match_method="continuity",
                    match_confidence=1.0,
                    link_method="continuity",
                    candidate_site_ids=candidate_ids,
                    affected_site_ids=[current_site_id],
                )

        reference_value = source_row.get("reference_value")
        normalized_reference = _normalize_ref(reference_value)
        if normalized_reference and normalized_reference != "unknown":
            alias_candidates = self._fetch_alias_candidates(source_row["authority_name"], normalized_reference)
            alias_site_ids = [candidate["canonical_site_id"] for candidate in alias_candidates]
            if len(alias_site_ids) > 1:
                return MatchOutcome(
                    status="review_required",
                    publish_state=self._review_publish_state(state, self._reference_conflict_reason(source_row["source_family"])),
                    review_reason_code=self._reference_conflict_reason(source_row["source_family"]),
                    candidate_site_ids=alias_site_ids,
                )
            if len(alias_site_ids) == 1:
                alias_site_id = alias_site_ids[0]
                if current_site_id and current_site_id != alias_site_id:
                    return MatchOutcome(
                        status="review_required",
                        publish_state="blocked",
                        review_reason_code="reassignment_conflict",
                        candidate_site_ids=alias_site_ids,
                    )
                match_method = "direct_reference" if source_row["source_family"] == "hla" else "trusted_alias"
                match_confidence = 1.0 if match_method == "direct_reference" else float(alias_candidates[0]["confidence"] or 0.9)
                return MatchOutcome(
                    status="published",
                    publish_state="published",
                    canonical_site_id=alias_site_id,
                    previous_canonical_site_id=state.get("previous_canonical_site_id"),
                    match_method=match_method,
                    match_confidence=match_confidence,
                    link_method=match_method,
                    candidate_site_ids=alias_site_ids,
                    affected_site_ids=[alias_site_id],
                )

        if geometry is None:
            if source_row["source_family"] == "planning":
                return MatchOutcome(
                    status="review_required",
                    publish_state=self._review_publish_state(state, "geometry_missing_for_new_record"),
                    review_reason_code="geometry_missing_for_new_record",
                )
            return MatchOutcome(
                status="review_required",
                publish_state=self._review_publish_state(state, "data_integrity_anomaly"),
                review_reason_code="data_integrity_anomaly",
            )

        material_overlap_candidates = [candidate for candidate in candidates if candidate["overlap_pct_of_source"] >= 20.0]
        if len(material_overlap_candidates) >= 2:
            reason_code = "possible_split" if current_site_id else "possible_merge"
            return MatchOutcome(
                status="review_required",
                publish_state="blocked",
                review_reason_code=reason_code,
                candidate_site_ids=candidate_ids,
            )

        winner = candidates[0] if candidates else None
        second = candidates[1] if len(candidates) > 1 else None
        if winner and winner["overlap_area_sqm"] > 0:
            dominant = winner["overlap_pct_of_source"] >= 60.0 or winner["site_contains_source"]
            dominant = dominant and (
                second is None
                or second["overlap_area_sqm"] <= 0
                or winner["overlap_area_sqm"] >= (second["overlap_area_sqm"] * 1.5)
            )
            if dominant:
                if current_site_id and current_site_id != winner["site_id"]:
                    return MatchOutcome(
                        status="review_required",
                        publish_state="blocked",
                        review_reason_code="reassignment_conflict",
                        candidate_site_ids=candidate_ids,
                    )
                return MatchOutcome(
                    status="published",
                    publish_state="published",
                    canonical_site_id=winner["site_id"],
                    previous_canonical_site_id=state.get("previous_canonical_site_id"),
                    match_method="spatial_dominance",
                    match_confidence=0.85,
                    link_method="spatial_dominance",
                    candidate_site_ids=candidate_ids,
                    affected_site_ids=[winner["site_id"]],
                )
            review_reason = "spatial_ambiguous" if second and second["overlap_area_sqm"] > 0 else "weak_spatial_overlap"
            return MatchOutcome(
                status="review_required",
                publish_state=self._review_publish_state(state, review_reason),
                review_reason_code=review_reason,
                candidate_site_ids=candidate_ids,
            )

        near_candidates = [candidate for candidate in candidates if candidate["distance_m"] is not None and candidate["distance_m"] <= 100.0]
        if near_candidates:
            return MatchOutcome(
                status="review_required",
                publish_state=self._review_publish_state(state, "near_existing_site_conflict"),
                review_reason_code="near_existing_site_conflict",
                candidate_site_ids=candidate_ids,
            )

        if source_row["source_family"] == "planning" and float(source_row.get("area_acres") or 0.0) < self.settings.planning_new_site_min_area_acres:
            return MatchOutcome(
                status="review_required",
                publish_state="blocked",
                review_reason_code="new_site_below_area_floor",
            )

        new_site_id = self._create_canonical_site_from_source(source_row)
        return MatchOutcome(
            status="published",
            publish_state="published",
            canonical_site_id=new_site_id,
            previous_canonical_site_id=state.get("current_canonical_site_id"),
            match_method="balanced_auto_create",
            match_confidence=0.8,
            link_method="balanced_auto_create",
            candidate_site_ids=candidate_ids,
            affected_site_ids=[new_site_id],
            created_new_site=True,
        )

    def _resolve_retirement(self, state: dict[str, Any], source_row: dict[str, Any] | None) -> MatchOutcome:
        if source_row is not None:
            return MatchOutcome(status="review_required", publish_state="blocked", review_reason_code="data_integrity_anomaly")
        current_site_id = state.get("current_canonical_site_id")
        if current_site_id and not self._site_has_other_active_support(current_site_id, state["id"]):
            return MatchOutcome(
                status="review_required",
                publish_state=self._review_publish_state(state, "retirement_orphan_risk"),
                review_reason_code="retirement_orphan_risk",
                candidate_site_ids=[current_site_id],
            )
        return MatchOutcome(
            status="retired",
            publish_state="blocked",
            previous_canonical_site_id=current_site_id,
            affected_site_ids=[current_site_id] if current_site_id else [],
        )

    def _apply_published_outcome(
        self,
        item: dict[str, Any],
        state: dict[str, Any],
        source_row: dict[str, Any] | None,
        outcome: MatchOutcome,
    ) -> None:
        assert source_row is not None
        previous_site_id = state.get("current_canonical_site_id")
        affected_site_ids = self._dedupe_site_ids((outcome.affected_site_ids or []) + ([previous_site_id] if previous_site_id and previous_site_id != outcome.canonical_site_id else []))
        metadata = self._json_dumps(
            {
                "match_method": outcome.match_method,
                "publish_state": outcome.publish_state,
                "candidate_site_ids": outcome.candidate_site_ids or [],
            }
        )
        with self.database.engine.begin() as connection:
            connection.execute(
                text(self._update_source_record_sql(source_row["source_family"])),
                {"authority_name": source_row["authority_name"], "source_record_id": source_row["source_record_id"], "canonical_site_id": outcome.canonical_site_id},
            )
            connection.execute(
                text(
                    """
                    update landintel.source_reconcile_state
                    set active_flag = true,
                        lifecycle_status = 'active',
                        current_canonical_site_id = cast(:canonical_site_id as uuid),
                        previous_canonical_site_id = case
                            when current_canonical_site_id is distinct from cast(:canonical_site_id as uuid)
                                then current_canonical_site_id
                            else previous_canonical_site_id
                        end,
                        match_method = :match_method,
                        match_confidence = :match_confidence,
                        publish_state = 'published',
                        review_required = false,
                        review_reason_code = null,
                        candidate_site_ids = cast(:candidate_site_ids as uuid[]),
                        last_processed_at = now(),
                        updated_at = now()
                    where id = cast(:state_id as uuid)
                    """
                ),
                {
                    "state_id": state["id"],
                    "canonical_site_id": outcome.canonical_site_id,
                    "match_method": outcome.match_method,
                    "match_confidence": outcome.match_confidence,
                    "candidate_site_ids": self._pg_uuid_array(outcome.candidate_site_ids),
                },
            )
            connection.execute(
                text(
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
                        metadata,
                        reconcile_state_id,
                        active_flag,
                        retired_at
                    )
                    values (
                        cast(:canonical_site_id as uuid),
                        :source_family,
                        :source_dataset,
                        :source_record_id,
                        :link_method,
                        :confidence,
                        cast(:source_registry_id as uuid),
                        cast(:ingest_run_id as uuid),
                        cast(:metadata as jsonb),
                        cast(:reconcile_state_id as uuid),
                        true,
                        null
                    )
                    on conflict (reconcile_state_id) do update
                    set canonical_site_id = excluded.canonical_site_id,
                        source_family = excluded.source_family,
                        source_dataset = excluded.source_dataset,
                        source_record_id = excluded.source_record_id,
                        link_method = excluded.link_method,
                        confidence = excluded.confidence,
                        source_registry_id = excluded.source_registry_id,
                        ingest_run_id = excluded.ingest_run_id,
                        metadata = excluded.metadata,
                        active_flag = true,
                        retired_at = null
                    """
                ),
                {
                    "canonical_site_id": outcome.canonical_site_id,
                    "source_family": source_row["source_family"],
                    "source_dataset": source_row["source_dataset"],
                    "source_record_id": source_row["source_record_id"],
                    "link_method": outcome.link_method,
                    "confidence": outcome.match_confidence,
                    "source_registry_id": source_row.get("source_registry_id"),
                    "ingest_run_id": source_row.get("ingest_run_id"),
                    "metadata": metadata,
                    "reconcile_state_id": state["id"],
                },
            )
            connection.execute(
                text(
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
                        metadata,
                        reconcile_state_id,
                        active_flag,
                        retired_at
                    )
                    values (
                        cast(:canonical_site_id as uuid),
                        :source_family,
                        :source_dataset,
                        :authority_name,
                        :site_name,
                        :raw_reference_value,
                        :normalized_reference_value,
                        :planning_reference,
                        :geometry_hash,
                        'matched',
                        :confidence,
                        cast(:source_registry_id as uuid),
                        cast(:ingest_run_id as uuid),
                        cast(:metadata as jsonb),
                        cast(:reconcile_state_id as uuid),
                        true,
                        null
                    )
                    on conflict (reconcile_state_id) do update
                    set canonical_site_id = excluded.canonical_site_id,
                        source_family = excluded.source_family,
                        source_dataset = excluded.source_dataset,
                        authority_name = excluded.authority_name,
                        site_name = excluded.site_name,
                        raw_reference_value = excluded.raw_reference_value,
                        normalized_reference_value = excluded.normalized_reference_value,
                        planning_reference = excluded.planning_reference,
                        geometry_hash = excluded.geometry_hash,
                        status = 'matched',
                        confidence = excluded.confidence,
                        source_registry_id = excluded.source_registry_id,
                        ingest_run_id = excluded.ingest_run_id,
                        metadata = excluded.metadata,
                        active_flag = true,
                        retired_at = null
                    """
                ),
                {
                    "canonical_site_id": outcome.canonical_site_id,
                    "source_family": source_row["source_family"],
                    "source_dataset": source_row["source_dataset"],
                    "authority_name": source_row["authority_name"],
                    "site_name": source_row["display_name"],
                    "raw_reference_value": source_row.get("reference_value"),
                    "normalized_reference_value": _normalize_ref(source_row.get("reference_value")),
                    "planning_reference": source_row.get("planning_reference"),
                    "geometry_hash": source_row.get("geometry_hash"),
                    "confidence": outcome.match_confidence,
                    "source_registry_id": source_row.get("source_registry_id"),
                    "ingest_run_id": source_row.get("ingest_run_id"),
                    "metadata": metadata,
                    "reconcile_state_id": state["id"],
                },
            )
            connection.execute(
                text(
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
                        metadata,
                        reconcile_state_id,
                        active_flag,
                        retired_at
                    )
                    values (
                        cast(:canonical_site_id as uuid),
                        :source_family,
                        :source_dataset,
                        :source_record_id,
                        :source_reference,
                        :confidence_label,
                        cast(:source_registry_id as uuid),
                        cast(:ingest_run_id as uuid),
                        cast(:metadata as jsonb),
                        cast(:reconcile_state_id as uuid),
                        true,
                        null
                    )
                    on conflict (reconcile_state_id) do update
                    set canonical_site_id = excluded.canonical_site_id,
                        source_family = excluded.source_family,
                        source_dataset = excluded.source_dataset,
                        source_record_id = excluded.source_record_id,
                        source_reference = excluded.source_reference,
                        confidence = excluded.confidence,
                        source_registry_id = excluded.source_registry_id,
                        ingest_run_id = excluded.ingest_run_id,
                        metadata = excluded.metadata,
                        active_flag = true,
                        retired_at = null
                    """
                ),
                {
                    "canonical_site_id": outcome.canonical_site_id,
                    "source_family": source_row["source_family"],
                    "source_dataset": source_row["source_dataset"],
                    "source_record_id": source_row["source_record_id"],
                    "source_reference": source_row.get("reference_value"),
                    "confidence_label": self._evidence_confidence(outcome.match_confidence),
                    "source_registry_id": source_row.get("source_registry_id"),
                    "ingest_run_id": source_row.get("ingest_run_id"),
                    "metadata": self._json_dumps(source_row.get("evidence_metadata", {})),
                    "reconcile_state_id": state["id"],
                },
            )
        self._mark_reconcile_queue_status(item, "completed", candidate_site_ids=outcome.candidate_site_ids)
        for site_id in affected_site_ids:
            self._enqueue_refresh(site_id, "site_outputs", source_row["source_family"], source_row["source_record_id"])

    def _apply_review_outcome(
        self,
        item: dict[str, Any],
        state: dict[str, Any],
        source_row: dict[str, Any] | None,
        outcome: MatchOutcome,
    ) -> None:
        reason_code = outcome.review_reason_code or "data_integrity_anomaly"
        lifecycle_status = "blocked" if outcome.publish_state == "blocked" else "review_required"
        candidate_site_ids = outcome.candidate_site_ids or []
        with self.database.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    update landintel.source_reconcile_state
                    set lifecycle_status = :lifecycle_status,
                        publish_state = :publish_state,
                        review_required = true,
                        review_reason_code = :review_reason_code,
                        candidate_site_ids = cast(:candidate_site_ids as uuid[]),
                        match_method = 'manual_review',
                        match_confidence = null,
                        last_processed_at = now(),
                        updated_at = now()
                    where id = cast(:state_id as uuid)
                    """
                ),
                {
                    "lifecycle_status": lifecycle_status,
                    "publish_state": outcome.publish_state,
                    "review_reason_code": reason_code,
                    "candidate_site_ids": self._pg_uuid_array(candidate_site_ids),
                    "state_id": state["id"],
                },
            )
            if source_row is not None and outcome.publish_state == "blocked" and not state.get("current_canonical_site_id"):
                connection.execute(
                    text(self._update_source_record_sql(source_row["source_family"])),
                    {"authority_name": source_row["authority_name"], "source_record_id": source_row["source_record_id"], "canonical_site_id": None},
                )
        self._mark_reconcile_queue_status(item, "review_required", review_reason_code=reason_code, candidate_site_ids=candidate_site_ids)

    def _apply_retirement_outcome(self, item: dict[str, Any], state: dict[str, Any], outcome: MatchOutcome) -> None:
        previous_site_id = state.get("current_canonical_site_id")
        source_family = item["source_family"]
        with self.database.engine.begin() as connection:
            connection.execute(
                text(self._update_source_record_sql(source_family)),
                {"authority_name": item["authority_name"], "source_record_id": item["source_record_id"], "canonical_site_id": None},
            )
            connection.execute(
                text(
                    """
                    update landintel.source_reconcile_state
                    set active_flag = false,
                        lifecycle_status = 'retired',
                        current_canonical_site_id = null,
                        previous_canonical_site_id = cast(:previous_canonical_site_id as uuid),
                        publish_state = 'blocked',
                        review_required = false,
                        review_reason_code = null,
                        candidate_site_ids = '{}'::uuid[],
                        match_method = 'retired',
                        match_confidence = null,
                        last_processed_at = now(),
                        updated_at = now()
                    where id = cast(:state_id as uuid)
                    """
                ),
                {"state_id": state["id"], "previous_canonical_site_id": previous_site_id},
            )
            for table_name in ("landintel.site_source_links", "landintel.site_reference_aliases", "landintel.evidence_references"):
                connection.execute(
                    text(
                        f"""
                        update {table_name}
                        set active_flag = false,
                            retired_at = now()
                        where reconcile_state_id = cast(:state_id as uuid)
                        """
                    ),
                    {"state_id": state["id"]},
                )
        self._mark_reconcile_queue_status(item, "completed")
        if previous_site_id:
            self._enqueue_refresh(previous_site_id, "site_outputs", source_family, item["source_record_id"])

    def _process_refresh_item(self, item: dict[str, Any], ingest_run_id: str) -> str:
        site_row = self.database.fetch_one(
            """
            select
                id,
                authority_name,
                site_code,
                site_name_primary,
                encode(st_asbinary(landintel.normalized_polygon_geometry(geometry)), 'hex') as geometry_wkb
            from landintel.canonical_sites
            where id = cast(:site_id as uuid)
            """,
            {"site_id": item["canonical_site_id"]},
        )
        if site_row is None:
            self._mark_refresh_queue_status(item, "superseded")
            return "superseded"
        source_geometries = self.database.fetch_all(
            """
            with active_states as (
                select id, source_family, source_record_id
                from landintel.source_reconcile_state
                where current_canonical_site_id = cast(:site_id as uuid)
                  and active_flag = true
                  and publish_state in ('published', 'provisional')
            )
            select
                active_states.source_family,
                active_states.source_record_id,
                encode(st_asbinary(landintel.normalized_polygon_geometry(hla.geometry)), 'hex') as geometry_wkb
            from active_states
            join landintel.hla_site_records as hla
              on active_states.source_family = 'hla'
             and hla.authority_name = :authority_name
             and hla.source_record_id = active_states.source_record_id
            where landintel.normalized_polygon_geometry(hla.geometry) is not null
            union all
            select
                active_states.source_family,
                active_states.source_record_id,
                encode(st_asbinary(landintel.normalized_polygon_geometry(planning.geometry)), 'hex') as geometry_wkb
            from active_states
            join landintel.planning_application_records as planning
              on active_states.source_family = 'planning'
             and planning.authority_name = :authority_name
             and planning.source_record_id = active_states.source_record_id
            where landintel.normalized_polygon_geometry(planning.geometry) is not null
            """,
            {"site_id": item["canonical_site_id"], "authority_name": site_row["authority_name"]},
        )
        if not source_geometries:
            self._mark_refresh_queue_status(item, "completed")
            return "completed"
        hla_geometries = [self._geometry_from_hex(row["geometry_wkb"]) for row in source_geometries if row["source_family"] == "hla"]
        planning_geometries = [self._geometry_from_hex(row["geometry_wkb"]) for row in source_geometries if row["source_family"] == "planning"]
        geometry_basis = "hla" if hla_geometries else "planning"
        geometry_set = hla_geometries or planning_geometries
        merged_geometry = _polygonize_geometry(unary_union([geometry for geometry in geometry_set if geometry is not None]))
        if merged_geometry is None:
            self._mark_refresh_queue_retryable(item, "Could not recompute a valid canonical site geometry.")
            return "retryable_failed"
        new_geometry_hex = _geometry_hex(merged_geometry)
        old_geometry_hex = site_row.get("geometry_wkb")
        geometry_changed = new_geometry_hex != old_geometry_hex
        with self.database.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    update landintel.canonical_sites
                    set geometry = ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                        centroid = ST_Centroid(ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700))),
                        area_acres = ST_Area(ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700))) / 4046.8564224,
                        metadata = coalesce(metadata, '{}'::jsonb)
                            || jsonb_build_object(
                                'geometry_basis', :geometry_basis,
                                'last_incremental_refresh_at', now()
                            ),
                        updated_at = now()
                    where id = cast(:site_id as uuid)
                    """
                ),
                {"geometry_wkb": new_geometry_hex, "geometry_basis": geometry_basis, "site_id": item["canonical_site_id"]},
            )
            if geometry_changed:
                connection.execute(
                    text(
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
                            'incremental_reconcile',
                            :version_label,
                            ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                            null,
                            cast(:ingest_run_id as uuid),
                            cast(:metadata as jsonb)
                        )
                        """
                    ),
                    {
                        "site_id": item["canonical_site_id"],
                        "version_label": f"incremental:{datetime.now(timezone.utc).date().isoformat()}",
                        "geometry_wkb": new_geometry_hex,
                        "ingest_run_id": ingest_run_id,
                        "metadata": self._json_dumps({"geometry_basis": geometry_basis}),
                    },
                )
        if geometry_changed or item["refresh_scope"] in {"parcel_only", "full_site_refresh", "site_outputs"}:
            self._refresh_primary_parcel(item["canonical_site_id"])
        self._mark_refresh_queue_status(item, "completed")
        return "completed"

    def _handle_processing_error(self, item: dict[str, Any], exc: Exception) -> str:
        error_message = str(exc)
        self.logger.warning(
            "incremental_reconcile_item_failed",
            extra={"queue_item_id": item["id"], "source_family": item["source_family"], "source_record_id": item["source_record_id"], "error": error_message},
        )
        if int(item.get("attempt_count") or 0) >= self.max_attempts:
            self._mark_reconcile_queue_status(item, "dead_letter", error_code="processing_failed", error_message=error_message)
            return "dead_letter"
        self._mark_reconcile_queue_retryable(item, error_message)
        return "retryable_failed"

    def _handle_refresh_error(self, item: dict[str, Any], exc: Exception) -> str:
        error_message = str(exc)
        self.logger.warning(
            "incremental_refresh_item_failed",
            extra={"refresh_item_id": item["id"], "canonical_site_id": item["canonical_site_id"], "error": error_message},
        )
        if int(item.get("attempt_count") or 0) >= self.max_attempts:
            self._mark_refresh_queue_status(item, "dead_letter", error_message=error_message)
            return "dead_letter"
        self._mark_refresh_queue_retryable(item, error_message)
        return "retryable_failed"

    def _claim_reconcile_items(self, batch_limit: int) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            with candidates as (
                select queue_row.id
                from landintel.source_reconcile_queue as queue_row
                where queue_row.status in ('pending', 'retryable_failed')
                  and coalesce(queue_row.next_attempt_at, now()) <= now()
                order by queue_row.priority desc, queue_row.updated_at asc, queue_row.created_at asc
                limit :batch_limit
                for update skip locked
            )
            update landintel.source_reconcile_queue as queue_row
            set status = 'processing',
                claimed_by = :worker_id,
                claimed_at = now(),
                lease_expires_at = now() + make_interval(secs => :lease_seconds),
                attempt_count = coalesce(queue_row.attempt_count, 0) + 1,
                updated_at = now()
            from candidates
            where queue_row.id = candidates.id
            returning queue_row.*
            """,
            {
                "batch_limit": batch_limit,
                "worker_id": self.worker_id,
                "lease_seconds": self.settings.reconcile_lease_seconds,
            },
        )

    def _supersede_stale_reconcile_items(self, batch_limit: int) -> int:
        row = self.database.fetch_one(
            """
            with pending_slice as (
                select queue_row.id
                from landintel.source_reconcile_queue as queue_row
                where queue_row.status in ('pending', 'retryable_failed')
                  and coalesce(queue_row.next_attempt_at, now()) <= now()
                order by queue_row.priority desc, queue_row.updated_at asc, queue_row.created_at asc
                limit :batch_limit
                for update skip locked
            ),
            candidates as (
                select queue_row.id
                from pending_slice
                join landintel.source_reconcile_queue as queue_row
                  on queue_row.id = pending_slice.id
                join landintel.source_reconcile_state as state_row
                  on state_row.id = queue_row.state_id
                where
                  (
                      queue_row.work_type = 'upsert'
                      and (
                          queue_row.source_signature is distinct from state_row.current_source_signature
                          or queue_row.geometry_hash is distinct from state_row.current_geometry_hash
                          or state_row.lifecycle_status = 'retired'
                      )
                  )
                  or (
                      queue_row.work_type = 'retire'
                      and state_row.lifecycle_status <> 'retired'
                  )
            ),
            superseded as (
                update landintel.source_reconcile_queue as queue_row
                set status = 'superseded',
                    processed_at = now(),
                    claimed_by = null,
                    claimed_at = null,
                    lease_expires_at = null,
                    next_attempt_at = null,
                    error_code = coalesce(queue_row.error_code, 'stale_before_claim'),
                    error_message = coalesce(queue_row.error_message, 'Queue row was stale before claim and was superseded in bulk.'),
                    updated_at = now()
                from candidates
                where queue_row.id = candidates.id
                returning queue_row.id
            )
            select count(*)::int as superseded_count
            from superseded
            """,
            {"batch_limit": batch_limit},
        )
        return int((row or {}).get("superseded_count", 0) or 0)

    def _claim_refresh_items(self, batch_limit: int) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            with candidates as (
                select queue_row.id
                from landintel.canonical_site_refresh_queue as queue_row
                where queue_row.status in ('pending', 'retryable_failed')
                  and coalesce(queue_row.next_attempt_at, now()) <= now()
                order by queue_row.updated_at asc, queue_row.created_at asc
                limit :batch_limit
                for update skip locked
            )
            update landintel.canonical_site_refresh_queue as queue_row
            set status = 'processing',
                claimed_by = :worker_id,
                claimed_at = now(),
                lease_expires_at = now() + make_interval(secs => :lease_seconds),
                attempt_count = coalesce(queue_row.attempt_count, 0) + 1,
                updated_at = now()
            from candidates
            where queue_row.id = candidates.id
            returning queue_row.*
            """,
            {
                "batch_limit": batch_limit,
                "worker_id": self.worker_id,
                "lease_seconds": self.settings.reconcile_refresh_lease_seconds,
            },
        )

    def _fetch_state(self, state_id: str) -> dict[str, Any]:
        row = self.database.fetch_one(
            "select * from landintel.source_reconcile_state where id = cast(:state_id as uuid)",
            {"state_id": state_id},
        )
        if row is None:
            raise RuntimeError(f"Missing source_reconcile_state row for {state_id}.")
        return row

    def _fetch_source_row(self, source_family: str, authority_name: str, source_record_id: str) -> dict[str, Any] | None:
        if source_family == "planning":
            row = self.database.fetch_one(
                """
                select
                    'planning'::text as source_family,
                    :source_dataset as source_dataset,
                    authority_name,
                    source_record_id,
                    planning_reference,
                    planning_reference as reference_value,
                    coalesce(left(proposal_text, 240), planning_reference, source_record_id) as display_name,
                    encode(st_asbinary(landintel.normalized_polygon_geometry(geometry)), 'hex') as geometry_wkb,
                    landintel.planning_reconcile_signature(
                        source_record_id,
                        authority_name,
                        planning_reference,
                        proposal_text,
                        application_status,
                        decision,
                        appeal_status,
                        raw_payload,
                        geometry
                    ) as source_signature,
                    landintel.normalized_geometry_hash(geometry) as geometry_hash,
                    case
                        when landintel.normalized_polygon_geometry(geometry) is null then null
                        else st_area(landintel.normalized_polygon_geometry(geometry)) / 4046.8564224
                    end as area_acres,
                    source_registry_id,
                    ingest_run_id,
                    canonical_site_id,
                    proposal_text,
                    decision
                from landintel.planning_application_records
                where authority_name = :authority_name
                  and source_record_id = :source_record_id
                """,
                {
                    "authority_name": authority_name,
                    "source_record_id": source_record_id,
                    "source_dataset": PLANNING_DATASET,
                },
            )
            if row is None:
                return None
            row["geometry"] = self._geometry_from_hex(row.get("geometry_wkb"))
            row["evidence_metadata"] = {"proposal_text": row.get("proposal_text"), "decision": row.get("decision")}
            return row
        row = self.database.fetch_one(
            """
            select
                'hla'::text as source_family,
                :source_dataset as source_dataset,
                authority_name,
                source_record_id,
                null::text as planning_reference,
                site_reference as reference_value,
                coalesce(left(site_name, 240), site_reference, source_record_id) as display_name,
                encode(st_asbinary(landintel.normalized_polygon_geometry(geometry)), 'hex') as geometry_wkb,
                landintel.hla_reconcile_signature(
                    source_record_id,
                    authority_name,
                    site_reference,
                    site_name,
                    effectiveness_status,
                    programming_horizon,
                    constraint_reasons,
                    remaining_capacity,
                    raw_payload,
                    geometry
                ) as source_signature,
                landintel.normalized_geometry_hash(geometry) as geometry_hash,
                case
                    when landintel.normalized_polygon_geometry(geometry) is null then null
                    else st_area(landintel.normalized_polygon_geometry(geometry)) / 4046.8564224
                end as area_acres,
                source_registry_id,
                ingest_run_id,
                canonical_site_id,
                site_reference,
                site_name,
                effectiveness_status,
                programming_horizon,
                constraint_reasons,
                remaining_capacity
            from landintel.hla_site_records
            where authority_name = :authority_name
              and source_record_id = :source_record_id
            """,
            {
                "authority_name": authority_name,
                "source_record_id": source_record_id,
                "source_dataset": HLA_DATASET,
            },
        )
        if row is None:
            return None
        row["geometry"] = self._geometry_from_hex(row.get("geometry_wkb"))
        row["evidence_metadata"] = {
            "site_reference": row.get("site_reference"),
            "site_name": row.get("site_name"),
            "effectiveness_status": row.get("effectiveness_status"),
            "programming_horizon": row.get("programming_horizon"),
            "constraint_reasons": row.get("constraint_reasons") or [],
            "remaining_capacity": row.get("remaining_capacity"),
        }
        return row

    def _fetch_candidate_sites(self, authority_name: str, geometry: BaseGeometry | None) -> list[dict[str, Any]]:
        if geometry is None:
            return []
        geometry_wkb = _geometry_hex(geometry)
        return self.database.fetch_all(
            """
            with source_geometry as (
                select ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)) as geometry_value
            )
            select
                site.id as site_id,
                coalesce(st_area(st_intersection(site.geometry, source_geometry.geometry_value)), 0)::float as overlap_area_sqm,
                case
                    when nullif(st_area(source_geometry.geometry_value), 0) is null then 0::float
                    else (st_area(st_intersection(site.geometry, source_geometry.geometry_value)) / nullif(st_area(source_geometry.geometry_value), 0) * 100)::float
                end as overlap_pct_of_source,
                st_distance(site.geometry, source_geometry.geometry_value)::float as distance_m,
                st_covers(site.geometry, source_geometry.geometry_value) as site_contains_source
            from landintel.canonical_sites as site
            cross join source_geometry
            where site.authority_name = :authority_name
              and site.geometry is not null
              and site.geometry OPERATOR(extensions.&&) st_expand(source_geometry.geometry_value, 100)
              and (
                  st_intersects(site.geometry, source_geometry.geometry_value)
                  or st_dwithin(site.geometry, source_geometry.geometry_value, 100)
              )
            order by overlap_area_sqm desc, distance_m asc, site.id asc
            limit 8
            """,
            {"authority_name": authority_name, "geometry_wkb": geometry_wkb},
        )

    def _fetch_alias_candidates(self, authority_name: str, normalized_reference: str) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            select
                canonical_site_id,
                max(confidence)::float as confidence
            from landintel.site_reference_aliases
            where authority_name = :authority_name
              and active_flag = true
              and status = 'matched'
              and confidence >= 0.9
              and normalized_reference_value = :normalized_reference
            group by canonical_site_id
            order by max(confidence) desc, canonical_site_id asc
            """,
            {"authority_name": authority_name, "normalized_reference": normalized_reference},
        )

    def _create_canonical_site_from_source(self, source_row: dict[str, Any]) -> str:
        prefix = "PLN" if source_row["source_family"] == "planning" else "HLA"
        site_code = self._canonical_site_code(prefix, source_row["authority_name"], source_row.get("reference_value") or source_row["source_record_id"])
        surfaced_reason = "Surfaced from planning history evidence." if source_row["source_family"] == "planning" else "Surfaced from Housing Land Supply evidence."
        metadata = {
            "seed_source": source_row["source_family"],
            "incremental_reconcile": True,
            "reference_value": source_row.get("reference_value"),
        }
        return self._upsert_canonical_site(
            site_code=site_code,
            site_name=source_row["display_name"],
            authority_name=source_row["authority_name"],
            geometry=source_row.get("geometry"),
            surfaced_reason=surfaced_reason,
            metadata=metadata,
        )

    def _refresh_primary_parcel(self, canonical_site_id: str) -> None:
        parcel_row = self.database.fetch_one(
            """
            with ranked_matches as (
                select
                    parcel.id as parcel_id,
                    st_area(st_intersection(parcel.geometry, site.geometry)) as overlap_area_sqm
                from landintel.canonical_sites as site
                join public.ros_cadastral_parcels as parcel
                  on parcel.authority_name = site.authority_name
                 and site.id = cast(:site_id as uuid)
                 and site.geometry is not null
                 and parcel.geometry OPERATOR(extensions.&&) site.geometry
                 and st_intersects(parcel.geometry, site.geometry)
                order by overlap_area_sqm desc nulls last, parcel.id asc
                limit 1
            )
            select parcel_id from ranked_matches
            """,
            {"site_id": canonical_site_id},
        )
        self.database.execute(
            """
            update landintel.canonical_sites
            set primary_ros_parcel_id = cast(:parcel_id as uuid),
                updated_at = now()
            where id = cast(:site_id as uuid)
            """,
            {"site_id": canonical_site_id, "parcel_id": parcel_row.get("parcel_id") if parcel_row else None},
        )

    def _site_has_other_active_support(self, canonical_site_id: str, state_id: str) -> bool:
        count_value = self.database.scalar(
            """
            select count(*)
            from landintel.source_reconcile_state
            where current_canonical_site_id = cast(:site_id as uuid)
              and active_flag = true
              and id <> cast(:state_id as uuid)
            """,
            {"site_id": canonical_site_id, "state_id": state_id},
        )
        return int(count_value or 0) > 0

    def _passes_continuity_rule(self, continuity_candidate: dict[str, Any], candidates: list[dict[str, Any]]) -> bool:
        if continuity_candidate["overlap_pct_of_source"] < 20.0 and (continuity_candidate.get("distance_m") or 0) > 100.0:
            return False
        for candidate in candidates:
            if candidate["site_id"] == continuity_candidate["site_id"]:
                continue
            if candidate["overlap_area_sqm"] > (continuity_candidate["overlap_area_sqm"] * 1.5):
                return False
        return True

    def _queue_item_is_outdated(
        self,
        item: dict[str, Any],
        state: dict[str, Any],
        source_row: dict[str, Any] | None,
    ) -> bool:
        if item["work_type"] == "retire":
            if source_row is not None:
                return True
            return state.get("lifecycle_status") != "retired"
        if source_row is None:
            return True
        if item.get("source_signature") != source_row.get("source_signature"):
            return True
        if item.get("geometry_hash") != source_row.get("geometry_hash"):
            return True
        if state.get("current_source_signature") != source_row.get("source_signature"):
            return True
        if state.get("current_geometry_hash") != source_row.get("geometry_hash"):
            return True
        return False

    def _source_signature_drifted(
        self,
        item: dict[str, Any],
        state: dict[str, Any],
        source_row: dict[str, Any],
    ) -> bool:
        return (
            item.get("source_signature") != source_row.get("source_signature")
            or item.get("geometry_hash") != source_row.get("geometry_hash")
            or state.get("current_source_signature") != source_row.get("source_signature")
            or state.get("current_geometry_hash") != source_row.get("geometry_hash")
        )

    def _refresh_reconcile_item_to_current_source(
        self,
        item: dict[str, Any],
        state: dict[str, Any],
        source_row: dict[str, Any],
    ) -> None:
        with self.database.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    update landintel.source_reconcile_state
                    set active_flag = true,
                        lifecycle_status = case
                            when lifecycle_status = 'retired' then 'active'
                            else lifecycle_status
                        end,
                        current_source_signature = :source_signature,
                        current_geometry_hash = :geometry_hash,
                        last_seen_ingest_run_id = cast(:ingest_run_id as uuid),
                        last_seen_at = now(),
                        metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                            'currentised_from_processing_worker', true,
                            'currentised_at', now()
                        ),
                        updated_at = now()
                    where id = cast(:state_id as uuid)
                    """
                ),
                {
                    "state_id": state["id"],
                    "source_signature": source_row.get("source_signature"),
                    "geometry_hash": source_row.get("geometry_hash"),
                    "ingest_run_id": source_row.get("ingest_run_id"),
                },
            )
            connection.execute(
                text(
                    """
                    update landintel.source_reconcile_queue
                    set source_signature = :source_signature,
                        geometry_hash = :geometry_hash,
                        ingest_run_id = cast(:ingest_run_id as uuid),
                        error_code = null,
                        error_message = null,
                        review_reason_code = null,
                        metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object(
                            'currentised_from_processing_worker', true,
                            'currentised_at', now()
                        ),
                        updated_at = now()
                    where id = cast(:queue_id as uuid)
                    """
                ),
                {
                    "queue_id": item["id"],
                    "source_signature": source_row.get("source_signature"),
                    "geometry_hash": source_row.get("geometry_hash"),
                    "ingest_run_id": source_row.get("ingest_run_id"),
                },
            )
        item["source_signature"] = source_row.get("source_signature")
        item["geometry_hash"] = source_row.get("geometry_hash")
        item["ingest_run_id"] = source_row.get("ingest_run_id")
        state["active_flag"] = True
        state["lifecycle_status"] = "active" if state.get("lifecycle_status") == "retired" else state.get("lifecycle_status")
        state["current_source_signature"] = source_row.get("source_signature")
        state["current_geometry_hash"] = source_row.get("geometry_hash")
        state["last_seen_ingest_run_id"] = source_row.get("ingest_run_id")

    def _mark_reconcile_queue_retryable(self, item: dict[str, Any], error_message: str) -> None:
        next_attempt_minutes = 5 if int(item.get("attempt_count") or 0) <= 1 else 15 if int(item.get("attempt_count") or 0) == 2 else 60
        self._mark_reconcile_queue_status(
            item,
            "retryable_failed",
            error_code="processing_failed",
            error_message=error_message,
            next_attempt=f"now() + interval '{next_attempt_minutes} minutes'",
        )

    def _mark_reconcile_queue_status(
        self,
        item: dict[str, Any],
        status: str,
        *,
        review_reason_code: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        candidate_site_ids: list[str] | None = None,
        next_attempt: str | None = None,
    ) -> None:
        self.database.fetch_one(
            f"""
            update landintel.source_reconcile_queue
            set status = :status,
                processed_at = now(),
                review_reason_code = :review_reason_code,
                error_code = :error_code,
                error_message = :error_message,
                candidate_site_ids = cast(:candidate_site_ids as uuid[]),
                claimed_by = null,
                claimed_at = null,
                lease_expires_at = null,
                next_attempt_at = {next_attempt or 'null'},
                updated_at = now()
            where id = cast(:queue_id as uuid)
              and source_signature is not distinct from :source_signature
              and geometry_hash is not distinct from :geometry_hash
            returning id
            """,
            {
                "status": status,
                "review_reason_code": review_reason_code,
                "error_code": error_code,
                "error_message": error_message,
                "candidate_site_ids": self._pg_uuid_array(candidate_site_ids),
                "queue_id": item["id"],
                "source_signature": item.get("source_signature"),
                "geometry_hash": item.get("geometry_hash"),
            },
        )

    def _mark_refresh_queue_retryable(self, item: dict[str, Any], error_message: str) -> None:
        next_attempt_minutes = 5 if int(item.get("attempt_count") or 0) <= 1 else 15 if int(item.get("attempt_count") or 0) == 2 else 60
        self._mark_refresh_queue_status(item, "retryable_failed", error_message=error_message, next_attempt=f"now() + interval '{next_attempt_minutes} minutes'")

    def _mark_refresh_queue_status(
        self,
        item: dict[str, Any],
        status: str,
        *,
        error_message: str | None = None,
        next_attempt: str | None = None,
    ) -> None:
        self.database.fetch_one(
            f"""
            update landintel.canonical_site_refresh_queue
            set status = :status,
                processed_at = now(),
                error_message = :error_message,
                claimed_by = null,
                claimed_at = null,
                lease_expires_at = null,
                next_attempt_at = {next_attempt or 'null'},
                updated_at = now()
            where id = cast(:queue_id as uuid)
              and claimed_by = :worker_id
            returning id
            """,
            {
                "status": status,
                "error_message": error_message,
                "queue_id": item["id"],
                "worker_id": self.worker_id,
            },
        )

    def _enqueue_refresh(self, canonical_site_id: str, refresh_scope: str, source_family: str, source_record_id: str) -> None:
        self.database.execute(
            """
            insert into landintel.canonical_site_refresh_queue (
                canonical_site_id,
                refresh_scope,
                trigger_source,
                source_family,
                source_record_id,
                status,
                claimed_by,
                claimed_at,
                lease_expires_at,
                attempt_count,
                next_attempt_at,
                processed_at,
                error_message,
                metadata,
                updated_at
            )
            values (
                cast(:canonical_site_id as uuid),
                :refresh_scope,
                'incremental_reconcile',
                :source_family,
                :source_record_id,
                'pending',
                null,
                null,
                null,
                0,
                null,
                null,
                null,
                cast(:metadata as jsonb),
                now()
            )
            on conflict (canonical_site_id, refresh_scope) do update
            set trigger_source = excluded.trigger_source,
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
            """,
            {
                "canonical_site_id": canonical_site_id,
                "refresh_scope": refresh_scope,
                "source_family": source_family,
                "source_record_id": source_record_id,
                "metadata": self._json_dumps({"source_record_id": source_record_id}),
            },
        )

    def _reference_conflict_reason(self, source_family: str) -> str:
        return "reference_conflict" if source_family == "hla" else "trusted_alias_conflict"

    def _review_publish_state(self, state: dict[str, Any], reason_code: str) -> str:
        if reason_code in STRUCTURAL_REVIEW_REASON_CODES:
            return "blocked"
        if state.get("current_canonical_site_id"):
            return "provisional"
        return "blocked"

    def _evidence_confidence(self, match_confidence: float | None) -> str:
        if match_confidence is None:
            return "medium"
        if match_confidence >= 0.9:
            return "high"
        return "medium"

    def _update_source_record_sql(self, source_family: str) -> str:
        if source_family == "planning":
            return (
                "update landintel.planning_application_records "
                "set canonical_site_id = cast(:canonical_site_id as uuid) "
                "where authority_name = :authority_name and source_record_id = :source_record_id"
            )
        return (
            "update landintel.hla_site_records "
            "set canonical_site_id = cast(:canonical_site_id as uuid) "
            "where authority_name = :authority_name and source_record_id = :source_record_id"
        )

    def _runtime_limit_reached(self, started: float, runtime_minutes: int) -> bool:
        return (monotonic() - started) >= (runtime_minutes * 60)

    def _geometry_from_hex(self, geometry_wkb: str | None) -> BaseGeometry | None:
        if not geometry_wkb:
            return None
        return _polygonize_geometry(shapely_wkb.loads(bytes.fromhex(geometry_wkb)))

    def _pg_uuid_array(self, values: list[str] | None) -> str:
        cleaned = [value for value in values or [] if value]
        if not cleaned:
            return "{}"
        return "{" + ",".join(cleaned) + "}"

    def _json_dumps(self, payload: Any) -> str:
        return json.dumps(payload, default=_json_default, ensure_ascii=False)

    def _dedupe_site_ids(self, site_ids: list[str]) -> list[str]:
        ordered: list[str] = []
        for site_id in site_ids:
            if site_id and site_id not in ordered:
                ordered.append(site_id)
        return ordered


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run incremental reconcile commands for LandIntel.")
    parser.add_argument(
        "command",
        choices=(
            "process-reconcile-queue",
            "reconcile-catchup-scan",
            "refresh-affected-sites",
            "weekly-reconcile-maintenance",
        ),
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--runtime-minutes", type=int, default=None)
    parser.add_argument("--source-family", choices=("planning", "hla"), default=None)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = IncrementalReconcileRunner(settings, logger)
    try:
        if args.command == "process-reconcile-queue":
            runner.process_reconcile_queue(limit=args.limit, runtime_minutes=args.runtime_minutes)
        elif args.command == "reconcile-catchup-scan":
            runner.reconcile_catchup_scan(source_family=args.source_family)
        elif args.command == "refresh-affected-sites":
            runner.refresh_affected_sites(limit=args.limit, runtime_minutes=args.runtime_minutes)
        elif args.command == "weekly-reconcile-maintenance":
            runner.weekly_reconcile_maintenance()
        runner.logger.info("incremental_reconcile_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        runner.logger.exception("incremental_reconcile_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

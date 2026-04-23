"""Phase One orchestration for search, review, and refresh flows."""

from __future__ import annotations

import logging
from typing import Any

from config.settings import Settings
from src.db import Database
from src.opportunity_engine.brief import build_opportunity_brief
from src.opportunity_engine.ranking import build_assessment, build_geometry_diagnostics, build_signals
from src.opportunity_engine.refresh import assessment_to_record, signals_to_rows
from src.opportunity_engine.repository import OpportunityRepository
from src.opportunity_engine.snapshot import build_snapshot
from src.opportunity_engine.types import OpportunitySearchFilters


class OpportunityService:
    """Expose the live Phase One opportunity flows to the UI and jobs."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger.getChild("opportunity_engine")
        self.database = Database(settings)
        self.repository = OpportunityRepository(self.database)
        self.target_authorities = settings.load_target_councils()

    def close(self) -> None:
        self.database.dispose()

    def search_opportunities(self, filters: OpportunitySearchFilters) -> dict[str, Any]:
        results = self.repository.search_opportunities(filters)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in results:
            grouped.setdefault(str(row["queue_name"]), []).append(row)
        return {
            "filters": filters,
            "results": results,
            "grouped_results": grouped,
            "options": self.repository.fetch_filter_options(),
        }

    def get_opportunity_review(self, canonical_site_id: str) -> dict[str, Any] | None:
        detail = self.repository.fetch_opportunity_detail(canonical_site_id)
        if not detail:
            return None
        return {
            "detail": detail,
            "brief": build_opportunity_brief(detail),
        }

    def refresh_site(self, canonical_site_id: str, *, triggered_by: str = "manual") -> dict[str, Any]:
        snapshot = build_snapshot(self.repository, canonical_site_id)
        geometry = build_geometry_diagnostics(snapshot)
        signals = build_signals(snapshot, target_authorities=self.target_authorities)
        assessment = build_assessment(snapshot, signals, geometry)

        self.repository.upsert_geometry_diagnostics(canonical_site_id, geometry)
        self.repository.upsert_signals(canonical_site_id, signals_to_rows(signals))
        assessment_id = self.repository.insert_assessment(
            canonical_site_id,
            assessment_to_record(assessment),
        )

        previous_tier = str((snapshot.latest_assessment or {}).get("overall_tier") or "")
        if previous_tier and previous_tier != assessment.overall_tier:
            self.repository.record_change_event(
                canonical_site_id,
                source_family="assessment",
                change_category="rerank",
                event_type="tier_changed",
                event_summary=f"Site moved from {previous_tier} to {assessment.overall_tier}.",
                alert_priority="high" if assessment.overall_tier == "Tier 1" else "normal",
                resurfaced_flag=assessment.queue_recommendation == "Watchlist / Resurfaced",
                metadata={"triggered_by": triggered_by},
                enqueue_refresh=False,
            )

        return {
            "canonical_site_id": canonical_site_id,
            "assessment_id": assessment_id,
            "overall_tier": assessment.overall_tier,
            "overall_rank_score": assessment.overall_rank_score,
            "queue_recommendation": assessment.queue_recommendation,
            "human_review_required": assessment.human_review_required,
        }

    def process_pending_refresh_requests(self, limit: int = 20) -> dict[str, Any]:
        requests = self.repository.fetch_pending_refresh_requests(limit)
        processed: list[str] = []
        failed: list[dict[str, str]] = []

        for request in requests:
            request_id = str(request["id"])
            canonical_site_id = str(request["canonical_site_id"])
            if not self.repository.claim_refresh_request(request_id, "opportunity_engine"):
                continue
            try:
                self.refresh_site(canonical_site_id, triggered_by=str(request.get("trigger_source") or "queue"))
                self.repository.complete_refresh_request(request_id)
                processed.append(canonical_site_id)
            except Exception as exc:  # pragma: no cover - runtime behaviour
                self.logger.exception("phase_one_refresh_failed", extra={"canonical_site_id": canonical_site_id})
                self.repository.fail_refresh_request(request_id, str(exc))
                failed.append({"canonical_site_id": canonical_site_id, "error": str(exc)})

        return {
            "processed_count": len(processed),
            "processed_site_ids": processed,
            "failed": failed,
        }

    def refresh_explicit_sites(self, site_ids: list[str]) -> dict[str, Any]:
        processed: list[str] = []
        failed: list[dict[str, str]] = []
        for canonical_site_id in site_ids:
            try:
                self.refresh_site(canonical_site_id, triggered_by="explicit_request")
                processed.append(canonical_site_id)
            except Exception as exc:  # pragma: no cover - runtime behaviour
                failed.append({"canonical_site_id": canonical_site_id, "error": str(exc)})
        return {
            "processed_count": len(processed),
            "processed_site_ids": processed,
            "failed": failed,
        }

    def publish_planning_links(self, limit: int = 1000) -> dict[str, Any]:
        published_count = self.repository.publish_reconciled_planning_links(limit)
        return {"published_count": published_count}

    def run_weekly_planning_review(self, *, publish_limit: int = 1000, refresh_limit: int = 200) -> dict[str, Any]:
        publish_summary = self.publish_planning_links(limit=publish_limit)
        recent_changes = self.repository.fetch_recent_planning_changes(days_back=8)
        for row in recent_changes[: refresh_limit * 2]:
            event_type = _planning_event_type(row)
            summary = _planning_event_summary(row)
            self.repository.record_change_event(
                str(row["canonical_site_id"]),
                source_family="planning",
                change_category="planning_change",
                event_type=event_type,
                event_summary=summary,
                source_record_id=row.get("source_record_id"),
                alert_priority="high" if event_type in {"new_application", "refusal", "withdrawal", "lapse"} else "normal",
                resurfaced_flag=True,
                metadata={"planning_reference": row.get("planning_reference")},
                enqueue_refresh=True,
            )
        refresh_summary = self.process_pending_refresh_requests(limit=refresh_limit)
        return {
            "publish_summary": publish_summary,
            "planning_events_considered": len(recent_changes),
            "refresh_summary": refresh_summary,
        }

    def run_weekly_policy_review(self, *, refresh_limit: int = 200) -> dict[str, Any]:
        recent_changes = self.repository.fetch_recent_policy_changes(days_back=8)
        for row in recent_changes[: refresh_limit * 2]:
            self.repository.record_change_event(
                str(row["canonical_site_id"]),
                source_family=str(row["source_family"]),
                change_category="policy_change",
                event_type="weekly_policy_review",
                event_summary=f"Policy or audit context changed in {row['source_family']} evidence.",
                source_record_id=row.get("source_record_id"),
                alert_priority="high" if str(row["source_family"]) in {"ldp", "hla"} else "normal",
                resurfaced_flag=True,
                metadata={"headline_value": row.get("headline_value")},
                enqueue_refresh=True,
            )
        refresh_summary = self.process_pending_refresh_requests(limit=refresh_limit)
        return {
            "policy_events_considered": len(recent_changes),
            "refresh_summary": refresh_summary,
        }

    def record_review_status(
        self,
        canonical_site_id: str,
        review_status: str,
        actor_name: str,
        reason_text: str | None = None,
    ) -> None:
        self.repository.record_review_status(canonical_site_id, review_status, actor_name, reason_text)

    def record_review_note(
        self,
        canonical_site_id: str,
        actor_name: str,
        note_text: str,
    ) -> None:
        self.repository.record_review_note(canonical_site_id, actor_name, note_text)

    def record_manual_override(
        self,
        canonical_site_id: str,
        actor_name: str,
        override_key: str,
        override_value: dict[str, Any],
        reason_text: str | None = None,
    ) -> None:
        self.repository.record_manual_override(canonical_site_id, actor_name, override_key, override_value, reason_text)

    def record_title_action(
        self,
        canonical_site_id: str,
        action: str,
        actor_name: str,
        reason_text: str | None = None,
        title_number: str | None = None,
    ) -> None:
        self.repository.record_title_action(canonical_site_id, action, actor_name, reason_text, title_number)


def _planning_event_type(row: dict[str, Any]) -> str:
    decision = str(row.get("decision") or "").lower()
    status = str(row.get("application_status") or "").lower()
    if "refus" in decision:
        return "refusal"
    if "withdraw" in decision or "withdraw" in status:
        return "withdrawal"
    if "lapse" in decision or "lapse" in status:
        return "lapse"
    if status:
        return "new_application"
    return "planning_update"


def _planning_event_summary(row: dict[str, Any]) -> str:
    reference = str(row.get("planning_reference") or row.get("source_record_id") or "planning record")
    decision = str(row.get("decision") or row.get("application_status") or "updated").strip()
    return f"{reference} moved in the planning record set: {decision}."

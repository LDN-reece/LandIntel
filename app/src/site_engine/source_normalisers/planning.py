"""Normalise planning history and policy-position fields."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence
from src.site_engine.signal_extractors import extract_refusal_themes
from src.site_engine.site_evidence_schema import PlanningEvidence


ALLOCATION_PRIORITY = {"allocated": 4, "emerging": 3, "supportive": 2, "unallocated": 1, "unknown": 0}


def normalise_planning(
    planning_records: list[dict[str, Any]],
    context_records: list[dict[str, Any]],
    location: dict[str, Any] | None,
) -> tuple[PlanningEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    meaningful_records = [
        record
        for record in planning_records
        if str(record.get("record_type") or "").lower() in {"application", "pre_app", "appeal", "ppp"}
    ]
    latest_record = _latest_planning_record(meaningful_records)
    refusal_records = [
        record
        for record in meaningful_records
        if str(record.get("application_outcome") or "").lower() in {"refused", "dismissed"}
    ]
    refusal_themes = tuple(extract_refusal_themes(refusal_records))
    allocation_records = [
        record
        for record in context_records
        if str(record.get("context_type") or "").lower() in {"allocation", "ldp_allocation", "emerging_ldp"}
    ]
    allocation_status = "unknown"
    if allocation_records:
        allocation_status = max(
            (str(record.get("context_status") or "unknown").lower() for record in allocation_records),
            key=lambda value: ALLOCATION_PRIORITY.get(value, -1),
        )
        for row in allocation_records:
            add_evidence(
                field_evidence,
                "planning.allocation_status",
                "public.planning_context_records",
                row,
                f"Planning context shows allocation status '{row.get('context_status')}'.",
            )

    settlement_position = _settlement_position(location)
    if location:
        add_evidence(
            field_evidence,
            "planning.settlement_position",
            "public.site_locations",
            location,
            f"Site location indicates settlement position '{settlement_position}'.",
        )

    if meaningful_records:
        for row in meaningful_records:
            add_evidence(
                field_evidence,
                "planning.prior_application_count",
                "public.planning_records",
                row,
                "Meaningful Scottish planning history is linked to the site.",
            )
        if latest_record:
            add_evidence(
                field_evidence,
                "planning.latest_application_status",
                "public.planning_records",
                latest_record,
                f"Latest planning record status is '{latest_record.get('application_status') or 'unknown'}'.",
            )
            add_evidence(
                field_evidence,
                "planning.latest_application_outcome",
                "public.planning_records",
                latest_record,
                f"Latest planning record outcome is '{latest_record.get('application_outcome') or 'unknown'}'.",
            )
    for row in refusal_records:
        add_evidence(
            field_evidence,
            "planning.refusal_themes",
            "public.planning_records",
            row,
            "Planning refusal themes were extracted from linked decision history.",
        )

    appeal_status = "unknown"
    appeal_rows = [row for row in planning_records if str(row.get("record_type") or "").lower() == "appeal"]
    if appeal_rows:
        latest_appeal = _latest_planning_record(appeal_rows)
        appeal_status = str(latest_appeal.get("application_outcome") or "unknown").lower()
        add_evidence(
            field_evidence,
            "planning.appeal_status",
            "public.planning_records",
            latest_appeal,
            f"Latest linked appeal status is '{appeal_status}'.",
        )

    if meaningful_records:
        summary = f"{len(meaningful_records)} linked planning record(s); latest outcome is '{latest_record.get('application_outcome') or 'unknown'}'."
    else:
        summary = "No linked planning history."

    evidence = PlanningEvidence(
        allocation_status=allocation_status,
        settlement_position=settlement_position,
        prior_application_count=len(meaningful_records),
        latest_application_status=str(latest_record.get("application_status") or "unknown").lower() if latest_record else "unknown",
        latest_application_outcome=str(latest_record.get("application_outcome") or "unknown").lower() if latest_record else "unknown",
        refusal_themes=refusal_themes,
        appeal_status=appeal_status,
        planning_history_summary=summary,
    )
    return evidence, field_evidence


def _latest_planning_record(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not records:
        return None
    return max(records, key=lambda row: str(row.get("decision_date") or row.get("created_at") or ""))


def _settlement_position(location: dict[str, Any] | None) -> str:
    if not location:
        return "unknown"
    if location.get("within_settlement_boundary") is True:
        return "within_settlement_boundary"
    distance = location.get("distance_to_settlement_boundary_m")
    if distance is not None:
        try:
            if float(distance) <= 250:
                return "edge_of_settlement"
        except (TypeError, ValueError):
            pass
    relationship = str(location.get("settlement_relationship") or "").lower()
    return relationship or "outside_logical_growth_area"


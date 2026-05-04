"""Frontend-ready briefing helpers for the Phase One opportunity screen."""

from __future__ import annotations

from typing import Any


def build_opportunity_brief(detail: dict[str, Any]) -> dict[str, Any]:
    """Shape raw detail rows into LDN's two-layer review structure."""

    summary = detail["summary"]
    assessment = detail.get("assessment") or {}
    title = detail.get("title") or {}
    constraints = detail.get("constraints") or {}
    review_state = detail.get("review_state") or {}
    change_log = detail.get("change_log") or []

    latest_change = change_log[0] if change_log else {}
    good_items = list(assessment.get("good_items") or [])
    bad_items = list(assessment.get("bad_items") or [])
    ugly_items = list(assessment.get("ugly_items") or [])

    return {
        "header": {
            "canonical_site_id": summary["canonical_site_id"],
            "site_code": summary["site_code"],
            "site_name": summary["site_name"],
            "authority_name": summary.get("authority_name"),
            "area_acres": summary.get("area_acres"),
            "queue_name": review_state.get("review_queue") or assessment.get("queue_recommendation") or "New Candidates",
            "review_status": review_state.get("review_status") or "New candidate",
            "overall_tier": assessment.get("overall_tier") or "Unranked",
            "overall_rank_score": assessment.get("overall_rank_score"),
            "latest_change_summary": latest_change.get("event_summary"),
            "resurfaced_flag": latest_change.get("resurfaced_flag", False),
        },
        "headline_summary": {
            "why_it_surfaced": assessment.get("why_it_surfaced") or summary.get("surfaced_reason"),
            "why_it_survived": assessment.get("why_it_survived") or "No survival note recorded yet.",
            "source_route": _source_route(summary),
            "size_headline": _rank_label(assessment.get("size_rank")),
            "settlement_headline": _rank_label(assessment.get("settlement_position")),
            "planning_headline": _rank_label(assessment.get("planning_context_band")),
            "title_state": title.get("title_state") or assessment.get("title_state") or "commercial_inference",
            "ownership_control_fact_label": title.get("ownership_control_fact_label")
            or assessment.get("ownership_control_fact_label")
            or "commercial_inference",
            "key_positives": good_items[:3],
            "key_warnings": (bad_items + ugly_items)[:4],
        },
        "good_items": good_items,
        "bad_items": bad_items,
        "ugly_items": ugly_items,
        "due_diligence": {
            "summary": summary,
            "readiness": detail.get("readiness"),
            "title": title,
            "constraints": constraints,
            "assessment": assessment,
            "review_state": review_state,
            "sources": detail.get("source_rows") or [],
            "planning_records": detail.get("planning_records") or [],
            "hla_records": detail.get("hla_records") or [],
            "ldp_records": detail.get("ldp_records") or [],
            "ela_records": detail.get("ela_records") or [],
            "vdl_records": detail.get("vdl_records") or [],
            "bgs_records": detail.get("bgs_records") or [],
            "flood_records": detail.get("flood_records") or [],
            "parcel_rows": detail.get("parcel_rows") or [],
            "title_links": detail.get("title_links") or [],
            "title_validations": detail.get("title_validations") or [],
            "geometry_diagnostics": detail.get("geometry_diagnostics") or {},
            "constraint_overview": detail.get("constraint_overview") or {},
            "constraint_group_summaries": detail.get("constraint_group_summaries") or [],
            "constraint_measurements": detail.get("constraint_measurements") or [],
            "constraint_friction_facts": detail.get("constraint_friction_facts") or [],
            "signal_rows": detail.get("signal_rows") or [],
            "review_events": detail.get("review_events") or [],
            "manual_overrides": detail.get("manual_overrides") or [],
            "change_log": change_log,
        },
    }


def _source_route(summary: dict[str, Any]) -> str:
    source_families = list(summary.get("source_families_present") or [])
    if "planning" in source_families and "hla" in source_families:
        return "multi-source"
    if "planning" in source_families:
        return "planning-led"
    if "hla" in source_families:
        return "hla/hls-led"
    if "ela" in source_families or "vdl" in source_families:
        return "brownfield-led"
    if "ldp" in source_families:
        return "policy-led"
    return "parcel-led"


def _rank_label(value: str | None) -> str:
    if not value:
        return "Unknown"
    return value.replace("_", " ").title()

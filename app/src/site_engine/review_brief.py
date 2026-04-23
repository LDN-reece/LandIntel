"""Shape current site evidence into an analyst-style review brief."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def build_site_review_brief(detail: dict[str, Any]) -> dict[str, Any]:
    """Return a frontend-ready brief without exposing raw operational joins."""

    summary = detail["summary"]
    interpretation_evidence = detail["interpretation_evidence"]
    signal_evidence = detail["signal_evidence"]
    assessment = detail.get("assessment") or {}
    assessment_evidence = detail.get("assessment_evidence") or {}
    assessment_score_evidence = detail.get("assessment_score_evidence") or {}

    grouped_interpretations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for interpretation in detail["interpretations"]:
        grouped_interpretations[interpretation["category"]].append(
            {
                **interpretation,
                "evidence": interpretation_evidence.get(str(interpretation["id"]), []),
            }
        )

    signals = []
    for signal in detail["signals"]:
        signals.append({**signal, "evidence": signal_evidence.get(str(signal["id"]), [])})

    scorecards = []
    for score in detail.get("assessment_scores", []):
        scorecards.append(
            {
                **score,
                "evidence": assessment_score_evidence.get(str(score["id"]), []),
            }
        )

    why_this_site = [
        item["summary"]
        for item in grouped_interpretations.get("positive", [])[:3]
    ]
    if not why_this_site:
        why_this_site = [assessment.get("primary_reason") or summary.get("surfaced_reason") or "Further manual review is required to determine whether the site deserves escalation."]

    source_references = _dedupe_sources(
        [
            evidence
            for evidence_list in interpretation_evidence.values()
            for evidence in evidence_list
        ]
        + [
            evidence
            for evidence_list in signal_evidence.values()
            for evidence in evidence_list
        ]
        + [
            evidence
            for evidence_list in assessment_evidence.values()
            for evidence in evidence_list
        ]
        + [
            evidence
            for evidence_list in assessment_score_evidence.values()
            for evidence in evidence_list
        ]
    )

    return {
        "site_summary": {
            "site_code": summary["site_code"],
            "site_name": summary["site_name"],
            "workflow_status": summary["workflow_status"],
            "authority_name": summary.get("authority_name"),
            "nearest_settlement": summary.get("nearest_settlement"),
            "settlement_relationship": summary.get("settlement_relationship"),
            "area_acres": summary.get("area_acres"),
            "parcel_count": summary.get("parcel_count"),
            "component_count": summary.get("component_count"),
            "primary_title_number": summary.get("primary_title_number"),
            "surfaced_reason": summary.get("surfaced_reason"),
            "current_ruleset_version": summary.get("current_ruleset_version"),
        },
        "canonical_site": {
            "site_id": (detail.get("canonical_site") or {}).get("site_id"),
            "site_code": (detail.get("canonical_site") or {}).get("site_code"),
            "site_name_primary": (detail.get("canonical_site") or {}).get("site_name_primary"),
            "site_name_aliases": (detail.get("canonical_site") or {}).get("site_name_aliases") or [],
            "source_refs": (detail.get("canonical_site") or {}).get("source_refs") or [],
            "planning_refs": (detail.get("canonical_site") or {}).get("planning_refs") or [],
            "ldp_refs": (detail.get("canonical_site") or {}).get("ldp_refs") or [],
            "hla_refs": (detail.get("canonical_site") or {}).get("hla_refs") or [],
            "ela_refs": (detail.get("canonical_site") or {}).get("ela_refs") or [],
            "vdl_refs": (detail.get("canonical_site") or {}).get("vdl_refs") or [],
            "council_refs": (detail.get("canonical_site") or {}).get("council_refs") or [],
            "title_numbers": (detail.get("canonical_site") or {}).get("title_numbers") or [],
            "uprns": (detail.get("canonical_site") or {}).get("uprns") or [],
            "usrns": (detail.get("canonical_site") or {}).get("usrns") or [],
            "toids": (detail.get("canonical_site") or {}).get("toids") or [],
            "authority_refs": (detail.get("canonical_site") or {}).get("authority_refs") or [],
            "geometry_versions": (detail.get("canonical_site") or {}).get("geometry_versions") or [],
            "match_confidence": (detail.get("canonical_site") or {}).get("match_confidence"),
            "matched_reference_count": (detail.get("canonical_site") or {}).get("matched_reference_count") or 0,
            "unresolved_reference_count": (detail.get("canonical_site") or {}).get("unresolved_reference_count") or 0,
            "match_notes": (detail.get("canonical_site") or {}).get("match_notes"),
        },
        "assessment": {
            "id": assessment.get("id"),
            "bucket_code": assessment.get("bucket_code"),
            "bucket_label": assessment.get("bucket_label"),
            "likely_opportunity_type": assessment.get("likely_opportunity_type"),
            "monetisation_horizon": assessment.get("monetisation_horizon"),
            "horizon_year_band": assessment.get("horizon_year_band"),
            "dominant_blocker": assessment.get("dominant_blocker"),
            "primary_reason": assessment.get("primary_reason"),
            "secondary_reasons": assessment.get("secondary_reasons") or [],
            "buyer_profile_guess": assessment.get("buyer_profile_guess"),
            "likely_buyer_profiles": assessment.get("likely_buyer_profiles") or [],
            "cost_to_control_band": assessment.get("cost_to_control_band"),
            "human_review_required": assessment.get("human_review_required"),
            "review_flags": assessment.get("review_flags") or [],
            "hard_fail_flags": assessment.get("hard_fail_flags") or [],
            "explanation_text": assessment.get("explanation_text"),
            "scores": scorecards,
            "evidence": assessment_evidence.get(str(assessment.get("id")), []),
        },
        "site_construction": detail["geometry_components"],
        "facts": {
            "parcels": detail["parcels"],
            "planning_records": detail["planning_records"],
            "planning_context_records": detail["planning_context_records"],
            "constraints": detail["constraints"],
            "reference_aliases": detail.get("reference_aliases", []),
            "geometry_versions": detail.get("geometry_versions", []),
            "reconciliation_matches": detail.get("reconciliation_matches", []),
            "reconciliation_review_items": detail.get("reconciliation_review_items", []),
            "infrastructure_records": detail.get("infrastructure_records", []),
            "control_records": detail.get("control_records", []),
            "comparable_market_records": detail["comparable_market_records"],
            "buyer_matches": detail["buyer_matches"],
        },
        "signals": signals,
        "positives": grouped_interpretations.get("positive", []),
        "risks": grouped_interpretations.get("risk", []),
        "possible_fatal_issues": grouped_interpretations.get("possible_fatal", []),
        "unknowns": grouped_interpretations.get("unknown", []),
        "why_this_site_might_deserve_review": why_this_site,
        "source_references": source_references,
    }


def _dedupe_sources(evidence_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str | None, str | None, str | None]] = set()
    references: list[dict[str, Any]] = []
    for row in evidence_rows:
        key = (
            row.get("dataset_name"),
            row.get("source_table"),
            row.get("source_record_id"),
        )
        if key in seen:
            continue
        seen.add(key)
        references.append(row)
    return sorted(
        references,
        key=lambda row: (
            str(row.get("dataset_name") or ""),
            str(row.get("source_identifier") or ""),
            str(row.get("source_record_id") or ""),
        ),
    )

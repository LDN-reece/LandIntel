"""Derive atomic site signals from the normalised Scottish evidence model."""

from __future__ import annotations

from typing import Any

from src.site_engine.site_evidence_schema import SiteEvidence
from src.site_engine.source_normalisers import normalise_site_evidence
from src.site_engine.types import EvidenceItem, SignalResult, SiteSnapshot


def build_site_signals(snapshot: SiteSnapshot, evidence: SiteEvidence | None = None) -> list[SignalResult]:
    """Build atomic signals from linked site evidence before routing or scoring."""

    evidence = evidence or normalise_site_evidence(snapshot)
    location = snapshot.location or {}
    comparable_count = len(
        [row for row in snapshot.comparable_market_records if str(row.get("comparable_type") or "").lower() == "new_build"]
    )
    legal_issue_count = len(evidence.ownership.legal_control_issue_flags)
    buyer_fit_count = evidence.market.strong_buyer_fit_count + evidence.market.moderate_buyer_fit_count
    high_constraint_count = len(
        [row for row in snapshot.constraints if str(row.get("severity") or "").lower() == "high"]
    )

    signals = [
        _text_signal(
            "canonical_match_confidence",
            "Canonical match confidence",
            "reconciliation",
            evidence.reconciliation.match_confidence,
            True,
            "Confidence that linked source references resolve cleanly to the canonical site.",
            evidence.field_evidence.get("reconciliation.source_refs", []),
        ),
        _numeric_signal(
            "matched_reference_count",
            "Matched reference count",
            "reconciliation",
            evidence.reconciliation.matched_reference_count,
            True,
            "Count of source references already rolled up into the canonical site.",
            evidence.field_evidence.get("reconciliation.source_refs", []),
        ),
        _numeric_signal(
            "unresolved_reference_count",
            "Unresolved reference count",
            "reconciliation",
            evidence.reconciliation.unresolved_reference_count,
            True,
            "Count of linked references that still require manual reconciliation review.",
            evidence.field_evidence.get("reconciliation.source_refs", []),
        ),
        _text_signal(
            "settlement_boundary_position",
            "Settlement boundary position",
            "boundary",
            evidence.boundary.settlement_boundary.position,
            evidence.boundary.settlement_boundary.position != "unknown",
            "Boundary engine classification for how the site sits relative to the settlement boundary.",
            evidence.field_evidence.get("boundary.settlement_boundary.position", []),
        ),
        _text_signal(
            "council_boundary_position",
            "Council boundary position",
            "boundary",
            evidence.boundary.council_boundary.position,
            evidence.boundary.council_boundary.position != "unknown",
            "Boundary engine classification for how the site sits within the council boundary.",
            evidence.field_evidence.get("boundary.council_boundary.position", []),
        ),
        _text_signal(
            "green_belt_position",
            "Green belt position",
            "boundary",
            evidence.boundary.green_belt.position,
            evidence.boundary.green_belt.position != "unknown",
            "Boundary engine classification for how the site relates to green belt.",
            evidence.field_evidence.get("boundary.green_belt.position", []),
        ),
        _bool_signal(
            "within_settlement_boundary",
            "Within settlement boundary",
            "location",
            location.get("within_settlement_boundary"),
            location.get("within_settlement_boundary") is not None,
            "Settlement boundary evidence from the canonical site location.",
            evidence.field_evidence.get("planning.settlement_position", []),
        ),
        _numeric_signal(
            "distance_to_settlement_boundary_m",
            "Distance to settlement boundary (m)",
            "location",
            location.get("distance_to_settlement_boundary_m"),
            location.get("distance_to_settlement_boundary_m") is not None,
            "Distance from the canonical site geometry to the settlement boundary.",
            evidence.field_evidence.get("ldp.settlement_boundary_relation", []),
        ),
        _numeric_signal(
            "planning_application_count",
            "Meaningful planning application count",
            "planning",
            evidence.planning.prior_application_count,
            True,
            "Count of meaningful linked planning records.",
            evidence.field_evidence.get("planning.prior_application_count", []),
        ),
        _bool_signal(
            "previous_application_exists",
            "Previous application exists",
            "planning",
            evidence.planning.prior_application_count > 0,
            True,
            "Whether meaningful planning history is already linked to the site.",
            evidence.field_evidence.get("planning.prior_application_count", []),
        ),
        _text_signal(
            "previous_application_outcome",
            "Previous application outcome",
            "planning",
            evidence.planning.latest_application_outcome,
            evidence.planning.latest_application_outcome != "unknown",
            evidence.planning.planning_history_summary,
            evidence.field_evidence.get("planning.latest_application_outcome", []),
        ),
        _json_signal(
            "refusal_themes",
            "Refusal themes",
            "planning",
            list(evidence.planning.refusal_themes),
            bool(evidence.planning.refusal_themes),
            "Structured refusal themes extracted from linked planning history.",
            evidence.field_evidence.get("planning.refusal_themes", []),
        ),
        _text_signal(
            "allocation_status",
            "Allocation status",
            "planning_context",
            evidence.planning.allocation_status,
            evidence.planning.allocation_status != "unknown",
            "Current allocation or plan-support status for the site.",
            evidence.field_evidence.get("planning.allocation_status", []),
        ),
        _text_signal(
            "policy_support_level",
            "Policy support level",
            "planning_context",
            evidence.ldp.policy_support_level,
            evidence.ldp.policy_support_level != "unknown",
            "Combined adopted, emerging, and settlement policy support signal.",
            evidence.field_evidence.get("ldp.adopted_ldp_status", []) + evidence.field_evidence.get("ldp.emerging_ldp_status", []),
        ),
        _text_signal(
            "hla_effectiveness_status",
            "HLA effectiveness status",
            "planning_context",
            evidence.hla.effectiveness_status,
            evidence.hla.effectiveness_status != "unknown",
            "Housing land audit effectiveness status where linked.",
            evidence.field_evidence.get("hla.effectiveness_status", []),
        ),
        _text_signal(
            "hla_programming_horizon",
            "HLA programming horizon",
            "planning_context",
            evidence.hla.programming_horizon,
            evidence.hla.programming_horizon != "unknown",
            "Housing land audit programming horizon where linked.",
            evidence.field_evidence.get("hla.programming_horizon", []),
        ),
        _text_signal(
            "flood_risk",
            "Flood risk",
            "constraints",
            evidence.flood.flood_combined_severity,
            evidence.flood.flood_combined_severity != "unknown",
            "Combined Scottish flood signal derived from linked evidence.",
            evidence.field_evidence.get("flood.flood_combined_severity", []),
        ),
        _numeric_signal(
            "river_flood_overlap_pct",
            "River flood overlap (%)",
            "constraints",
            evidence.flood.river_flood_overlap_pct,
            True,
            "Percentage of the site linked to river flood evidence.",
            evidence.field_evidence.get("flood.river_flood_overlap_pct", []),
        ),
        _numeric_signal(
            "surface_water_overlap_pct",
            "Surface water overlap (%)",
            "constraints",
            evidence.flood.surface_water_overlap_pct,
            True,
            "Percentage of the site linked to surface water flood evidence.",
            evidence.field_evidence.get("flood.surface_water_overlap_pct", []),
        ),
        _text_signal(
            "mining_risk",
            "Mining risk",
            "constraints",
            "high" if evidence.bgs.opencast_overlap else "low",
            True,
            "Mining and opencast legacy signal derived from BGS-style evidence.",
            evidence.field_evidence.get("bgs.opencast_overlap", []),
        ),
        _text_signal(
            "bgs_investigation_intensity",
            "BGS investigation intensity",
            "bgs",
            evidence.bgs_reasoning.investigation_intensity,
            evidence.bgs_reasoning.investigation_intensity != "none",
            "BGS reasoning engine estimate of prior investigation intensity.",
            evidence.field_evidence.get("bgs_reasoning.investigation_intensity", []),
        ),
        _text_signal(
            "bgs_ground_complexity_signal",
            "BGS ground complexity signal",
            "bgs",
            evidence.bgs_reasoning.ground_complexity_signal,
            True,
            "BGS reasoning engine estimate of subsurface complexity.",
            evidence.field_evidence.get("bgs_reasoning.ground_complexity_signal", []),
        ),
        _text_signal(
            "hydrogeology_caution",
            "Hydrogeology caution",
            "bgs",
            evidence.bgs_reasoning.hydrogeology_caution,
            True,
            "BGS reasoning engine hydrogeology caution signal.",
            evidence.field_evidence.get("bgs_reasoning.hydrogeology_caution", []),
        ),
        _text_signal(
            "access_status",
            "Access status",
            "infrastructure",
            _access_status(evidence.infrastructure.access_complexity),
            evidence.infrastructure.access_complexity != "unknown",
            "Access status derived from infrastructure and constraint evidence.",
            evidence.field_evidence.get("infrastructure.access_complexity", []),
        ),
        _text_signal(
            "drainage_burden",
            "Drainage burden",
            "infrastructure",
            evidence.infrastructure.drainage_burden,
            evidence.infrastructure.drainage_burden != "unknown",
            "Drainage burden from linked evidence.",
            evidence.field_evidence.get("infrastructure.drainage_burden", []),
        ),
        _text_signal(
            "wastewater_burden",
            "Wastewater burden",
            "infrastructure",
            evidence.infrastructure.wastewater_burden,
            evidence.infrastructure.wastewater_burden != "unknown",
            "Wastewater burden from linked evidence.",
            evidence.field_evidence.get("infrastructure.wastewater_burden", []),
        ),
        _text_signal(
            "roads_burden",
            "Roads burden",
            "infrastructure",
            evidence.infrastructure.roads_burden,
            evidence.infrastructure.roads_burden != "unknown",
            "Roads burden from linked evidence.",
            evidence.field_evidence.get("infrastructure.roads_burden", []),
        ),
        _text_signal(
            "education_burden",
            "Education burden",
            "infrastructure",
            evidence.infrastructure.education_burden,
            evidence.infrastructure.education_burden != "unknown",
            "Education burden from linked evidence.",
            evidence.field_evidence.get("infrastructure.education_burden", []),
        ),
        _text_signal(
            "utilities_burden",
            "Utilities burden",
            "infrastructure",
            evidence.infrastructure.utilities_burden,
            evidence.infrastructure.utilities_burden != "unknown",
            "Utilities burden from linked evidence.",
            evidence.field_evidence.get("infrastructure.utilities_burden", []),
        ),
        _text_signal(
            "overall_utility_burden",
            "Overall utility burden",
            "infrastructure",
            evidence.utility.overall_utility_burden,
            evidence.utility.overall_utility_burden != "unknown",
            "Utility burden inference from drainage, wastewater, utilities, and plan-cycle evidence.",
            evidence.field_evidence.get("utility.overall_utility_burden", [])
            + evidence.field_evidence.get("utility.water_and_wastewater_signal", [])
            + evidence.field_evidence.get("infrastructure.drainage_burden", [])
            + evidence.field_evidence.get("infrastructure.wastewater_burden", [])
            + evidence.field_evidence.get("infrastructure.utilities_burden", []),
        ),
        _text_signal(
            "broadband_connectivity_signal",
            "Broadband connectivity signal",
            "market",
            evidence.utility.broadband_connectivity_signal,
            evidence.utility.broadband_connectivity_signal != "unknown",
            "Connectivity proxy used as a lightweight marketability signal.",
            evidence.field_evidence.get("utility.broadband_connectivity_signal", []),
        ),
        _text_signal(
            "progression_level",
            "Progression level",
            "prior_progression",
            evidence.prior_progression.progression_level,
            evidence.prior_progression.progression_level != "none",
            "Prior progression signal assembled from planning and investigation history.",
            evidence.field_evidence.get("prior_progression.progression_level", []),
        ),
        _bool_signal(
            "sponsor_failure_indicator",
            "Sponsor failure indicator",
            "prior_progression",
            evidence.prior_progression.sponsor_failure_indicator,
            True,
            "Whether prior failure appears linked to sponsor or timing issues.",
            evidence.field_evidence.get("prior_progression.sponsor_failure_indicator", []),
        ),
        _bool_signal(
            "vdl_register_status",
            "On vacant and derelict land register",
            "ground",
            evidence.vdl.on_vdl_register,
            True,
            "Vacant and derelict land register signal.",
            evidence.field_evidence.get("vdl.on_vdl_register", []),
        ),
        _text_signal(
            "previous_use_type",
            "Previous use type",
            "ground",
            evidence.use_classification.previous_site_use,
            evidence.use_classification.previous_site_use != "mixed / unclear",
            "Previous use type inferred from VDL, planning, and brownfield evidence.",
            evidence.field_evidence.get("use.previous_site_use", []),
        ),
        _text_signal(
            "current_building_use",
            "Current building use",
            "ground",
            evidence.use_classification.current_building_use,
            evidence.use_classification.current_building_use != "unknown",
            "Current building use inferred from linked source evidence.",
            evidence.field_evidence.get("use.current_building_use", []),
        ),
        _numeric_signal(
            "years_on_vdl_register",
            "Years on VDL register",
            "ground",
            evidence.vdl.years_on_register,
            evidence.vdl.on_vdl_register,
            "Approximate years on the vacant and derelict land register.",
            evidence.field_evidence.get("vdl.years_on_register", []),
        ),
        _numeric_signal(
            "title_count",
            "Title count",
            "ownership",
            evidence.ownership.title_count,
            evidence.ownership.title_count > 0,
            "Distinct title count linked to the canonical site.",
            evidence.field_evidence.get("ownership.title_count", []),
        ),
        _text_signal(
            "ownership_fragmentation_level",
            "Ownership fragmentation level",
            "ownership",
            evidence.ownership.ownership_fragmentation_level,
            evidence.ownership.ownership_fragmentation_level != "unknown",
            "Ownership fragmentation level from linked control evidence.",
            evidence.field_evidence.get("ownership.ownership_fragmentation_level", []),
        ),
        _numeric_signal(
            "legal_control_issue_count",
            "Legal control issue count",
            "ownership",
            legal_issue_count,
            True,
            "Count of linked legal or control issue flags.",
            evidence.field_evidence.get("ownership.legal_control_issue_flags", []),
        ),
        _text_signal(
            "new_build_comparable_strength",
            "New build comparable strength",
            "market",
            evidence.market.comparable_strength,
            evidence.market.comparable_strength != "unknown",
            "Strength of linked new-build comparable evidence.",
            evidence.field_evidence.get("market.comparable_strength", []),
        ),
        _numeric_signal(
            "comparable_sale_count",
            "Comparable sale count",
            "market",
            comparable_count,
            True,
            "Count of linked new-build comparable sale records.",
            evidence.field_evidence.get("market.comparable_strength", []),
        ),
        _numeric_signal(
            "buyer_fit_count",
            "Buyer fit count",
            "market",
            buyer_fit_count,
            True,
            "Number of strong or moderate buyer-profile matches.",
            evidence.field_evidence.get("market.buyer_profile_fit", []),
        ),
        _text_signal(
            "buyer_depth_estimate",
            "Buyer depth estimate",
            "market",
            evidence.market.buyer_depth_estimate,
            evidence.market.buyer_depth_estimate != "unknown",
            "Estimated depth of the future buyer universe.",
            evidence.field_evidence.get("market.buyer_depth_estimate", []),
        ),
        _text_signal(
            "settlement_strength",
            "Settlement strength",
            "market",
            evidence.market.settlement_strength,
            evidence.market.settlement_strength != "unknown",
            "Market strength of the surrounding settlement.",
            evidence.field_evidence.get("market.settlement_strength", []),
        ),
        _numeric_signal(
            "critical_constraint_count",
            "Critical constraint count",
            "constraints",
            high_constraint_count,
            True,
            "Count of linked constraint rows currently screening at high severity.",
            _first_non_empty(
                evidence.field_evidence.get("flood.flood_combined_severity", []),
                evidence.field_evidence.get("bgs.opencast_overlap", []),
                evidence.field_evidence.get("vdl.on_vdl_register", []),
            ),
        ),
    ]
    return signals


def _bool_signal(
    key: str,
    label: str,
    group: str,
    value: bool | None,
    is_known: bool,
    reasoning: str,
    evidence: list[EvidenceItem],
) -> SignalResult:
    state = "known" if is_known and evidence else "inferred" if is_known else "unknown"
    return SignalResult(
        key=key,
        label=label,
        group=group,
        value_type="boolean",
        state=state,
        bool_value=value if is_known else None,
        reasoning=reasoning if is_known else f"{label} is not yet evidenced for this site.",
        evidence=evidence,
    )


def _numeric_signal(
    key: str,
    label: str,
    group: str,
    value: Any,
    is_known: bool,
    reasoning: str,
    evidence: list[EvidenceItem],
) -> SignalResult:
    numeric_value = float(value) if is_known and value is not None else None
    state = "known" if is_known and evidence else "inferred" if is_known else "unknown"
    return SignalResult(
        key=key,
        label=label,
        group=group,
        value_type="numeric",
        state=state,
        numeric_value=numeric_value,
        reasoning=reasoning if is_known else f"{label} is not yet evidenced for this site.",
        evidence=evidence,
    )


def _text_signal(
    key: str,
    label: str,
    group: str,
    value: str,
    is_known: bool,
    reasoning: str,
    evidence: list[EvidenceItem],
) -> SignalResult:
    text_value = value if is_known else "unknown"
    state = "known" if is_known and evidence else "inferred" if is_known else "unknown"
    return SignalResult(
        key=key,
        label=label,
        group=group,
        value_type="text",
        state=state,
        text_value=text_value,
        reasoning=reasoning if is_known else f"{label} is not yet evidenced for this site.",
        evidence=evidence,
    )


def _json_signal(
    key: str,
    label: str,
    group: str,
    value: list[Any] | dict[str, Any],
    is_known: bool,
    reasoning: str,
    evidence: list[EvidenceItem],
) -> SignalResult:
    state = "known" if is_known and evidence else "inferred" if is_known else "unknown"
    return SignalResult(
        key=key,
        label=label,
        group=group,
        value_type="json",
        state=state,
        json_value=value if is_known else [],
        reasoning=reasoning if is_known else f"{label} is not yet evidenced for this site.",
        evidence=evidence,
    )


def _access_status(access_complexity: str) -> str:
    mapping = {
        "confirmed": "confirmed",
        "straightforward": "confirmed",
        "direct": "confirmed",
        "possible": "possible",
        "constrained": "possible",
        "third_party": "possible",
        "problematic": "problematic",
    }
    return mapping.get(access_complexity, "unknown")


def _first_non_empty(*groups: list[EvidenceItem]) -> list[EvidenceItem]:
    evidence: list[EvidenceItem] = []
    for group in groups:
        evidence.extend(group)
    return evidence

"""Seven-score commercial scoring for Scottish site routing."""

from __future__ import annotations

from collections import defaultdict

from src.site_engine.confidence_engine import infer_score_confidence
from src.site_engine.signal_extractors import extract_blocker_signals
from src.site_engine.site_evidence_schema import SiteEvidence
from src.site_engine.types import ScoreContribution, SiteAssessmentScore


SCORE_LABELS = {
    "P": "Planning Strength",
    "G": "Ground / Constraints",
    "I": "Infrastructure",
    "R": "Prior Progress",
    "F": "Fixability",
    "K": "Cost to Control",
    "B": "Buyer Depth",
}
BASE_SCORES = {"P": 2, "G": 3, "I": 3, "R": 1, "F": 3, "K": 3, "B": 3}


def build_scorecard(evidence: SiteEvidence) -> tuple[dict[str, SiteAssessmentScore], dict[str, object]]:
    """Turn normalised evidence into seven explainable commercial scores."""

    contributions: dict[str, list[ScoreContribution]] = defaultdict(list)
    blocker_signals = extract_blocker_signals(evidence)

    _planning_contributions(contributions, evidence)
    _ground_contributions(contributions, evidence)
    _infrastructure_contributions(contributions, evidence)
    _progress_contributions(contributions, evidence)
    _fixability_contributions(contributions, evidence, blocker_signals)
    _control_cost_contributions(contributions, evidence)
    _buyer_depth_contributions(contributions, evidence)

    scorecard: dict[str, SiteAssessmentScore] = {}
    for score_code, label in SCORE_LABELS.items():
        score_contributions = contributions.get(score_code, [])
        score_value = max(1, min(5, BASE_SCORES[score_code] + sum(item.delta for item in score_contributions)))
        has_unknown_pressure = _has_unknown_pressure(score_code, evidence)
        confidence = infer_score_confidence(score_contributions, has_unknown_pressure=has_unknown_pressure)
        dominant_contribution = max(score_contributions, key=lambda item: abs(item.delta), default=None)
        summary = dominant_contribution.summary if dominant_contribution else f"{label} remains weakly evidenced."
        reasoning_parts = [item.reasoning for item in score_contributions]
        reasoning = " ".join(reasoning_parts) if reasoning_parts else "No strong evidence contributions were available."
        blocker_theme = dominant_contribution.blocker_theme if dominant_contribution else None
        scorecard[score_code] = SiteAssessmentScore(
            score_code=score_code,
            label=label,
            value=score_value,
            confidence_label=confidence,
            summary=summary,
            reasoning=reasoning,
            blocker_theme=blocker_theme,
            contributions=score_contributions,
        )

    return scorecard, blocker_signals


def _planning_contributions(contributions: dict[str, list[ScoreContribution]], evidence: SiteEvidence) -> None:
    if evidence.planning.allocation_status == "allocated" or evidence.ldp.adopted_ldp_status == "allocated":
        contributions["P"].append(
            ScoreContribution("P", "planning_policy", 2, "Allocated policy position strengthens planning.", "Allocated or adopted policy support gives the site a real planning route.", ["planning.allocation_status", "ldp.adopted_ldp_status"], "planning")
        )
    elif evidence.planning.allocation_status == "emerging" or evidence.ldp.emerging_ldp_status == "emerging":
        contributions["P"].append(
            ScoreContribution("P", "planning_policy", 1, "Emerging support suggests a plausible planning route.", "Emerging LDP support improves planning strength even if the site is not yet market-ready.", ["planning.allocation_status", "ldp.emerging_ldp_status"], "planning")
        )
    if evidence.boundary.settlement_boundary.position in {"fully_inside", "mostly_inside", "edge_straddling", "just_outside"} or evidence.planning.settlement_position in {"within_settlement_boundary", "edge_of_settlement"}:
        contributions["P"].append(
            ScoreContribution("P", "settlement_logic", 1, "Settlement relationship supports planning logic.", "Being within, straddling, or tightly rounding off a settlement improves the Scottish policy narrative.", ["planning.settlement_position", "ldp.settlement_boundary_relation", "boundary.settlement_boundary.position"], "planning")
        )
    if evidence.hla.effectiveness_status == "effective":
        contributions["P"].append(
            ScoreContribution("P", "hla", 1, "HLA effectiveness supports planning confidence.", "Effective housing-land status is a direct planning strength signal.", ["hla.effectiveness_status"], "planning")
        )
    if evidence.boundary.green_belt.position in {"fully_inside", "mostly_inside"} and evidence.planning.allocation_status != "allocated":
        contributions["P"].append(
            ScoreContribution("P", "planning_policy", -1, "Green belt position weakens the planning route.", "Green-belt positioning does not kill the site automatically, but it materially weakens the immediate policy narrative unless allocation evidence is stronger.", ["boundary.green_belt.position"], "planning")
        )
    if "planning_principle" in evidence.planning.refusal_themes or evidence.ldp.policy_support_level == "weak":
        contributions["P"].append(
            ScoreContribution("P", "planning_history", -2, "Planning principle looks weak.", "Refusal on planning principle or weak policy support materially reduces the credibility of the route.", ["planning.refusal_themes", "ldp.policy_support_level"], "planning")
        )
    elif evidence.planning.latest_application_outcome in {"refused", "dismissed"} and evidence.planning.refusal_themes:
        contributions["P"].append(
            ScoreContribution("P", "planning_history", -1, "Planning history shows resistance.", "Linked refusals do not kill the site automatically, but they do weaken current planning strength.", ["planning.latest_application_outcome", "planning.refusal_themes"], "planning")
        )


def _ground_contributions(contributions: dict[str, list[ScoreContribution]], evidence: SiteEvidence) -> None:
    if evidence.flood.flood_combined_severity in {"none", "low"} and not evidence.vdl.on_vdl_register and not evidence.bgs.opencast_overlap:
        contributions["G"].append(
            ScoreContribution("G", "ground_screen", 2, "Ground profile currently screens as clean.", "Low flood exposure and no strong brownfield or mining proxy improve ground workability.", ["flood.flood_combined_severity"], "ground")
        )
    if evidence.use_classification.previous_site_use in {"virgin greenfield", "agricultural field"}:
        contributions["G"].append(
            ScoreContribution("G", "use_inference", 1, "Previous use looks clean and uncomplicated.", "Agricultural or virgin greenfield history supports a cleaner ground profile than prior industrial or derelict use.", ["use.previous_site_use"], "ground")
        )
    if evidence.vdl.on_vdl_register and evidence.vdl.previous_use_type in {"industrial", "depot", "storage"}:
        contributions["G"].append(
            ScoreContribution("G", "vdl", -2, "Brownfield history reduces ground cleanliness.", "Industrial or depot VDL history implies abnormal ground or remediation risk.", ["vdl.on_vdl_register", "vdl.previous_use_type"], "ground")
        )
    elif evidence.vdl.on_vdl_register:
        contributions["G"].append(
            ScoreContribution("G", "vdl", -1, "VDL status introduces some technical uncertainty.", "Vacant and derelict status is a risk flag but not an automatic fatality.", ["vdl.on_vdl_register"], "ground")
        )
    if evidence.flood.flood_combined_severity == "medium":
        contributions["G"].append(
            ScoreContribution("G", "flood", -1, "Flood exposure adds material technical friction.", "Flood mitigation may still be workable, but it lowers the clean-ground score.", ["flood.flood_combined_severity", "flood.surface_water_overlap_pct"], "ground")
        )
    elif evidence.flood.flood_combined_severity == "high":
        contributions["G"].append(
            ScoreContribution("G", "flood", -2, "Flood exposure may dominate ground risk.", "High flood severity can move the site toward open-ended abnormal cost risk.", ["flood.flood_combined_severity", "flood.river_flood_overlap_pct"], "ground")
        )
    if evidence.bgs.opencast_overlap:
        contributions["G"].append(
            ScoreContribution("G", "bgs", -2, "Mining legacy materially reduces ground confidence.", "Opencast or mining overlap is a stronger technical warning than boreholes alone.", ["bgs.opencast_overlap"], "ground")
        )
    elif evidence.bgs_reasoning.ground_complexity_signal == "medium" or evidence.bgs.aquifer_presence or evidence.bgs.water_well_presence:
        contributions["G"].append(
            ScoreContribution("G", "bgs", -1, "Groundwater context adds technical complexity.", "Hydrogeological sensitivity can complicate foundations and drainage, even where fixable.", ["bgs.aquifer_presence", "bgs.water_well_presence", "bgs_reasoning.ground_complexity_signal"], "ground")
        )


def _infrastructure_contributions(contributions: dict[str, list[ScoreContribution]], evidence: SiteEvidence) -> None:
    low_friction = all(
        value in {"none", "low", "unknown"}
        for value in (
            evidence.infrastructure.drainage_burden,
            evidence.infrastructure.wastewater_burden,
            evidence.infrastructure.roads_burden,
            evidence.infrastructure.education_burden,
            evidence.infrastructure.utilities_burden,
        )
    )
    if evidence.infrastructure.access_complexity in {"confirmed", "straightforward", "direct"} and low_friction:
        contributions["I"].append(
            ScoreContribution("I", "infrastructure", 2, "Infrastructure burden looks light.", "Straightforward access and limited servicing pressure improve deliverability.", ["infrastructure.access_complexity"], "infrastructure")
        )
    if evidence.infrastructure.access_complexity in {"possible", "constrained", "third_party"}:
        contributions["I"].append(
            ScoreContribution("I", "infrastructure", -1, "Access is not yet straightforward.", "Unproven or third-party access reduces delivery ease.", ["infrastructure.access_complexity"], "roads")
        )
    if evidence.infrastructure.access_complexity == "problematic":
        contributions["I"].append(
            ScoreContribution("I", "infrastructure", -2, "Access may be a major blocker.", "Problematic access can freeze an otherwise attractive site.", ["infrastructure.access_complexity"], "roads")
        )
    for field_key, label in (
        ("drainage_burden", "Drainage"),
        ("wastewater_burden", "Wastewater"),
        ("roads_burden", "Roads"),
        ("education_burden", "Education"),
        ("utilities_burden", "Utilities"),
    ):
        value = getattr(evidence.infrastructure, field_key)
        if value == "high":
            contributions["I"].append(
                ScoreContribution("I", "infrastructure", -1, f"{label} burden is material.", f"{label} burden is significant enough to slow or complicate delivery.", [f"infrastructure.{field_key}"], label.lower())
            )
        elif value == "critical":
            contributions["I"].append(
                ScoreContribution("I", "infrastructure", -2, f"{label} burden dominates delivery risk.", f"{label} burden currently looks like the main constraint on delivery timing.", [f"infrastructure.{field_key}"], label.lower())
            )
    if evidence.hla.effectiveness_status == "effective":
        contributions["I"].append(
            ScoreContribution("I", "hla", 1, "HLA evidence supports deliverability.", "Effective HLA status is a positive infrastructure and delivery signal.", ["hla.effectiveness_status"], "infrastructure")
        )
    if evidence.utility.overall_utility_burden == "high":
        contributions["I"].append(
            ScoreContribution("I", "utility", -1, "Utility burden is material.", "Drainage, wastewater, or broader utility friction looks meaningful enough to slow delivery.", ["utility.overall_utility_burden", "utility.water_and_wastewater_signal", "infrastructure.drainage_burden", "infrastructure.wastewater_burden", "infrastructure.utilities_burden"], "utilities")
        )
    elif evidence.utility.overall_utility_burden == "critical":
        contributions["I"].append(
            ScoreContribution("I", "utility", -2, "Utility burden may dominate delivery timing.", "Combined drainage, wastewater, or utility pressure currently looks like a dominant blocker.", ["utility.overall_utility_burden", "utility.water_and_wastewater_signal", "infrastructure.drainage_burden", "infrastructure.wastewater_burden", "infrastructure.utilities_burden"], "utilities")
        )


def _progress_contributions(contributions: dict[str, list[ScoreContribution]], evidence: SiteEvidence) -> None:
    level = evidence.prior_progression.progression_level
    if level == "advanced":
        delta = 4
    elif level == "high":
        delta = 3
    elif level == "medium":
        delta = 1
    else:
        delta = 0
    if delta:
        contributions["R"].append(
            ScoreContribution("R", "progression", delta, "There is meaningful prior progression.", "Planning history, layouts, reports, or SI indicators show the site has already absorbed real effort.", ["prior_progression.progression_level"], "progression")
        )
    if evidence.bgs_reasoning.prior_progression_signal_strength in {"medium", "high"} or evidence.bgs.site_investigation_overlap or evidence.bgs.borehole_count_site:
        contributions["R"].append(
            ScoreContribution("R", "bgs", 1, "Ground investigation points to prior work.", "BGS investigation evidence is read mainly as prior progression rather than automatic bad ground.", ["bgs.site_investigation_overlap", "bgs.borehole_count_site", "bgs_reasoning.investigation_intensity"], "progression")
        )


def _fixability_contributions(
    contributions: dict[str, list[ScoreContribution]],
    evidence: SiteEvidence,
    blocker_signals: dict[str, object],
) -> None:
    if evidence.prior_progression.sponsor_failure_indicator:
        contributions["F"].append(
            ScoreContribution("F", "progression", 2, "Failure history looks commercial rather than fatal.", "Sponsor failure or timing-led stall reasons usually improve revivability.", ["prior_progression.sponsor_failure_indicator"], "timing")
        )
    if any(theme in {"design_density", "access", "roads", "drainage"} for theme in evidence.planning.refusal_themes):
        contributions["F"].append(
            ScoreContribution("F", "planning_history", 1, "Prior issues look potentially fixable.", "Technical or design-led planning issues are often fixable with redesign, servicing, or repricing.", ["planning.refusal_themes"], "fixability")
        )
    if "planning_principle" in evidence.planning.refusal_themes:
        contributions["F"].append(
            ScoreContribution("F", "planning_history", -2, "Planning principle weakness hurts fixability.", "If the core policy route is wrong, the problem may be structural rather than fixable.", ["planning.refusal_themes"], "planning")
        )
    if blocker_signals.get("bounded_flags"):
        contributions["F"].append(
            ScoreContribution("F", "blockers", 1, "Current issues look bounded.", "The dominant blockers appear diagnosable and priceable rather than open-ended.", ["flood.flood_combined_severity", "infrastructure.access_complexity"], "fixability")
        )
    if blocker_signals.get("open_ended_flags"):
        contributions["F"].append(
            ScoreContribution("F", "blockers", -2, "Some risks may be open-ended.", "Open-ended technical, planning, or control problems materially weaken fixability.", ["ownership.legal_control_issue_flags", "flood.flood_combined_severity", "planning.refusal_themes"], "fixability")
        )
    if evidence.utility.overall_utility_burden == "critical":
        contributions["F"].append(
            ScoreContribution("F", "utility", -1, "Utility burden may be hard to unwind.", "Where drainage, wastewater, or utility issues dominate, the fix may depend on third-party solutions rather than simple redesign.", ["utility.overall_utility_burden", "infrastructure.drainage_burden", "infrastructure.wastewater_burden", "infrastructure.utilities_burden"], "utilities")
        )


def _control_cost_contributions(contributions: dict[str, list[ScoreContribution]], evidence: SiteEvidence) -> None:
    if evidence.ownership.ownership_fragmentation_level == "single" and not evidence.ownership.legal_control_issue_flags:
        contributions["K"].append(
            ScoreContribution("K", "control", 2, "Control route looks efficient.", "Single-title or low-fragmentation control reduces cost before value inflection.", ["ownership.title_count", "ownership.ownership_fragmentation_level"], "control")
        )
    elif evidence.ownership.ownership_fragmentation_level in {"multiple", "many"}:
        contributions["K"].append(
            ScoreContribution("K", "control", -1 if evidence.ownership.ownership_fragmentation_level == "multiple" else -2, "Ownership fragmentation increases control cost.", "Multiple ownership interests usually increase legal friction, carry, and negotiation cost.", ["ownership.ownership_fragmentation_level"], "control")
        )
    if evidence.ownership.legal_control_issue_flags:
        contributions["K"].append(
            ScoreContribution("K", "control", -2, "Legal control issues may make control expensive.", "Ransom, legal, or access-control issues are expensive to solve before sale.", ["ownership.legal_control_issue_flags"], "control")
        )
    if evidence.vdl.on_vdl_register or evidence.flood.flood_combined_severity in {"medium", "high"}:
        contributions["K"].append(
            ScoreContribution("K", "ground_delivery", -1, "Abnormal cost burden is likely.", "Brownfield or flood-related mitigation usually increases spend before value can be realised.", ["vdl.on_vdl_register", "flood.flood_combined_severity"], "control")
        )
    if any(value in {"high", "critical"} for value in (
        evidence.infrastructure.drainage_burden,
        evidence.infrastructure.wastewater_burden,
        evidence.infrastructure.roads_burden,
    )):
        contributions["K"].append(
            ScoreContribution("K", "infrastructure", -1, "Infrastructure burden increases carry and control cost.", "Major enabling works reduce efficiency of the control basis.", ["infrastructure.drainage_burden", "infrastructure.wastewater_burden", "infrastructure.roads_burden"], "control")
        )
    if evidence.utility.overall_utility_burden in {"high", "critical"}:
        contributions["K"].append(
            ScoreContribution("K", "utility", -1, "Utility friction increases pre-exit spend.", "Utility and wastewater burdens can absorb capital before the site reaches a saleable inflection point.", ["utility.overall_utility_burden", "infrastructure.drainage_burden", "infrastructure.wastewater_burden", "infrastructure.utilities_burden"], "control")
        )


def _buyer_depth_contributions(contributions: dict[str, list[ScoreContribution]], evidence: SiteEvidence) -> None:
    if evidence.market.buyer_depth_estimate == "broad":
        contributions["B"].append(
            ScoreContribution("B", "market", 2, "Buyer depth looks broad.", "Multiple buyer fits and active comparable evidence suggest a strong future exit pool.", ["market.buyer_depth_estimate", "market.buyer_profile_fit"], "buyer_depth")
        )
    elif evidence.market.buyer_depth_estimate == "workable":
        contributions["B"].append(
            ScoreContribution("B", "market", 1, "Buyer depth looks workable.", "There appears to be a credible, if not universal, buyer universe.", ["market.buyer_depth_estimate"], "buyer_depth")
        )
    elif evidence.market.buyer_depth_estimate == "narrow":
        contributions["B"].append(
            ScoreContribution("B", "market", -1, "Buyer pool may be narrow.", "The site may only appeal to a limited group of buyers.", ["market.buyer_depth_estimate"], "buyer_depth")
        )
    elif evidence.market.buyer_depth_estimate == "thin":
        contributions["B"].append(
            ScoreContribution("B", "market", -2, "Buyer depth looks weak.", "Thin buyer depth is a serious commercial warning even if planning improves.", ["market.buyer_depth_estimate"], "buyer_depth")
        )
    if evidence.market.settlement_strength == "strong":
        contributions["B"].append(
            ScoreContribution("B", "settlement_logic", 1, "Settlement strength supports exit.", "A strong market settlement improves downstream buyer relevance.", ["market.settlement_strength"], "buyer_depth")
        )
    if evidence.utility.broadband_connectivity_signal == "poor":
        contributions["B"].append(
            ScoreContribution("B", "connectivity", -1, "Connectivity may narrow buyer depth.", "Weak connectivity is not fatal on its own, but it can narrow exit appetite in weaker settlements.", ["utility.broadband_connectivity_signal"], "buyer_depth")
        )


def _has_unknown_pressure(score_code: str, evidence: SiteEvidence) -> bool:
    if score_code == "P":
        return evidence.ldp.policy_support_level == "unknown" or evidence.reconciliation.match_confidence == "low"
    if score_code == "G":
        return evidence.flood.flood_combined_severity == "unknown" or evidence.use_classification.previous_site_use_confidence == "low"
    if score_code == "I":
        return evidence.infrastructure.access_complexity == "unknown" or evidence.utility.overall_utility_burden == "unknown"
    if score_code == "F":
        return evidence.prior_progression.progression_level == "none" and evidence.planning.prior_application_count == 0
    if score_code == "K":
        return evidence.ownership.title_count == 0
    if score_code == "B":
        return evidence.market.buyer_depth_estimate == "unknown" or evidence.utility.broadband_connectivity_signal == "unknown"
    return False

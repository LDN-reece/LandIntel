"""Scottish scoring, routing, and interpretation orchestration."""

from __future__ import annotations

from itertools import chain

from src.site_engine.bucket_router import route_bucket, route_horizon, evaluate_hard_fail_gates
from src.site_engine.explanation_fragments import (
    build_explanation,
    build_primary_reason,
    build_secondary_reasons,
    determine_buyer_profile_guess,
    determine_cost_to_control_band,
)
from src.site_engine.score_engine import build_scorecard
from src.site_engine.site_evidence_schema import SiteEvidence
from src.site_engine.types import InterpretationResult, SignalResult, SiteAssessmentResult, SiteSnapshot


ASSESSMENT_VERSION = "scottish_portfolio_v1"

SCORE_SIGNAL_KEYS = {
    "P": ["allocation_status", "policy_support_level", "previous_application_outcome", "refusal_themes", "hla_effectiveness_status", "settlement_boundary_position", "green_belt_position"],
    "G": ["flood_risk", "river_flood_overlap_pct", "surface_water_overlap_pct", "mining_risk", "vdl_register_status", "previous_use_type", "bgs_ground_complexity_signal"],
    "I": ["access_status", "drainage_burden", "wastewater_burden", "roads_burden", "education_burden", "utilities_burden", "overall_utility_burden"],
    "R": ["planning_application_count", "progression_level", "sponsor_failure_indicator"],
    "F": ["refusal_themes", "progression_level", "access_status", "flood_risk", "legal_control_issue_count"],
    "K": ["title_count", "ownership_fragmentation_level", "legal_control_issue_count", "drainage_burden", "wastewater_burden"],
    "B": ["buyer_fit_count", "buyer_depth_estimate", "new_build_comparable_strength", "settlement_strength", "broadband_connectivity_signal"],
}


def build_site_assessment(
    snapshot: SiteSnapshot,
    evidence: SiteEvidence,
) -> SiteAssessmentResult:
    """Build the persisted Scottish portfolio assessment."""

    scorecard, blocker_signals = build_scorecard(evidence)
    hard_fail_flags = evaluate_hard_fail_gates(evidence, scorecard, blocker_signals)
    bucket_code, bucket_label = route_bucket(evidence, scorecard, hard_fail_flags)
    monetisation_horizon, horizon_year_band = route_horizon(bucket_code)
    dominant_blocker = str(blocker_signals.get("dominant_blocker") or "timing")
    secondary_reasons = build_secondary_reasons(scorecard, hard_fail_flags)
    primary_reason = build_primary_reason(bucket_code, scorecard)
    cost_to_control_band = determine_cost_to_control_band(scorecard["K"].value)
    likely_buyer_profiles = list(evidence.market.buyer_profile_fit)
    buyer_profile_guess = determine_buyer_profile_guess(evidence, bucket_code)

    review_flags: list[str] = []
    for score_code in ("P", "G", "I", "F"):
        if scorecard[score_code].confidence_label == "low":
            review_flags.append(f"low_confidence_{score_code}")
    if evidence.reconciliation.match_confidence == "low" or evidence.reconciliation.unresolved_reference_count > 0:
        review_flags.append("reference_reconciliation_review")
    review_flags.extend(flag.gate for flag in hard_fail_flags)
    human_review_required = bool(review_flags)

    if hard_fail_flags:
        risk_shape = "high risk"
    elif scorecard["F"].value >= 4:
        risk_shape = "fixable"
    elif scorecard["F"].value >= 3:
        risk_shape = "bounded"
    else:
        risk_shape = "higher risk"

    next_checks = list(blocker_signals.get("next_checks") or [])
    if scorecard["P"].confidence_label == "low":
        next_checks.append("Validate the planning route against adopted and emerging Scottish policy evidence.")
    if scorecard["G"].confidence_label == "low":
        next_checks.append("Confirm whether ground and flood issues are bounded or open-ended.")
    if scorecard["I"].confidence_label == "low":
        next_checks.append("Check infrastructure burden, especially access, drainage, and wastewater.")
    if scorecard["F"].confidence_label == "low":
        next_checks.append("Test whether the main blocker is actually fixable or just not yet understood.")
    if evidence.reconciliation.unresolved_reference_count > 0:
        next_checks.append("Resolve any unresolved site references before relying on linked policy or audit evidence.")
    next_checks = list(dict.fromkeys(next_checks))[:3]

    explanation_text = build_explanation(
        bucket_label=bucket_label,
        primary_reason=primary_reason,
        dominant_blocker=dominant_blocker,
        secondary_reasons=secondary_reasons,
        risk_shape=risk_shape,
        monetisation_horizon=monetisation_horizon,
        next_checks=next_checks,
    )
    explanation_fragments = [
        scorecard["P"].summary,
        scorecard["G"].summary,
        scorecard["I"].summary,
        scorecard["R"].summary,
        scorecard["F"].summary,
        scorecard["K"].summary,
        scorecard["B"].summary,
    ]
    evidence_keys = list(
        dict.fromkeys(
            chain.from_iterable(
                contribution.evidence_keys
                for score in scorecard.values()
                for contribution in score.contributions
            )
        )
    )

    likely_opportunity_type = {
        "A": "clean strategic greenfield",
        "B": "emerging / coming forward",
        "C": "stalled / re-entry",
        "D": "messy but workable",
        "E": "infrastructure-locked",
        "F": "dead / do not chase",
    }[bucket_code]

    return SiteAssessmentResult(
        site_id=str(snapshot.site["id"]),
        jurisdiction="scotland",
        assessment_version=ASSESSMENT_VERSION,
        bucket_code=bucket_code,
        bucket_label=bucket_label,
        likely_opportunity_type=likely_opportunity_type,
        monetisation_horizon=monetisation_horizon,
        horizon_year_band=horizon_year_band,
        scores=scorecard,
        hard_fail_flags=hard_fail_flags,
        dominant_blocker=dominant_blocker,
        blocker_themes=list(blocker_signals.get("blocker_themes") or []),
        primary_reason=primary_reason,
        secondary_reasons=secondary_reasons,
        buyer_profile_guess=buyer_profile_guess,
        likely_buyer_profiles=likely_buyer_profiles,
        cost_to_control_band=cost_to_control_band,
        human_review_required=human_review_required,
        review_flags=review_flags,
        explanation_fragments=explanation_fragments,
        next_checks=next_checks,
        explanation_text=explanation_text,
        evidence_keys=evidence_keys,
    )


def apply_interpretation_rules(
    assessment: SiteAssessmentResult,
    signal_map: dict[str, SignalResult],
) -> list[InterpretationResult]:
    """Translate scorecard and routing outputs into analyst-facing interpretation groups."""

    interpretations: list[InterpretationResult] = []
    bucket_signal_keys = _bucket_signal_keys(assessment.bucket_code)
    bucket_category = "possible_fatal" if assessment.bucket_code == "F" else "positive"
    interpretations.append(
        InterpretationResult(
            key=f"bucket_{assessment.bucket_code.lower()}",
            category=bucket_category,
            title=assessment.bucket_label,
            summary=assessment.primary_reason.capitalize() + ".",
            reasoning=assessment.explanation_text.split("\n", 1)[0],
            rule_code=f"bucket_{assessment.bucket_code.lower()}",
            priority=5,
            signal_keys=bucket_signal_keys,
        )
    )

    if assessment.bucket_code != "F":
        blocker_score_code = _blocker_score_code(assessment.dominant_blocker)
        interpretations.append(
            InterpretationResult(
                key="dominant_blocker",
                category="risk",
                title="Dominant blocker",
                summary=f"The main blocker currently appears to be {assessment.dominant_blocker.replace('_', ' ')}.",
                reasoning=assessment.scores[blocker_score_code].reasoning,
                rule_code="dominant_blocker",
                priority=20,
                signal_keys=SCORE_SIGNAL_KEYS[blocker_score_code],
            )
        )

    if assessment.scores["B"].value >= 4:
        interpretations.append(
            InterpretationResult(
                key="buyer_depth_positive",
                category="positive",
                title="Credible buyer depth",
                summary="The site appears relevant to multiple buyer classes.",
                reasoning=assessment.scores["B"].reasoning,
                rule_code="buyer_depth_positive",
                priority=25,
                signal_keys=SCORE_SIGNAL_KEYS["B"],
            )
        )
    if assessment.scores["R"].value >= 4:
        interpretations.append(
            InterpretationResult(
                key="prior_progression_positive",
                category="positive",
                title="Meaningful prior progression exists",
                summary="Prior planning or technical work suggests the site has already been materially progressed.",
                reasoning=assessment.scores["R"].reasoning,
                rule_code="prior_progression_positive",
                priority=26,
                signal_keys=SCORE_SIGNAL_KEYS["R"],
            )
        )

    for hard_fail in assessment.hard_fail_flags:
        interpretations.append(
            InterpretationResult(
                key=hard_fail.gate,
                category="possible_fatal",
                title=hard_fail.title,
                summary=hard_fail.reason,
                reasoning=hard_fail.reason,
                rule_code=hard_fail.gate,
                priority=10,
                signal_keys=_signal_keys_from_evidence_keys(hard_fail.evidence_keys),
            )
        )

    for score_code in ("P", "G", "I", "F"):
        if assessment.scores[score_code].confidence_label == "low":
            interpretations.append(
                InterpretationResult(
                    key=f"low_confidence_{score_code.lower()}",
                    category="unknown",
                    title=f"Low confidence in {assessment.scores[score_code].label}",
                    summary="The current evidence coverage is too thin to treat this dimension as settled.",
                    reasoning=assessment.scores[score_code].reasoning,
                    rule_code=f"low_confidence_{score_code.lower()}",
                    priority=40,
                    signal_keys=SCORE_SIGNAL_KEYS[score_code],
                )
            )

    if assessment.scores["K"].value <= 2 and assessment.bucket_code != "F":
        interpretations.append(
            InterpretationResult(
                key="control_cost_risk",
                category="risk",
                title="Control basis may be expensive",
                summary="The cost or complexity of securing and progressing control looks meaningful.",
                reasoning=assessment.scores["K"].reasoning,
                rule_code="control_cost_risk",
                priority=30,
                signal_keys=SCORE_SIGNAL_KEYS["K"],
            )
        )

    if assessment.scores["G"].value <= 2 and assessment.bucket_code not in {"F"}:
        interpretations.append(
            InterpretationResult(
                key="ground_risk",
                category="risk",
                title="Technical issues need pricing discipline",
                summary="Ground or brownfield issues appear material, even if still potentially workable.",
                reasoning=assessment.scores["G"].reasoning,
                rule_code="ground_risk",
                priority=32,
                signal_keys=SCORE_SIGNAL_KEYS["G"],
            )
        )

    return sorted(interpretations, key=lambda item: (item.priority, item.title))


def _bucket_signal_keys(bucket_code: str) -> list[str]:
    if bucket_code == "A":
        return ["allocation_status", "flood_risk", "access_status", "progression_level", "settlement_boundary_position"]
    if bucket_code == "B":
        return ["allocation_status", "policy_support_level", "hla_effectiveness_status", "roads_burden", "overall_utility_burden"]
    if bucket_code == "C":
        return ["planning_application_count", "progression_level", "sponsor_failure_indicator", "buyer_depth_estimate", "bgs_investigation_intensity"]
    if bucket_code == "D":
        return ["vdl_register_status", "previous_use_type", "flood_risk", "buyer_depth_estimate", "bgs_ground_complexity_signal"]
    if bucket_code == "E":
        return ["roads_burden", "drainage_burden", "wastewater_burden", "policy_support_level", "overall_utility_burden"]
    return ["policy_support_level", "flood_risk", "legal_control_issue_count", "buyer_depth_estimate", "canonical_match_confidence"]


def _blocker_score_code(dominant_blocker: str) -> str:
    if dominant_blocker in {"planning"}:
        return "P"
    if dominant_blocker in {"ground"}:
        return "G"
    if dominant_blocker in {"roads", "drainage", "wastewater", "education", "utilities", "infrastructure"}:
        return "I"
    if dominant_blocker in {"control"}:
        return "K"
    if dominant_blocker in {"buyer_depth"}:
        return "B"
    return "F"


def _signal_keys_from_evidence_keys(evidence_keys: list[str]) -> list[str]:
    mapping = {
        "ldp.policy_support_level": "policy_support_level",
        "planning.refusal_themes": "refusal_themes",
        "flood.flood_combined_severity": "flood_risk",
        "bgs.opencast_overlap": "mining_risk",
        "vdl.on_vdl_register": "vdl_register_status",
        "market.buyer_depth_estimate": "buyer_depth_estimate",
        "market.settlement_strength": "settlement_strength",
        "ownership.legal_control_issue_flags": "legal_control_issue_count",
        "ownership.ownership_fragmentation_level": "ownership_fragmentation_level",
        "boundary.green_belt.position": "green_belt_position",
        "boundary.settlement_boundary.position": "settlement_boundary_position",
        "bgs_reasoning.investigation_intensity": "bgs_investigation_intensity",
        "bgs_reasoning.ground_complexity_signal": "bgs_ground_complexity_signal",
        "utility.overall_utility_burden": "overall_utility_burden",
        "utility.broadband_connectivity_signal": "broadband_connectivity_signal",
    }
    return list(dict.fromkeys(mapping[key] for key in evidence_keys if key in mapping))

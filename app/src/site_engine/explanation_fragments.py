"""Commercial explanation assembly for the Scottish assessment output."""

from __future__ import annotations

from src.site_engine.site_evidence_schema import SiteEvidence
from src.site_engine.types import HardFailFlag, SiteAssessmentScore


def determine_cost_to_control_band(k_score: int) -> str:
    if k_score <= 1:
        return "Very high cost / heavy carry"
    if k_score == 2:
        return "High cost"
    if k_score == 3:
        return "Medium cost"
    if k_score == 4:
        return "Relatively efficient"
    return "Low cost / efficient"


def determine_buyer_profile_guess(evidence: SiteEvidence, bucket_code: str) -> str | None:
    if evidence.market.buyer_profile_fit:
        return evidence.market.buyer_profile_fit[0]
    return {
        "A": "strategic_masterplan_plc",
        "B": "regional_family_homes",
        "C": "value_builder_scotland",
        "D": "value_builder_scotland",
        "E": "strategic_masterplan_plc",
        "F": None,
    }.get(bucket_code)


def build_explanation(
    *,
    bucket_label: str,
    primary_reason: str,
    dominant_blocker: str,
    secondary_reasons: list[str],
    risk_shape: str,
    monetisation_horizon: str,
    next_checks: list[str],
) -> str:
    despite_clause = secondary_reasons[0] if secondary_reasons else "remaining uncertainty"
    lines = [
        f"This site is classified as {bucket_label} because {primary_reason.lower()}. The main blocker appears to be {dominant_blocker.replace('_', ' ')}. Despite {despite_clause.lower()}, the site looks {risk_shape} with likely monetisation in the {monetisation_horizon.lower()} term.",
        "",
        "Key reasons for this classification:",
        *[f"- {reason}" for reason in secondary_reasons[:3]],
        "",
        "What needs checked next:",
        *[f"- {check}" for check in next_checks[:3]],
    ]
    return "\n".join(lines)


def build_primary_reason(bucket_code: str, scorecard: dict[str, SiteAssessmentScore]) -> str:
    if bucket_code == "A":
        return "the site looks physically clean, infrastructure appears relatively light, and there is little prior progression, so the main value sits in a longer-term planning route"
    if bucket_code == "B":
        return "planning logic is present and the site appears to be coming forward, even though delivery friction remains"
    if bucket_code == "C":
        return "meaningful prior progression exists and the blockage still looks commercially or technically fixable"
    if bucket_code == "D":
        return "technical messiness is evident, but the issues still look bounded enough to price and work through"
    if bucket_code == "E":
        return "planning logic is reasonable, but infrastructure burden is clearly the dominant blocker"
    return "the current evidence suggests the planning route, delivery risk, control position, or exit logic is too weak to justify active pursuit"


def build_secondary_reasons(
    scorecard: dict[str, SiteAssessmentScore],
    hard_fail_flags: list[HardFailFlag],
) -> list[str]:
    reasons = [flag.reason for flag in hard_fail_flags]
    reasons.extend(score.summary for score in sorted(scorecard.values(), key=lambda item: item.value) if score.value <= 2)
    reasons.extend(score.summary for score in sorted(scorecard.values(), key=lambda item: item.value, reverse=True) if score.value >= 4)
    return list(dict.fromkeys(reason for reason in reasons if reason))[:4]

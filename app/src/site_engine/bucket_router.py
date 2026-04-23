"""Deterministic bucket routing for the Scottish portfolio model."""

from __future__ import annotations

from src.site_engine.site_evidence_schema import SiteEvidence
from src.site_engine.types import HardFailFlag, SiteAssessmentScore


BUCKET_LABELS = {
    "A": "Clean Strategic Greenfield",
    "B": "Emerging / Coming Forward",
    "C": "Stalled / Re-Entry",
    "D": "Messy But Workable",
    "E": "Infrastructure-Locked",
    "F": "Dead / Do Not Chase",
}


def evaluate_hard_fail_gates(
    evidence: SiteEvidence,
    scorecard: dict[str, SiteAssessmentScore],
    blocker_signals: dict[str, object],
) -> list[HardFailFlag]:
    flags: list[HardFailFlag] = []
    if scorecard["P"].value <= 1 and evidence.ldp.policy_support_level == "weak":
        flags.append(
            HardFailFlag(
                gate="planning_fatality",
                title="Planning fatality gate",
                reason="No credible Scottish planning route is currently evidenced.",
                evidence_keys=["ldp.policy_support_level", "planning.refusal_themes"],
            )
        )
    if scorecard["G"].value <= 1 and (blocker_signals.get("open_ended_flags") or evidence.flood.flood_combined_severity == "high"):
        flags.append(
            HardFailFlag(
                gate="technical_fatality",
                title="Technical fatality gate",
                reason="Technical risk currently looks open-ended rather than bounded.",
                evidence_keys=["flood.flood_combined_severity", "bgs.opencast_overlap", "vdl.on_vdl_register"],
            )
        )
    if scorecard["B"].value <= 1:
        flags.append(
            HardFailFlag(
                gate="exit_fatality",
                title="Exit fatality gate",
                reason="Buyer depth currently looks too weak to support a sensible exit.",
                evidence_keys=["market.buyer_depth_estimate", "market.settlement_strength"],
            )
        )
    if scorecard["K"].value <= 1 and (
        evidence.ownership.legal_control_issue_flags or evidence.ownership.ownership_fragmentation_level == "many"
    ):
        flags.append(
            HardFailFlag(
                gate="control_fatality",
                title="Control fatality gate",
                reason="Control currently looks unrealistic or disproportionately expensive.",
                evidence_keys=["ownership.legal_control_issue_flags", "ownership.ownership_fragmentation_level"],
            )
        )
    return flags


def route_bucket(
    evidence: SiteEvidence,
    scorecard: dict[str, SiteAssessmentScore],
    hard_fail_flags: list[HardFailFlag],
) -> tuple[str, str]:
    if hard_fail_flags:
        return "F", BUCKET_LABELS["F"]
    if scorecard["G"].value >= 4 and scorecard["I"].value >= 4 and scorecard["R"].value <= 2:
        return "A", BUCKET_LABELS["A"]
    if scorecard["I"].value <= 2 and scorecard["P"].value >= 3:
        return "E", BUCKET_LABELS["E"]
    if scorecard["G"].value <= 3 and scorecard["F"].value >= 3:
        return "D", BUCKET_LABELS["D"]
    if (
        scorecard["R"].value >= 4
        and scorecard["F"].value >= 3
        and evidence.prior_progression.sponsor_failure_indicator
    ):
        return "C", BUCKET_LABELS["C"]
    if scorecard["P"].value >= 3:
        return "B", BUCKET_LABELS["B"]
    return "F", BUCKET_LABELS["F"]


def route_horizon(bucket_code: str) -> tuple[str, str]:
    if bucket_code == "C":
        return "Short Term", "0 to 2 years"
    if bucket_code == "A":
        return "Long Term", "5+ years"
    if bucket_code in {"B", "D", "E"}:
        return "Medium Term", "2 to 5 years"
    return "None / Reject / Watchlist", "Reject / watchlist"

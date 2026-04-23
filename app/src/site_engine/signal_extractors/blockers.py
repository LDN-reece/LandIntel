"""Commercial blocker and risk-shape extraction for routing."""

from __future__ import annotations

from collections import Counter

from src.site_engine.site_evidence_schema import SiteEvidence


def extract_blocker_signals(evidence: SiteEvidence) -> dict[str, object]:
    themes: list[str] = []
    bounded_flags: list[str] = []
    open_ended_flags: list[str] = []
    next_checks: list[str] = []

    if "planning_principle" in evidence.planning.refusal_themes or evidence.ldp.policy_support_level == "weak":
        themes.append("planning")
        open_ended_flags.append("planning_route")
        next_checks.append("Confirm whether there is any credible Scottish policy or settlement narrative.")
    elif evidence.planning.refusal_themes:
        themes.append("planning")
        bounded_flags.append("planning_redesign")

    if evidence.flood.flood_combined_severity in {"medium", "high"}:
        themes.append("drainage")
        next_checks.append("Check flood extent, drainage strategy, and whether mitigation looks priceable.")
        if evidence.flood.flood_combined_severity == "high":
            open_ended_flags.append("flood")
        else:
            bounded_flags.append("flood_mitigation")

    if evidence.infrastructure.access_complexity in {"constrained", "third_party", "problematic"}:
        themes.append("roads")
        next_checks.append("Verify adopted frontage, third-party rights, and junction delivery route.")
        if evidence.infrastructure.access_complexity == "problematic":
            open_ended_flags.append("access")
        else:
            bounded_flags.append("access")

    for infrastructure_theme, burden in (
        ("roads", evidence.infrastructure.roads_burden),
        ("drainage", evidence.infrastructure.drainage_burden),
        ("wastewater", evidence.infrastructure.wastewater_burden),
        ("education", evidence.infrastructure.education_burden),
        ("utilities", evidence.infrastructure.utilities_burden),
    ):
        if burden in {"high", "critical"}:
            themes.append(infrastructure_theme)
            next_checks.append(f"Confirm whether {infrastructure_theme} burden is fundable and time-bounded.")
            if burden == "critical":
                open_ended_flags.append(infrastructure_theme)
            else:
                bounded_flags.append(infrastructure_theme)

    if evidence.utility.overall_utility_burden in {"high", "critical"}:
        themes.append("utilities")
        next_checks.append("Confirm whether utility and wastewater burden is time-bounded, fundable, and third-party dependent.")
        if evidence.utility.overall_utility_burden == "critical":
            open_ended_flags.append("utilities")
        else:
            bounded_flags.append("utilities")

    if evidence.vdl.on_vdl_register or evidence.bgs.opencast_overlap:
        themes.append("ground")
        next_checks.append("Price abnormal ground costs and confirm whether remediation is bounded.")
        if evidence.bgs.opencast_overlap:
            open_ended_flags.append("ground")
        else:
            bounded_flags.append("ground")

    if evidence.ownership.legal_control_issue_flags or evidence.ownership.ownership_fragmentation_level in {"many", "severe"}:
        themes.append("control")
        next_checks.append("Confirm title, control route, and whether any ransom or fragmentation issue can be solved.")
        if evidence.ownership.legal_control_issue_flags:
            open_ended_flags.append("control")
        else:
            bounded_flags.append("control")

    if evidence.market.buyer_depth_estimate in {"thin", "narrow"}:
        themes.append("buyer_depth")
        next_checks.append("Test buyer universe against settlement, product, and abnormal cost burden.")

    if evidence.reconciliation.unresolved_reference_count > 0 or evidence.reconciliation.match_confidence == "low":
        next_checks.append("Review unresolved site references before relying on policy, HLA, or VDL links.")

    dominant_blocker = "timing"
    if themes:
        dominant_blocker = Counter(themes).most_common(1)[0][0]

    deduped_next_checks = list(dict.fromkeys(next_checks))[:3]
    return {
        "blocker_themes": list(dict.fromkeys(themes)),
        "bounded_flags": list(dict.fromkeys(bounded_flags)),
        "open_ended_flags": list(dict.fromkeys(open_ended_flags)),
        "dominant_blocker": dominant_blocker,
        "next_checks": deduped_next_checks,
    }

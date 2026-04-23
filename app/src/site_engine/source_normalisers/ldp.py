"""Normalise Scottish LDP and settlement-boundary context."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, row_payload
from src.site_engine.site_evidence_schema import LdpEvidence


def normalise_ldp(
    context_records: list[dict[str, Any]],
    location: dict[str, Any] | None,
) -> tuple[LdpEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    adopted_rows = [
        row
        for row in context_records
        if str(row.get("context_type") or "").lower() in {"allocation", "ldp_adopted", "policy_support"}
    ]
    emerging_rows = [
        row
        for row in context_records
        if str(row.get("context_type") or "").lower() in {"emerging_ldp", "allocation"}
        and str(row.get("context_status") or "").lower() == "emerging"
    ]
    policy_constraint_rows = [
        row
        for row in context_records
        if str(row.get("context_type") or "").lower() in {"policy_constraint", "ldp_requirement", "hla_constraint"}
    ]
    adopted_status = _best_policy_status(adopted_rows)
    emerging_status = _best_policy_status(emerging_rows)
    settlement_boundary_relation = _settlement_relation(location)
    policy_constraints = tuple(
        sorted(
            {
                *(str(item) for row in policy_constraint_rows for item in row_payload(row).get("policy_constraints", [])),
                *(str(row.get("context_label") or "") for row in policy_constraint_rows if row.get("context_label")),
            }
        )
    )
    if adopted_rows:
        for row in adopted_rows:
            add_evidence(
                field_evidence,
                "ldp.adopted_ldp_status",
                "public.planning_context_records",
                row,
                f"Adopted policy context is recorded as '{row.get('context_status') or 'unknown'}'.",
            )
    if emerging_rows:
        for row in emerging_rows:
            add_evidence(
                field_evidence,
                "ldp.emerging_ldp_status",
                "public.planning_context_records",
                row,
                f"Emerging policy context is recorded as '{row.get('context_status') or 'unknown'}'.",
            )
    if location:
        add_evidence(
            field_evidence,
            "ldp.settlement_boundary_relation",
            "public.site_locations",
            location,
            f"Settlement boundary relation is interpreted as '{settlement_boundary_relation}'.",
        )
    for row in policy_constraint_rows:
        add_evidence(
            field_evidence,
            "ldp.policy_constraints",
            "public.planning_context_records",
            row,
            "Policy-linked site requirements are attached to the site context.",
        )

    if adopted_status in {"allocated", "supportive"}:
        policy_support_level = "strong"
    elif emerging_status == "emerging" or settlement_boundary_relation in {"within_settlement_boundary", "edge_of_settlement"}:
        policy_support_level = "moderate"
    elif settlement_boundary_relation == "outside_logical_growth_area":
        policy_support_level = "weak"
    else:
        policy_support_level = "mixed"

    evidence_rows = [*adopted_rows, *emerging_rows, *policy_constraint_rows]
    if evidence_rows:
        for row in evidence_rows:
            add_evidence(
                field_evidence,
                "ldp.policy_support_level",
                "public.planning_context_records",
                row,
                f"Combined policy support is interpreted as '{policy_support_level}'.",
            )
    elif location:
        add_evidence(
            field_evidence,
            "ldp.policy_support_level",
            "public.site_locations",
            location,
            f"Settlement position contributes to policy support being '{policy_support_level}'.",
        )

    return (
        LdpEvidence(
            adopted_ldp_status=adopted_status,
            emerging_ldp_status=emerging_status,
            settlement_boundary_relation=settlement_boundary_relation,
            policy_constraints=policy_constraints,
            policy_support_level=policy_support_level,
        ),
        field_evidence,
    )


def _best_policy_status(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "unknown"
    priority = {"allocated": 4, "supportive": 3, "approved": 3, "emerging": 2, "unallocated": 1, "unknown": 0}
    return max(
        (str(row.get("context_status") or "unknown").lower() for row in rows),
        key=lambda value: priority.get(value, -1),
    )


def _settlement_relation(location: dict[str, Any] | None) -> str:
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
    if relationship in {"open_countryside", "isolated", "rural_market", "outside_settlement"}:
        return "outside_logical_growth_area"
    return relationship or "outside_logical_growth_area"

"""Utility-burden inference for the Scottish MVP."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, row_payload
from src.site_engine.site_evidence_schema import InfrastructureEvidence, MarketEvidence, UtilityEvidence


BURDEN_PRIORITY = {"unknown": 0, "none": 1, "low": 2, "medium": 3, "high": 4, "critical": 5}


def infer_utility_burden(
    infrastructure: InfrastructureEvidence,
    planning_records: list[dict[str, Any]],
    context_records: list[dict[str, Any]],
    market: MarketEvidence,
) -> tuple[UtilityEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    water_and_wastewater_signal = _max_burden(infrastructure.drainage_burden, infrastructure.wastewater_burden)
    electricity_grid_signal = infrastructure.utilities_burden
    overall_utility_burden = _max_burden(water_and_wastewater_signal, electricity_grid_signal)
    broadband_connectivity_signal = "unknown"

    for row in planning_records:
        payload = row_payload(row)
        if payload.get("utilities_burden"):
            overall_utility_burden = _max_burden(overall_utility_burden, str(payload["utilities_burden"]).lower())
            add_evidence(
                field_evidence,
                "utility.overall_utility_burden",
                "public.planning_records",
                row,
                f"Planning evidence references utility burden '{payload['utilities_burden']}'.",
            )

    for row in context_records:
        payload = row_payload(row)
        reasons = [str(reason).lower() for reason in payload.get("constraint_reasons", [])]
        if any(reason in {"drainage", "wastewater"} for reason in reasons):
            water_and_wastewater_signal = _max_burden(water_and_wastewater_signal, "high")
            add_evidence(
                field_evidence,
                "utility.water_and_wastewater_signal",
                "public.planning_context_records",
                row,
                "HLA or plan-cycle evidence cites drainage or wastewater as a utility burden.",
            )
        if any(reason in {"utilities", "grid", "electricity"} for reason in reasons):
            electricity_grid_signal = _max_burden(electricity_grid_signal, "high")
            add_evidence(
                field_evidence,
                "utility.electricity_grid_signal",
                "public.planning_context_records",
                row,
                "Plan-cycle evidence cites utilities or grid pressure.",
            )
        if payload.get("broadband_connectivity_signal"):
            broadband_connectivity_signal = str(payload["broadband_connectivity_signal"]).lower()
            add_evidence(
                field_evidence,
                "utility.broadband_connectivity_signal",
                "public.planning_context_records",
                row,
                f"Connectivity evidence classifies broadband signal as '{broadband_connectivity_signal}'.",
            )

    if water_and_wastewater_signal == "unknown" and all(
        value in {"unknown", "none", "low"}
        for value in (infrastructure.drainage_burden, infrastructure.wastewater_burden)
    ):
        water_and_wastewater_signal = "low"
    if electricity_grid_signal == "unknown" and infrastructure.utilities_burden in {"unknown", "none", "low"}:
        electricity_grid_signal = "low"

    if broadband_connectivity_signal == "unknown":
        if market.settlement_strength == "strong":
            broadband_connectivity_signal = "good"
        elif market.settlement_strength == "mid":
            broadband_connectivity_signal = "adequate"
        elif market.settlement_strength == "weak":
            broadband_connectivity_signal = "poor"

    overall_utility_burden = _max_burden(overall_utility_burden, water_and_wastewater_signal, electricity_grid_signal)
    return (
        UtilityEvidence(
            overall_utility_burden=overall_utility_burden,
            water_and_wastewater_signal=water_and_wastewater_signal,
            electricity_grid_signal=electricity_grid_signal,
            broadband_connectivity_signal=broadband_connectivity_signal,
        ),
        field_evidence,
    )


def _max_burden(*values: str) -> str:
    cleaned = [str(value or "unknown").lower() for value in values]
    return max(cleaned, key=lambda value: BURDEN_PRIORITY.get(value, 0), default="unknown")

"""Normalise Scottish HLA status and constraints."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, row_payload
from src.site_engine.site_evidence_schema import HlaEvidence


def normalise_hla(context_records: list[dict[str, Any]]) -> tuple[HlaEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    hla_rows = [
        row
        for row in context_records
        if str(row.get("context_type") or "").lower() in {"hla", "hla_status", "hla_constraint"}
    ]
    if not hla_rows:
        return HlaEvidence(), field_evidence

    present_in_hla = True
    effectiveness_status = "unknown"
    programming_horizon = "unknown"
    constraint_reasons: list[str] = []

    for row in hla_rows:
        payload = row_payload(row)
        context_type = str(row.get("context_type") or "").lower()
        context_status = str(row.get("context_status") or "unknown").lower()
        if context_type in {"hla", "hla_status"} and context_status != "unknown":
            effectiveness_status = context_status
            add_evidence(
                field_evidence,
                "hla.effectiveness_status",
                "public.planning_context_records",
                row,
                f"HLA effectiveness is recorded as '{effectiveness_status}'.",
            )
        if payload.get("programming_horizon"):
            programming_horizon = str(payload["programming_horizon"])
            add_evidence(
                field_evidence,
                "hla.programming_horizon",
                "public.planning_context_records",
                row,
                f"HLA programming horizon is recorded as '{programming_horizon}'.",
            )
        reasons = payload.get("constraint_reasons")
        if isinstance(reasons, list):
            constraint_reasons.extend(str(item) for item in reasons if item)
        elif context_type == "hla_constraint" and row.get("context_label"):
            constraint_reasons.append(str(row["context_label"]))
        add_evidence(
            field_evidence,
            "hla.present_in_hla",
            "public.planning_context_records",
            row,
            "The site is represented in linked HLA-style evidence.",
        )

    return (
        HlaEvidence(
            present_in_hla=present_in_hla,
            effectiveness_status=effectiveness_status,
            programming_horizon=programming_horizon,
            hla_constraint_reasons=tuple(sorted(set(constraint_reasons))),
        ),
        field_evidence,
    )


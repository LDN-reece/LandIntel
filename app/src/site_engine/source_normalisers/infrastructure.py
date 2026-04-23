"""Normalise infrastructure burden by delivery theme."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, row_payload
from src.site_engine.site_evidence_schema import InfrastructureEvidence


BURDEN_PRIORITY = {"unknown": 0, "none": 1, "low": 2, "medium": 3, "high": 4, "critical": 5}


def normalise_infrastructure(
    infrastructure_records: list[dict[str, Any]],
    constraint_records: list[dict[str, Any]],
    planning_records: list[dict[str, Any]],
    context_records: list[dict[str, Any]],
) -> tuple[InfrastructureEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    values = {
        "access_complexity": "unknown",
        "drainage_burden": "unknown",
        "wastewater_burden": "unknown",
        "roads_burden": "unknown",
        "education_burden": "unknown",
        "utilities_burden": "unknown",
    }

    def apply_value(field_key: str, value: str, source_table: str, row: dict[str, Any], assertion: str) -> None:
        if BURDEN_PRIORITY.get(str(value).lower(), 0) >= BURDEN_PRIORITY.get(str(values[field_key]).lower(), 0):
            values[field_key] = str(value).lower()
        add_evidence(field_evidence, f"infrastructure.{field_key}", source_table, row, assertion)

    for row in infrastructure_records:
        infra_type = str(row.get("infrastructure_type") or "").lower()
        burden = str(row.get("burden_level") or "unknown").lower()
        if infra_type == "access":
            access_complexity = str(row_payload(row).get("access_complexity") or row.get("status") or burden or "unknown").lower()
            values["access_complexity"] = access_complexity
            add_evidence(
                field_evidence,
                "infrastructure.access_complexity",
                "public.site_infrastructure_records",
                row,
                f"Access complexity is recorded as '{access_complexity}'.",
            )
        elif infra_type in {"drainage", "wastewater", "roads", "education", "utilities"}:
            apply_value(
                f"{infra_type}_burden",
                burden,
                "public.site_infrastructure_records",
                row,
                f"{infra_type.title()} burden is recorded as '{burden}'.",
            )

    for row in constraint_records:
        constraint_type = str(row.get("constraint_type") or "").lower()
        severity = str(row.get("severity") or "unknown").lower()
        if constraint_type == "access" and values["access_complexity"] == "unknown":
            values["access_complexity"] = str(row.get("status") or "unknown").lower()
            add_evidence(
                field_evidence,
                "infrastructure.access_complexity",
                "public.site_constraints",
                row,
                f"Constraint-linked access status is '{values['access_complexity']}'.",
            )
        if constraint_type in {"drainage", "wastewater", "roads", "education", "utilities"}:
            apply_value(
                f"{constraint_type}_burden",
                severity,
                "public.site_constraints",
                row,
                f"{constraint_type.title()} burden is screened at '{severity}'.",
            )

    for row in planning_records:
        payload = row_payload(row)
        for field_key, label in (
            ("drainage_burden", "drainage"),
            ("roads_burden", "roads"),
            ("education_burden", "education"),
        ):
            if payload.get(field_key):
                apply_value(
                    field_key,
                    str(payload[field_key]),
                    "public.planning_records",
                    row,
                    f"Planning history references {label} burden '{payload[field_key]}'.",
                )

    for row in context_records:
        context_type = str(row.get("context_type") or "").lower()
        if context_type == "hla_constraint":
            payload = row_payload(row)
            for reason in payload.get("constraint_reasons", []):
                field_key = f"{str(reason).lower()}_burden"
                if field_key in values:
                    apply_value(
                        field_key,
                        "high",
                        "public.planning_context_records",
                        row,
                        f"HLA evidence cites {reason} as a delivery constraint.",
                    )

    return (
        InfrastructureEvidence(
            access_complexity=values["access_complexity"],
            drainage_burden=values["drainage_burden"],
            wastewater_burden=values["wastewater_burden"],
            roads_burden=values["roads_burden"],
            education_burden=values["education_burden"],
            utilities_burden=values["utilities_burden"],
        ),
        field_evidence,
    )


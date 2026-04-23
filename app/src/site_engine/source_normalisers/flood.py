"""Normalise flood and drainage-overlap evidence."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, as_float, row_payload
from src.site_engine.site_evidence_schema import FloodEvidence


SEVERITY_PRIORITY = {"none": 0, "low": 1, "medium": 2, "high": 3}


def normalise_flood(constraint_records: list[dict[str, Any]]) -> tuple[FloodEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    river_overlap = 0.0
    surface_overlap = 0.0
    severity = "none"
    for row in constraint_records:
        constraint_type = str(row.get("constraint_type") or "").lower()
        if constraint_type not in {"flood", "river_flood", "surface_water_flood"}:
            continue
        payload = row_payload(row)
        overlap_pct = as_float(payload.get("overlap_pct"), 0.0)
        row_severity = str(payload.get("combined_severity") or row.get("severity") or "unknown").lower()
        if constraint_type in {"flood", "river_flood"}:
            river_overlap = max(river_overlap, overlap_pct)
            add_evidence(
                field_evidence,
                "flood.river_flood_overlap_pct",
                "public.site_constraints",
                row,
                f"River flood overlap is recorded at approximately {overlap_pct:.1f}%.",
            )
        if constraint_type in {"flood", "surface_water_flood"}:
            surface_overlap = max(surface_overlap, overlap_pct)
            add_evidence(
                field_evidence,
                "flood.surface_water_overlap_pct",
                "public.site_constraints",
                row,
                f"Surface water flood overlap is recorded at approximately {overlap_pct:.1f}%.",
            )
        if SEVERITY_PRIORITY.get(row_severity, -1) > SEVERITY_PRIORITY.get(severity, -1):
            severity = row_severity
            add_evidence(
                field_evidence,
                "flood.flood_combined_severity",
                "public.site_constraints",
                row,
                f"Flood combined severity is interpreted as '{severity}'.",
            )
    return (
        FloodEvidence(
            river_flood_overlap_pct=river_overlap,
            surface_water_overlap_pct=surface_overlap,
            flood_combined_severity=severity,
        ),
        field_evidence,
    )


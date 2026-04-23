"""Boundary-position reasoning for Scottish council, settlement, and green-belt logic."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, as_float, row_payload
from src.site_engine.site_evidence_schema import BoundaryEvidence, BoundaryPosition


def infer_boundary_positions(
    location: dict[str, Any] | None,
    context_records: list[dict[str, Any]],
) -> tuple[BoundaryEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    settlement_boundary = _settlement_boundary_position(location, field_evidence)
    council_boundary = _council_boundary_position(location, field_evidence)
    green_belt = _green_belt_position(context_records, field_evidence)
    return (
        BoundaryEvidence(
            council_boundary=council_boundary,
            settlement_boundary=settlement_boundary,
            green_belt=green_belt,
        ),
        field_evidence,
    )


def _settlement_boundary_position(
    location: dict[str, Any] | None,
    field_evidence: dict[str, list],
) -> BoundaryPosition:
    if not location:
        return BoundaryPosition()
    distance = as_float(location.get("distance_to_settlement_boundary_m"))
    if location.get("within_settlement_boundary") is True:
        position = "fully_inside" if distance in {None, 0.0} else "mostly_inside"
    elif distance is None:
        relationship = str(location.get("settlement_relationship") or "").lower()
        position = "edge_straddling" if relationship in {"edge_of_settlement", "settlement_extension"} else "unknown"
    elif distance <= 60:
        position = "edge_straddling"
    elif distance <= 180:
        position = "just_outside"
    elif distance <= 500:
        position = "near_outside"
    else:
        position = "outside"
    add_evidence(
        field_evidence,
        "boundary.settlement_boundary.position",
        "public.site_locations",
        location,
        f"Settlement-boundary position is classified as '{position}'.",
    )
    return BoundaryPosition(position=position, distance_m=distance)


def _council_boundary_position(
    location: dict[str, Any] | None,
    field_evidence: dict[str, list],
) -> BoundaryPosition:
    if not location or not location.get("authority_name"):
        return BoundaryPosition()
    payload = row_payload(location)
    overlap_ratio = as_float(payload.get("authority_overlap_ratio"))
    distance = as_float(payload.get("authority_boundary_distance_m"))
    if payload.get("boundary_position"):
        position = str(payload["boundary_position"]).lower()
    elif overlap_ratio is not None:
        if overlap_ratio >= 0.95:
            position = "fully_inside"
        elif overlap_ratio >= 0.75:
            position = "mostly_inside"
        elif overlap_ratio >= 0.45:
            position = "edge_straddling"
        else:
            position = "just_outside"
    else:
        position = "fully_inside"
    add_evidence(
        field_evidence,
        "boundary.council_boundary.position",
        "public.site_locations",
        location,
        f"Council-boundary position is classified as '{position}'.",
    )
    return BoundaryPosition(position=position, overlap_ratio=overlap_ratio, distance_m=distance)


def _green_belt_position(
    context_records: list[dict[str, Any]],
    field_evidence: dict[str, list],
) -> BoundaryPosition:
    green_belt_rows = [
        row
        for row in context_records
        if str(row.get("context_type") or "").lower() in {"green_belt", "greenbelt"}
    ]
    if not green_belt_rows:
        return BoundaryPosition(position="unknown")
    row = green_belt_rows[0]
    payload = row_payload(row)
    overlap_ratio = as_float(payload.get("overlap_ratio") or payload.get("overlap_pct"))
    distance = as_float(row.get("distance_m") or payload.get("distance_m"))
    status = str(row.get("context_status") or payload.get("relation") or "").lower()
    if status in {"inside", "fully_inside"}:
        position = "fully_inside"
    elif status in {"mostly_inside", "majority_inside"}:
        position = "mostly_inside"
    elif status in {"straddling", "edge", "edge_straddling"}:
        position = "edge_straddling"
    elif status in {"just_outside", "outside"}:
        position = "just_outside"
    elif overlap_ratio is not None:
        if overlap_ratio >= 95:
            position = "fully_inside"
        elif overlap_ratio >= 60:
            position = "mostly_inside"
        elif overlap_ratio >= 20:
            position = "edge_straddling"
        else:
            position = "just_outside"
    elif distance is not None and distance <= 300:
        position = "near_outside"
    else:
        position = "unknown"
    add_evidence(
        field_evidence,
        "boundary.green_belt.position",
        "public.planning_context_records",
        row,
        f"Green-belt position is classified as '{position}'.",
    )
    return BoundaryPosition(position=position, overlap_ratio=overlap_ratio, distance_m=distance)


"""Previous-use and current-building-use inference for canonical sites."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, row_payload
from src.site_engine.site_evidence_schema import UseClassificationEvidence
from src.site_engine.types import ConfidenceLabel


PREVIOUS_USE_MAP = {
    "agricultural": "agricultural field",
    "agriculture": "agricultural field",
    "greenfield": "virgin greenfield",
    "farm": "farm steading / farmyard",
    "farmyard": "farm steading / farmyard",
    "residential": "former residential",
    "housing": "former residential",
    "commercial": "commercial",
    "industrial": "industrial",
    "storage": "storage / yard",
    "yard": "storage / yard",
    "depot": "depot / transport",
    "office": "office / civic",
    "civic": "office / civic",
    "education": "education / community",
    "community": "education / community",
    "utility": "utilities / infrastructure",
    "utilities": "utilities / infrastructure",
    "cleared": "vacant cleared site",
    "vacant": "vacant cleared site",
    "derelict": "derelict / brownfield unknown",
    "brownfield": "derelict / brownfield unknown",
    "mineral": "mineral / extraction related",
    "extraction": "mineral / extraction related",
}

CURRENT_USE_MAP = {
    "residential": "residential",
    "retail": "commercial retail",
    "commercial": "commercial retail",
    "office": "office",
    "industrial": "industrial",
    "workshop": "workshop / factory / store",
    "factory": "workshop / factory / store",
    "store": "workshop / factory / store",
    "agricultural": "agricultural building",
    "farm": "agricultural building",
    "civic": "civic / public",
    "public": "civic / public",
    "education": "education",
    "health": "healthcare",
    "utility": "utilities / infrastructure",
    "utilities": "utilities / infrastructure",
    "mixed": "mixed-use",
    "vacant": "vacant / disused",
    "disused": "vacant / disused",
}


def infer_site_use(
    location: dict[str, Any] | None,
    planning_records: list[dict[str, Any]],
    context_records: list[dict[str, Any]],
    constraint_records: list[dict[str, Any]],
) -> tuple[UseClassificationEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    previous_site_use = "mixed / unclear"
    previous_confidence: ConfidenceLabel = "low"
    current_building_use = "unknown"
    current_confidence: ConfidenceLabel = "low"

    for row in constraint_records:
        constraint_type = str(row.get("constraint_type") or "").lower()
        payload = row_payload(row)
        previous_use_value = payload.get("previous_use_type")
        current_use_value = payload.get("current_building_use")
        if previous_use_value:
            previous_site_use, previous_confidence = _map_previous_use(str(previous_use_value), "high")
            add_evidence(
                field_evidence,
                "use.previous_site_use",
                "public.site_constraints",
                row,
                f"Previous site use is inferred as '{previous_site_use}'.",
            )
        if current_use_value:
            current_building_use, current_confidence = _map_current_use(str(current_use_value), "high")
            add_evidence(
                field_evidence,
                "use.current_building_use",
                "public.site_constraints",
                row,
                f"Current building use is inferred as '{current_building_use}'.",
            )
        if constraint_type in {"contamination", "made_ground"} and previous_site_use == "mixed / unclear":
            previous_site_use, previous_confidence = "derelict / brownfield unknown", "medium"
            add_evidence(
                field_evidence,
                "use.previous_site_use",
                "public.site_constraints",
                row,
                "Constraint evidence implies a brownfield or previously developed land history.",
            )

    for row in context_records:
        payload = row_payload(row)
        if payload.get("previous_use_type") and previous_confidence != "high":
            previous_site_use, previous_confidence = _map_previous_use(str(payload["previous_use_type"]), "high")
            add_evidence(
                field_evidence,
                "use.previous_site_use",
                "public.planning_context_records",
                row,
                f"Context evidence classifies previous site use as '{previous_site_use}'.",
            )
        if payload.get("current_building_use") and current_confidence != "high":
            current_building_use, current_confidence = _map_current_use(str(payload["current_building_use"]), "high")
            add_evidence(
                field_evidence,
                "use.current_building_use",
                "public.planning_context_records",
                row,
                f"Context evidence classifies current building use as '{current_building_use}'.",
            )

    for row in planning_records:
        if previous_confidence == "high" and current_confidence == "high":
            break
        description = " ".join(
            filter(
                None,
                [
                    str(row.get("description") or ""),
                    str(row_payload(row).get("land_use") or ""),
                    str(row_payload(row).get("previous_use_type") or ""),
                    str(row_payload(row).get("current_building_use") or ""),
                ],
            )
        ).lower()
        if previous_confidence != "high":
            inferred_previous = _classify_from_text(description, PREVIOUS_USE_MAP)
            if inferred_previous:
                previous_site_use = inferred_previous
                previous_confidence = "medium"
                add_evidence(
                    field_evidence,
                    "use.previous_site_use",
                    "public.planning_records",
                    row,
                    f"Planning language implies previous site use '{previous_site_use}'.",
                )
        if current_confidence != "high":
            inferred_current = _classify_from_text(description, CURRENT_USE_MAP)
            if inferred_current:
                current_building_use = inferred_current
                current_confidence = "medium"
                add_evidence(
                    field_evidence,
                    "use.current_building_use",
                    "public.planning_records",
                    row,
                    f"Planning language implies current building use '{current_building_use}'.",
                )

    if previous_site_use == "mixed / unclear":
        settlement_relationship = str((location or {}).get("settlement_relationship") or "").lower()
        if settlement_relationship in {"edge_of_settlement", "settlement_extension"}:
            previous_site_use = "agricultural field"
            previous_confidence = "medium"
            if location:
                add_evidence(
                    field_evidence,
                    "use.previous_site_use",
                    "public.site_locations",
                    location,
                    "Edge-of-settlement location with no brownfield signals defaults to agricultural-field use.",
                )
        else:
            previous_site_use = "virgin greenfield"

    if current_building_use == "unknown" and previous_site_use in {"virgin greenfield", "agricultural field"}:
        current_building_use = "vacant / disused"
        current_confidence = "low"

    return (
        UseClassificationEvidence(
            previous_site_use=previous_site_use,
            previous_site_use_confidence=previous_confidence,
            current_building_use=current_building_use,
            current_building_use_confidence=current_confidence,
        ),
        field_evidence,
    )


def _map_previous_use(raw_value: str, confidence: ConfidenceLabel) -> tuple[str, ConfidenceLabel]:
    lowered = raw_value.lower()
    for fragment, label in PREVIOUS_USE_MAP.items():
        if fragment in lowered:
            return label, confidence
    return "mixed / unclear", "low"


def _map_current_use(raw_value: str, confidence: ConfidenceLabel) -> tuple[str, ConfidenceLabel]:
    lowered = raw_value.lower()
    for fragment, label in CURRENT_USE_MAP.items():
        if fragment in lowered:
            return label, confidence
    return "unknown", "low"


def _classify_from_text(description: str, mapping: dict[str, str]) -> str | None:
    for fragment, label in mapping.items():
        if fragment in description:
            return label
    return None


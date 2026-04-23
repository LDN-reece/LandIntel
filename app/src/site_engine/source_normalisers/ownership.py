"""Normalise ownership and control evidence."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, as_bool, row_payload
from src.site_engine.site_evidence_schema import OwnershipEvidence


def normalise_ownership(
    parcels: list[dict[str, Any]],
    control_records: list[dict[str, Any]],
) -> tuple[OwnershipEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    title_count = len({str(parcel.get("title_number")) for parcel in parcels if parcel.get("title_number")})
    if title_count == 0 and parcels:
        title_count = len(parcels)

    ownership_fragmentation_level = "single"
    public_ownership_indicator = False
    legal_issue_flags: list[str] = []
    if title_count >= 4:
        ownership_fragmentation_level = "many"
    elif title_count >= 2:
        ownership_fragmentation_level = "multiple"

    for parcel in parcels:
        add_evidence(
            field_evidence,
            "ownership.title_count",
            "public.site_parcels",
            parcel,
            "Linked parcel and title references contribute to title-count evidence.",
        )

    for row in control_records:
        payload = row_payload(row)
        control_type = str(row.get("control_type") or "").lower()
        control_level = str(row.get("control_level") or "unknown").lower()
        if control_type == "ownership_fragmentation":
            ownership_fragmentation_level = payload.get("fragmentation_level") or control_level or ownership_fragmentation_level
            add_evidence(
                field_evidence,
                "ownership.ownership_fragmentation_level",
                "public.site_control_records",
                row,
                f"Ownership fragmentation is recorded as '{ownership_fragmentation_level}'.",
            )
        if control_type == "public_ownership":
            public_ownership_indicator = as_bool(payload.get("public_ownership_indicator")) or True
            add_evidence(
                field_evidence,
                "ownership.public_ownership_indicator",
                "public.site_control_records",
                row,
                "Public ownership or public disposal route is flagged in control evidence.",
            )
        if control_type in {"legal_issue", "ransom_strip", "access_control"} or control_level in {"high", "critical"}:
            legal_issue_flags.append(control_type or "control_issue")
            add_evidence(
                field_evidence,
                "ownership.legal_control_issue_flags",
                "public.site_control_records",
                row,
                "Legal or control complexity is linked to the site.",
            )

    return (
        OwnershipEvidence(
            title_count=title_count,
            ownership_fragmentation_level=str(ownership_fragmentation_level),
            public_ownership_indicator=public_ownership_indicator,
            legal_control_issue_flags=tuple(sorted(set(flag for flag in legal_issue_flags if flag))),
        ),
        field_evidence,
    )


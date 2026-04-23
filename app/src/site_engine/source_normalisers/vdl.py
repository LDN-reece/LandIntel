"""Normalise vacant and derelict land evidence."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, as_int, row_payload
from src.site_engine.site_evidence_schema import VdlEvidence


def normalise_vdl(constraint_records: list[dict[str, Any]]) -> tuple[VdlEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    vdl_rows = [
        row for row in constraint_records if str(row.get("constraint_type") or "").lower() in {"vdl", "vacant_derelict"}
    ]
    if not vdl_rows:
        return VdlEvidence(), field_evidence

    row = vdl_rows[0]
    payload = row_payload(row)
    previous_use = str(payload.get("previous_use_type") or row.get("status") or "unknown").lower()
    years = as_int(payload.get("years_on_register"), 0)
    add_evidence(
        field_evidence,
        "vdl.on_vdl_register",
        "public.site_constraints",
        row,
        "The site is linked to vacant and derelict land evidence.",
    )
    add_evidence(
        field_evidence,
        "vdl.previous_use_type",
        "public.site_constraints",
        row,
        f"VDL evidence indicates previous use type '{previous_use}'.",
    )
    if years:
        add_evidence(
            field_evidence,
            "vdl.years_on_register",
            "public.site_constraints",
            row,
            f"VDL evidence records approximately {years} year(s) on register.",
        )

    return (
        VdlEvidence(
            on_vdl_register=True,
            previous_use_type=previous_use,
            years_on_register=years,
        ),
        field_evidence,
    )


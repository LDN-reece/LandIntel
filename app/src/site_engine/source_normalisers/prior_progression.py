"""Normalise prior site progression signals."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import add_evidence, row_payload
from src.site_engine.signal_extractors import extract_progression_signal
from src.site_engine.site_evidence_schema import PriorProgressionEvidence


def normalise_prior_progression(
    planning_records: list[dict[str, Any]],
    constraint_records: list[dict[str, Any]],
) -> tuple[PriorProgressionEvidence, dict[str, list]]:
    field_evidence: dict[str, list] = {}
    extracted = extract_progression_signal(planning_records, constraint_records)

    for row in planning_records:
        payload = row_payload(row)
        if payload.get("has_layouts"):
            add_evidence(
                field_evidence,
                "prior_progression.has_layouts",
                "public.planning_records",
                row,
                "Linked planning history indicates prior layout or masterplan work.",
            )
        if payload.get("has_prior_reports"):
            add_evidence(
                field_evidence,
                "prior_progression.has_prior_reports",
                "public.planning_records",
                row,
                "Linked planning history indicates prior consultant or technical reports.",
            )
        if payload.get("sponsor_failure_indicator"):
            add_evidence(
                field_evidence,
                "prior_progression.sponsor_failure_indicator",
                "public.planning_records",
                row,
                "Linked history indicates the prior sponsor stopped for a commercial or event reason.",
            )
        if payload.get("scheme_scale") in {"major", "strategic", "phasing"}:
            add_evidence(
                field_evidence,
                "prior_progression.has_major_prior_scheme",
                "public.planning_records",
                row,
                "Linked planning history indicates a major prior scheme was worked up.",
            )

    for row in constraint_records:
        if str(row.get("constraint_type") or "").lower() in {"borehole", "site_investigation", "drillcore"}:
            add_evidence(
                field_evidence,
                "prior_progression.has_si_indicators",
                "public.site_constraints",
                row,
                "Ground investigation evidence suggests prior progression rather than automatic technical failure.",
            )

    if planning_records:
        for row in planning_records:
            add_evidence(
                field_evidence,
                "prior_progression.progression_level",
                "public.planning_records",
                row,
                f"Planning history contributes to progression level '{extracted['progression_level']}'.",
            )

    return (
        PriorProgressionEvidence(
            progression_level=str(extracted["progression_level"]),
            has_layouts=bool(extracted["has_layouts"]),
            has_prior_reports=bool(extracted["has_prior_reports"]),
            has_si_indicators=bool(extracted["has_si_indicators"]),
            has_major_prior_scheme=bool(extracted["has_major_prior_scheme"]),
            sponsor_failure_indicator=bool(extracted["sponsor_failure_indicator"]),
        ),
        field_evidence,
    )


"""Prior progression extraction for Scottish site reasoning."""

from __future__ import annotations

from typing import Any

from src.site_engine.evidence_utils import as_bool, row_payload


def extract_progression_signal(
    planning_records: list[dict[str, Any]],
    constraint_records: list[dict[str, Any]],
) -> dict[str, Any]:
    meaningful_records = [
        record
        for record in planning_records
        if str(record.get("record_type") or "").lower() in {"application", "pre_app", "appeal", "ppp"}
    ]
    payloads = [row_payload(record) for record in planning_records]
    has_layouts = any(as_bool(payload.get("has_layouts")) for payload in payloads)
    has_prior_reports = any(as_bool(payload.get("has_prior_reports")) for payload in payloads)
    sponsor_failure_indicator = any(as_bool(payload.get("sponsor_failure_indicator")) for payload in payloads)
    has_major_prior_scheme = any(
        str(payload.get("scheme_scale") or "").lower() in {"major", "strategic", "phasing"}
        for payload in payloads
    )
    has_si_indicators = any(
        str(record.get("constraint_type") or "").lower() in {"borehole", "site_investigation", "drillcore"}
        or as_bool(row_payload(record).get("si_indicator"))
        for record in constraint_records
    )

    if len(meaningful_records) >= 3 or has_major_prior_scheme or (has_layouts and has_prior_reports and has_si_indicators):
        progression_level = "advanced"
    elif len(meaningful_records) >= 2 or (has_layouts and has_prior_reports):
        progression_level = "high"
    elif meaningful_records or has_layouts or has_prior_reports or has_si_indicators:
        progression_level = "medium"
    else:
        progression_level = "none"

    return {
        "progression_level": progression_level,
        "has_layouts": has_layouts,
        "has_prior_reports": has_prior_reports,
        "has_si_indicators": has_si_indicators,
        "has_major_prior_scheme": has_major_prior_scheme,
        "sponsor_failure_indicator": sponsor_failure_indicator,
    }


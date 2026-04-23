"""Shared evidence helpers for the Scottish reasoning layer."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from src.site_engine.types import EvidenceItem


def evidence_from_row(source_table: str, row: dict[str, Any], assertion: str) -> EvidenceItem:
    """Create a traceable evidence object from a structured source row."""

    if not row:
        return EvidenceItem(dataset_name="unknown", source_table=source_table, assertion=assertion)

    source_record_id = row.get("source_record_id") or row.get("id")
    raw_payload = _row_payload(row)
    source_identifier = (
        row.get("application_reference")
        or row.get("context_label")
        or row.get("constraint_type")
        or row.get("infrastructure_type")
        or row.get("control_type")
        or row.get("address")
        or row.get("profile_code")
        or row.get("source_identifier")
        or raw_payload.get("source_identifier")
    )
    excerpt = (
        row.get("description")
        or row.get("notes")
        or row.get("evidence_summary")
        or raw_payload.get("notes")
        or raw_payload.get("summary")
    )
    observed_at = row.get("decision_date") or row.get("sale_date") or row.get("observed_at")

    return EvidenceItem(
        dataset_name=str(row.get("source_dataset") or row.get("dataset_name") or source_table),
        source_table=source_table,
        source_record_id=str(source_record_id) if source_record_id is not None else None,
        source_identifier=str(source_identifier) if source_identifier is not None else None,
        source_url=row.get("source_url"),
        observed_at=normalise_timestamp(observed_at),
        import_version=row.get("import_version"),
        confidence_label=row.get("record_strength") or raw_payload.get("confidence_label"),
        confidence_score=_as_float(raw_payload.get("confidence_score")),
        excerpt=str(excerpt) if excerpt else None,
        assertion=assertion,
        metadata={},
    )


def add_evidence(
    field_evidence: dict[str, list[EvidenceItem]],
    field_key: str,
    source_table: str,
    row: dict[str, Any],
    assertion: str,
) -> None:
    field_evidence.setdefault(field_key, []).append(evidence_from_row(source_table, row, assertion))


def merge_field_evidence(
    base: dict[str, list[EvidenceItem]],
    extra: dict[str, list[EvidenceItem]],
) -> dict[str, list[EvidenceItem]]:
    merged = {key: list(items) for key, items in base.items()}
    for key, items in extra.items():
        merged.setdefault(key, []).extend(items)
    return merged


def row_payload(row: dict[str, Any]) -> dict[str, Any]:
    return _row_payload(row)


def as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "yes", "1", "y"}:
        return True
    if text in {"false", "no", "0", "n"}:
        return False
    return None


def as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_float(value: Any, default: float = 0.0) -> float:
    return _as_float(value, default)


def normalise_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("raw_payload")
    return payload if isinstance(payload, dict) else {}


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


"""Canonical-site reference bridge for Scottish datasets."""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
import re
from typing import Any, Iterable

from src.site_engine.site_evidence_schema import ReconciliationEvidence
from src.site_engine.types import ConfidenceLabel, EvidenceItem, SiteSnapshot


REFERENCE_PAYLOAD_KEYS: dict[str, str] = {
    "site_reference": "source_ref",
    "site_code": "source_ref",
    "allocation_code": "ldp_ref",
    "ldp_ref": "ldp_ref",
    "hla_ref": "hla_ref",
    "ela_ref": "ela_ref",
    "vdl_ref": "vdl_ref",
    "council_site_code": "council_ref",
    "authority_ref": "authority_ref",
    "planning_reference": "planning_ref",
    "title_number": "title_number",
    "uprn": "uprn",
    "usrn": "usrn",
    "toid": "toid",
}

MULTI_VALUE_PAYLOAD_KEYS: dict[str, str] = {
    "site_aliases": "site_name_alias",
    "uprns": "uprn",
    "usrns": "usrn",
    "toids": "toid",
    "authority_refs": "authority_ref",
}

FAMILY_TO_EVIDENCE_KEY = {
    "site_name_alias": "reconciliation.site_name_aliases",
    "source_ref": "reconciliation.source_refs",
    "planning_ref": "reconciliation.planning_refs",
    "ldp_ref": "reconciliation.ldp_refs",
    "hla_ref": "reconciliation.hla_refs",
    "ela_ref": "reconciliation.ela_refs",
    "vdl_ref": "reconciliation.vdl_refs",
    "council_ref": "reconciliation.council_refs",
    "title_number": "reconciliation.title_numbers",
    "uprn": "reconciliation.uprns",
    "usrn": "reconciliation.usrns",
    "toid": "reconciliation.toids",
    "authority_ref": "reconciliation.authority_refs",
}


@dataclass(frozen=True)
class ReferenceCandidate:
    reference_family: str
    raw_value: str
    normalised_value: str
    source_dataset: str
    source_table: str
    source_record_id: str | None
    site_name_hint: str | None = None
    authority_name: str | None = None
    plan_period: str | None = None
    source_identifier: str | None = None
    source_url: str | None = None
    relation_type: str = "direct_reference"
    match_notes: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SiteIndexEntry:
    site_id: str
    site_code: str
    site_name: str
    authority_name: str | None
    nearest_settlement: str | None
    geometry_overlap_ratio: float | None = None
    geometry_distance_m: float | None = None
    reference_values: tuple[str, ...] = ()
    planning_refs: tuple[str, ...] = ()
    title_numbers: tuple[str, ...] = ()
    site_name_aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class MatchDecision:
    site_id: str | None
    status: str
    relation_type: str
    confidence: float
    notes: str


@dataclass(frozen=True)
class SiteReconciliationBundle:
    aliases_to_upsert: list[dict[str, Any]]
    matches_to_record: list[dict[str, Any]]
    review_items: list[dict[str, Any]]
    inventory: ReconciliationEvidence
    field_evidence: dict[str, list[EvidenceItem]]


def normalise_reference_value(raw_value: str | None) -> str:
    """Collapse council-specific punctuation and spacing into a stable alias key."""

    if not raw_value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", raw_value.upper())


def reconcile_candidate_to_site_index(
    candidate: ReferenceCandidate,
    site_index: Iterable[SiteIndexEntry],
) -> MatchDecision:
    """Match an external dataset row to the best canonical site using ordered logic."""

    best_fuzzy: tuple[float, SiteIndexEntry] | None = None
    best_spatial: tuple[float, str, SiteIndexEntry] | None = None
    for entry in site_index:
        if candidate.normalised_value and candidate.normalised_value == normalise_reference_value(entry.site_code):
            return MatchDecision(entry.site_id, "matched", "direct_reference", 0.99, "Matched canonical site code directly.")

        reference_pool = {normalise_reference_value(value) for value in entry.reference_values}
        if candidate.normalised_value and candidate.normalised_value in reference_pool:
            return MatchDecision(entry.site_id, "matched", "alias_table", 0.95, "Matched the canonical alias table.")

        planning_pool = {normalise_reference_value(value) for value in entry.planning_refs}
        if candidate.reference_family == "planning_ref" and candidate.normalised_value in planning_pool:
            return MatchDecision(entry.site_id, "matched", "planning_reference", 0.91, "Matched via linked planning reference.")

        title_pool = {normalise_reference_value(value) for value in entry.title_numbers}
        if candidate.reference_family == "title_number" and candidate.normalised_value in title_pool:
            return MatchDecision(entry.site_id, "matched", "title_linkage", 0.9, "Matched via linked title number.")

        overlap_ratio = entry.geometry_overlap_ratio
        distance_m = entry.geometry_distance_m
        if overlap_ratio is not None and overlap_ratio >= 0.75:
            return MatchDecision(entry.site_id, "matched", "geometry_overlap", 0.88, "Matched by strong geometry overlap with the canonical site.")
        if overlap_ratio is not None and overlap_ratio >= 0.35:
            spatial_score = round(min(0.8, 0.55 + overlap_ratio / 3), 2)
            if best_spatial is None or spatial_score > best_spatial[0]:
                best_spatial = (spatial_score, "probable", entry)
        elif distance_m is not None and distance_m <= 50:
            spatial_score = 0.69
            if best_spatial is None or spatial_score > best_spatial[0]:
                best_spatial = (spatial_score, "probable", entry)

        fuzzy_score = _fuzzy_similarity(
            candidate.site_name_hint or candidate.raw_value,
            " ".join(filter(None, [entry.site_name, *entry.site_name_aliases])),
        )
        if candidate.authority_name and entry.authority_name and candidate.authority_name == entry.authority_name:
            fuzzy_score += 0.08
        if candidate.metadata.get("nearest_settlement") and entry.nearest_settlement:
            if str(candidate.metadata["nearest_settlement"]).lower() == str(entry.nearest_settlement).lower():
                fuzzy_score += 0.08
        if best_fuzzy is None or fuzzy_score > best_fuzzy[0]:
            best_fuzzy = (fuzzy_score, entry)

    if best_spatial and best_spatial[0] >= 0.68:
        return MatchDecision(
            best_spatial[2].site_id,
            best_spatial[1],
            "geometry_overlap",
            best_spatial[0],
            "Spatial proximity or overlap suggests a likely canonical site bridge, but it still needs review.",
        )
    if best_fuzzy and best_fuzzy[0] >= 0.84:
        return MatchDecision(
            best_fuzzy[1].site_id,
            "matched",
            "fuzzy_documentary",
            round(min(best_fuzzy[0], 0.9), 2),
            "Matched on documentary similarity within the same place context.",
        )
    if best_fuzzy and best_fuzzy[0] >= 0.68:
        return MatchDecision(
            best_fuzzy[1].site_id,
            "probable",
            "fuzzy_documentary",
            round(min(best_fuzzy[0], 0.8), 2),
            "Likely documentary match, but still weak enough for human review.",
        )
    return MatchDecision(None, "unresolved", "manual_review", 0.35, "No strong direct, alias, planning, title, or fuzzy bridge was found.")


def prepare_site_reconciliation(snapshot: SiteSnapshot) -> SiteReconciliationBundle:
    """Build canonical reference inventory, match audit rows, and review queue items."""

    field_evidence: dict[str, list[EvidenceItem]] = {}
    existing_alias_keys = {
        (
            str(row.get("reference_family") or ""),
            str(row.get("source_dataset") or ""),
            str(row.get("source_record_id") or ""),
            normalise_reference_value(str(row.get("raw_reference_value") or "")),
        )
        for row in snapshot.reference_aliases
    }

    candidates = _extract_reference_candidates(snapshot)
    aliases_to_upsert: list[dict[str, Any]] = []
    matches_to_record: list[dict[str, Any]] = []
    review_items: list[dict[str, Any]] = []

    for candidate in candidates:
        candidate_key = (
            candidate.reference_family,
            candidate.source_dataset,
            str(candidate.source_record_id or ""),
            candidate.normalised_value,
        )
        if candidate_key not in existing_alias_keys:
            aliases_to_upsert.append(
                {
                    "reference_family": candidate.reference_family,
                    "raw_reference_value": candidate.raw_value,
                    "normalised_reference_value": candidate.normalised_value,
                    "source_dataset": candidate.source_dataset,
                    "authority_name": candidate.authority_name,
                    "plan_period": candidate.plan_period,
                    "site_name_hint": candidate.site_name_hint,
                    "geometry_hash": candidate.metadata.get("geometry_hash"),
                    "source_record_id": candidate.source_record_id,
                    "source_identifier": candidate.source_identifier,
                    "source_url": candidate.source_url,
                    "relation_type": candidate.relation_type,
                    "status": "matched",
                    "linked_confidence": _candidate_confidence(candidate),
                    "match_notes": candidate.match_notes,
                    "metadata_json": candidate.metadata,
                }
            )

        matches_to_record.append(
            {
                "source_dataset": candidate.source_dataset,
                "source_table": candidate.source_table,
                "source_record_id": candidate.source_record_id,
                "raw_site_name": candidate.site_name_hint,
                "raw_reference_value": candidate.raw_value,
                "normalised_reference_value": candidate.normalised_value,
                "planning_reference": candidate.raw_value if candidate.reference_family == "planning_ref" else candidate.metadata.get("planning_reference"),
                "title_number": candidate.raw_value if candidate.reference_family == "title_number" else candidate.metadata.get("title_number"),
                "uprn": candidate.raw_value if candidate.reference_family == "uprn" else candidate.metadata.get("uprn"),
                "usrn": candidate.raw_value if candidate.reference_family == "usrn" else candidate.metadata.get("usrn"),
                "toid": candidate.raw_value if candidate.reference_family == "toid" else candidate.metadata.get("toid"),
                "authority_name": candidate.authority_name,
                "settlement_name": candidate.metadata.get("nearest_settlement"),
                "relation_type": candidate.relation_type,
                "confidence_score": _candidate_confidence(candidate),
                "status": "matched",
                "geometry_overlap_ratio": candidate.metadata.get("geometry_overlap_ratio"),
                "geometry_distance_m": candidate.metadata.get("geometry_distance_m"),
                "match_notes": candidate.match_notes,
                "metadata_json": candidate.metadata,
            }
        )

        evidence_key = FAMILY_TO_EVIDENCE_KEY.get(candidate.reference_family)
        if evidence_key:
            field_evidence.setdefault(evidence_key, []).append(
                _candidate_evidence(
                    candidate,
                    f"Canonical site bridge captured {candidate.reference_family.replace('_', ' ')} '{candidate.raw_value}'.",
                )
            )

    for row in snapshot.reconciliation_review_items:
        review_items.append(
            {
                "source_dataset": row.get("source_dataset"),
                "source_table": row.get("source_table"),
                "source_record_id": row.get("source_record_id"),
                "raw_site_name": row.get("raw_site_name"),
                "raw_reference_value": row.get("raw_reference_value"),
                "normalised_reference_value": row.get("normalised_reference_value"),
                "planning_reference": row.get("planning_reference"),
                "authority_name": row.get("authority_name"),
                "settlement_name": row.get("settlement_name"),
                "confidence_score": row.get("confidence_score"),
                "failure_reasons_json": row.get("failure_reasons") or [],
                "candidate_matches_json": row.get("candidate_matches") or [],
                "metadata_json": row.get("metadata") or {},
            }
        )

    inventory = build_reference_inventory(snapshot)
    if snapshot.geometry_versions:
        field_evidence.setdefault("reconciliation.geometry_versions", []).extend(
            [
                EvidenceItem(
                    dataset_name=str(row.get("source_dataset") or "canonical_geometry"),
                    source_table="public.site_geometry_versions",
                    source_record_id=str(row.get("id") or row.get("source_record_id") or ""),
                    source_identifier=str(row.get("geometry_hash") or row.get("version_label") or ""),
                    source_url=row.get("source_url"),
                    import_version=row.get("import_version"),
                    confidence_label=row.get("confidence_label"),
                    assertion=f"Canonical site geometry version '{row.get('version_label') or row.get('geometry_hash')}' is stored for traceability.",
                    metadata=row.get("metadata") or {},
                )
                for row in snapshot.geometry_versions
            ]
        )

    return SiteReconciliationBundle(
        aliases_to_upsert=aliases_to_upsert,
        matches_to_record=matches_to_record,
        review_items=review_items,
        inventory=inventory,
        field_evidence=field_evidence,
    )


def build_reference_inventory(snapshot: SiteSnapshot) -> ReconciliationEvidence:
    """Roll linked aliases into the canonical-site reference summary."""

    site_metadata = snapshot.site.get("metadata") or {}
    families: dict[str, set[str]] = {
        "site_name_alias": {str(value) for value in site_metadata.get("site_name_aliases", []) if value},
        "source_ref": {str(snapshot.site.get("site_code") or "")},
        "planning_ref": {
            str(row.get("application_reference"))
            for row in snapshot.planning_records
            if row.get("application_reference")
        },
        "ldp_ref": set(),
        "hla_ref": set(),
        "ela_ref": set(),
        "vdl_ref": set(),
        "council_ref": set(),
        "title_number": {str(row.get("title_number")) for row in snapshot.parcels if row.get("title_number")},
        "uprn": set(),
        "usrn": set(),
        "toid": set(),
        "authority_ref": set(),
    }
    status_flags: list[str] = []
    notes: list[str] = []

    for row in snapshot.reference_aliases:
        family = str(row.get("reference_family") or "")
        raw_value = str(row.get("raw_reference_value") or "").strip()
        if family in families and raw_value:
            families[family].add(raw_value)
        status = str(row.get("status") or "").lower()
        if status:
            status_flags.append(status)
        if row.get("match_notes"):
            notes.append(str(row["match_notes"]))
        if row.get("site_name_hint"):
            families["site_name_alias"].add(str(row["site_name_hint"]))

    for row_group in (
        snapshot.planning_records,
        snapshot.planning_context_records,
        snapshot.constraints,
        snapshot.infrastructure_records,
        snapshot.control_records,
    ):
        for row in row_group:
            payload = row.get("raw_payload") or {}
            if not isinstance(payload, dict):
                continue
            for key, family in REFERENCE_PAYLOAD_KEYS.items():
                value = payload.get(key)
                if family in families and value:
                    families[family].add(str(value))
            for key, family in MULTI_VALUE_PAYLOAD_KEYS.items():
                values = payload.get(key) or []
                if family in families and isinstance(values, list):
                    families[family].update(str(value) for value in values if value)

    geometry_versions = tuple(
        dict.fromkeys(
            str(row.get("version_label") or row.get("geometry_hash") or "")
            for row in snapshot.geometry_versions
            if row.get("version_label") or row.get("geometry_hash")
        )
    )
    unresolved_reference_count = len(
        [
            row
            for row in snapshot.reference_aliases
            if str(row.get("status") or "").lower() == "unresolved"
        ]
    ) + len(snapshot.reconciliation_review_items)
    matched_reference_count = sum(len([value for value in values if value]) for values in families.values())
    if snapshot.reference_aliases:
        matched_reference_count = max(
            matched_reference_count,
            len(
                [
                    row
                    for row in snapshot.reference_aliases
                    if str(row.get("status") or "matched").lower() == "matched"
                ]
            ),
        )
    match_confidence: ConfidenceLabel = "high"
    if unresolved_reference_count > 0:
        match_confidence = "low"
    elif any(flag == "probable" for flag in status_flags):
        match_confidence = "medium"

    return ReconciliationEvidence(
        site_name_primary=str(snapshot.site.get("site_name") or "Unknown site"),
        site_name_aliases=tuple(sorted(value for value in families["site_name_alias"] if value)),
        source_refs=tuple(sorted(value for value in families["source_ref"] if value)),
        planning_refs=tuple(sorted(value for value in families["planning_ref"] if value)),
        ldp_refs=tuple(sorted(value for value in families["ldp_ref"] if value)),
        hla_refs=tuple(sorted(value for value in families["hla_ref"] if value)),
        ela_refs=tuple(sorted(value for value in families["ela_ref"] if value)),
        vdl_refs=tuple(sorted(value for value in families["vdl_ref"] if value)),
        council_refs=tuple(sorted(value for value in families["council_ref"] if value)),
        title_numbers=tuple(sorted(value for value in families["title_number"] if value)),
        uprns=tuple(sorted(value for value in families["uprn"] if value)),
        usrns=tuple(sorted(value for value in families["usrn"] if value)),
        toids=tuple(sorted(value for value in families["toid"] if value)),
        authority_refs=tuple(sorted(value for value in families["authority_ref"] if value)),
        geometry_versions=geometry_versions,
        match_confidence=match_confidence,
        match_notes=tuple(dict.fromkeys(note for note in notes if note)),
        matched_reference_count=matched_reference_count,
        unresolved_reference_count=unresolved_reference_count,
    )


def _extract_reference_candidates(snapshot: SiteSnapshot) -> list[ReferenceCandidate]:
    candidates: list[ReferenceCandidate] = []
    seen: set[tuple[str, str, str, str]] = set()
    site = snapshot.site
    location = snapshot.location or {}

    def push(candidate: ReferenceCandidate) -> None:
        key = (
            candidate.reference_family,
            candidate.source_dataset,
            str(candidate.source_record_id or ""),
            candidate.normalised_value,
        )
        if not candidate.normalised_value or key in seen:
            return
        seen.add(key)
        candidates.append(candidate)

    site_code = str(site.get("site_code") or "").strip()
    if site_code:
        push(
            ReferenceCandidate(
                reference_family="source_ref",
                raw_value=site_code,
                normalised_value=normalise_reference_value(site_code),
                source_dataset="canonical_site",
                source_table="public.sites",
                source_record_id=str(site.get("id") or ""),
                site_name_hint=str(site.get("site_name") or ""),
                authority_name=location.get("authority_name"),
                source_identifier=site_code,
                relation_type="direct_reference",
                match_notes="Canonical site code is the root internal reference.",
                metadata={"nearest_settlement": location.get("nearest_settlement")},
            )
        )

    for parcel in snapshot.parcels:
        title_number = str(parcel.get("title_number") or "").strip()
        if not title_number:
            continue
        push(
            ReferenceCandidate(
                reference_family="title_number",
                raw_value=title_number,
                normalised_value=normalise_reference_value(title_number),
                source_dataset=str(parcel.get("source_dataset") or "site_parcel"),
                source_table="public.site_parcels",
                source_record_id=str(parcel.get("id") or parcel.get("source_record_id") or ""),
                site_name_hint=str(site.get("site_name") or ""),
                authority_name=location.get("authority_name"),
                source_identifier=parcel.get("parcel_reference"),
                relation_type="title_linkage",
                match_notes="Title linkage anchors the canonical site to the cadastral spine.",
                metadata={"nearest_settlement": location.get("nearest_settlement"), "title_number": title_number},
            )
        )

    for row in snapshot.planning_records:
        application_reference = str(row.get("application_reference") or "").strip()
        if application_reference:
            push(
                ReferenceCandidate(
                    reference_family="planning_ref",
                    raw_value=application_reference,
                    normalised_value=normalise_reference_value(application_reference),
                    source_dataset=str(row.get("source_dataset") or "planning"),
                    source_table="public.planning_records",
                    source_record_id=str(row.get("id") or row.get("source_record_id") or ""),
                    site_name_hint=str(site.get("site_name") or ""),
                    authority_name=location.get("authority_name"),
                    source_identifier=application_reference,
                    source_url=row.get("source_url"),
                    relation_type="planning_reference",
                    match_notes="Planning chronology creates a high-value bridge across datasets.",
                    metadata={"nearest_settlement": location.get("nearest_settlement"), "planning_reference": application_reference},
                )
            )
        _push_payload_candidates(push, row, "public.planning_records", location, site)

    for row in snapshot.planning_context_records:
        _push_payload_candidates(push, row, "public.planning_context_records", location, site)

    for row in snapshot.constraints:
        _push_payload_candidates(push, row, "public.site_constraints", location, site)

    for row in snapshot.control_records:
        _push_payload_candidates(push, row, "public.site_control_records", location, site)

    for row in snapshot.infrastructure_records:
        _push_payload_candidates(push, row, "public.site_infrastructure_records", location, site)

    metadata = site.get("metadata") or {}
    for alias in metadata.get("site_name_aliases", []):
        raw_value = str(alias or "").strip()
        if not raw_value:
            continue
        push(
            ReferenceCandidate(
                reference_family="site_name_alias",
                raw_value=raw_value,
                normalised_value=normalise_reference_value(raw_value),
                source_dataset="canonical_site",
                source_table="public.sites",
                source_record_id=str(site.get("id") or ""),
                site_name_hint=raw_value,
                authority_name=location.get("authority_name"),
                relation_type="fuzzy_documentary",
                match_notes="Canonical alias kept for documentary and schedule-style matching.",
                metadata={"nearest_settlement": location.get("nearest_settlement")},
            )
        )

    return candidates


def _push_payload_candidates(
    push: Any,
    row: dict[str, Any],
    source_table: str,
    location: dict[str, Any],
    site: dict[str, Any],
) -> None:
    payload = row.get("raw_payload") or {}
    if isinstance(payload, str):
        return
    source_dataset = str(row.get("source_dataset") or "unknown_dataset")
    authority_name = location.get("authority_name")
    nearest_settlement = location.get("nearest_settlement")
    for key, family in REFERENCE_PAYLOAD_KEYS.items():
        value = payload.get(key)
        if value:
            raw_value = str(value).strip()
            push(
                ReferenceCandidate(
                    reference_family=family,
                    raw_value=raw_value,
                    normalised_value=normalise_reference_value(raw_value),
                    source_dataset=source_dataset,
                    source_table=source_table,
                    source_record_id=str(row.get("id") or row.get("source_record_id") or ""),
                    site_name_hint=str(site.get("site_name") or ""),
                    authority_name=authority_name,
                    plan_period=payload.get("plan_period"),
                    source_identifier=row.get("source_record_id"),
                    source_url=row.get("source_url"),
                    relation_type=_family_relation_type(family),
                    match_notes=f"Source payload exposes {family.replace('_', ' ')} '{raw_value}'.",
                    metadata={
                        "nearest_settlement": nearest_settlement,
                        "geometry_hash": payload.get("geometry_hash"),
                        "planning_reference": payload.get("planning_reference"),
                        "title_number": payload.get("title_number"),
                        "uprn": payload.get("uprn"),
                        "usrn": payload.get("usrn"),
                        "toid": payload.get("toid"),
                    },
                )
            )
    for key, family in MULTI_VALUE_PAYLOAD_KEYS.items():
        values = payload.get(key) or []
        if not isinstance(values, list):
            continue
        for value in values:
            raw_value = str(value or "").strip()
            if not raw_value:
                continue
            push(
                ReferenceCandidate(
                    reference_family=family,
                    raw_value=raw_value,
                    normalised_value=normalise_reference_value(raw_value),
                    source_dataset=source_dataset,
                    source_table=source_table,
                    source_record_id=str(row.get("id") or row.get("source_record_id") or ""),
                    site_name_hint=str(site.get("site_name") or raw_value),
                    authority_name=authority_name,
                    source_identifier=row.get("source_record_id"),
                    source_url=row.get("source_url"),
                    relation_type=_family_relation_type(family),
                    match_notes=f"Source payload exposes {family.replace('_', ' ')} '{raw_value}'.",
                    metadata={"nearest_settlement": nearest_settlement, "geometry_hash": payload.get("geometry_hash")},
                )
            )


def _family_relation_type(reference_family: str) -> str:
    if reference_family == "planning_ref":
        return "planning_reference"
    if reference_family == "title_number":
        return "title_linkage"
    if reference_family == "site_name_alias":
        return "fuzzy_documentary"
    if reference_family == "uprn":
        return "uprn_linkage"
    if reference_family == "usrn":
        return "usrn_linkage"
    if reference_family == "toid":
        return "toid_linkage"
    if reference_family in {"ldp_ref", "hla_ref", "ela_ref", "vdl_ref", "council_ref", "authority_ref"}:
        return "alias_table"
    return "direct_reference"


def _candidate_confidence(candidate: ReferenceCandidate) -> float:
    if candidate.relation_type == "direct_reference":
        return 0.99
    if candidate.relation_type in {"planning_reference", "title_linkage"}:
        return 0.94
    if candidate.relation_type in {"uprn_linkage", "usrn_linkage", "toid_linkage"}:
        return 0.93
    if candidate.relation_type == "alias_table":
        return 0.9
    return 0.75


def _candidate_evidence(candidate: ReferenceCandidate, assertion: str) -> EvidenceItem:
    return EvidenceItem(
        dataset_name=candidate.source_dataset,
        source_table=candidate.source_table,
        source_record_id=candidate.source_record_id,
        source_identifier=candidate.raw_value,
        source_url=candidate.source_url,
        assertion=assertion,
        confidence_label=_confidence_label(_candidate_confidence(candidate)),
        confidence_score=_candidate_confidence(candidate),
        metadata=candidate.metadata,
    )


def _confidence_label(score: float) -> ConfidenceLabel:
    if score >= 0.9:
        return "high"
    if score >= 0.65:
        return "medium"
    return "low"


def _fuzzy_similarity(left: str | None, right: str | None) -> float:
    left_value = _normalise_phrase(left)
    right_value = _normalise_phrase(right)
    if not left_value or not right_value:
        return 0.0
    return SequenceMatcher(None, left_value, right_value).ratio()


def _normalise_phrase(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()

"""Dedicated BGS reasoning layer for prior progression and ground complexity."""

from __future__ import annotations

from src.site_engine.site_evidence_schema import BgsEvidence, BgsReasoningEvidence
from src.site_engine.types import EvidenceItem


def derive_bgs_reasoning(
    bgs: BgsEvidence,
    field_evidence: dict[str, list[EvidenceItem]],
) -> tuple[BgsReasoningEvidence, dict[str, list[EvidenceItem]]]:
    reasoning_evidence: dict[str, list[EvidenceItem]] = {}
    investigation_intensity = "none"
    prior_progression_signal_strength = "none"
    ground_complexity_signal = "low"
    hydrogeology_caution = "low"
    extraction_legacy_caution = "low"

    if bgs.site_investigation_overlap or bgs.geophysical_logs_presence or bgs.drillcore_presence:
        investigation_intensity = "high"
        prior_progression_signal_strength = "high"
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.investigation_intensity", "bgs.site_investigation_overlap", "bgs.geophysical_logs_presence", "bgs.drillcore_presence")
    elif bgs.borehole_count_site >= 3 or bgs.borehole_count_100m >= 5:
        investigation_intensity = "medium"
        prior_progression_signal_strength = "medium"
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.investigation_intensity", "bgs.borehole_count_site")
    elif bgs.borehole_count_site > 0 or bgs.borehole_count_100m > 0:
        investigation_intensity = "low"
        prior_progression_signal_strength = "low"
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.investigation_intensity", "bgs.borehole_count_site")

    if bgs.opencast_overlap:
        ground_complexity_signal = "high"
        extraction_legacy_caution = "high"
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.ground_complexity_signal", "bgs.opencast_overlap")
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.extraction_legacy_caution", "bgs.opencast_overlap")
    elif bgs.aquifer_presence or bgs.water_well_presence:
        ground_complexity_signal = "medium"
        hydrogeology_caution = "high" if bgs.aquifer_presence and bgs.water_well_presence else "medium"
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.ground_complexity_signal", "bgs.aquifer_presence", "bgs.water_well_presence")
        _carry_evidence(reasoning_evidence, field_evidence, "bgs_reasoning.hydrogeology_caution", "bgs.aquifer_presence", "bgs.water_well_presence")

    return (
        BgsReasoningEvidence(
            investigation_intensity=investigation_intensity,
            prior_progression_signal_strength=prior_progression_signal_strength,
            ground_complexity_signal=ground_complexity_signal,
            hydrogeology_caution=hydrogeology_caution,
            extraction_legacy_caution=extraction_legacy_caution,
        ),
        reasoning_evidence,
    )


def _carry_evidence(
    target: dict[str, list[EvidenceItem]],
    source: dict[str, list[EvidenceItem]],
    target_key: str,
    *source_keys: str,
) -> None:
    evidence_items: list[EvidenceItem] = []
    seen: set[tuple[str, str, str | None, str]] = set()
    for source_key in source_keys:
        for evidence in source.get(source_key, []):
            key = (
                evidence.dataset_name,
                evidence.source_table,
                evidence.source_record_id,
                evidence.assertion,
            )
            if key in seen:
                continue
            seen.add(key)
            evidence_items.append(evidence)
    if evidence_items:
        target[target_key] = evidence_items

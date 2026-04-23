"""Normalise linked Scottish source rows into a stable site-evidence object."""

from __future__ import annotations

from src.site_engine.bgs_reasoning_engine import derive_bgs_reasoning
from src.site_engine.boundary_position_engine import infer_boundary_positions
from src.site_engine.evidence_utils import merge_field_evidence
from src.site_engine.site_evidence_schema import SiteEvidence
from src.site_engine.site_reference_reconciliation_engine import prepare_site_reconciliation
from src.site_engine.source_normalisers.bgs import normalise_bgs
from src.site_engine.source_normalisers.flood import normalise_flood
from src.site_engine.source_normalisers.hla import normalise_hla
from src.site_engine.source_normalisers.infrastructure import normalise_infrastructure
from src.site_engine.source_normalisers.ldp import normalise_ldp
from src.site_engine.source_normalisers.market import normalise_market
from src.site_engine.source_normalisers.ownership import normalise_ownership
from src.site_engine.source_normalisers.planning import normalise_planning
from src.site_engine.source_normalisers.prior_progression import normalise_prior_progression
from src.site_engine.source_normalisers.vdl import normalise_vdl
from src.site_engine.use_classification_engine import infer_site_use
from src.site_engine.utility_burden_engine import infer_utility_burden
from src.site_engine.types import SiteSnapshot


def normalise_site_evidence(snapshot: SiteSnapshot) -> SiteEvidence:
    """Create the source-aware evidence model consumed by scoring and routing."""

    field_evidence: dict[str, list] = {}
    reconciliation_bundle = prepare_site_reconciliation(snapshot)
    reconciliation = reconciliation_bundle.inventory
    field_evidence = merge_field_evidence(field_evidence, reconciliation_bundle.field_evidence)

    boundary, boundary_evidence = infer_boundary_positions(
        snapshot.location,
        snapshot.planning_context_records,
    )
    field_evidence = merge_field_evidence(field_evidence, boundary_evidence)

    planning, planning_evidence = normalise_planning(
        snapshot.planning_records,
        snapshot.planning_context_records,
        snapshot.location,
    )
    field_evidence = merge_field_evidence(field_evidence, planning_evidence)

    ldp, ldp_evidence = normalise_ldp(snapshot.planning_context_records, snapshot.location)
    field_evidence = merge_field_evidence(field_evidence, ldp_evidence)

    hla, hla_evidence = normalise_hla(snapshot.planning_context_records)
    field_evidence = merge_field_evidence(field_evidence, hla_evidence)

    prior_progression, progression_evidence = normalise_prior_progression(
        snapshot.planning_records,
        snapshot.constraints,
    )
    field_evidence = merge_field_evidence(field_evidence, progression_evidence)

    bgs, bgs_evidence = normalise_bgs(snapshot.constraints)
    field_evidence = merge_field_evidence(field_evidence, bgs_evidence)
    bgs_reasoning, bgs_reasoning_evidence = derive_bgs_reasoning(bgs, field_evidence)
    field_evidence = merge_field_evidence(field_evidence, bgs_reasoning_evidence)

    flood, flood_evidence = normalise_flood(snapshot.constraints)
    field_evidence = merge_field_evidence(field_evidence, flood_evidence)

    vdl, vdl_evidence = normalise_vdl(snapshot.constraints)
    field_evidence = merge_field_evidence(field_evidence, vdl_evidence)

    use_classification, use_evidence = infer_site_use(
        snapshot.location,
        snapshot.planning_records,
        snapshot.planning_context_records,
        snapshot.constraints,
    )
    field_evidence = merge_field_evidence(field_evidence, use_evidence)

    ownership, ownership_evidence = normalise_ownership(snapshot.parcels, snapshot.control_records)
    field_evidence = merge_field_evidence(field_evidence, ownership_evidence)

    infrastructure, infrastructure_evidence = normalise_infrastructure(
        snapshot.infrastructure_records,
        snapshot.constraints,
        snapshot.planning_records,
        snapshot.planning_context_records,
    )
    field_evidence = merge_field_evidence(field_evidence, infrastructure_evidence)

    market, market_evidence = normalise_market(
        snapshot.location,
        snapshot.comparable_market_records,
        snapshot.buyer_matches,
    )
    field_evidence = merge_field_evidence(field_evidence, market_evidence)
    utility, utility_evidence = infer_utility_burden(
        infrastructure,
        snapshot.planning_records,
        snapshot.planning_context_records,
        market,
    )
    field_evidence = merge_field_evidence(field_evidence, utility_evidence)

    return SiteEvidence(
        site_id=str(snapshot.site["id"]),
        jurisdiction="scotland",
        reconciliation=reconciliation,
        boundary=boundary,
        planning=planning,
        ldp=ldp,
        hla=hla,
        prior_progression=prior_progression,
        bgs=bgs,
        bgs_reasoning=bgs_reasoning,
        flood=flood,
        vdl=vdl,
        use_classification=use_classification,
        ownership=ownership,
        infrastructure=infrastructure,
        utility=utility,
        market=market,
        field_evidence=field_evidence,
    )


__all__ = ["normalise_site_evidence"]

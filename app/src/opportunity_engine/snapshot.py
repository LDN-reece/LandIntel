"""Snapshot assembly helpers for the Phase One opportunity engine."""

from __future__ import annotations

from src.opportunity_engine.repository import OpportunityRepository
from src.opportunity_engine.types import OpportunitySnapshot


def build_snapshot(repository: OpportunityRepository, canonical_site_id: str) -> OpportunitySnapshot:
    """Assemble the live canonical-site snapshot used by ranking and review."""

    return repository.fetch_site_snapshot(canonical_site_id)

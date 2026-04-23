"""Typed primitives for the Phase One opportunity engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


VISIBLE_QUEUES = (
    "New Candidates",
    "Needs Review",
    "Strong Candidates",
    "Watchlist / Resurfaced",
)

REVIEW_STATUSES = (
    "New candidate",
    "Queued for review",
    "Under review",
    "Need more evidence",
    "Rejected",
    "Watchlist",
    "Conditional",
    "Strong candidate",
    "Buy title now",
    "Title ordered",
    "Title reviewed",
    "Likely missed / controlled",
    "Not for us",
    "Agency angle only",
    "Parked",
)


@dataclass(frozen=True)
class OpportunitySearchFilters:
    """Phase One browse filters for the queue-first review surface."""

    query: str | None = None
    queue_name: str | None = None
    authority_name: str | None = None
    source_route: str | None = None
    size_band: str | None = None
    planning_context_band: str | None = None
    settlement_position: str | None = None
    location_band: str | None = None
    constraint_severity: str | None = None
    access_strength: str | None = None
    geometry_quality: str | None = None
    ownership_control_state: str | None = None
    title_state: str | None = None
    review_status: str | None = None
    resurfaced_only: bool | None = None
    limit: int = 100


@dataclass(frozen=True)
class OpportunitySnapshot:
    """Canonical-site snapshot assembled from the live Phase One baseline."""

    summary: dict[str, Any]
    readiness: dict[str, Any] | None
    sources: list[dict[str, Any]]
    canonical_site: dict[str, Any]
    planning_records: list[dict[str, Any]]
    hla_records: list[dict[str, Any]]
    ldp_records: list[dict[str, Any]]
    settlement_boundary_records: list[dict[str, Any]]
    bgs_records: list[dict[str, Any]]
    flood_records: list[dict[str, Any]]
    ela_records: list[dict[str, Any]]
    vdl_records: list[dict[str, Any]]
    site_source_links: list[dict[str, Any]]
    site_reference_aliases: list[dict[str, Any]]
    evidence_references: list[dict[str, Any]]
    parcel_rows: list[dict[str, Any]]
    title_links: list[dict[str, Any]]
    title_validations: list[dict[str, Any]]
    geometry_metrics: dict[str, Any] | None
    geometry_diagnostics: dict[str, Any] | None
    constraint_overview: dict[str, Any] | None
    constraint_group_summaries: list[dict[str, Any]]
    constraint_measurements: list[dict[str, Any]]
    constraint_friction_facts: list[dict[str, Any]]
    review_events: list[dict[str, Any]]
    manual_overrides: list[dict[str, Any]]
    change_events: list[dict[str, Any]]
    latest_assessment: dict[str, Any] | None = None


@dataclass(frozen=True)
class OpportunitySignal:
    """A persisted current-state Phase One signal."""

    signal_key: str
    signal_label: str
    signal_group: str
    signal_status: str
    source_family: str
    confidence: str
    signal_value: dict[str, Any]
    reasoning: str
    fact_label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OpportunityAssessment:
    """Persistable ranking outcome for a canonical site."""

    overall_tier: str
    overall_rank_score: float
    queue_recommendation: str
    why_it_surfaced: str
    why_it_survived: str
    good_items: list[dict[str, Any]]
    bad_items: list[dict[str, Any]]
    ugly_items: list[dict[str, Any]]
    subrank_summary: dict[str, Any]
    title_state: str
    ownership_control_fact_label: str
    resurfaced_reason: str | None
    dominant_blocker: str
    human_review_required: bool
    explanation_text: str

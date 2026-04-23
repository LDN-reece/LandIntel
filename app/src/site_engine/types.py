"""Typed primitives for the site qualification engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


SignalState = Literal["known", "unknown", "inferred"]
SignalValueType = Literal["boolean", "numeric", "text", "json"]
InterpretationCategory = Literal["positive", "risk", "possible_fatal", "unknown"]
ScoreCode = Literal["P", "G", "I", "R", "F", "K", "B"]
ConfidenceLabel = Literal["high", "medium", "low"]
BucketCode = Literal["A", "B", "C", "D", "E", "F"]
HorizonLabel = Literal["Short Term", "Medium Term", "Long Term", "None / Reject / Watchlist"]


@dataclass(frozen=True)
class EvidenceItem:
    """Traceable support for a signal or interpretation."""

    dataset_name: str
    source_table: str
    assertion: str
    source_record_id: str | None = None
    source_identifier: str | None = None
    source_url: str | None = None
    observed_at: str | None = None
    import_version: str | None = None
    confidence_label: str | None = None
    confidence_score: float | None = None
    excerpt: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SignalResult:
    """Atomic, explainable signal derived from structured source data."""

    key: str
    label: str
    group: str
    value_type: SignalValueType
    state: SignalState
    reasoning: str
    bool_value: bool | None = None
    numeric_value: float | None = None
    text_value: str | None = None
    json_value: dict[str, Any] | list[Any] | None = None
    evidence: list[EvidenceItem] = field(default_factory=list)


@dataclass(frozen=True)
class InterpretationResult:
    """Rule output generated from one or more signals."""

    key: str
    category: InterpretationCategory
    title: str
    summary: str
    reasoning: str
    rule_code: str
    priority: int
    signal_keys: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ScoreContribution:
    """Explainable score movement sourced from explicit evidence families."""

    score_code: ScoreCode
    source_family: str
    delta: int
    summary: str
    reasoning: str
    evidence_keys: list[str] = field(default_factory=list)
    blocker_theme: str | None = None


@dataclass(frozen=True)
class SiteAssessmentScore:
    """One of the seven portfolio-routing scores."""

    score_code: ScoreCode
    label: str
    value: int
    confidence_label: ConfidenceLabel
    summary: str
    reasoning: str
    blocker_theme: str | None = None
    contributions: list[ScoreContribution] = field(default_factory=list)


@dataclass(frozen=True)
class HardFailFlag:
    """A deterministic gate that can immediately route the site to Bucket F."""

    gate: str
    title: str
    reason: str
    evidence_keys: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SiteAssessmentResult:
    """Persistable Scottish portfolio assessment for a site."""

    site_id: str
    jurisdiction: str
    assessment_version: str
    bucket_code: BucketCode
    bucket_label: str
    likely_opportunity_type: str
    monetisation_horizon: HorizonLabel
    horizon_year_band: str
    scores: dict[ScoreCode, SiteAssessmentScore]
    hard_fail_flags: list[HardFailFlag]
    dominant_blocker: str
    blocker_themes: list[str]
    primary_reason: str
    secondary_reasons: list[str]
    buyer_profile_guess: str | None
    likely_buyer_profiles: list[str]
    cost_to_control_band: str
    human_review_required: bool
    review_flags: list[str]
    explanation_fragments: list[str]
    next_checks: list[str]
    explanation_text: str
    evidence_keys: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SiteSnapshot:
    """Canonical site plus linked dataset rows, still kept dataset-separated."""

    site: dict[str, Any]
    location: dict[str, Any] | None
    parcels: list[dict[str, Any]]
    geometry_components: list[dict[str, Any]]
    geometry_versions: list[dict[str, Any]]
    reference_aliases: list[dict[str, Any]]
    reconciliation_matches: list[dict[str, Any]]
    reconciliation_review_items: list[dict[str, Any]]
    planning_records: list[dict[str, Any]]
    planning_context_records: list[dict[str, Any]]
    constraints: list[dict[str, Any]]
    infrastructure_records: list[dict[str, Any]]
    control_records: list[dict[str, Any]]
    comparable_market_records: list[dict[str, Any]]
    buyer_matches: list[dict[str, Any]]


@dataclass(frozen=True)
class SiteSearchFilters:
    """Lean search inputs for the internal review UI."""

    query: str | None = None
    authority_name: str | None = None
    workflow_status: str | None = None
    min_area_acres: float | None = None
    max_area_acres: float | None = None
    bucket_code: str | None = None
    monetisation_horizon: str | None = None
    previous_application_exists: bool | None = None
    allocation_status: str | None = None
    flood_risk: str | None = None
    access_status: str | None = None
    comparable_strength: str | None = None
    min_buyer_fit_count: int | None = None
    human_review_required: bool | None = None
    limit: int = 50

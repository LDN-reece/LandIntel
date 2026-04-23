"""Normalised Scottish site evidence model used before scoring and routing."""

from __future__ import annotations

from dataclasses import dataclass, field

from src.site_engine.types import ConfidenceLabel, EvidenceItem


@dataclass(frozen=True)
class PlanningEvidence:
    allocation_status: str = "unknown"
    settlement_position: str = "unknown"
    prior_application_count: int = 0
    latest_application_status: str = "unknown"
    latest_application_outcome: str = "unknown"
    refusal_themes: tuple[str, ...] = ()
    appeal_status: str = "unknown"
    planning_history_summary: str = "No linked planning history."


@dataclass(frozen=True)
class LdpEvidence:
    adopted_ldp_status: str = "unknown"
    emerging_ldp_status: str = "unknown"
    settlement_boundary_relation: str = "unknown"
    policy_constraints: tuple[str, ...] = ()
    policy_support_level: str = "unknown"


@dataclass(frozen=True)
class HlaEvidence:
    present_in_hla: bool = False
    effectiveness_status: str = "unknown"
    programming_horizon: str = "unknown"
    hla_constraint_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class PriorProgressionEvidence:
    progression_level: str = "none"
    has_layouts: bool = False
    has_prior_reports: bool = False
    has_si_indicators: bool = False
    has_major_prior_scheme: bool = False
    sponsor_failure_indicator: bool = False


@dataclass(frozen=True)
class BgsEvidence:
    borehole_count_site: int = 0
    borehole_count_100m: int = 0
    borehole_count_250m: int = 0
    borehole_count_500m: int = 0
    site_investigation_overlap: bool = False
    opencast_overlap: bool = False
    water_well_presence: bool = False
    aquifer_presence: bool = False
    geophysical_logs_presence: bool = False
    drillcore_presence: bool = False


@dataclass(frozen=True)
class BgsReasoningEvidence:
    investigation_intensity: str = "none"
    prior_progression_signal_strength: str = "none"
    ground_complexity_signal: str = "low"
    hydrogeology_caution: str = "low"
    extraction_legacy_caution: str = "low"


@dataclass(frozen=True)
class FloodEvidence:
    river_flood_overlap_pct: float = 0.0
    surface_water_overlap_pct: float = 0.0
    flood_combined_severity: str = "none"


@dataclass(frozen=True)
class VdlEvidence:
    on_vdl_register: bool = False
    previous_use_type: str = "unknown"
    years_on_register: int = 0


@dataclass(frozen=True)
class OwnershipEvidence:
    title_count: int = 0
    ownership_fragmentation_level: str = "unknown"
    public_ownership_indicator: bool = False
    legal_control_issue_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class InfrastructureEvidence:
    access_complexity: str = "unknown"
    drainage_burden: str = "unknown"
    wastewater_burden: str = "unknown"
    roads_burden: str = "unknown"
    education_burden: str = "unknown"
    utilities_burden: str = "unknown"


@dataclass(frozen=True)
class UtilityEvidence:
    overall_utility_burden: str = "unknown"
    water_and_wastewater_signal: str = "unknown"
    electricity_grid_signal: str = "unknown"
    broadband_connectivity_signal: str = "unknown"


@dataclass(frozen=True)
class MarketEvidence:
    settlement_strength: str = "unknown"
    buyer_profile_fit: tuple[str, ...] = ()
    buyer_depth_estimate: str = "unknown"
    comparable_strength: str = "unknown"
    strong_buyer_fit_count: int = 0
    moderate_buyer_fit_count: int = 0
    average_price_per_sqft_gbp: float | None = None


@dataclass(frozen=True)
class BoundaryPosition:
    position: str = "unknown"
    overlap_ratio: float | None = None
    distance_m: float | None = None


@dataclass(frozen=True)
class BoundaryEvidence:
    council_boundary: BoundaryPosition = field(default_factory=BoundaryPosition)
    settlement_boundary: BoundaryPosition = field(default_factory=BoundaryPosition)
    green_belt: BoundaryPosition = field(default_factory=BoundaryPosition)


@dataclass(frozen=True)
class UseClassificationEvidence:
    previous_site_use: str = "mixed / unclear"
    previous_site_use_confidence: ConfidenceLabel = "low"
    current_building_use: str = "unknown"
    current_building_use_confidence: ConfidenceLabel = "low"


@dataclass(frozen=True)
class ReconciliationEvidence:
    site_name_primary: str = "Unknown site"
    site_name_aliases: tuple[str, ...] = ()
    source_refs: tuple[str, ...] = ()
    planning_refs: tuple[str, ...] = ()
    ldp_refs: tuple[str, ...] = ()
    hla_refs: tuple[str, ...] = ()
    ela_refs: tuple[str, ...] = ()
    vdl_refs: tuple[str, ...] = ()
    council_refs: tuple[str, ...] = ()
    title_numbers: tuple[str, ...] = ()
    uprns: tuple[str, ...] = ()
    usrns: tuple[str, ...] = ()
    toids: tuple[str, ...] = ()
    authority_refs: tuple[str, ...] = ()
    geometry_versions: tuple[str, ...] = ()
    match_confidence: ConfidenceLabel = "low"
    match_notes: tuple[str, ...] = ()
    matched_reference_count: int = 0
    unresolved_reference_count: int = 0


@dataclass(frozen=True)
class SiteEvidence:
    site_id: str
    jurisdiction: str
    reconciliation: ReconciliationEvidence
    boundary: BoundaryEvidence
    planning: PlanningEvidence
    ldp: LdpEvidence
    hla: HlaEvidence
    prior_progression: PriorProgressionEvidence
    bgs: BgsEvidence
    bgs_reasoning: BgsReasoningEvidence
    flood: FloodEvidence
    vdl: VdlEvidence
    use_classification: UseClassificationEvidence
    ownership: OwnershipEvidence
    infrastructure: InfrastructureEvidence
    utility: UtilityEvidence
    market: MarketEvidence
    field_evidence: dict[str, list[EvidenceItem]] = field(default_factory=dict)

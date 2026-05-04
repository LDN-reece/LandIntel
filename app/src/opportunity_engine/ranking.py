"""Deterministic Phase One ranking for canonical-site opportunities."""

from __future__ import annotations

from typing import Any

from src.opportunity_engine.types import OpportunityAssessment, OpportunitySignal, OpportunitySnapshot


PRIMARY_SCORE_MAP = {
    "core_target": 3.0,
    "conditional_small": 2.0,
    "focus_area": 2.0,
    "extended_area": 1.0,
    "strong": 3.0,
    "workable": 2.0,
    "conditional": 2.0,
    "material": 1.0,
    "limited": 1.0,
    "unknown": 0.5,
    "below_threshold": 0.0,
    "severe": 0.0,
    "poor": 0.0,
}

SECONDARY_SCORE_MAP = {
    "strong": 2.0,
    "workable": 1.5,
    "conditional": 1.0,
    "limited": 0.5,
    "unknown": 0.0,
    "poor": 0.0,
    "below_threshold": 0.0,
}


def build_geometry_diagnostics(snapshot: OpportunitySnapshot) -> dict[str, Any]:
    """Create indicative-only geometry diagnostics without replacing parcel truth."""

    metrics = snapshot.geometry_metrics or {}
    area_acres = _as_float(metrics.get("original_area_acres"))
    component_count = int(metrics.get("component_count") or 0)
    parcel_count = int(metrics.get("parcel_count") or 0)
    bbox_width_m = _as_float(metrics.get("bbox_width_m"))
    bbox_height_m = _as_float(metrics.get("bbox_height_m"))
    compactness = _as_float(metrics.get("shape_compactness"))

    short_edge = min(value for value in (bbox_width_m, bbox_height_m) if value > 0) if bbox_width_m and bbox_height_m else 0.0
    long_edge = max(bbox_width_m, bbox_height_m)
    aspect_ratio = round(long_edge / short_edge, 3) if short_edge > 0 else 0.0

    sliver_flag = short_edge > 0 and short_edge <= 25 and area_acres >= 1.0
    fragmentation_flag = component_count > 1 or parcel_count > 3
    width_depth_warning = aspect_ratio >= 6.0 if aspect_ratio else False
    access_only_warning = sliver_flag and area_acres <= 2.0
    infrastructure_heavy_warning = False

    return {
        "original_area_acres": area_acres,
        "component_count": component_count,
        "parcel_count": parcel_count,
        "bbox_width_m": bbox_width_m,
        "bbox_height_m": bbox_height_m,
        "shape_compactness": compactness,
        "indicative_clean_area_acres": None,
        "indicative_usable_area_ratio": None,
        "sliver_flag": sliver_flag,
        "fragmentation_flag": fragmentation_flag,
        "width_depth_warning": width_depth_warning,
        "access_only_warning": access_only_warning,
        "infrastructure_heavy_warning": infrastructure_heavy_warning,
        "metadata": {
            "aspect_ratio": aspect_ratio or None,
            "measurement_basis": "canonical_site_geometry",
            "indicative_only": True,
        },
    }


def build_signals(snapshot: OpportunitySnapshot, *, target_authorities: list[str]) -> list[OpportunitySignal]:
    """Build current-state, evidence-led signals for one canonical site."""

    summary = snapshot.summary
    geometry = build_geometry_diagnostics(snapshot)
    area_acres = _as_float(summary.get("area_acres") or snapshot.canonical_site.get("area_acres"))

    size_rank = "core_target" if area_acres >= 7 else "conditional_small" if area_acres >= 4 else "below_threshold"
    size_reason = (
        f"The site measures {area_acres:.2f} acres, which sits in the 7+ acre core target band."
        if size_rank == "core_target"
        else f"The site measures {area_acres:.2f} acres, so it is smaller than the core band and needs exceptional context."
        if size_rank == "conditional_small"
        else f"The site measures {area_acres:.2f} acres, which is below the main sourcing threshold."
    )

    hla_support = len(snapshot.hla_records) > 0
    planning_history = len(snapshot.planning_records) > 0
    ldp_support = any(
        str(row.get("support_level") or "").lower() in {"supportive", "positive", "allocated", "allocation", "strong"}
        or str(row.get("allocation_status") or "").lower() in {"allocated", "emerging", "proposed"}
        for row in snapshot.ldp_records
    )
    planning_context_band = "strong" if (hla_support and (planning_history or ldp_support)) else "workable" if (planning_history or hla_support or ldp_support) else "unknown"
    planning_reason = (
        "Planning and future-context evidence overlap on the same opportunity."
        if planning_context_band == "strong"
        else "At least one planning or future-context source is linked, but the story is still incomplete."
        if planning_context_band == "workable"
        else "No linked planning, HLA, or LDP evidence is currently attached."
    )

    authority_name = str(summary.get("authority_name") or "")
    location_band = "focus_area" if authority_name and authority_name in set(target_authorities) else "extended_area" if authority_name else "unknown"
    location_reason = (
        f"{authority_name} is inside the current LDN operating geography."
        if location_band == "focus_area"
        else f"{authority_name} is outside the named focus authorities and needs stronger justification."
        if location_band == "extended_area"
        else "Location context is too thin to classify confidently yet."
    )

    constraint_band = _derive_constraint_band(snapshot)
    constraint_reason = _constraint_reason(snapshot, constraint_band)

    access_strength = _derive_access_strength(snapshot, geometry)
    geometry_quality = _derive_geometry_quality(geometry)
    title_state = _derive_title_state(snapshot)
    ownership_state, ownership_fact_label, ownership_reason = _derive_ownership_state(snapshot, title_state)
    utilities_burden = _derive_utilities_burden(snapshot)
    redevelopment_angle = _derive_redevelopment_angle(snapshot)
    stalled_site_angle = _derive_stalled_site_angle(snapshot)
    settlement_position = _derive_settlement_position(snapshot)

    return [
        OpportunitySignal(
            signal_key="size_band",
            signal_label="Size",
            signal_group="primary_rank",
            signal_status="known",
            source_family="canonical_site",
            confidence="high",
            signal_value={"rank": size_rank, "area_acres": round(area_acres, 3)},
            reasoning=size_reason,
            metadata={"indicative_only": False},
        ),
        OpportunitySignal(
            signal_key="planning_context",
            signal_label="Planning context",
            signal_group="primary_rank",
            signal_status="known" if planning_context_band != "unknown" else "unknown",
            source_family="planning",
            confidence="medium" if planning_context_band != "unknown" else "low",
            signal_value={"rank": planning_context_band, "planning_record_count": len(snapshot.planning_records), "hla_record_count": len(snapshot.hla_records), "ldp_record_count": len(snapshot.ldp_records)},
            reasoning=planning_reason,
            metadata={"planning_record_count": len(snapshot.planning_records), "hla_record_count": len(snapshot.hla_records)},
        ),
        OpportunitySignal(
            signal_key="location_band",
            signal_label="Location",
            signal_group="primary_rank",
            signal_status="known" if location_band != "unknown" else "unknown",
            source_family="location",
            confidence="medium" if location_band != "unknown" else "low",
            signal_value={"rank": location_band, "authority_name": authority_name},
            reasoning=location_reason,
        ),
        OpportunitySignal(
            signal_key="constraint_severity",
            signal_label="Constraints",
            signal_group="primary_rank",
            signal_status="known" if constraint_band != "unknown" else "unknown",
            source_family="constraints",
            confidence="medium" if constraint_band != "unknown" else "low",
            signal_value={"rank": constraint_band},
            reasoning=constraint_reason,
        ),
        OpportunitySignal(
            signal_key="access_strength",
            signal_label="Access",
            signal_group="secondary_rank",
            signal_status="known" if access_strength != "unknown" else "unknown",
            source_family="geometry",
            confidence="medium" if access_strength != "unknown" else "low",
            signal_value={"rank": access_strength},
            reasoning=_access_reason(access_strength, geometry),
        ),
        OpportunitySignal(
            signal_key="geometry_quality",
            signal_label="Geometry quality",
            signal_group="secondary_rank",
            signal_status="known",
            source_family="geometry",
            confidence="medium",
            signal_value={"rank": geometry_quality, "flags": geometry},
            reasoning=_geometry_reason(geometry_quality, geometry),
            metadata={"indicative_only": True},
        ),
        OpportunitySignal(
            signal_key="title_state",
            signal_label="Title state",
            signal_group="title",
            signal_status="known",
            source_family="title",
            confidence="high" if title_state == "title_reviewed" else "low",
            signal_value={"rank": title_state},
            reasoning="Legal ownership certainty only exists after title has been ordered and reviewed.",
            fact_label=ownership_fact_label,
        ),
        OpportunitySignal(
            signal_key="ownership_control",
            signal_label="Ownership and control",
            signal_group="secondary_rank",
            signal_status="inferred" if ownership_fact_label == "commercial_inference" else "known",
            source_family="ownership",
            confidence="high" if ownership_fact_label == "title_reviewed" else "medium" if ownership_state != "control_unclear" else "low",
            signal_value={"rank": ownership_state},
            reasoning=ownership_reason,
            fact_label=ownership_fact_label,
        ),
        OpportunitySignal(
            signal_key="utilities_burden",
            signal_label="Utilities burden",
            signal_group="secondary_rank",
            signal_status="known" if utilities_burden != "unknown" else "unknown",
            source_family="constraints",
            confidence="medium" if utilities_burden != "unknown" else "low",
            signal_value={"rank": utilities_burden},
            reasoning=_utilities_reason(utilities_burden),
        ),
        OpportunitySignal(
            signal_key="redevelopment_angle",
            signal_label="Redevelopment angle",
            signal_group="secondary_rank",
            signal_status="known" if redevelopment_angle != "unknown" else "unknown",
            source_family="future_context",
            confidence="medium" if redevelopment_angle != "unknown" else "low",
            signal_value={"rank": redevelopment_angle},
            reasoning=_redevelopment_reason(redevelopment_angle),
        ),
        OpportunitySignal(
            signal_key="stalled_site_angle",
            signal_label="Stalled-site angle",
            signal_group="secondary_rank",
            signal_status="known" if stalled_site_angle != "unknown" else "unknown",
            source_family="planning",
            confidence="medium" if stalled_site_angle != "unknown" else "low",
            signal_value={"rank": stalled_site_angle},
            reasoning=_stalled_reason(stalled_site_angle),
        ),
        OpportunitySignal(
            signal_key="settlement_position",
            signal_label="Settlement position",
            signal_group="secondary_rank",
            signal_status="known" if settlement_position != "unknown" else "unknown",
            source_family="policy",
            confidence="low",
            signal_value={"rank": settlement_position},
            reasoning=_settlement_reason(settlement_position),
        ),
    ]


def build_assessment(snapshot: OpportunitySnapshot, signals: list[OpportunitySignal], geometry: dict[str, Any]) -> OpportunityAssessment:
    """Translate current-state signals into a transparent Phase One assessment."""

    signal_map = {signal.signal_key: signal for signal in signals}
    size_rank = signal_map["size_band"].signal_value["rank"]
    planning_rank = signal_map["planning_context"].signal_value["rank"]
    location_rank = signal_map["location_band"].signal_value["rank"]
    constraints_rank = signal_map["constraint_severity"].signal_value["rank"]
    access_rank = signal_map["access_strength"].signal_value["rank"]
    geometry_rank = signal_map["geometry_quality"].signal_value["rank"]
    ownership_rank = _ownership_rank(signal_map["ownership_control"].signal_value["rank"])
    utilities_rank = signal_map["utilities_burden"].signal_value["rank"]
    redevelopment_rank = signal_map["redevelopment_angle"].signal_value["rank"]
    stalled_rank = signal_map["stalled_site_angle"].signal_value["rank"]
    settlement_position = signal_map["settlement_position"].signal_value["rank"]
    title_state = signal_map["title_state"].signal_value["rank"]
    ownership_fact_label = signal_map["ownership_control"].fact_label or "commercial_inference"

    primary_score = (
        PRIMARY_SCORE_MAP.get(size_rank, 0.0) * 1000
        + PRIMARY_SCORE_MAP.get(planning_rank, 0.0) * 100
        + PRIMARY_SCORE_MAP.get(location_rank, 0.0) * 10
        + PRIMARY_SCORE_MAP.get(constraints_rank, 0.0)
    )
    secondary_score = (
        SECONDARY_SCORE_MAP.get(access_rank, 0.0)
        + SECONDARY_SCORE_MAP.get(geometry_rank, 0.0)
        + SECONDARY_SCORE_MAP.get(ownership_rank, 0.0)
        + SECONDARY_SCORE_MAP.get(utilities_rank, 0.0)
        + SECONDARY_SCORE_MAP.get(redevelopment_rank, 0.0)
        + SECONDARY_SCORE_MAP.get(stalled_rank, 0.0)
    ) / 100.0
    overall_rank_score = round(primary_score + secondary_score, 4)

    severe_constraint = constraints_rank == "severe"
    likely_controlled = signal_map["ownership_control"].signal_value["rank"] == "likely_builder_controlled"
    below_threshold = size_rank == "below_threshold"
    poor_geometry = geometry_rank == "poor"

    if below_threshold or severe_constraint or likely_controlled:
        overall_tier = "Tier 4"
    elif size_rank == "core_target" and planning_rank in {"strong", "workable"} and location_rank in {"focus_area", "extended_area"} and constraints_rank not in {"severe"}:
        overall_tier = "Tier 1"
    elif size_rank in {"core_target", "conditional_small"} and planning_rank in {"strong", "workable"}:
        overall_tier = "Tier 2"
    else:
        overall_tier = "Tier 3"

    good_items, bad_items, ugly_items = _build_good_bad_ugly(snapshot, signal_map, geometry)
    dominant_blocker = _dominant_blocker(ugly_items, bad_items, signal_map)
    resurfaced_reason = snapshot.change_events[0]["event_summary"] if snapshot.change_events else None
    human_review_required = (
        ownership_fact_label == "commercial_inference"
        or planning_rank == "unknown"
        or constraints_rank == "unknown"
        or location_rank == "unknown"
        or bool(ugly_items)
    )

    queue_recommendation = (
        "Watchlist / Resurfaced"
        if snapshot.change_events and snapshot.change_events[0].get("resurfaced_flag")
        else "Strong Candidates"
        if overall_tier == "Tier 1" and not ugly_items
        else "Needs Review"
        if overall_tier in {"Tier 1", "Tier 2"}
        else "New Candidates"
    )

    why_it_surfaced = _why_it_surfaced(snapshot, signal_map)
    why_it_survived = _why_it_survived(good_items, bad_items, ugly_items)

    subrank_summary = {
        "size_rank": size_rank,
        "planning_context_rank": planning_rank,
        "planning_context_band": planning_rank,
        "location_rank": location_rank,
        "location_band": location_rank,
        "constraints_rank": constraints_rank,
        "constraint_severity": constraints_rank,
        "access_rank": access_rank,
        "access_strength": access_rank,
        "geometry_rank": geometry_rank,
        "geometry_quality": geometry_rank,
        "ownership_control_rank": ownership_rank,
        "ownership_control_state": signal_map["ownership_control"].signal_value["rank"],
        "utilities_burden_rank": utilities_rank,
        "redevelopment_angle_rank": redevelopment_rank,
        "stalled_site_angle_rank": stalled_rank,
        "settlement_position": settlement_position,
    }
    explanation_text = "\n".join(
        [
            why_it_surfaced,
            why_it_survived,
            f"Ownership and control remain labelled as {ownership_fact_label}.",
        ]
    )

    return OpportunityAssessment(
        overall_tier=overall_tier,
        overall_rank_score=overall_rank_score,
        queue_recommendation=queue_recommendation,
        why_it_surfaced=why_it_surfaced,
        why_it_survived=why_it_survived,
        good_items=good_items,
        bad_items=bad_items,
        ugly_items=ugly_items,
        subrank_summary=subrank_summary,
        title_state=title_state,
        ownership_control_fact_label=ownership_fact_label,
        resurfaced_reason=resurfaced_reason,
        dominant_blocker=dominant_blocker,
        human_review_required=human_review_required,
        explanation_text=explanation_text,
    )


def _derive_constraint_band(snapshot: OpportunitySnapshot) -> str:
    constraint_summary = snapshot.constraint_overview or {}
    if snapshot.constraint_group_summaries:
        max_overlap = max(_as_float(row.get("max_overlap_pct_of_site")) for row in snapshot.constraint_group_summaries)
        if max_overlap >= 25:
            return "severe"
        if max_overlap >= 10:
            return "material"
        return "conditional"
    if snapshot.flood_records:
        max_overlap = max(_as_float(row.get("overlap_pct")) for row in snapshot.flood_records)
        if max_overlap >= 25:
            return "severe"
        if max_overlap >= 10:
            return "material"
        return "conditional"
    if constraint_summary:
        return "conditional" if _as_float(constraint_summary.get("constraint_groups_measured")) > 0 else "unknown"
    return "unknown"


def _constraint_reason(snapshot: OpportunitySnapshot, band: str) -> str:
    if band == "severe":
        return "A material share of the site overlaps severe constraint evidence."
    if band == "material":
        return "Constraint evidence overlaps a meaningful part of the site and needs commercial review."
    if band == "conditional":
        return "Constraints are present, but the current evidence suggests they may still be workable."
    return "No measured constraint layer is linked yet, so the site cannot be treated as clean."


def _derive_access_strength(snapshot: OpportunitySnapshot, geometry: dict[str, Any]) -> str:
    if geometry.get("access_only_warning"):
        return "poor"
    for row in snapshot.title_links:
        role = str(row.get("link_role") or "").lower()
        object_type = str(row.get("linked_object_type") or "").lower()
        if "frontage" in role or "access" in role or "road" in object_type:
            return "workable"
    return "unknown"


def _access_reason(access_rank: str, geometry: dict[str, Any]) -> str:
    if access_rank == "poor":
        return "The geometry currently looks more like access-only land than a usable development parcel."
    if access_rank == "workable":
        return "There is at least one linked frontage or access signal, but it still needs human review."
    if geometry.get("width_depth_warning"):
        return "No explicit access signal is linked, and the geometry shape raises a frontage concern."
    return "No explicit access evidence has been linked yet."


def _derive_geometry_quality(geometry: dict[str, Any]) -> str:
    warning_count = sum(
        1
        for key in ("sliver_flag", "fragmentation_flag", "width_depth_warning", "access_only_warning", "infrastructure_heavy_warning")
        if geometry.get(key)
    )
    compactness = _as_float(geometry.get("shape_compactness"))
    if warning_count >= 3 or geometry.get("access_only_warning"):
        return "poor"
    if warning_count >= 1 or (compactness and compactness < 0.1):
        return "conditional"
    return "workable"


def _geometry_reason(geometry_rank: str, geometry: dict[str, Any]) -> str:
    if geometry_rank == "poor":
        return "The canonical site geometry throws multiple parcel-noise warnings and needs direct map review."
    if geometry_rank == "conditional":
        return "The geometry is workable enough to stay live, but it carries shape warnings."
    return "No major parcel-noise warning is currently visible from the canonical geometry."


def _derive_title_state(snapshot: OpportunitySnapshot) -> str:
    for row in snapshot.title_validations:
        if str(row.get("validation_status") or "").lower() in {"title_reviewed", "validated", "confirmed"}:
            return "title_reviewed"
    return "commercial_inference"


def _derive_ownership_state(snapshot: OpportunitySnapshot, title_state: str) -> tuple[str, str, str]:
    if title_state == "title_reviewed":
        return (
            "title_reviewed",
            "title_reviewed",
            "Ownership certainty only becomes legal fact after title review, and that review has been recorded here.",
        )
    if any(str(row.get("developer_name") or "").strip() for row in snapshot.hla_records):
        return (
            "likely_builder_controlled",
            "commercial_inference",
            "A named developer appears in HLA evidence, so the opportunity may already be controlled. This remains a commercial inference until title review.",
        )
    if snapshot.title_validations or snapshot.title_links or snapshot.canonical_site.get("primary_ros_parcel_id"):
        return (
            "control_unclear",
            "commercial_inference",
            "Parcel and title-style evidence exists, but legal ownership has not been confirmed through title review.",
        )
    return (
        "control_unclear",
        "commercial_inference",
        "No reliable ownership evidence has been confirmed yet, so control remains unclear.",
    )


def _derive_utilities_burden(snapshot: OpportunitySnapshot) -> str:
    for fact in snapshot.constraint_friction_facts:
        label = str(fact.get("fact_label") or "").lower()
        if any(keyword in label for keyword in ("utilities", "drainage", "wastewater", "sewer", "roads")):
            return "conditional"
    return "unknown"


def _utilities_reason(rank: str) -> str:
    if rank == "conditional":
        return "Utilities or infrastructure friction facts are present and need checking."
    return "No utility burden fact is linked yet, so this stays unknown."


def _derive_redevelopment_angle(snapshot: OpportunitySnapshot) -> str:
    hla_brownfield = any(bool(row.get("brownfield_indicator")) for row in snapshot.hla_records)
    ela_present = len(snapshot.ela_records) > 0
    vdl_present = len(snapshot.vdl_records) > 0
    if hla_brownfield or ela_present or vdl_present:
        return "strong"
    return "unknown"


def _redevelopment_reason(rank: str) -> str:
    if rank == "strong":
        return "The site shows a brownfield, employment, or vacant/derelict redevelopment angle."
    return "No live redevelopment angle is linked yet."


def _derive_stalled_site_angle(snapshot: OpportunitySnapshot) -> str:
    if any("delay" in str(row.get("effectiveness_status") or "").lower() or "stalled" in str(row.get("effectiveness_status") or "").lower() for row in snapshot.hla_records):
        return "strong"
    if any(str(row.get("decision") or "").lower() in {"refused", "withdrawn", "lapsed"} for row in snapshot.planning_records):
        return "workable"
    return "unknown"


def _stalled_reason(rank: str) -> str:
    if rank == "strong":
        return "Audit evidence suggests the site is delayed or stalled, which can create a reopening angle."
    if rank == "workable":
        return "Planning history shows refusal, withdrawal, or lapse, which can still be commercially interesting."
    return "No stalled-site signal is linked yet."


def _derive_settlement_position(snapshot: OpportunitySnapshot) -> str:
    if snapshot.ldp_records:
        return "policy_linked"
    if snapshot.settlement_boundary_records:
        return "needs_boundary_review"
    return "unknown"


def _settlement_reason(rank: str) -> str:
    if rank == "policy_linked":
        return "Settlement or LDP context is linked, but the exact position still needs human review."
    if rank == "needs_boundary_review":
        return "Boundary layers exist for the authority, but the site position still needs a proper overlay check."
    return "No linked settlement boundary position is available yet."


def _build_good_bad_ugly(
    snapshot: OpportunitySnapshot,
    signal_map: dict[str, OpportunitySignal],
    geometry: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    good: list[dict[str, Any]] = []
    bad: list[dict[str, Any]] = []
    ugly: list[dict[str, Any]] = []

    if signal_map["size_band"].signal_value["rank"] == "core_target":
        good.append(_item("Core target size", signal_map["size_band"].reasoning, "positive", "canonical_site"))
    elif signal_map["size_band"].signal_value["rank"] == "conditional_small":
        bad.append(_item("Below core size band", signal_map["size_band"].reasoning, "warning", "canonical_site"))
    else:
        ugly.append(_item("Below main sourcing threshold", signal_map["size_band"].reasoning, "critical", "canonical_site"))

    if signal_map["planning_context"].signal_value["rank"] in {"strong", "workable"}:
        good.append(_item("Live planning context exists", signal_map["planning_context"].reasoning, "positive", "planning"))
    else:
        bad.append(_item("Planning story still thin", signal_map["planning_context"].reasoning, "warning", "planning"))

    if signal_map["location_band"].signal_value["rank"] == "focus_area":
        good.append(_item("Inside current target geography", signal_map["location_band"].reasoning, "positive", "location"))
    elif signal_map["location_band"].signal_value["rank"] == "unknown":
        bad.append(_item("Location quality still unclear", signal_map["location_band"].reasoning, "warning", "location"))

    if signal_map["constraint_severity"].signal_value["rank"] == "severe":
        ugly.append(_item("Severe constraint overlap", signal_map["constraint_severity"].reasoning, "critical", "constraints"))
    elif signal_map["constraint_severity"].signal_value["rank"] == "material":
        bad.append(_item("Material constraint burden", signal_map["constraint_severity"].reasoning, "warning", "constraints"))
    elif signal_map["constraint_severity"].signal_value["rank"] == "unknown":
        bad.append(_item("Constraint evidence incomplete", signal_map["constraint_severity"].reasoning, "warning", "constraints"))

    if signal_map["geometry_quality"].signal_value["rank"] == "poor":
        ugly.append(_item("Parcel-noise geometry risk", signal_map["geometry_quality"].reasoning, "critical", "geometry"))
    elif signal_map["geometry_quality"].signal_value["rank"] == "conditional":
        bad.append(_item("Geometry needs map review", signal_map["geometry_quality"].reasoning, "warning", "geometry"))

    ownership_state = signal_map["ownership_control"].signal_value["rank"]
    if ownership_state == "likely_builder_controlled":
        ugly.append(_item("Likely already controlled", signal_map["ownership_control"].reasoning, "critical", "ownership"))
    elif ownership_state == "title_reviewed":
        good.append(_item("Title reviewed", signal_map["ownership_control"].reasoning, "positive", "title"))
    else:
        bad.append(_item("Ownership still inferred", signal_map["ownership_control"].reasoning, "warning", "ownership"))

    if signal_map["redevelopment_angle"].signal_value["rank"] == "strong":
        good.append(_item("Redevelopment angle present", signal_map["redevelopment_angle"].reasoning, "positive", "future_context"))
    if signal_map["stalled_site_angle"].signal_value["rank"] in {"strong", "workable"}:
        good.append(_item("Stalled-site reopening angle", signal_map["stalled_site_angle"].reasoning, "positive", "planning"))
    if signal_map["access_strength"].signal_value["rank"] == "poor":
        ugly.append(_item("Access-only character risk", signal_map["access_strength"].reasoning, "critical", "geometry"))
    elif signal_map["access_strength"].signal_value["rank"] == "unknown":
        bad.append(_item("Access still unproven", signal_map["access_strength"].reasoning, "warning", "geometry"))

    if geometry.get("indicative_clean_area_acres") is not None:
        bad.append(_item("Indicative area only", "Any adjusted area estimate is indicative only and does not replace the original parcel measurement.", "warning", "geometry"))

    return good[:4], bad[:4], ugly[:4]


def _dominant_blocker(ugly_items: list[dict[str, Any]], bad_items: list[dict[str, Any]], signal_map: dict[str, OpportunitySignal]) -> str:
    if ugly_items:
        return ugly_items[0]["headline"].lower().replace(" ", "_")
    if bad_items:
        return bad_items[0]["headline"].lower().replace(" ", "_")
    if signal_map["planning_context"].signal_value["rank"] == "unknown":
        return "missing_planning_context"
    return "none"


def _why_it_surfaced(snapshot: OpportunitySnapshot, signal_map: dict[str, OpportunitySignal]) -> str:
    if snapshot.summary.get("planning_record_count", 0):
        return "The site surfaced because live planning evidence is already linked into the canonical site spine."
    if snapshot.summary.get("hla_record_count", 0):
        return "The site surfaced because HLA evidence already points at future delivery or stalled-site context."
    if signal_map["redevelopment_angle"].signal_value["rank"] == "strong":
        return "The site surfaced because the data shows a redevelopment or brownfield angle worth review."
    return "The site surfaced because the canonical spine still shows enough evidence to justify human review."


def _why_it_survived(good_items: list[dict[str, Any]], bad_items: list[dict[str, Any]], ugly_items: list[dict[str, Any]]) -> str:
    if good_items and not ugly_items:
        return "It survived because the positives still outweigh the current warnings."
    if good_items and ugly_items:
        return "It survived because the opportunity still has an angle, but the ugly issues are explicit and must be checked."
    if bad_items and not ugly_items:
        return "It survived because the issues are material but still potentially workable."
    return "It survived only as a live watch item because the story could still change."


def _ownership_rank(state: str) -> str:
    if state == "title_reviewed":
        return "strong"
    if state == "likely_builder_controlled":
        return "poor"
    if state == "control_unclear":
        return "conditional"
    return "unknown"


def _item(headline: str, summary: str, severity: str, source_family: str) -> dict[str, Any]:
    return {
        "headline": headline,
        "summary": summary,
        "severity": severity,
        "source_family": source_family,
        "evidence_refs": [],
    }


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0

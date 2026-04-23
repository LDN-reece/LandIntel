"""Shared helpers for test discovery under the tests root."""

from __future__ import annotations

from copy import deepcopy

from src.site_engine.seed_data import SEED_SITE_SCENARIOS
from src.site_engine.types import SiteSnapshot


def snapshot_from_scenario(site_code: str) -> SiteSnapshot:
    scenario = next(item for item in SEED_SITE_SCENARIOS if item["site_code"] == site_code)
    return SiteSnapshot(
        site={
            "id": str(scenario["site_code"]),
            "site_code": str(scenario["site_code"]),
            "site_name": str(scenario["site_name"]),
            "metadata": {"site_name_aliases": [str(scenario["site_name"])]},
        },
        location={
            "source_record_id": str(scenario["site_code"]),
            "within_settlement_boundary": scenario.get("within_settlement_boundary"),
            "distance_to_settlement_boundary_m": scenario.get("distance_to_settlement_boundary_m"),
            "settlement_relationship": scenario.get("settlement_relationship"),
            "nearest_settlement": scenario.get("nearest_settlement"),
            "authority_name": scenario["preferred_authorities"][0],
        },
        parcels=[
            {
                "ros_parcel_id": f"{scenario['site_code']}-parcel",
                "parcel_reference": f"{scenario['site_code']}-parcel",
                "title_number": f"{scenario['site_code']}-TITLE",
                "is_primary": True,
                "area_acres": float(scenario["min_acres"]),
            }
        ],
        geometry_components=[{"source_record_id": f"{scenario['site_code']}-geom", "component_role": "parcel_boundary"}],
        geometry_versions=[{"version_label": f"{scenario['site_code']}-canonical", "geometry_hash": f"{scenario['site_code']}-hash"}],
        reference_aliases=[],
        reconciliation_matches=[],
        reconciliation_review_items=[],
        planning_records=deepcopy(list(scenario.get("planning_records", []))),
        planning_context_records=deepcopy(list(scenario.get("planning_context_records", []))),
        constraints=deepcopy(list(scenario.get("constraints", []))),
        infrastructure_records=deepcopy(list(scenario.get("infrastructure_records", []))),
        control_records=deepcopy(list(scenario.get("control_records", []))),
        comparable_market_records=deepcopy(list(scenario.get("comparables", []))),
        buyer_matches=deepcopy(list(scenario.get("buyer_matches", []))),
    )

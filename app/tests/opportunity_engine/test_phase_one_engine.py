"""Unit tests for the Phase One opportunity engine."""

from __future__ import annotations

import unittest

from src.opportunity_engine.brief import build_opportunity_brief
from src.opportunity_engine.ranking import build_assessment, build_geometry_diagnostics, build_signals
from src.opportunity_engine.types import OpportunitySnapshot


def _snapshot(
    *,
    developer_name: str = "",
    title_reviewed: bool = False,
    area_acres: float = 8.5,
    authority_name: str = "Glasgow City",
    change_events: list[dict[str, object]] | None = None,
) -> OpportunitySnapshot:
    canonical_site_id = "00000000-0000-0000-0000-000000000001"
    return OpportunitySnapshot(
        summary={
            "canonical_site_id": canonical_site_id,
            "site_code": "SITE-1",
            "site_name": "Test Site",
            "authority_name": authority_name,
            "area_acres": area_acres,
            "surfaced_reason": "Planning activity linked into the live site spine.",
            "planning_record_count": 1,
            "hla_record_count": 1,
            "source_families_present": ["planning", "hla"],
        },
        readiness=None,
        sources=[],
        canonical_site={
            "id": canonical_site_id,
            "site_code": "SITE-1",
            "site_name_primary": "Test Site",
            "authority_name": authority_name,
            "area_acres": area_acres,
            "primary_ros_parcel_id": None,
        },
        planning_records=[
            {
                "planning_reference": "24/00001/PPP",
                "application_status": "Closed",
                "decision": "Withdrawn",
                "proposal_text": "Residential development",
            }
        ],
        hla_records=[
            {
                "site_reference": "HLA-1",
                "effectiveness_status": "Delayed",
                "developer_name": developer_name,
                "brownfield_indicator": False,
            }
        ],
        ldp_records=[],
        settlement_boundary_records=[{"settlement_name": "Glasgow"}],
        bgs_records=[],
        flood_records=[],
        ela_records=[],
        vdl_records=[],
        site_source_links=[],
        site_reference_aliases=[],
        evidence_references=[],
        parcel_rows=[],
        title_links=[],
        title_validations=(
            [
                {
                    "validation_status": "title_reviewed",
                    "title_number": "GLA12345",
                }
            ]
            if title_reviewed
            else []
        ),
        geometry_metrics={
            "original_area_acres": area_acres,
            "component_count": 1,
            "parcel_count": 1,
            "bbox_width_m": 120.0,
            "bbox_height_m": 90.0,
            "shape_compactness": 0.68,
        },
        geometry_diagnostics=None,
        constraint_overview=None,
        constraint_group_summaries=[],
        constraint_measurements=[],
        constraint_friction_facts=[],
        review_events=[],
        manual_overrides=[],
        change_events=change_events or [],
        latest_assessment=None,
    )


class PhaseOneEngineTest(unittest.TestCase):
    """Keep the Phase One operating rules explicit."""

    def test_builder_control_stays_a_commercial_inference(self) -> None:
        snapshot = _snapshot(developer_name="Known Builder PLC")

        geometry = build_geometry_diagnostics(snapshot)
        signals = build_signals(snapshot, target_authorities=["Glasgow City"])
        assessment = build_assessment(snapshot, signals, geometry)
        signal_map = {signal.signal_key: signal for signal in signals}

        self.assertEqual(signal_map["ownership_control"].fact_label, "commercial_inference")
        self.assertEqual(signal_map["ownership_control"].signal_value["rank"], "likely_builder_controlled")
        self.assertEqual(signal_map["title_state"].signal_value["rank"], "commercial_inference")
        self.assertEqual(assessment.overall_tier, "Tier 4")
        self.assertTrue(any(item["headline"] == "Likely already controlled" for item in assessment.ugly_items))
        self.assertTrue(any(item["headline"] == "Live planning context exists" for item in assessment.good_items))

    def test_title_reviewed_turns_control_into_fact_and_keeps_geometry_indicative(self) -> None:
        snapshot = _snapshot(title_reviewed=True)

        geometry = build_geometry_diagnostics(snapshot)
        signals = build_signals(snapshot, target_authorities=["Glasgow City"])
        assessment = build_assessment(snapshot, signals, geometry)
        signal_map = {signal.signal_key: signal for signal in signals}

        self.assertEqual(signal_map["ownership_control"].fact_label, "title_reviewed")
        self.assertEqual(signal_map["ownership_control"].signal_value["rank"], "title_reviewed")
        self.assertEqual(signal_map["title_state"].signal_value["rank"], "title_reviewed")
        self.assertTrue(any(item["headline"] == "Title reviewed" for item in assessment.good_items))
        self.assertTrue(geometry["metadata"]["indicative_only"])
        self.assertIsNone(geometry["indicative_clean_area_acres"])

    def test_brief_preserves_two_layer_review_shape(self) -> None:
        snapshot = _snapshot(change_events=[{"event_summary": "Planning status changed.", "resurfaced_flag": True}])
        geometry = build_geometry_diagnostics(snapshot)
        signals = build_signals(snapshot, target_authorities=["Glasgow City"])
        assessment = build_assessment(snapshot, signals, geometry)

        detail = {
            "summary": snapshot.summary,
            "assessment": {
                "overall_tier": assessment.overall_tier,
                "overall_rank_score": assessment.overall_rank_score,
                "queue_recommendation": assessment.queue_recommendation,
                "why_it_surfaced": assessment.why_it_surfaced,
                "why_it_survived": assessment.why_it_survived,
                "good_items": assessment.good_items,
                "bad_items": assessment.bad_items,
                "ugly_items": assessment.ugly_items,
                "title_state": assessment.title_state,
                "ownership_control_fact_label": assessment.ownership_control_fact_label,
                **assessment.subrank_summary,
            },
            "title": {
                "title_state": assessment.title_state,
                "ownership_control_fact_label": assessment.ownership_control_fact_label,
            },
            "constraints": {"constraint_summary": "No linked constraint summary yet."},
            "review_state": {"review_status": "Under review", "review_queue": "Needs Review"},
            "change_log": snapshot.change_events,
            "geometry_diagnostics": geometry,
            "parcel_rows": [{"id": "parcel-1", "area_acres": 8.5, "is_primary": True}],
            "source_rows": [{"source_family": "planning", "source_dataset": "Planning", "linked_source_record_count": 1}],
            "planning_records": snapshot.planning_records,
            "hla_records": snapshot.hla_records,
            "ldp_records": [],
            "ela_records": [],
            "vdl_records": [],
            "bgs_records": [],
            "flood_records": [],
            "title_links": [],
            "title_validations": snapshot.title_validations,
            "constraint_overview": {},
            "constraint_group_summaries": [],
            "constraint_measurements": [],
            "constraint_friction_facts": [],
            "signal_rows": [],
            "review_events": [],
            "manual_overrides": [],
            "readiness": None,
        }

        brief = build_opportunity_brief(detail)

        self.assertEqual(brief["header"]["queue_name"], "Needs Review")
        self.assertEqual(brief["header"]["review_status"], "Under review")
        self.assertEqual(brief["headline_summary"]["ownership_control_fact_label"], "commercial_inference")
        self.assertTrue(brief["good_items"])
        self.assertTrue(brief["bad_items"] or brief["ugly_items"])
        self.assertIn("geometry_diagnostics", brief["due_diligence"])
        self.assertEqual(brief["due_diligence"]["parcel_rows"][0]["id"], "parcel-1")


if __name__ == "__main__":
    unittest.main()

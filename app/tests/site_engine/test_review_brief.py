"""Unit tests for analyst brief assembly."""

from __future__ import annotations

import unittest

from src.site_engine.review_brief import build_site_review_brief


class ReviewBriefTest(unittest.TestCase):
    """Ensure the frontend payload stays grouped and traceable."""

    def test_includes_assessment_scorecard_and_deduped_sources(self) -> None:
        detail = {
            "summary": {
                "site_code": "TEST-004",
                "site_name": "Seed Site",
                "workflow_status": "new",
                "authority_name": "East Lothian",
                "nearest_settlement": "Tranent",
                "settlement_relationship": "edge_of_settlement",
                "area_acres": 12.4,
                "parcel_count": 2,
                "component_count": 2,
                "primary_title_number": "AB123",
                "surfaced_reason": "Supportive growth context.",
                "current_ruleset_version": "test_v1",
            },
            "canonical_site": {
                "site_id": "site-1",
                "site_code": "TEST-004",
                "site_name_primary": "Seed Site",
                "site_name_aliases": ["Land North of Tranent"],
                "source_refs": ["TEST-004"],
                "planning_refs": ["24/00123/PPP"],
                "geometry_versions": ["TEST-004-canonical"],
                "match_confidence": "high",
                "matched_reference_count": 3,
                "unresolved_reference_count": 0,
                "match_notes": "Canonical alias table is aligned.",
            },
            "geometry_components": [{"source_record_id": "parcel-1"}],
            "geometry_versions": [{"version_label": "TEST-004-canonical", "source_dataset": "canonical_site"}],
            "reference_aliases": [{"reference_family": "planning_ref", "raw_reference_value": "24/00123/PPP", "relation_type": "planning_reference", "status": "matched"}],
            "reconciliation_matches": [{"raw_reference_value": "24/00123/PPP", "relation_type": "planning_reference", "status": "matched", "match_notes": "Matched planning ref."}],
            "reconciliation_review_items": [],
            "parcels": [],
            "planning_records": [],
            "planning_context_records": [],
            "constraints": [],
            "infrastructure_records": [],
            "control_records": [],
            "comparable_market_records": [],
            "buyer_matches": [],
            "signals": [],
            "interpretations": [],
            "assessment": {
                "id": "assessment-1",
                "bucket_code": "A",
                "bucket_label": "Clean Strategic Greenfield",
                "primary_reason": "The site is physically clean and lightly constrained.",
                "monetisation_horizon": "Long Term",
                "explanation_text": "This site is classified as Clean Strategic Greenfield because the site is clean.",
                "human_review_required": False,
                "review_flags": [],
                "hard_fail_flags": [],
                "secondary_reasons": ["Planning route is emerging."],
                "likely_buyer_profiles": ["strategic_masterplan_plc"],
            },
            "assessment_scores": [
                {
                    "id": "score-1",
                    "score_code": "P",
                    "score_label": "Planning Strength",
                    "score_value": 4,
                    "confidence_label": "high",
                    "score_summary": "Allocated policy position strengthens planning.",
                }
            ],
            "signal_evidence": {},
            "interpretation_evidence": {},
            "assessment_evidence": {
                "assessment-1": [
                    {
                        "dataset_name": "test.context",
                        "source_table": "public.planning_context_records",
                        "source_record_id": "ctx-1",
                        "source_identifier": "ALLOC-1",
                        "assertion": "Allocation record linked.",
                    }
                ]
            },
            "assessment_score_evidence": {
                "score-1": [
                    {
                        "dataset_name": "test.context",
                        "source_table": "public.planning_context_records",
                        "source_record_id": "ctx-1",
                        "source_identifier": "ALLOC-1",
                        "assertion": "Allocation record linked.",
                    },
                    {
                        "dataset_name": "test.context",
                        "source_table": "public.planning_context_records",
                        "source_record_id": "ctx-1",
                        "source_identifier": "ALLOC-1",
                        "assertion": "Allocation record linked.",
                    }
                ]
            },
        }

        brief = build_site_review_brief(detail)

        self.assertEqual(brief["assessment"]["bucket_code"], "A")
        self.assertEqual(brief["canonical_site"]["match_confidence"], "high")
        self.assertEqual(len(brief["assessment"]["scores"]), 1)
        self.assertEqual(len(brief["source_references"]), 1)


if __name__ == "__main__":
    unittest.main()

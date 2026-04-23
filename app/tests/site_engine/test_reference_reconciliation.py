"""Unit tests for the canonical-site reference bridge."""

from __future__ import annotations

import unittest

from src.site_engine.site_reference_reconciliation_engine import (
    ReferenceCandidate,
    SiteIndexEntry,
    normalise_reference_value,
    reconcile_candidate_to_site_index,
)


class ReferenceReconciliationEngineTest(unittest.TestCase):
    """Keep reference matching ordered, explicit, and deterministic."""

    def test_normalises_common_scottish_site_code_variants(self) -> None:
        self.assertEqual(normalise_reference_value("H-12"), "H12")
        self.assertEqual(normalise_reference_value("H 12"), "H12")
        self.assertEqual(normalise_reference_value("H12A"), "H12A")
        self.assertEqual(normalise_reference_value("H13-GL1"), "H13GL1")
        self.assertEqual(normalise_reference_value("VDL-07"), "VDL07")

    def test_prefers_alias_table_before_fuzzy_matching(self) -> None:
        candidate = ReferenceCandidate(
            reference_family="ldp_ref",
            raw_value="H-12",
            normalised_value=normalise_reference_value("H-12"),
            source_dataset="test.ldp",
            source_table="public.planning_context_records",
            source_record_id="ldp-1",
            site_name_hint="Land west of Main Street",
            authority_name="West Lothian",
        )
        site_index = [
            SiteIndexEntry(
                site_id="site-a",
                site_code="SC-A-001",
                site_name="East Calder North Fields",
                authority_name="West Lothian",
                nearest_settlement="East Calder",
                reference_values=("H12", "LDP-H12"),
                site_name_aliases=("Land west of Main Street",),
            ),
            SiteIndexEntry(
                site_id="site-b",
                site_code="SC-B-002",
                site_name="Some Other Site",
                authority_name="West Lothian",
                nearest_settlement="East Calder",
                site_name_aliases=("Land west of Main Street",),
            ),
        ]

        decision = reconcile_candidate_to_site_index(candidate, site_index)

        self.assertEqual(decision.site_id, "site-a")
        self.assertEqual(decision.relation_type, "alias_table")
        self.assertEqual(decision.status, "matched")

    def test_uses_planning_reference_before_documentary_similarity(self) -> None:
        candidate = ReferenceCandidate(
            reference_family="planning_ref",
            raw_value="24/00123/PPP",
            normalised_value=normalise_reference_value("24/00123/PPP"),
            source_dataset="test.planning",
            source_table="public.planning_records",
            source_record_id="plan-1",
            site_name_hint="Former Depot Blackburn Road",
            authority_name="Fife",
        )
        site_index = [
            SiteIndexEntry(
                site_id="site-c",
                site_code="SC-C-003",
                site_name="Blackburn Road Depot",
                authority_name="Fife",
                nearest_settlement="Dunfermline",
                planning_refs=("24/00123/PPP",),
                site_name_aliases=("Former Depot Blackburn Road",),
            )
        ]

        decision = reconcile_candidate_to_site_index(candidate, site_index)

        self.assertEqual(decision.site_id, "site-c")
        self.assertEqual(decision.relation_type, "planning_reference")
        self.assertEqual(decision.status, "matched")

    def test_uses_spatial_overlap_before_fuzzy_matching(self) -> None:
        candidate = ReferenceCandidate(
            reference_family="source_ref",
            raw_value="Uncoded schedule row",
            normalised_value=normalise_reference_value("Uncoded schedule row"),
            source_dataset="test.schedule",
            source_table="public.external_schedule",
            source_record_id="row-9",
            site_name_hint="Land at Main Street",
            authority_name="Fife",
        )
        site_index = [
            SiteIndexEntry(
                site_id="site-spatial",
                site_code="SC-X-001",
                site_name="Different Documentary Name",
                authority_name="Fife",
                nearest_settlement="Cupar",
                geometry_overlap_ratio=0.82,
            ),
            SiteIndexEntry(
                site_id="site-fuzzy",
                site_code="SC-X-002",
                site_name="Land at Main Street",
                authority_name="Fife",
                nearest_settlement="Cupar",
            ),
        ]

        decision = reconcile_candidate_to_site_index(candidate, site_index)

        self.assertEqual(decision.site_id, "site-spatial")
        self.assertEqual(decision.relation_type, "geometry_overlap")


if __name__ == "__main__":
    unittest.main()

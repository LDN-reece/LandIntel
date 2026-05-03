from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "063_operator_safe_sourced_site_views.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "landintel_sourced_operator_views.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class OperatorSafeSourcedViewsContractTests(unittest.TestCase):
    def test_required_views_are_created_with_create_or_replace_view(self) -> None:
        for view_name in (
            "landintel_sourced.v_sourced_sites",
            "landintel_sourced.v_sourced_site_briefs",
            "landintel_sourced.v_review_queue",
            "landintel_sourced.v_title_spend_candidates",
            "landintel_sourced.v_resurfacing_candidates",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql_or_physical_sourced_table(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+", re.IGNORECASE))
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"alter\s+table\s+", re.IGNORECASE))
        self.assertNotIn("create table", MIGRATION_LOWER)
        self.assertNotIn("sourced_sites table", MIGRATION_LOWER)
        self.assertNotIn("landintel_sourced.sourced_sites", MIGRATION_LOWER)

    def test_sourced_sites_references_canonical_site_spine_and_existing_layers(self) -> None:
        self.assertIn("from landintel.canonical_sites as site", MIGRATION_LOWER)
        for required_relation in (
            "landintel.site_prove_it_assessments",
            "landintel.site_ldn_candidate_screen",
            "landintel.site_urgent_address_title_pack",
            "landintel.site_assessments",
            "landintel.evidence_references",
            "landintel.site_signals",
            "landintel.site_change_events",
            "public.site_title_validation",
            "public.site_title_resolution_candidates",
            "public.site_ros_parcel_link_candidates",
            "landintel.title_order_workflow",
            "landintel.title_review_records",
            "landintel.ownership_control_signals",
        ):
            self.assertIn(required_relation, MIGRATION_LOWER)

    def test_operator_safe_title_and_resurfacing_rules_are_explicit(self) -> None:
        self.assertIn("public.is_scottish_title_number_candidate", MIGRATION_LOWER)
        self.assertIn("validation.validation_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("candidate.resolution_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("ownership is unconfirmed until landintel.title_review_records", MIGRATION_LOWER)
        self.assertIn("rejected or sct-like parcel references are not exposed as title numbers", MIGRATION_LOWER)
        self.assertIn("v_resurfacing_candidates", MIGRATION_LOWER)
        self.assertIn("ignore verdict is not a physical deletion", MIGRATION_LOWER)

    def test_docs_state_operator_safety_rules(self) -> None:
        for required_phrase in (
            "ownership remains unconfirmed unless `landintel.title_review_records` supports it",
            "rejected sct-like parcel references must not be treated as title numbers",
            "this pr creates views only",
            "it does not create a physical `sourced_sites` table",
            "it does not",
            "move data",
            "ingest new datasets",
            "rejected, watchlist, monitor and currently weak sites remain capable of resurfacing",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

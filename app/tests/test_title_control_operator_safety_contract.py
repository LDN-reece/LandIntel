from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "064_title_control_operator_safety_views.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "title_control_operator_safety.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class TitleControlOperatorSafetyContractTests(unittest.TestCase):
    def test_operator_safe_views_exist(self) -> None:
        for view_name in (
            "landintel_reporting.v_title_control_status",
            "landintel_reporting.v_title_candidates_operator_safe",
            "landintel_reporting.v_sites_needing_title_review",
            "landintel_reporting.v_title_spend_queue",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+", re.IGNORECASE))
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"alter\s+table\s+", re.IGNORECASE))
        self.assertNotIn("create table", MIGRATION_LOWER)

    def test_status_taxonomy_and_source_tables_are_present(self) -> None:
        for status in (
            "title_not_required_yet",
            "title_candidate_available",
            "title_order_recommended",
            "title_ordered",
            "title_reviewed_confirmed",
            "title_reviewed_issue",
            "ownership_unconfirmed",
            "control_hypothesis_only",
        ):
            self.assertIn(status, MIGRATION_LOWER)

        for relation in (
            "landintel.title_order_workflow",
            "landintel.title_review_records",
            "landintel.ownership_control_signals",
            "public.site_title_validation",
            "public.site_title_resolution_candidates",
            "public.site_ros_parcel_link_candidates",
            "landintel.canonical_sites",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

    def test_title_candidates_are_filtered_for_operator_safety(self) -> None:
        self.assertIn("public.is_scottish_title_number_candidate", MIGRATION_LOWER)
        self.assertIn("validation.validation_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("candidate.resolution_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("rejected_sct_like_audit_count", MIGRATION_LOWER)
        self.assertIn("ros parcel references are not title numbers", MIGRATION_LOWER)
        self.assertIn("sct-like rejected values remain audit-only", MIGRATION_LOWER)

    def test_control_signals_remain_hypotheses_until_title_review(self) -> None:
        self.assertIn("companies house, fca and control signals are hypotheses only until title review", MIGRATION_LOWER)
        self.assertIn("ownership remains unconfirmed because no human title review record exists", MIGRATION_LOWER)
        self.assertIn("title spend queue only. this view does not confirm ownership", MIGRATION_LOWER)

    def test_docs_capture_non_negotiable_title_rules(self) -> None:
        for required_phrase in (
            "title_review_records=0",
            "ownership remains unconfirmed without title review",
            "sct-like references are not title numbers",
            "rejected sct-like values remain audit-only",
            "companies house, fca and control signals are hypotheses until title review",
            "ros parcel reference is not a title number",
            "it does not",
            "delete rejected sct-like audit rows",
            "alter title source truth destructively",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

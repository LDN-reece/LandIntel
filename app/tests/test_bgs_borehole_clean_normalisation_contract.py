from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "065_bgs_borehole_clean_normalisation.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "bgs_borehole_clean_normalisation.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class BgsBoreholeCleanNormalisationContractTests(unittest.TestCase):
    def test_migration_creates_clean_and_reporting_views(self) -> None:
        for view_name in (
            "landintel_store.v_bgs_borehole_master_clean",
            "landintel_reporting.v_bgs_borehole_operator_index",
            "landintel_reporting.v_bgs_borehole_data_quality",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_migration_is_resilient_when_master_table_is_missing(self) -> None:
        self.assertIn("to_regclass('landintel.bgs_borehole_master') is null", MIGRATION_LOWER)
        self.assertIn("skipping bgs borehole clean normalisation views", MIGRATION_LOWER)

    def test_migration_references_existing_master_without_creating_duplicate_truth_table(self) -> None:
        self.assertIn("from landintel.bgs_borehole_master", MIGRATION_LOWER)
        self.assertNotIn("create table", MIGRATION_LOWER)
        self.assertIn("known_origin_manual_bulk_upload", MIGRATION_LOWER)
        self.assertIn("high_value_governance_incomplete", MIGRATION_LOWER)

    def test_migration_contains_operator_safe_fields_and_caveats(self) -> None:
        for required_phrase in (
            "has_valid_geometry",
            "has_log_available",
            "operator_use_status",
            "safe_use_caveat",
            "safe_for_proximity_density_and_log_availability_not_ground_condition_interpretation",
            "not final ground-condition interpretation",
            "piling",
            "grouting",
            "remediation",
            "abnormal-cost",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_migration_updates_object_ownership_registry_without_data_movement(self) -> None:
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION_LOWER)
        self.assertIn("v_bgs_borehole_master_clean", MIGRATION_LOWER)
        self.assertIn("v_bgs_borehole_operator_index", MIGRATION_LOWER)
        self.assertIn("v_bgs_borehole_data_quality", MIGRATION_LOWER)
        self.assertIn("on conflict (schema_name, object_name, object_type) do update", MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"alter\s+table\s+[^;]+\s+rename\s+", re.IGNORECASE))
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+", re.IGNORECASE))

    def test_docs_describe_safe_bgs_use_and_limits(self) -> None:
        for required_phrase in (
            "bgs single onshore borehole index",
            "known-origin manual bulk upload",
            "high_value_governance_incomplete",
            "does not re-upload",
            "does not prove ground conditions",
            "proximity",
            "borehole density",
            "log availability",
            "not safe",
            "final ground-condition interpretation",
            "piling",
            "grouting",
            "remediation",
            "abnormal-cost",
        ):
            self.assertIn(required_phrase, DOC_LOWER)

    def test_docs_define_future_enrichment_without_ocr_by_default(self) -> None:
        for required_phrase in (
            "bounded enrichment workflow",
            "canonical sites",
            "nearest borehole distance",
            "evidence_references",
            "site_signals",
            "site_ground_risk_context",
            "site_abnormal_cost_flags",
            "ocr",
            "opt-in",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

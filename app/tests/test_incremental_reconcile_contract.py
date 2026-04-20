from __future__ import annotations

import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
SQL_MIGRATION = (APP_DIR / "sql" / "040_incremental_reconcile_mvp.sql").read_text(encoding="utf-8")
WORKER = (APP_DIR / "src" / "source_reconcile_incremental.py").read_text(encoding="utf-8")
SETTINGS = (APP_DIR / "config" / "settings.py").read_text(encoding="utf-8")
WORKFLOW = (
    APP_DIR.parent / ".github" / "workflows" / "run-landintel-sources.yml"
).read_text(encoding="utf-8")


class IncrementalReconcileContractTests(unittest.TestCase):
    def test_migration_declares_required_reconcile_tables(self) -> None:
        for object_name in (
            "landintel.source_reconcile_state",
            "landintel.source_reconcile_queue",
            "landintel.canonical_site_refresh_queue",
            "landintel.canonical_site_lineage",
        ):
            self.assertIn(object_name, SQL_MIGRATION)

    def test_migration_declares_required_reconcile_views(self) -> None:
        for view_name in (
            "analytics.v_reconcile_review_queue",
            "analytics.v_reconcile_queue_health",
            "analytics.v_reconcile_drift_summary",
            "analytics.v_source_link_publish_status",
        ):
            self.assertIn(view_name, SQL_MIGRATION)

    def test_migration_locks_review_reason_codes(self) -> None:
        for reason_code in (
            "reference_conflict",
            "trusted_alias_conflict",
            "spatial_ambiguous",
            "weak_spatial_overlap",
            "geometry_missing_for_new_record",
            "new_site_below_area_floor",
            "near_existing_site_conflict",
            "reassignment_conflict",
            "possible_merge",
            "possible_split",
            "retirement_orphan_risk",
            "data_integrity_anomaly",
        ):
            self.assertIn(reason_code, SQL_MIGRATION)
            self.assertIn(reason_code, WORKER)

    def test_migration_exposes_provisional_and_blocked_labels(self) -> None:
        self.assertIn("Provisional link — analyst review required", SQL_MIGRATION)
        self.assertIn("Blocked — structural review required", SQL_MIGRATION)
        self.assertIn("Contains provisional source links", SQL_MIGRATION)

    def test_migration_wires_ingest_trigger(self) -> None:
        self.assertIn("ingest_runs_incremental_reconcile_queue_trigger", SQL_MIGRATION)
        self.assertIn("queue_planning_reconcile_from_ingest", SQL_MIGRATION)
        self.assertIn("queue_hla_reconcile_from_ingest", SQL_MIGRATION)

    def test_worker_rechecks_stale_items_before_processing(self) -> None:
        self.assertIn("def _queue_item_is_outdated", WORKER)
        self.assertIn('"superseded"', WORKER)
        self.assertIn("source_signature", WORKER)
        self.assertIn("current_source_signature", WORKER)

    def test_worker_locks_no_fuzzy_auto_match(self) -> None:
        for forbidden_snippet in ("SequenceMatcher", "difflib", "rapidfuzz", "fuzzywuzzy"):
            self.assertNotIn(forbidden_snippet, WORKER)

    def test_worker_uses_operator_only_structural_review_path(self) -> None:
        self.assertIn("STRUCTURAL_REVIEW_REASON_CODES", WORKER)
        self.assertIn("possible_merge", WORKER)
        self.assertIn("possible_split", WORKER)
        self.assertIn("Blocked — structural review required", WORKER)

    def test_settings_expose_incremental_reconcile_controls(self) -> None:
        for setting_name in (
            "planning_new_site_min_area_acres",
            "reconcile_queue_batch_limit",
            "reconcile_refresh_batch_limit",
            "reconcile_runtime_minutes",
            "reconcile_lease_seconds",
            "reconcile_refresh_lease_seconds",
            "reconcile_max_attempts",
        ):
            self.assertIn(setting_name, SETTINGS)
        self.assertIn('alias="PLANNING_NEW_SITE_MIN_AREA_ACRES"', SETTINGS)

    def test_workflow_offers_incremental_reconcile_commands(self) -> None:
        for command_name in (
            "- process-reconcile-queue",
            "- reconcile-catchup-scan",
            "- refresh-affected-sites",
            "- weekly-reconcile-maintenance",
            "- full-reconcile-canonical-sites",
        ):
            self.assertIn(command_name, WORKFLOW)

    def test_workflow_sets_incremental_reconcile_environment(self) -> None:
        for env_name in (
            "PLANNING_NEW_SITE_MIN_AREA_ACRES: \"4\"",
            "RECONCILE_QUEUE_BATCH_LIMIT: \"500\"",
            "RECONCILE_REFRESH_BATCH_LIMIT: \"250\"",
            "RECONCILE_RUNTIME_MINUTES: \"45\"",
            "RECONCILE_LEASE_SECONDS: \"1800\"",
            "RECONCILE_REFRESH_LEASE_SECONDS: \"1200\"",
            "RECONCILE_MAX_ATTEMPTS: \"3\"",
        ):
            self.assertIn(env_name, WORKFLOW)

    def test_workflow_runs_incremental_worker_and_schedule(self) -> None:
        for snippet in (
            'python -m src.source_reconcile_incremental process-reconcile-queue',
            'python -m src.source_reconcile_incremental reconcile-catchup-scan',
            'python -m src.source_reconcile_incremental refresh-affected-sites',
            'python -m src.source_reconcile_incremental weekly-reconcile-maintenance',
            'python -m py_compile src/source_phase_runner.py src/source_catalog_sync.py src/source_reconcile_incremental.py',
            '- cron: "0 * * * *"',
            '- cron: "20 2 * * *"',
            '- cron: "40 3 * * 1"',
        ):
            self.assertIn(snippet, WORKFLOW)


if __name__ == "__main__":
    unittest.main()

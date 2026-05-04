from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
RUNNER = (APP_DIR / "src" / "source_expansion_runner.py").read_text(encoding="utf-8")
RUNNER_LOWER = RUNNER.lower()
PAGED_RUNNER = (APP_DIR / "src" / "source_expansion_runner_wfs_paging.py").read_text(
    encoding="utf-8"
)
PAGED_RUNNER_LOWER = PAGED_RUNNER.lower()
PROOF_RUNNER = (APP_DIR / "src" / "constraint_scaler_proof.py").read_text(encoding="utf-8")
PROOF_RUNNER_LOWER = PROOF_RUNNER.lower()
RUN_SOURCES_WORKFLOW = (
    APP_DIR.parents[0] / ".github" / "workflows" / "run-landintel-sources.yml"
).read_text(encoding="utf-8")
RUN_SOURCES_WORKFLOW_LOWER = RUN_SOURCES_WORKFLOW.lower()
BASE_MIGRATION = (APP_DIR / "sql" / "067_constraint_coverage_scaler.sql").read_text(
    encoding="utf-8"
)
BASE_MIGRATION_LOWER = BASE_MIGRATION.lower()
MIGRATION = (
    APP_DIR / "sql" / "068_constraint_coverage_scaler_performance_fix.sql"
).read_text(encoding="utf-8")
MIGRATION_LOWER = MIGRATION.lower()
SOURCE_FAMILY_QUEUE_MIGRATION = (
    APP_DIR / "sql" / "073_constraint_source_family_queue_fix.sql"
).read_text(encoding="utf-8")
SOURCE_FAMILY_QUEUE_MIGRATION_LOWER = SOURCE_FAMILY_QUEUE_MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "constraint_coverage_scaler.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class ConstraintCoverageScalerPerformanceContractTests(unittest.TestCase):
    def test_performance_fix_replaces_only_reporting_views(self) -> None:
        for view_name in (
            "landintel_reporting.v_constraint_priority_sites",
            "landintel_reporting.v_constraint_priority_measurement_queue",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

        self.assertNotIn("create table", MIGRATION_LOWER)
        self.assertNotIn("insert into public", MIGRATION_LOWER)
        self.assertNotIn("insert into landintel", MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("drop view", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+", re.IGNORECASE))
        self.assertNotRegex(
            MIGRATION_LOWER,
            re.compile(r"alter\s+table\s+[^;]+\s+rename\s+", re.IGNORECASE),
        )

    def test_migration_does_not_run_spatial_measurement(self) -> None:
        for forbidden in (
            "st_intersects",
            "st_intersection",
            "st_dwithin",
            "st_distance",
            "refresh_constraint_measurements_for_layer_sites",
            "measure_constraint_feature",
        ):
            self.assertNotIn(forbidden, MIGRATION_LOWER)

    def test_priority_spine_uses_current_tables_not_heavy_operator_views(self) -> None:
        for relation in (
            "landintel.site_urgent_address_title_pack",
            "landintel.site_prove_it_assessments",
            "landintel.site_ldn_candidate_screen",
            "landintel.title_order_workflow",
            "landintel.canonical_sites",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

        self.assertNotIn("landintel_sourced.v_sourced_sites", MIGRATION_LOWER)
        self.assertNotIn("landintel_sourced.v_review_queue", MIGRATION_LOWER)
        self.assertNotIn("landintel_sourced.v_title_spend_candidates", MIGRATION_LOWER)

    def test_queue_is_bounded_and_priority_layer_limited(self) -> None:
        self.assertIn("limit 5000", MIGRATION_LOWER)
        self.assertIn("constraint_priority_rank <= 8", MIGRATION_LOWER)
        self.assertIn("guidance only", MIGRATION_LOWER)
        self.assertIn("does not perform measurement", MIGRATION_LOWER)

    def test_source_family_queue_fix_keeps_lower_priority_sources_visible(self) -> None:
        self.assertIn(
            "create or replace view landintel_reporting.v_constraint_priority_measurement_queue",
            SOURCE_FAMILY_QUEUE_MIGRATION_LOWER,
        )
        self.assertIn("partition by candidate_pairs.source_family", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertIn("source_family_queue_rank <= 5000", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertIn("flood backlog does not hide coal", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertIn("guidance only", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertIn("no measurement is executed by this view", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)

    def test_earlier_queue_migrations_keep_same_view_column_shape(self) -> None:
        for migration_sql in (BASE_MIGRATION_LOWER, MIGRATION_LOWER, SOURCE_FAMILY_QUEUE_MIGRATION_LOWER):
            self.assertIn("source_family_queue_rank", migration_sql)
            self.assertIn("priority_family_queue_rank", migration_sql)

    def test_source_family_queue_fix_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertNotIn("drop view", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertNotIn("truncate", SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)
        self.assertNotRegex(
            SOURCE_FAMILY_QUEUE_MIGRATION_LOWER,
            re.compile(r"delete\s+from\s+", re.IGNORECASE),
        )
        self.assertNotRegex(
            SOURCE_FAMILY_QUEUE_MIGRATION_LOWER,
            re.compile(r"alter\s+table\s+[^;]+\s+rename\s+", re.IGNORECASE),
        )

    def test_source_family_queue_fix_does_not_run_spatial_measurement(self) -> None:
        for forbidden in (
            "st_intersects",
            "st_intersection",
            "st_dwithin",
            "st_distance",
            "refresh_constraint_measurements_for_layer_sites",
            "measure_constraint_feature",
        ):
            self.assertNotIn(forbidden, SOURCE_FAMILY_QUEUE_MIGRATION_LOWER)

    def test_indexes_are_additive_only(self) -> None:
        for index_name in (
            "site_constraint_scan_state_location_layer_scope_idx",
            "site_constraint_group_summaries_location_idx",
            "site_commercial_friction_facts_location_idx",
            "site_prove_it_assessments_latest_idx",
            "site_urgent_address_title_pack_urgency_site_idx",
        ):
            self.assertIn(f"create index if not exists {index_name}", MIGRATION_LOWER)

    def test_docs_record_codex_challenge_and_reason(self) -> None:
        for required_phrase in (
            "performance follow-up",
            "codex challenge and evidence",
            "site-priority coverage took several minutes",
            "use the underlying current tables",
            "the operator sourced-site views remain valid decision surfaces",
            "no data is moved",
            "no measurement is run",
            "no truth table changes",
        ):
            self.assertIn(required_phrase, DOC_LOWER)

    def test_constraint_audit_reports_scaler_view_proof(self) -> None:
        for required_phrase in (
            "constraint_measurement_audit_stdout_proof",
            "constraint_scaler_counts",
            "estimated_without_expanding_priority_queue",
            "count_caveat",
            "constraint_scaler_site_priority",
            "constraint_scaler_queue_sample",
            "\"coverage\": result[\"coverage\"]",
            "flush=true",
            "landintel_reporting.v_constraint_coverage_by_layer",
            "landintel_reporting.v_constraint_coverage_by_site_priority",
            "landintel_reporting.v_constraint_priority_measurement_queue",
        ):
            self.assertIn(required_phrase, RUNNER_LOWER)

        self.assertNotIn(
            "select count(*)::integer from landintel_reporting.v_constraint_priority_measurement_queue",
            RUNNER_LOWER,
        )

        for required_phrase in (
            "source_expansion_runner_command_start",
            "source_expansion_runner_command_completed",
            "flush=true",
        ):
            self.assertIn(required_phrase, PAGED_RUNNER_LOWER)

    def test_workflow_prints_bounded_constraint_scaler_proof_before_audit(self) -> None:
        for required_phrase in (
            "constraint_scaler_workflow_proof",
            "landintel_reporting.v_constraint_coverage_by_layer",
            "landintel_reporting.v_constraint_coverage_by_site_priority",
            "landintel_reporting.v_constraint_measurement_backlog",
            "landintel_reporting.v_constraint_priority_measurement_queue",
            "measured_row_count",
            "commercial_friction_fact_count",
            "target_site_layer_pairs",
            "backlog_site_layer_pairs",
            "limit 20",
        ):
            self.assertIn(required_phrase, PROOF_RUNNER_LOWER)

        self.assertIn("src/constraint_scaler_proof.py", RUN_SOURCES_WORKFLOW)
        self.assertIn(
            "python -m src.constraint_scaler_proof print-constraint-scaler-proof",
            RUN_SOURCES_WORKFLOW_LOWER,
        )
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-constraint-measurements", RUN_SOURCES_WORKFLOW_LOWER)


if __name__ == "__main__":
    unittest.main()

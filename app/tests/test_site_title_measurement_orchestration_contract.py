from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[0]
MIGRATION = (ROOT / "sql" / "077_site_title_measurement_orchestration.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
PERFORMANCE_FIX = (ROOT / "sql" / "078_site_dd_orchestration_performance_fix.sql").read_text(
    encoding="utf-8"
)
PERFORMANCE_FIX_LOWER = PERFORMANCE_FIX.lower()
DOC = (ROOT / "docs" / "source_completion" / "site_title_measurement_orchestration.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()
AUDIT_RUNNER = (ROOT / "src" / "site_dd_orchestration_audit.py").read_text(encoding="utf-8").lower()
SOURCE_RUNNER = (ROOT / "src" / "source_expansion_runner.py").read_text(encoding="utf-8").lower()
WORKFLOW = (REPO / ".github" / "workflows" / "run-landintel-sources.yml").read_text(
    encoding="utf-8"
).lower()


class SiteTitleMeasurementOrchestrationContractTests(unittest.TestCase):
    def test_reporting_views_are_created(self) -> None:
        for view_name in (
            "landintel_reporting.v_site_title_traceability_matrix",
            "landintel_reporting.v_site_measurement_readiness_matrix",
            "landintel_reporting.v_site_dd_orchestration_queue",
            "landintel_reporting.v_site_dd_orchestration_summary",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_existing_title_and_constraint_truth_tables_are_used(self) -> None:
        for relation in (
            "landintel.canonical_sites",
            "public.site_ros_parcel_link_candidates",
            "public.ros_cadastral_parcels",
            "public.site_title_resolution_candidates",
            "landintel_reporting.v_title_candidates_operator_safe",
            "landintel.title_order_workflow",
            "landintel.title_review_records",
            "landintel_reporting.v_title_control_status",
            "landintel_reporting.v_constraint_priority_measurement_queue",
            "public.site_constraint_measurements",
            "public.site_constraint_measurement_scan_state",
            "public.site_commercial_friction_facts",
            "landintel.evidence_references",
            "landintel.site_signals",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql_or_duplicate_truth_table(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\bdrop\s+view\b",
            r"\btruncate\b",
            r"\bdelete\s+from\s+(landintel|public|analytics|landintel_store)\b",
            r"\balter\s+table\s+\S+\s+rename\b",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION_LOWER), pattern)

        self.assertNotIn("create table if not exists landintel_reporting.site", MIGRATION_LOWER)
        self.assertNotIn("create table if not exists landintel.site_title_traceability", MIGRATION_LOWER)
        self.assertNotIn("create table if not exists public.site_title_traceability", MIGRATION_LOWER)

    def test_no_spatial_measurement_runs_in_migration(self) -> None:
        for forbidden_pattern in (
            r"\bst_intersects\s*\(",
            r"\bst_intersection\s*\(",
            r"\bst_dwithin\s*\(",
            r"\bst_distance\s*\(",
            r"\brefresh_constraint_measurements_for_layer\s*\(",
            r"\bmeasure_constraint_feature\s*\(",
        ):
            self.assertIsNone(re.search(forbidden_pattern, MIGRATION_LOWER), forbidden_pattern)

        self.assertIn("guidance only", MIGRATION_LOWER)
        self.assertIn("does not execute measurement", MIGRATION_LOWER)

    def test_title_safety_rules_are_encoded(self) -> None:
        for required_phrase in (
            "ownership is unconfirmed unless landintel.title_review_records supports it",
            "ros parcel references, sct-like cadastral references",
            "are not title numbers or legal ownership proof",
            "safe_title_candidate_available",
            "parcel_linked_needs_licensed_title_bridge",
            "needs_ros_parcel_linking",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_orchestration_commands_are_bounded_and_existing(self) -> None:
        for required_phrase in (
            "link-sites-to-ros-parcels",
            "resolve-title-numbers",
            "constraint-measurement-proof-title-spend-source-family",
            "measure-constraints-duckdb",
            "refresh-title-readiness",
            "audit-site-dd-orchestration",
            "constraint_measure_source_family=",
            "do not run broad all-site/all-layer scans",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_docs_capture_operating_pattern(self) -> None:
        for required_phrase in (
            "single operating spine",
            "every canonical site can be traced",
            "ownership remains unconfirmed unless",
            "ros parcel references are not title numbers",
            "sct-like cadastral references are not title numbers",
            "constraint outputs remain measured facts",
            "does not create a second constraint measurement system",
            "no broad all-site/all-layer scans",
            "site-title-traceability-proof",
            "preserves existing rows",
            "link-sites-to-ros-parcels",
            "resolve-title-numbers",
            "measure-constraints-duckdb",
            "audit-site-dd-orchestration",
        ):
            self.assertIn(required_phrase, DOC_LOWER)

    def test_read_only_audit_command_is_wired_to_actions(self) -> None:
        self.assertIn("site_dd_orchestration_workflow_proof", AUDIT_RUNNER)
        self.assertIn("landintel_reporting.v_site_dd_orchestration_queue", AUDIT_RUNNER)
        self.assertIn("constraint_source_family_direct_counts", AUDIT_RUNNER)
        self.assertIn("public.constraint_layer_registry", AUDIT_RUNNER)
        self.assertIn("set_config('statement_timeout', '30s', false)", AUDIT_RUNNER)
        self.assertIn('"audit-site-dd-orchestration"', AUDIT_RUNNER)
        self.assertIn("- audit-site-dd-orchestration", WORKFLOW)
        self.assertIn("python -m src.site_dd_orchestration_audit audit-site-dd-orchestration", WORKFLOW)
        self.assertIn("src/site_dd_orchestration_audit.py", WORKFLOW)

    def test_bounded_title_traceability_proof_is_wired_and_non_destructive(self) -> None:
        self.assertIn("def site_title_traceability_proof", SOURCE_RUNNER)
        self.assertIn("site_title_traceability_proof_site_batch_size", SOURCE_RUNNER)
        self.assertIn("site_title_traceability_proof_priority_band", SOURCE_RUNNER)
        self.assertIn("requested_priority_band == \"auto\"", SOURCE_RUNNER)
        self.assertIn("review_queue", SOURCE_RUNNER)
        self.assertIn("wider_canonical_sites", SOURCE_RUNNER)
        self.assertIn("public.refresh_site_ros_parcel_link_candidates_for_sites", SOURCE_RUNNER)
        self.assertIn("public.refresh_site_title_resolution_bridge_for_sites", SOURCE_RUNNER)
        self.assertIn('"site-title-traceability-proof"', SOURCE_RUNNER)
        self.assertIn("- site-title-traceability-proof", WORKFLOW)
        self.assertIn("site_title_traceability_proof_site_batch_size: \"10\"", WORKFLOW)
        self.assertIn("site_title_traceability_proof_priority_band: \"auto\"", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging site-title-traceability-proof", WORKFLOW)

        method_match = re.search(
            r"def site_title_traceability_proof\(self\).*?\n    def link_sites_to_ros_parcels",
            SOURCE_RUNNER,
            flags=re.S,
        )
        self.assertIsNotNone(method_match)
        method_body = method_match.group(0)
        self.assertNotIn("delete from public.site_ros_parcel_link_candidates", method_body)
        self.assertNotIn("delete from public.site_title_resolution_candidates", method_body)
        self.assertNotIn("delete from public.site_title_validation", method_body)
        self.assertIn("ownership remains unconfirmed unless landintel.title_review_records supports it", method_body)

    def test_performance_fix_keeps_views_readable_without_heavy_title_status_join(self) -> None:
        self.assertIn("create or replace view landintel_reporting.v_site_title_traceability_matrix", PERFORMANCE_FIX_LOWER)
        self.assertIn("create or replace view landintel_reporting.v_site_dd_orchestration_queue", PERFORMANCE_FIX_LOWER)
        self.assertNotIn("landintel_reporting.v_title_control_status", PERFORMANCE_FIX_LOWER)
        self.assertNotIn("row_number() over", PERFORMANCE_FIX_LOWER)
        self.assertIn("performance-fixed", PERFORMANCE_FIX_LOWER)
        self.assertIn("deterministic guidance only", PERFORMANCE_FIX_LOWER)

    def test_audit_runner_avoids_full_all_site_summary_queries(self) -> None:
        self.assertNotIn("from landintel_reporting.v_site_dd_orchestration_summary", AUDIT_RUNNER)
        self.assertNotIn("from landintel_reporting.v_site_measurement_readiness_matrix", AUDIT_RUNNER)
        self.assertNotIn("from landintel_reporting.v_site_dd_orchestration_queue", AUDIT_RUNNER)
        self.assertNotIn("from landintel_reporting.v_constraint_priority_measurement_queue", AUDIT_RUNNER)
        self.assertIn("direct_title_traceability_counts", AUDIT_RUNNER)
        self.assertIn("direct_measurement_counts", AUDIT_RUNNER)


if __name__ == "__main__":
    unittest.main()

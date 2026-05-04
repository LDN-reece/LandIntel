import csv
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "sql" / "069_source_completion_matrix.sql").read_text(encoding="utf-8").lower()
DOC = (ROOT / "docs" / "source_completion" / "landintel_source_completion_matrix.md").read_text(encoding="utf-8").lower()
CSV_PATH = ROOT / "docs" / "source_completion" / "landintel_source_completion_matrix.csv"
CSV_TEXT = CSV_PATH.read_text(encoding="utf-8")
AUDIT_RUNNER = (ROOT / "src" / "source_completion_audit.py").read_text(encoding="utf-8").lower()
WORKFLOW = (ROOT.parents[0] / ".github" / "workflows" / "run-landintel-sources.yml").read_text(
    encoding="utf-8"
).lower()


class SourceCompletionMatrixContractTests(unittest.TestCase):
    def test_migration_creates_reporting_view_without_new_truth_table(self) -> None:
        self.assertIn("create or replace view landintel_reporting.v_source_completion_matrix", MIGRATION)
        self.assertIn("landintel.source_estate_registry", MIGRATION)
        self.assertIn("landintel.source_catalog", MIGRATION)
        self.assertIn("landintel.source_endpoint_catalog", MIGRATION)
        self.assertIn("public.source_registry", MIGRATION)
        self.assertIn("analytics.v_landintel_source_estate_matrix", MIGRATION)
        self.assertIn("analytics.v_phase_one_source_estate_matrix", MIGRATION)
        self.assertNotIn("create table if not exists landintel_reporting.source_completion", MIGRATION)

    def test_status_taxonomy_is_encoded(self) -> None:
        for required_status in (
            "live_complete",
            "live_partial",
            "registered_only",
            "discovery_only",
            "manual_only",
            "blocked",
            "retired_or_replaced",
        ):
            self.assertIn(required_status, MIGRATION)

    def test_matrix_exposes_required_operator_fields(self) -> None:
        for required_field in (
            "source_key",
            "source_name",
            "source_family",
            "source_category",
            "jurisdiction",
            "current_status",
            "target_status",
            "current_table_or_view",
            "target_table_or_view",
            "workflow_command",
            "github_actions_command_available",
            "source_discovery_method",
            "ingestion_method",
            "storage_method",
            "enrichment_method",
            "site_linking_method",
            "evidence_method",
            "signal_method",
            "freshness_method",
            "audit_view_or_audit_command",
            "tests_present",
            "known_blocker",
            "next_action",
            "priority",
            "owner_layer",
        ):
            self.assertIn(required_field, MIGRATION)

    def test_workflow_gap_logic_is_bounded_and_auditable(self) -> None:
        for required_phrase in (
            "workflow_commands(command_name",
            "run-landintel-sources.yml",
            "run-landintel-open-data-completion.yml",
            "audit-source-completion-matrix",
            "broad_run_risk",
            "no_bounded_github_actions_command_mapped",
            "workflow_exists_but_must_be_bounded_by_inputs",
        ):
            self.assertIn(required_phrase, MIGRATION)

    def test_migration_contains_no_destructive_sql(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bdelete\s+from\s+(landintel|public|analytics|landintel_store)\b",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION), pattern)

    def test_docs_explain_no_ingestion_and_live_vs_static_matrix(self) -> None:
        for required_phrase in (
            "does not ingest data",
            "does not replace",
            "landintel_reporting.v_source_completion_matrix",
            "live_complete",
            "live_partial",
            "registered_only",
            "blocked",
            "the csv is a repo audit companion",
            "the live view is the actual database truth surface",
            "audit-source-completion-matrix",
        ):
            self.assertIn(required_phrase, DOC)

    def test_read_only_audit_command_is_available_in_actions(self) -> None:
        self.assertIn("source_completion_matrix_workflow_proof", AUDIT_RUNNER)
        self.assertIn("landintel_reporting.v_source_completion_matrix", AUDIT_RUNNER)
        self.assertIn('"audit-source-completion-matrix"', AUDIT_RUNNER)
        self.assertIn("- audit-source-completion-matrix", WORKFLOW)
        self.assertIn("python -m src.source_completion_audit audit-source-completion-matrix", WORKFLOW)
        self.assertIn("src/source_completion_audit.py", WORKFLOW)

    def test_constraint_source_family_completion_command_is_documented(self) -> None:
        self.assertIn("constraint-measurement-proof-title-spend-source-family", MIGRATION)
        self.assertIn("constraint-measurement-proof-title-spend-source-family", DOC)
        self.assertIn("constraint_measure_source_family=coal_authority", DOC)
        self.assertIn("queue now caps candidate pairs per source family", DOC)
        self.assertIn("flood backlog from hiding", DOC)
        self.assertIn("constraint-measurement-proof-title-spend-source-family", CSV_TEXT)
        self.assertIn("constraint_measure_source_family=coal_authority", CSV_TEXT)
        self.assertIn("constraint_measure_source_family=greenbelt", CSV_TEXT)

    def test_csv_contains_required_sources_and_columns(self) -> None:
        rows = list(csv.DictReader(CSV_TEXT.splitlines()))
        self.assertGreaterEqual(len(rows), 60)
        fieldnames = set(rows[0])
        for required_field in (
            "source_key",
            "source_name",
            "source_family",
            "current_status",
            "target_status",
            "workflow_command",
            "github_actions_command_available",
            "known_blocker",
            "next_action",
            "priority",
            "owner_layer",
        ):
            self.assertIn(required_field, fieldnames)

        source_keys = {row["source_key"] for row in rows}
        for required_source in (
            "planning_applications_spatialhub",
            "title_number_control_spine",
            "sepa_flood_maps",
            "dpea_planning_appeals",
            "companies_house_control_context",
            "statistics_gov_scot_demographics",
            "council_planning_documents",
        ):
            self.assertIn(required_source, source_keys)


if __name__ == "__main__":
    unittest.main()

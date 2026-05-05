import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "sql" / "076_register_context_cleanse_merge.sql").read_text(encoding="utf-8").lower()
DOC = (ROOT / "docs" / "source_completion" / "register_context_cleanse_merge.md").read_text(
    encoding="utf-8"
).lower()
AUDIT_RUNNER = (ROOT / "src" / "register_context_audit.py").read_text(encoding="utf-8").lower()
SOURCE_RUNNER = (ROOT / "src" / "source_phase_runner.py").read_text(encoding="utf-8").lower()
DD_WORKFLOW = (ROOT.parent / ".github" / "workflows" / "run-landintel-dd-source-data-load.yml").read_text(
    encoding="utf-8"
).lower()
SOURCES_WORKFLOW = (ROOT.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text(
    encoding="utf-8"
).lower()


class RegisterContextCleanseMergeContractTests(unittest.TestCase):
    def test_migration_creates_clean_register_surfaces(self) -> None:
        for required_view in (
            "create or replace view landintel_store.v_register_context_records_clean",
            "create or replace view landintel_store.v_register_context_records_current",
            "create or replace view landintel_reporting.v_register_context_merge_status",
            "create or replace view landintel_reporting.v_register_context_duplicate_diagnostics",
            "create or replace view landintel_reporting.v_register_context_source_completion_overlay",
            "create or replace view landintel_reporting.v_register_context_freshness",
            "create or replace view landintel_sourced.v_site_register_context",
        ):
            self.assertIn(required_view, MIGRATION)

    def test_migration_reads_existing_register_tables_without_new_truth_table(self) -> None:
        for required_table in (
            "landintel.hla_site_records",
            "landintel.ela_site_records",
            "landintel.vdl_site_records",
            "landintel.ldp_site_records",
            "landintel.settlement_boundary_records",
        ):
            self.assertIn(required_table, MIGRATION)

        self.assertNotIn("create table if not exists landintel_store.register", MIGRATION)
        self.assertNotIn("create table if not exists landintel_sourced.register", MIGRATION)

    def test_register_evidence_is_caveated_and_corroboration_led(self) -> None:
        for required_phrase in (
            "corroboration_required",
            "commercial_weight",
            "does not prove availability",
            "does not prove availability, deliverability, clean ownership, buyer depth or commercial viability",
            "register presence does not prove availability",
            "independent corroboration is required",
        ):
            self.assertIn(required_phrase, MIGRATION)

    def test_migration_contains_no_destructive_sql(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bdelete\s+from\b",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION), pattern)

    def test_docs_explain_loaded_registers_and_no_duplicate_uploads(self) -> None:
        for required_phrase in (
            "does not move data",
            "hla_site_records is already populated",
            "vdl_site_records is already populated",
            "invalid source geometry during linking",
            "drive register files are governance and refresh inputs",
            "not duplicate truth",
            "use these for operator and sourcing views instead of reading",
            "audit-register-context",
            "registers tell landintel where to look",
        ):
            self.assertIn(required_phrase, DOC)

    def test_dd_source_workflow_routes_hla_to_core_runner_and_audits_register_context(self) -> None:
        self.assertIn("core_phase()", DD_WORKFLOW)
        self.assertIn('core_phase "ingest hla source data" ingest-hla', DD_WORKFLOW)
        self.assertNotIn('source_phase "ingest hla source data" ingest-hla', DD_WORKFLOW)
        self.assertIn("audit-register-context", DD_WORKFLOW)
        self.assertIn("src/register_context_audit.py", DD_WORKFLOW)

    def test_sources_workflow_exposes_register_context_audit(self) -> None:
        self.assertIn("- audit-register-context", SOURCES_WORKFLOW)
        self.assertIn("python -m src.register_context_audit audit-register-context", SOURCES_WORKFLOW)
        self.assertIn("src/register_context_audit.py", SOURCES_WORKFLOW)

    def test_audit_runner_is_read_only_and_targets_register_views(self) -> None:
        self.assertIn("audit-register-context", AUDIT_RUNNER)
        for required_view in (
            "v_register_context_merge_status",
            "v_register_context_source_completion_overlay",
            "v_register_context_freshness",
            "v_register_context_duplicate_diagnostics",
        ):
            self.assertIn(required_view, AUDIT_RUNNER)
        self.assertNotIn("insert into", AUDIT_RUNNER)
        self.assertNotIn("update ", AUDIT_RUNNER)
        self.assertNotIn("delete from", AUDIT_RUNNER)

    def test_register_linking_repairs_invalid_geometry_without_broad_rewrite(self) -> None:
        self.assertIn("def _repair_polygonal_geometry", SOURCE_RUNNER)
        self.assertIn("geometry.buffer(0)", SOURCE_RUNNER)
        self.assertIn("geometry = _polygonize_geometry(geometry)", SOURCE_RUNNER)
        self.assertIn("candidate_polygon = _polygonize_geometry(candidate_geometry)", SOURCE_RUNNER)


if __name__ == "__main__":
    unittest.main()

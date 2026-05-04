import re
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
MIGRATION = (APP_DIR / "sql" / "072_bgs_borehole_scan_assets.sql").read_text(encoding="utf-8").lower()
RUNNER = (APP_DIR / "src" / "bgs_borehole_scan_asset_runner.py").read_text(encoding="utf-8").lower()
WORKFLOW = (REPO_ROOT / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8").lower()
DOC = (APP_DIR / "docs" / "schema" / "bgs_borehole_scan_assets.md").read_text(encoding="utf-8").lower()


class BgsBoreholeScanAssetsContractTests(unittest.TestCase):
    def test_migration_creates_asset_manifest_table_and_view(self) -> None:
        self.assertIn("create table if not exists landintel_store.bgs_borehole_scan_assets", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_bgs_scan_assets", MIGRATION)
        self.assertIn("references landintel_store.bgs_borehole_scan_fetch_queue", MIGRATION)
        self.assertIn("references landintel_store.bgs_borehole_scan_registry", MIGRATION)
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION)

    def test_migration_stores_manifest_not_pdf_blobs(self) -> None:
        for required_column in (
            "source_url",
            "asset_status",
            "linked_not_downloaded",
            "storage_bucket",
            "storage_path",
            "source_sha256",
            "safe_use_caveat",
        ):
            self.assertIn(required_column, MIGRATION)
        for forbidden in ("bytea", "raw_pdf bytea", "ocr_text"):
            self.assertNotIn(forbidden, MIGRATION)

    def test_runner_is_bounded_queue_based_and_downloads_disabled_by_default(self) -> None:
        self.assertIn("fetch-bgs-borehole-scans", RUNNER)
        self.assertIn("audit-bgs-borehole-scan-assets", RUNNER)
        self.assertIn("landintel_store.bgs_borehole_scan_fetch_queue", RUNNER)
        self.assertIn("_env_int(\"bgs_scan_asset_max_per_run", RUNNER)
        self.assertIn("_env_bool(\"bgs_scan_fetch_enable_downloads\", false)", RUNNER)
        self.assertIn("limit :max_assets", RUNNER)
        self.assertIn("linked_not_downloaded", RUNNER)
        self.assertIn("storage_not_configured", RUNNER)

    def test_runner_does_not_create_ground_evidence_or_store_blobs(self) -> None:
        self.assertNotIn("landintel.evidence_references", RUNNER)
        self.assertNotIn("landintel.site_signals", RUNNER)
        self.assertNotIn("bytea", RUNNER)
        self.assertNotIn("ocr_text", RUNNER)
        self.assertIn('"pdf_blob_in_postgres": false', RUNNER)

    def test_workflow_exposes_bounded_commands_without_new_inputs(self) -> None:
        self.assertIn("- fetch-bgs-borehole-scans", WORKFLOW)
        self.assertIn("- audit-bgs-borehole-scan-assets", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_scan_asset_runner fetch-bgs-borehole-scans", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_scan_asset_runner audit-bgs-borehole-scan-assets", WORKFLOW)
        self.assertIn("src/bgs_borehole_scan_asset_runner.py", WORKFLOW)
        self.assertIn('bgs_scan_asset_max_per_run: "5"', WORKFLOW)
        self.assertIn('bgs_scan_fetch_enable_downloads: "false"', WORKFLOW)

        workflow_text = (REPO_ROOT / ".github" / "workflows" / "run-landintel-sources.yml").read_text(
            encoding="utf-8"
        )
        in_inputs = False
        input_count = 0
        for line in workflow_text.splitlines():
            if line.startswith("    inputs:"):
                in_inputs = True
                continue
            if in_inputs and line and not line.startswith("      "):
                break
            if in_inputs and line.startswith("      ") and not line.startswith("        ") and line.strip().endswith(":"):
                input_count += 1
        self.assertLessEqual(input_count, 25)

    def test_source_completion_matrix_lists_asset_commands(self) -> None:
        matrix = (APP_DIR / "sql" / "069_source_completion_matrix.sql").read_text(encoding="utf-8").lower()
        self.assertIn("fetch-bgs-borehole-scans", matrix)
        self.assertIn("audit-bgs-borehole-scan-assets", matrix)

    def test_docs_lock_scope(self) -> None:
        for required_phrase in (
            "pdf blobs in postgres",
            "ocr output",
            "linked_not_downloaded",
            "candidate-site first",
            "max-assets-per-run capped",
            "final ground-condition interpretation",
            "abnormal-cost conclusions",
        ):
            self.assertIn(required_phrase, DOC)

    def test_no_destructive_sql(self) -> None:
        forbidden_migration_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bdelete\s+from\s+(landintel_store|public|landintel)\b",
        )
        for pattern in forbidden_migration_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION), pattern)


if __name__ == "__main__":
    unittest.main()

import re
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
MIGRATION = (APP_DIR / "sql" / "071_bgs_borehole_scan_registry_queue.sql").read_text(encoding="utf-8").lower()
RUNNER = (APP_DIR / "src" / "bgs_borehole_scan_queue_runner.py").read_text(encoding="utf-8").lower()
WORKFLOW = (REPO_ROOT / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8").lower()
DOC = (APP_DIR / "docs" / "schema" / "bgs_borehole_scan_registry_queue.md").read_text(encoding="utf-8").lower()


class BgsBoreholeScanRegistryQueueContractTests(unittest.TestCase):
    def test_migration_creates_registry_queue_and_operator_views(self) -> None:
        self.assertIn("create table if not exists landintel_store.bgs_borehole_scan_registry", MIGRATION)
        self.assertIn("create table if not exists landintel_store.bgs_borehole_scan_fetch_queue", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_bgs_scan_registry", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_bgs_scan_queue", MIGRATION)
        self.assertIn("references landintel.canonical_sites", MIGRATION)
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION)

    def test_migration_keeps_source_links_not_assets(self) -> None:
        for required_column in (
            "ags_log_url",
            "linked_not_downloaded",
            "manual_pre_si_log_review",
            "registry_status",
            "fetch_status",
            "safe_use_caveat",
        ):
            self.assertIn(required_column, MIGRATION)

        for forbidden in ("pdf_blob", "bytea", "ocr_text", "scan_asset_payload"):
            self.assertNotIn(forbidden, MIGRATION)

    def test_runner_is_bounded_and_candidate_site_first(self) -> None:
        self.assertIn("refresh-bgs-borehole-scan-registry", RUNNER)
        self.assertIn("queue-bgs-borehole-scans", RUNNER)
        self.assertIn("audit-bgs-borehole-scan-queue", RUNNER)
        self.assertIn("landintel_store.site_bgs_borehole_context", RUNNER)
        self.assertIn("context.log_available_within_1km > 0", RUNNER)
        self.assertIn("_env_int(\"bgs_scan_queue_site_batch_size", RUNNER)
        self.assertIn("_env_int(\"bgs_scan_queue_max_per_site", RUNNER)
        self.assertIn("_env_int(\"bgs_scan_queue_max_rows", RUNNER)
        self.assertIn("limit :site_batch_size", RUNNER)
        self.assertIn("limit :max_per_site", RUNNER)
        self.assertIn("limit :max_queue_rows", RUNNER)

    def test_runner_does_not_create_ground_evidence_or_fetch_assets(self) -> None:
        self.assertNotIn("landintel.evidence_references", RUNNER)
        self.assertNotIn("landintel.site_signals", RUNNER)
        self.assertIn("'download_assets', false", RUNNER)
        self.assertIn("'ocr', false", RUNNER)
        for forbidden in ("fetch-bgs-borehole-scans", "download_and_store_scan_asset"):
            self.assertNotIn(forbidden, RUNNER)

    def test_workflow_exposes_commands_without_new_inputs(self) -> None:
        self.assertIn("- refresh-bgs-borehole-scan-registry", WORKFLOW)
        self.assertIn("- queue-bgs-borehole-scans", WORKFLOW)
        self.assertIn("- audit-bgs-borehole-scan-queue", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_scan_queue_runner refresh-bgs-borehole-scan-registry", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_scan_queue_runner queue-bgs-borehole-scans", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_scan_queue_runner audit-bgs-borehole-scan-queue", WORKFLOW)
        self.assertIn("src/bgs_borehole_scan_queue_runner.py", WORKFLOW)
        self.assertIn('bgs_scan_queue_site_batch_size: "10"', WORKFLOW)
        self.assertIn('bgs_scan_queue_max_rows: "25"', WORKFLOW)

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

    def test_source_completion_matrix_lists_g2_commands(self) -> None:
        matrix = (APP_DIR / "sql" / "069_source_completion_matrix.sql").read_text(encoding="utf-8").lower()
        self.assertIn("refresh-bgs-borehole-scan-registry", matrix)
        self.assertIn("queue-bgs-borehole-scans", matrix)
        self.assertIn("audit-bgs-borehole-scan-queue", matrix)

    def test_docs_lock_scope(self) -> None:
        for required_phrase in (
            "does not fetch scans",
            "run ocr",
            "download pdfs",
            "store pdf blobs in postgres",
            "re-upload bgs data",
            "candidate-site first",
            "linked-not-downloaded",
            "final ground-condition interpretation",
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

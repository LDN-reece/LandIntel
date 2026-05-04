import re
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "sql" / "074_drive_source_file_registry.sql").read_text(encoding="utf-8").lower()
MANIFEST = yaml.safe_load((ROOT / "config" / "scotland_drive_source_manifest.yaml").read_text(encoding="utf-8"))
RUNNER = (ROOT / "src" / "drive_source_sync.py").read_text(encoding="utf-8").lower()
SETTINGS = (ROOT / "config" / "settings.py").read_text(encoding="utf-8").lower()
WORKFLOW = (ROOT.parent / ".github" / "workflows" / "run-landintel-drive-source-sync.yml").read_text(
    encoding="utf-8"
).lower()
DOC = (ROOT / "docs" / "source_completion" / "drive_source_file_sync.md").read_text(encoding="utf-8").lower()


class DriveSourceFileSyncContractTests(unittest.TestCase):
    def test_migration_creates_metadata_registry_and_reporting_views(self) -> None:
        self.assertIn("create table if not exists landintel_store.drive_source_file_registry", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_drive_source_ready_upload_files", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_drive_source_sync_status", MIGRATION)
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION)
        self.assertIn("metadata-only registry", MIGRATION)
        self.assertIn("operator_priority", MIGRATION)
        self.assertIn("immediate_add_flag", MIGRATION)
        self.assertIn("source_completion_next_action", MIGRATION)

    def test_migration_contains_no_destructive_sql(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\s*\S+\s+rename\b",
            r"\bdelete\s+from\s+(landintel|public|analytics|landintel_store)\b",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION), pattern)

    def test_manifest_covers_scotland_drive_folder_and_ready_files(self) -> None:
        self.assertEqual(MANIFEST["root"]["folder_id"], "1aXGeNM6AeqJ6IDH-jaVrZkhmjczYVTHt")
        folders = {folder["folder_path"]: folder for folder in MANIFEST["folders"]}
        for required_folder in (
            "Settlement Boundaries",
            "Mining",
            "Ground Condition - Borehole Data",
            "Vacant and Derelict Land",
            "Council Cadastral Data",
            "LDP's",
            "Housing Land Audits",
        ):
            self.assertIn(required_folder, folders)

        files = [file for folder in MANIFEST["folders"] for file in folder.get("files", [])]
        ready_files = [file for file in files if file.get("ready_to_upload")]
        self.assertGreaterEqual(len(ready_files), 35)
        for required_file in (
            "Green_Belt_-_Scotland.zip",
            "Contaminated_Land_-_Scotland.zip",
            "Culverts_-_Scotland.zip",
            "Tree_Preservation_Orders_-_Scotland.zip",
            "Housing_Land_Supply_-_Scotland.zip",
            "Employment_Land_Supply_-_Scotland.zip",
            "Vacant_and_Derelict_Land_-_Scotland.zip",
            "Council_Asset_Register_-_Scotland.zip",
            "GLA.zip",
            "WLN.zip",
        ):
            self.assertIn(required_file, {file["file_name"] for file in ready_files})

    def test_manifest_marks_user_requested_eight_as_immediate_priority(self) -> None:
        files = [file for folder in MANIFEST["folders"] for file in folder.get("files", [])]
        immediate = {file["file_name"]: file for file in files if file.get("immediate_add")}

        expected = {
            "Green_Belt_-_Scotland.zip": 1,
            "Contaminated_Land_-_Scotland.zip": 2,
            "Culverts_-_Scotland.zip": 3,
            "Tree_Preservation_Orders_-_Scotland.zip": 4,
            "Conservation_Areas_-_Scotland.zip": 5,
            "Council_Asset_Register_-_Scotland.zip": 6,
            "Local_Landscape_Areas_-_Scotland.zip": 7,
            "School_Catchments_-_Scotland.zip": 8,
        }

        self.assertEqual(set(immediate), set(expected))
        for file_name, rank in expected.items():
            self.assertEqual(immediate[file_name]["operator_priority"], "immediate")
            self.assertEqual(immediate[file_name]["priority_rank"], rank)
            self.assertIn("source_completion_next_action", immediate[file_name])

    def test_manifest_keeps_bgs_paused_and_loose_shapefiles_not_ready(self) -> None:
        files = [file for folder in MANIFEST["folders"] for file in folder.get("files", [])]
        by_name = {file["file_name"]: file for file in files}
        self.assertEqual(by_name["single-onshore-borehole-index-dataset-26-01-26.zip"]["asset_role"], "known_origin_manual_bulk_upload")
        self.assertFalse(by_name["single-onshore-borehole-index-dataset-26-01-26.zip"]["ready_to_upload"])
        self.assertFalse(by_name["WLN_bng.shp"]["ready_to_upload"])
        self.assertEqual(by_name["WLN_bng.shp"]["asset_role"], "loose_shapefile_component")
        self.assertEqual(by_name["BritishGeologicalSurvey.github.io-master.zip"]["asset_role"], "misfiled_review")

    def test_runner_is_metadata_first_and_downloads_are_opt_in(self) -> None:
        self.assertIn("audit-drive-source-manifest", RUNNER)
        self.assertIn("sync-drive-source-manifest", RUNNER)
        self.assertIn("sync-drive-ready-upload-files", RUNNER)
        self.assertIn("drive_source_sync_enable_downloads", RUNNER)
        self.assertIn("metadata_only", RUNNER)
        self.assertIn("does not ingest source datasets", RUNNER)
        self.assertIn("immediate_add_count", RUNNER)
        self.assertIn("operator_priority", RUNNER)
        self.assertIn("google_drive_api_key", SETTINGS)
        self.assertIn("default=false", SETTINGS)

    def test_workflow_is_dedicated_to_drive_sync_and_not_source_ingestion(self) -> None:
        self.assertIn("run landintel drive source sync", WORKFLOW)
        self.assertIn("sync-drive-source-manifest", WORKFLOW)
        self.assertIn("sync-drive-ready-upload-files", WORKFLOW)
        self.assertIn("drive_source_sync_enable_downloads", WORKFLOW)
        self.assertIn("python -m src.drive_source_sync", WORKFLOW)
        self.assertIn("python -m src.source_phase_runner run-migrations", WORKFLOW)
        self.assertNotIn("ingest-", WORKFLOW)
        self.assertNotIn("measure-constraints", WORKFLOW)
        self.assertNotIn("refresh-site-bgs-borehole-context", WORKFLOW)

    def test_docs_explain_safety_boundaries_and_operator_verification(self) -> None:
        for required_phrase in (
            "does not ingest",
            "ready_to_upload_flag = true",
            "does not mean",
            "bgs remains paused",
            "loose shapefile components are not ready",
            "misfiled planning folder item",
            "run landintel drive source sync",
            "v_drive_source_ready_upload_files",
            "v_drive_source_sync_status",
            "immediate priority set",
            "council_asset_register_-_scotland.zip",
        ):
            self.assertIn(required_phrase, DOC)


if __name__ == "__main__":
    unittest.main()

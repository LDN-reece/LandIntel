"""Contract tests for Phase One source estate registration."""

from __future__ import annotations

from pathlib import Path
import unittest

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


class SourceEstateRegistryContractTests(unittest.TestCase):
    def test_manifest_bags_and_tags_phase_one_sources_without_plaintext_secrets(self) -> None:
        manifest_path = REPO_ROOT / "config" / "phase_one_source_estate.yaml"
        manifest = yaml.safe_load(manifest_path.read_text())
        source_families = {source["source_family"] for source in manifest["sources"]}

        for source_family in {
            "planning",
            "hla",
            "title_number",
            "ldp",
            "settlement",
            "ela",
            "vdl",
            "sepa_flood",
            "coal_authority",
            "bgs",
            "hes",
            "naturescot",
            "contaminated_land",
            "tpo",
            "culverts",
            "conservation_areas",
            "greenbelt",
            "local_authority_boundaries",
            "sgn_assets",
        }:
            self.assertIn(source_family, source_families)

        manifest_text = manifest_path.read_text()
        self.assertIn("IMPROVEMENT_SERVICE_AUTHKEY", manifest_text)
        self.assertNotIn("eyJ0eXAiOiJKV1Qi", manifest_text)
        self.assertNotIn("Bother158631", manifest_text)

    def test_core_spatial_hub_manifest_exposes_hla_source(self) -> None:
        manifest_path = REPO_ROOT / "config" / "scotland_core_sources.yaml"
        manifest = yaml.safe_load(manifest_path.read_text())

        self.assertEqual(manifest["spatial_hub"]["authkey_env"], "IMPROVEMENT_SERVICE_AUTHKEY")
        self.assertIn("planning_history", manifest["spatial_hub"])
        self.assertIn("hla", manifest["spatial_hub"])
        self.assertIn("sources", manifest["spatial_hub"])
        self.assertIn("planning_history", manifest["spatial_hub"]["sources"])
        self.assertIn("hla", manifest["spatial_hub"]["sources"])
        self.assertEqual(
            manifest["spatial_hub"]["sources"]["hla"]["dataset_id"],
            "housing_land_supply-is",
        )
        self.assertIn("source_record_id", manifest["spatial_hub"]["hla"]["field_mappings"])
        self.assertIn("constraint_reason", manifest["spatial_hub"]["hla"]["field_mappings"])
        self.assertIn("source_record_id", manifest["spatial_hub"]["planning_history"]["field_mappings"])
        self.assertIn("refusal_reason", manifest["spatial_hub"]["planning_history"]["field_mappings"])

    def test_source_estate_schema_exposes_live_matrix(self) -> None:
        sql = (REPO_ROOT / "sql" / "043_phase_one_source_estate_registry.sql").read_text()

        self.assertIn("landintel.source_estate_registry", sql)
        self.assertIn("landintel.source_corpus_assets", sql)
        self.assertIn("analytics.v_phase_one_source_estate_matrix", sql)

    def test_workflow_exposes_source_estate_commands(self) -> None:
        workflow = (REPO_ROOT.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text()

        for command in (
            "source-estate-maintenance",
            "register-source-estate",
            "probe-source-estate",
            "audit-source-estate",
            "audit-title-number-control",
            "discover-ldp-sources",
            "discover-settlement-sources",
        ):
            self.assertIn(command, workflow)
        self.assertIn("IMPROVEMENT_SERVICE_AUTHKEY", workflow)
        self.assertIn(
            "Automated schedules are intentionally paused during incremental reconcile burn-in.",
            workflow,
        )
        self.assertNotIn("schedule:", workflow)
        self.assertIn("app/config/phase_one_source_estate.yaml", workflow)
        self.assertIn("src/source_policy_discovery.py", workflow)
        self.assertIn("discover-ldp-geonetwork", workflow)
        self.assertIn("register-settlement-boundaries", workflow)
        self.assertIn("audit-title-number-control", workflow)

    def test_legacy_lean_workflow_is_retired(self) -> None:
        workflow = (REPO_ROOT.parent / ".github" / "workflows" / "run-landintel-lean.yml").read_text()

        self.assertIn("Run LandIntel Lean (Retired)", workflow)
        self.assertIn("No Supabase secrets are loaded here.", workflow)
        self.assertIn("No Supabase writes are performed here.", workflow)
        self.assertIn("Run LandIntel Sources workflow", workflow)
        for forbidden_snippet in (
            "src.lean_ops",
            "SUPABASE_DB_URL",
            "SUPABASE_SERVICE_ROLE_KEY",
            "BOUNDARY_AUTHKEY",
            "audit-operational-footprint",
            "cleanup-operational-footprint",
            "ingest-ros-cadastral-lean",
            "full-refresh-lean",
        ):
            self.assertNotIn(forbidden_snippet, workflow)

    def test_legacy_lean_runner_is_retired(self) -> None:
        runner = (REPO_ROOT / "src" / "lean_ops.py").read_text()

        self.assertIn("Retired lean operations entrypoint", runner)
        self.assertIn("RETIREMENT_MESSAGE", runner)
        self.assertIn("No Supabase writes are available from this entrypoint.", runner)
        self.assertNotIn("LandIntelPipeline", runner)
        self.assertNotIn("cleanup_operational_footprint", runner)
        self.assertNotIn("ingest_ros_cadastral_lean", runner)
        self.assertNotIn("full_refresh_lean", runner)

    def test_legacy_lean_runbooks_are_retired(self) -> None:
        root_runbook = (REPO_ROOT.parent / "GITHUB_ACTIONS_RUNBOOK.md").read_text()
        lean_runbook = (REPO_ROOT / "docs" / "github-actions-lean-runbook.md").read_text()

        for document in (root_runbook, lean_runbook):
            self.assertIn("Run LandIntel Lean", document)
            self.assertIn("retired", document.lower())
            self.assertIn("Run LandIntel Sources", document)
        self.assertIn("Old full-refresh, lean parcel, and local execution paths are intentionally blocked.", root_runbook)
        self.assertIn("Do not use it for source orchestration", lean_runbook)

    def test_policy_discovery_uses_geonetwork_and_registers_topography(self) -> None:
        runner = (REPO_ROOT / "src" / "source_policy_discovery.py").read_text()

        self.assertIn("geonetwork/srv/api/search/records/_search", runner)
        self.assertIn("topography_os_terrain_50", runner)
        self.assertIn("topography_scottish_lidar", runner)
        self.assertIn("adopted_roads_authority_discovery", runner)
        self.assertIn("utilities_water_electric_discovery", runner)
        self.assertIn("NRS_SETTLEMENT_WFS_URL", runner)
        self.assertIn("register_settlement_boundaries", runner)

    def test_manifest_marks_title_ldp_settlement_as_core_priority_spine(self) -> None:
        manifest_path = REPO_ROOT / "config" / "phase_one_source_estate.yaml"
        manifest = yaml.safe_load(manifest_path.read_text())
        source_by_family = {source["source_family"]: source for source in manifest["sources"]}

        self.assertEqual(source_by_family["title_number"]["source_status"], "live_internal_validation")
        self.assertEqual(source_by_family["title_number"]["target_table"], "public.site_title_validation")
        self.assertEqual(source_by_family["ldp"]["source_status"], "live_target")
        self.assertEqual(source_by_family["ldp"]["orchestration_mode"], "spatialhub_ckan_package_zips")
        self.assertEqual(source_by_family["ldp"]["target_table"], "landintel.ldp_site_records")
        self.assertEqual(source_by_family["ldp"]["spatialhub_package_id"], "local_development_plans-is")
        self.assertFalse(source_by_family["ldp"]["ranking_eligible"])
        self.assertTrue(source_by_family["ldp"]["review_output_eligible"])
        self.assertEqual(source_by_family["settlement"]["source_status"], "live_target")
        self.assertEqual(source_by_family["settlement"]["orchestration_mode"], "nrs_wfs_geojson")
        self.assertEqual(source_by_family["settlement"]["target_table"], "landintel.settlement_boundary_records")
        self.assertEqual(source_by_family["settlement"]["wfs_type_name"], "NRS:SettlementBoundaries")
        self.assertFalse(source_by_family["settlement"]["ranking_eligible"])
        self.assertTrue(source_by_family["settlement"]["review_output_eligible"])


if __name__ == "__main__":
    unittest.main()

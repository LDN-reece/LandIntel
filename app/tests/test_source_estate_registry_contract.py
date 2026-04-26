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
        self.assertIn("discover-settlement-geonetwork", workflow)

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

    def test_policy_discovery_uses_geonetwork_and_registers_topography(self) -> None:
        runner = (REPO_ROOT / "src" / "source_policy_discovery.py").read_text()

        self.assertIn("geonetwork/srv/api/search/records/_search", runner)
        self.assertIn("topography_os_terrain_50", runner)
        self.assertIn("topography_scottish_lidar", runner)
        self.assertIn("adopted_roads_authority_discovery", runner)
        self.assertIn("utilities_water_electric_discovery", runner)


if __name__ == "__main__":
    unittest.main()

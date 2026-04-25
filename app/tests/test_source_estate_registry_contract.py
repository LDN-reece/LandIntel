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

    def test_source_estate_schema_exposes_live_matrix(self) -> None:
        sql = (REPO_ROOT / "sql" / "043_phase_one_source_estate_registry.sql").read_text()

        self.assertIn("landintel.source_estate_registry", sql)
        self.assertIn("landintel.source_corpus_assets", sql)
        self.assertIn("analytics.v_phase_one_source_estate_matrix", sql)

    def test_workflow_exposes_source_estate_commands(self) -> None:
        workflow = (REPO_ROOT.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text()

        for command in (
            "register-source-estate",
            "probe-source-estate",
            "audit-source-estate",
            "discover-ldp-sources",
            "discover-settlement-sources",
        ):
            self.assertIn(command, workflow)
        self.assertIn("IMPROVEMENT_SERVICE_AUTHKEY", workflow)


if __name__ == "__main__":
    unittest.main()

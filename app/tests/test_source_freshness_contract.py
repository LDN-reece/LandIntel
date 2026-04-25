"""Contract tests for the Phase One source freshness audit layer."""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class SourceFreshnessContractTests(unittest.TestCase):
    def test_source_freshness_migration_covers_phase_one_source_matrix(self) -> None:
        sql = (REPO_ROOT / "sql" / "042_source_freshness.sql").read_text()

        self.assertIn("create table if not exists landintel.source_freshness_states", sql)
        self.assertIn("analytics.v_source_freshness_matrix", sql)
        self.assertIn("public.ingest_runs", sql)

        for source_family in (
            "planning",
            "hla",
            "canonical",
            "ros_cadastral",
            "local_authority_boundaries",
            "ldp",
            "settlement",
            "flood",
            "bgs",
            "ela",
            "vdl",
            "sepa_flood",
            "coal_authority",
            "hes",
            "naturescot",
            "contaminated_land",
            "tpo",
            "culverts",
            "conservation_areas",
            "sgn_assets",
        ):
            self.assertIn(source_family, sql)

    def test_github_action_exposes_source_freshness_audit_command(self) -> None:
        workflow = (REPO_ROOT.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text()

        self.assertIn("audit-source-freshness", workflow)
        self.assertIn("src/source_freshness_audit.py", workflow)
        self.assertIn("python -m src.source_freshness_audit audit-source-freshness", workflow)

    def test_source_freshness_audit_reads_live_matrix(self) -> None:
        runner = (REPO_ROOT / "src" / "source_freshness_audit.py").read_text()

        self.assertIn("analytics.v_source_freshness_matrix", runner)
        self.assertIn("blocked_sources", runner)
        self.assertIn("unknown_sources", runner)


if __name__ == "__main__":
    unittest.main()

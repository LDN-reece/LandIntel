"""Contract tests for the Phase One source freshness audit layer."""

from __future__ import annotations

from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class SourceFreshnessContractTests(unittest.TestCase):
    def test_source_freshness_migration_covers_phase_one_source_matrix(self) -> None:
        sql = (REPO_ROOT / "sql" / "042_source_freshness.sql").read_text()

        self.assertIn("create table if not exists landintel.source_freshness_states", sql)
        self.assertIn("analytics.v_source_freshness_matrix", sql)
        self.assertIn("drop view if exists analytics.v_phase_one_source_estate_matrix;", sql)
        self.assertIn("public.ingest_runs", sql)

        for source_family in (
            "planning",
            "hla",
            "title_number",
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

        self.assertIn("core_policy_storage_licence_gated", sql)
        self.assertIn("pass_core_policy_storage_licence_gated", sql)
        self.assertIn("core_policy_storage_interpreter_gated", sql)
        self.assertIn("pass_core_policy_storage_interpreter_gated", sql)
        self.assertIn("control_spine", sql)

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

    def test_rerunnable_migration_preflight_drops_late_analytics_dependents(self) -> None:
        preflight_path = REPO_ROOT / "sql" / "032z_rerunnable_analytics_dependency_drops.sql"
        preflight = preflight_path.read_text()
        migration_names = [path.name for path in sorted((REPO_ROOT / "sql").glob("*.sql"))]

        self.assertIn("drop view if exists landintel_reporting.v_source_completion_matrix;", preflight)
        self.assertIn("drop view if exists analytics.v_phase_one_source_estate_matrix;", preflight)
        self.assertIn("drop view if exists analytics.v_live_source_coverage_freshness;", preflight)
        self.assertIn("drop view if exists analytics.v_phase_one_control_policy_priority;", preflight)
        self.assertLess(
            migration_names.index(preflight_path.name),
            migration_names.index("033_landintel_live_audit_views.sql"),
        )
        self.assertLess(
            migration_names.index(preflight_path.name),
            migration_names.index("042_source_freshness.sql"),
        )
        self.assertLess(
            migration_names.index(preflight_path.name),
            migration_names.index("044_phase_one_source_expansion.sql"),
        )
        self.assertLess(
            migration_names.index(preflight_path.name),
            migration_names.index("046_phase_one_control_policy_priority.sql"),
        )

    def test_raw_sql_migrations_escape_literal_percent_for_psycopg(self) -> None:
        offenders: list[str] = []
        for path in sorted((REPO_ROOT / "sql").glob("*.sql")):
            sql = path.read_text()
            index = 0
            while index < len(sql):
                if sql[index] != "%":
                    index += 1
                    continue
                next_char = sql[index + 1 : index + 2]
                if next_char in {"%", "s", "b", "t"}:
                    index += 2
                    continue
                line_number = sql.count("\n", 0, index) + 1
                line = sql.splitlines()[line_number - 1].strip()
                offenders.append(f"{path.name}:{line_number}: {line}")
                index += 1

        self.assertEqual(
            [],
            offenders,
            "Raw migration scripts run through psycopg; literal percent signs must be escaped as %%.",
        )


if __name__ == "__main__":
    unittest.main()

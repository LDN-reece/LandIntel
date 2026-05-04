import re
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_DIR.parent
MIGRATION = (APP_DIR / "sql" / "070_site_bgs_borehole_context.sql").read_text(encoding="utf-8").lower()
RUNNER = (APP_DIR / "src" / "bgs_borehole_context_runner.py").read_text(encoding="utf-8").lower()
WORKFLOW = (REPO_ROOT / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8").lower()
DOC = (APP_DIR / "docs" / "schema" / "site_bgs_borehole_context.md").read_text(encoding="utf-8").lower()


class SiteBgsBoreholeContextContractTests(unittest.TestCase):
    def test_migration_creates_context_table_and_operator_view(self) -> None:
        self.assertIn("create table if not exists landintel_store.site_bgs_borehole_context", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_site_bgs_borehole_context", MIGRATION)
        self.assertIn("references landintel.canonical_sites", MIGRATION)
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION)

    def test_migration_exposes_required_context_metrics(self) -> None:
        for required_column in (
            "nearest_borehole_distance_m",
            "boreholes_inside_site",
            "boreholes_within_100m",
            "boreholes_within_250m",
            "boreholes_within_500m",
            "boreholes_within_1km",
            "deep_boreholes_within_500m",
            "deep_boreholes_within_1km",
            "log_available_within_500m",
            "log_available_within_1km",
            "evidence_density_signal",
            "ground_uncertainty_signal",
            "safe_use_caveat",
        ):
            self.assertIn(required_column, MIGRATION)

    def test_bgs_master_gets_additive_spatial_index_only(self) -> None:
        self.assertIn("create index if not exists bgs_borehole_master_geom_27700_gix", MIGRATION)
        self.assertIn("using gist (geom_27700)", MIGRATION)
        self.assertNotIn("create table if not exists landintel.bgs_borehole_master", MIGRATION)

    def test_runner_is_bounded_candidate_site_first(self) -> None:
        self.assertIn("refresh-site-bgs-borehole-context", RUNNER)
        self.assertIn("audit-site-bgs-borehole-context", RUNNER)
        self.assertIn("landintel_reporting.v_constraint_priority_sites", RUNNER)
        self.assertIn('bgs_borehole_context_batch_size: "25"', WORKFLOW)
        self.assertIn("bgs_borehole_context_authority: ${{ inputs.phase2_authority", WORKFLOW)
        self.assertIn("_env_int(\"bgs_borehole_context_batch_size", RUNNER)
        self.assertIn("min(value, maximum)", RUNNER)
        self.assertIn("limit :batch_size", RUNNER)
        self.assertIn("max_age_days", RUNNER)

    def test_runner_creates_evidence_and_restrained_signals(self) -> None:
        self.assertIn("landintel.evidence_references", RUNNER)
        self.assertIn("landintel.site_signals", RUNNER)
        self.assertIn("bgs_borehole_evidence_density", RUNNER)
        self.assertIn("bgs_borehole_ground_uncertainty", RUNNER)
        self.assertIn("bgs_borehole_nearest_distance", RUNNER)
        self.assertIn("safe_use_caveat", RUNNER)
        self.assertIn("cast(:source_key as text)", RUNNER)
        self.assertIn("cast(:source_family as text)", RUNNER)
        self.assertIn("cast(:safe_use_caveat as text)", RUNNER)

    def test_workflow_exposes_refresh_and_audit_commands(self) -> None:
        self.assertIn("- refresh-site-bgs-borehole-context", WORKFLOW)
        self.assertIn("- audit-site-bgs-borehole-context", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_context_runner refresh-site-bgs-borehole-context", WORKFLOW)
        self.assertIn("python -m src.bgs_borehole_context_runner audit-site-bgs-borehole-context", WORKFLOW)
        self.assertIn("src/bgs_borehole_context_runner.py", WORKFLOW)
        self.assertIn("bgs_borehole_context_force_refresh: ${{ inputs.phase2_force_refresh", WORKFLOW)

    def test_workflow_dispatch_input_count_stays_within_github_limit(self) -> None:
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

    def test_docs_lock_safe_use_and_no_broad_scan(self) -> None:
        for required_phrase in (
            "does not download borehole scans",
            "run ocr",
            "re-upload bgs data",
            "final ground-condition interpretation",
            "abnormal-cost quantification",
            "candidate-site first",
            "batch-limited",
            "no broad scan rule",
        ):
            self.assertIn(required_phrase, DOC)

    def test_no_destructive_sql_or_scan_fetching(self) -> None:
        forbidden_migration_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bdelete\s+from\s+(landintel_store|public)\b",
        )
        for pattern in forbidden_migration_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION), pattern)

        for forbidden in ("fetch-bgs-borehole-scans", "ocr", "download scan", "pdf blob"):
            self.assertNotIn(forbidden, RUNNER)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "067_constraint_coverage_scaler.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "constraint_coverage_scaler.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class ConstraintCoverageScalerContractTests(unittest.TestCase):
    def test_reporting_views_exist(self) -> None:
        for view_name in (
            "landintel_reporting.v_constraint_coverage_by_layer",
            "landintel_reporting.v_constraint_coverage_by_site_priority",
            "landintel_reporting.v_constraint_measurement_backlog",
            "landintel_reporting.v_constraint_priority_measurement_queue",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_helper_views_are_reporting_only(self) -> None:
        for view_name in (
            "landintel_reporting.v_constraint_priority_layers",
            "landintel_reporting.v_constraint_priority_sites",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_existing_constraint_truth_tables_are_used(self) -> None:
        for relation in (
            "public.constraint_layer_registry",
            "public.constraint_source_features",
            "public.site_constraint_measurements",
            "public.site_constraint_group_summaries",
            "public.site_commercial_friction_facts",
            "public.site_constraint_measurement_scan_state",
            "landintel.canonical_sites",
            "landintel.site_ldn_candidate_screen",
            "landintel.site_prove_it_assessments",
            "landintel_sourced.v_title_spend_candidates",
            "landintel_sourced.v_review_queue",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("drop view", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+", re.IGNORECASE))
        self.assertNotRegex(
            MIGRATION_LOWER,
            re.compile(r"alter\s+table\s+[^;]+\s+rename\s+", re.IGNORECASE),
        )

    def test_migration_does_not_run_spatial_measurement(self) -> None:
        for forbidden in (
            "st_intersects",
            "st_intersection",
            "st_dwithin",
            "st_distance",
            "refresh_constraint_measurements_for_layer_sites",
            "measure_constraint_feature",
        ):
            self.assertNotIn(forbidden, MIGRATION_LOWER)

        self.assertIn("guidance only", MIGRATION_LOWER)
        self.assertIn("where queue_rank <= 5000", MIGRATION_LOWER)

    def test_priority_ordering_is_encoded(self) -> None:
        for required_phrase in (
            "title_spend_candidates",
            "review_queue",
            "ldn_candidate_screen",
            "prove_it_candidates",
            "wider_canonical_sites",
            "flood",
            "coal_mining",
            "green_belt",
            "contaminated_land",
            "culverts",
            "heritage_conservation",
            "ecology_naturescot",
            "tpo_landscape",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_object_registry_updates_reporting_surfaces_only(self) -> None:
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION_LOWER)
        self.assertIn("'reporting_surface'", MIGRATION_LOWER)
        self.assertIn("safe_to_write", MIGRATION_LOWER)
        self.assertIn("safe_for_operator", MIGRATION_LOWER)

    def test_docs_explain_bounded_constraint_scale_up(self) -> None:
        for required_phrase in (
            "existing constraint model remains the source of truth",
            "does not create a second constraint engine",
            "does not run broad all-site/all-layer measurement",
            "does not run spatial joins in the migration",
            "does not create rag scoring",
            "does not create pass/fail conclusions",
            "title spend candidates",
            "review queue",
            "flood",
            "coal/mining",
            "green belt",
            "constraint outputs remain measured facts",
            "not legal certainty",
            "not engineering certainty",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

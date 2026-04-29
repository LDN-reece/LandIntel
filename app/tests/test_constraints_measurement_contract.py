from __future__ import annotations

import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
SQL_TABLES = (APP_DIR / "sql" / "033_constraints_measurement_tables.sql").read_text(encoding="utf-8")
SQL_FUNCTIONS = (APP_DIR / "sql" / "034_constraints_measurement_functions.sql").read_text(encoding="utf-8")
SQL_INDEXES = (APP_DIR / "sql" / "035_constraints_measurement_indexes.sql").read_text(encoding="utf-8")
SQL_VIEWS = (APP_DIR / "sql" / "036_constraints_measurement_views.sql").read_text(encoding="utf-8")
SQL_POLICIES = (APP_DIR / "sql" / "037_constraints_measurement_policies.sql").read_text(encoding="utf-8")
SQL_COMMENTS = (APP_DIR / "sql" / "038_constraints_measurement_comments.sql").read_text(encoding="utf-8")
SQL_ENGINE = (APP_DIR / "sql" / "049_constraint_measurement_engine.sql").read_text(encoding="utf-8")
SQL_SMOKE = (APP_DIR / "sql_checks" / "constraints_measurement_smoke.sql").read_text(encoding="utf-8")
DOC_CONSTRAINTS = (APP_DIR / "docs" / "constraints-tab-mvp.md").read_text(encoding="utf-8")
DOC_QUALIFICATION = (APP_DIR / "docs" / "site-qualification-mvp.md").read_text(encoding="utf-8")
DOC_ARCHITECTURE = (APP_DIR / "docs" / "operational-architecture.md").read_text(encoding="utf-8")


class ConstraintsMeasurementContractTests(unittest.TestCase):
    def test_declares_required_measurement_tables(self) -> None:
        for table_name in (
            "public.site_spatial_links",
            "public.site_title_validation",
            "public.constraint_layer_registry",
            "public.constraint_source_features",
            "public.site_constraint_measurements",
            "public.site_constraint_group_summaries",
            "public.site_commercial_friction_facts",
        ):
            self.assertIn(table_name, SQL_TABLES)

    def test_functions_anchor_to_live_site_spine(self) -> None:
        for snippet in (
            "public.constraints_site_anchor",
            "public.measure_constraint_feature",
            "public.normalize_site_title_number",
            "public.sites",
            "public.site_locations",
            "public.calculate_area_acres",
        ):
            self.assertIn(snippet, SQL_FUNCTIONS)

    def test_measurement_function_preserves_null_distance(self) -> None:
        self.assertIn("round(metrics.nearest_distance_m::numeric, 2) as nearest_distance_m", SQL_FUNCTIONS)
        self.assertNotIn("coalesce(metrics.nearest_distance_m, 0)", SQL_FUNCTIONS)

    def test_measurement_sql_does_not_depend_on_canonical_sites(self) -> None:
        combined_sql = "\n".join((SQL_TABLES, SQL_FUNCTIONS, SQL_INDEXES, SQL_VIEWS, SQL_POLICIES, SQL_COMMENTS, SQL_SMOKE))
        self.assertNotIn("landintel.canonical_sites", combined_sql)
        self.assertNotIn("canonical_site_id", combined_sql)

    def test_declares_constraints_tab_views_and_columns(self) -> None:
        for view_name in (
            "analytics.v_constraints_tab_overview",
            "analytics.v_constraints_tab_measurements",
            "analytics.v_constraints_tab_group_summaries",
            "analytics.v_constraints_tab_commercial_friction",
        ):
            self.assertIn(view_name, SQL_VIEWS)

        for column_name in (
            "site_id",
            "site_location_id",
            "site_name",
            "authority_name",
            "site_area_acres",
            "location_label",
            "layer_key",
            "constraint_group",
            "intersects",
            "within_buffer",
            "overlap_pct_of_site",
            "nearest_distance_m",
            "fact_key",
            "fact_label",
            "fact_basis",
            "constraint_groups_measured",
            "friction_fact_count",
        ):
            self.assertIn(column_name, SQL_VIEWS)

    def test_overview_rollup_counts_distinct_constraint_groups(self) -> None:
        self.assertIn("count(distinct layer.constraint_group) as constraint_groups_measured", SQL_VIEWS)
        self.assertIn(
            "count(distinct layer.constraint_group) filter (where summaries.intersecting_feature_count > 0) as constraint_groups_intersecting",
            SQL_VIEWS,
        )

    def test_indexes_cover_measurement_write_paths(self) -> None:
        for snippet in (
            "site_spatial_links_site_record_role_uidx",
            "site_title_validation_site_title_method_uidx",
            "constraint_source_features_layer_feature_uidx",
            "site_constraint_measurements_site_layer_feature_source_uidx",
            "site_constraint_group_summaries_site_layer_group_scope_uidx",
            "site_commercial_friction_facts_site_layer_group_key_uidx",
        ):
            self.assertIn(snippet, SQL_INDEXES)

    def test_policies_cover_new_tables_and_views(self) -> None:
        for object_name in (
            "public.site_spatial_links",
            "public.site_title_validation",
            "public.constraint_layer_registry",
            "public.constraint_source_features",
            "public.site_constraint_measurements",
            "public.site_constraint_group_summaries",
            "public.site_commercial_friction_facts",
            "analytics.v_constraints_tab_overview",
            "analytics.v_constraints_tab_measurements",
            "analytics.v_constraints_tab_group_summaries",
            "analytics.v_constraints_tab_commercial_friction",
        ):
            self.assertIn(object_name, SQL_POLICIES)

    def test_policies_enable_read_only_rls_on_public_measurement_tables(self) -> None:
        for table_name in (
            "public.site_spatial_links",
            "public.site_title_validation",
            "public.constraint_layer_registry",
            "public.constraint_source_features",
            "public.site_constraint_measurements",
            "public.site_constraint_group_summaries",
            "public.site_commercial_friction_facts",
        ):
            self.assertIn(f"alter table {table_name} enable row level security;", SQL_POLICIES)
            self.assertIn(f"grant select on table {table_name} to authenticated;", SQL_POLICIES)

        for policy_name in (
            "site_spatial_links_authenticated_select",
            "site_title_validation_authenticated_select",
            "constraint_layer_registry_authenticated_select",
            "constraint_source_features_authenticated_select",
            "site_constraint_measurements_authenticated_select",
            "site_constraint_group_summaries_authenticated_select",
            "site_commercial_friction_facts_authenticated_select",
        ):
            self.assertIn(policy_name, SQL_POLICIES)

        self.assertNotIn("disable row level security", SQL_POLICIES)

    def test_comments_mark_new_architecture_and_legacy_path(self) -> None:
        for object_name in (
            "public.site_spatial_links",
            "public.site_title_validation",
            "public.constraint_layer_registry",
            "public.constraint_source_features",
            "public.site_constraint_measurements",
            "public.site_constraint_group_summaries",
            "public.site_commercial_friction_facts",
            "analytics.v_constraints_tab_overview",
            "analytics.v_constraints_tab_measurements",
            "analytics.v_constraints_tab_group_summaries",
            "analytics.v_constraints_tab_commercial_friction",
            "public.site_constraints",
        ):
            self.assertIn(object_name, SQL_COMMENTS)

        self.assertIn("legacy", SQL_COMMENTS.lower())
        self.assertIn("severity-style", SQL_COMMENTS.lower())

    def test_docs_explain_live_spine_and_measurement_only_scope(self) -> None:
        combined_docs = "\n".join((DOC_CONSTRAINTS, DOC_QUALIFICATION, DOC_ARCHITECTURE)).lower()
        for snippet in (
            "public.sites",
            "public.site_locations.geometry",
            "public.site_constraints",
            "legacy",
            "scoring",
            "pass/fail",
            "rag",
        ):
            self.assertIn(snippet, combined_docs)

    def test_smoke_sql_checks_measurement_surfaces(self) -> None:
        for snippet in (
            "public.constraints_site_anchor()",
            "public.measure_constraint_feature(geometry,geometry,numeric)",
            "analytics.v_constraints_tab_overview",
            "analytics.v_constraints_tab_measurements",
            "analytics.v_constraints_tab_group_summaries",
            "analytics.v_constraints_tab_commercial_friction",
            "information_schema.columns",
            "obj_description",
            "column_name like '%score%'",
            "column_name like '%pass_fail%'",
        ):
            self.assertIn(snippet, SQL_SMOKE)

    def test_priority_zero_engine_is_material_change_and_geometry_mode_aware(self) -> None:
        for snippet in (
            "public.refresh_constraint_measurements_for_layer_sites",
            "feature_geometry_dimension",
            "overlap_character",
            "constraint_character",
            "summary_signature",
            "tmp_constraint_changed_sites",
            "source_expansion_constraint",
            "constraint_evidence_state_changed",
            "constraint_measurement_engine",
            "set search_path = pg_catalog, public, extensions",
            "set search_path = pg_catalog, public, landintel, extensions",
            "analytics.v_constraint_measurement_coverage",
            "analytics.v_constraint_measurement_layer_coverage",
        ):
            self.assertIn(snippet, SQL_ENGINE)

        for snippet in (
            "st_dimension(feature_geometry) = 2",
            "st_collectionextract(st_intersection(site_geometry, feature_geometry), 3)",
            "OPERATOR(extensions.&&)",
            "st_dwithin(anchor.geometry, feature.geometry, layer_row.buffer_distance_m)",
            "st_intersects(anchor.geometry, feature.geometry)",
        ):
            self.assertIn(snippet, SQL_ENGINE)

        for forbidden in (" pass ", " fail ", " red ", " amber ", " green ", " viable ", " unviable "):
            self.assertNotIn(forbidden, f" {SQL_ENGINE.lower()} ")


if __name__ == "__main__":
    unittest.main()

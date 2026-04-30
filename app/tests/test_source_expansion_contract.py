from __future__ import annotations

from pathlib import Path
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
WORKFLOW = (APP_DIR.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8")
RUNNER = (APP_DIR / "src" / "source_expansion_runner.py").read_text(encoding="utf-8")
PAGED_RUNNER = (APP_DIR / "src" / "source_expansion_runner_wfs_paging.py").read_text(encoding="utf-8")
LOADER = (APP_DIR / "src" / "loaders" / "supabase_loader.py").read_text(encoding="utf-8")
MIGRATION = (APP_DIR / "sql" / "044_phase_one_source_expansion.sql").read_text(encoding="utf-8")
TITLE_BRIDGE_MIGRATION = (APP_DIR / "sql" / "047_title_resolution_bridge.sql").read_text(encoding="utf-8")
SITE_PARCEL_LINK_MIGRATION = (APP_DIR / "sql" / "048_site_ros_parcel_linking.sql").read_text(encoding="utf-8")
CONSTRAINT_ENGINE_MIGRATION = (APP_DIR / "sql" / "049_constraint_measurement_engine.sql").read_text(encoding="utf-8")
MANIFEST = (APP_DIR / "config" / "phase_one_source_estate.yaml").read_text(encoding="utf-8")


class SourceExpansionContractTests(unittest.TestCase):
    def test_workflow_exposes_missing_source_universe_commands(self) -> None:
        for command in (
            "audit-source-expansion",
            "link-sites-to-ros-parcels",
            "audit-site-parcel-links",
            "resolve-title-numbers",
            "audit-title-number-control",
            "measure-constraints",
            "audit-constraint-measurements",
            "ingest-ldp",
            "ingest-ela",
            "ingest-vdl",
            "ingest-sepa-flood",
            "ingest-coal-authority",
            "ingest-hes-designations",
            "ingest-naturescot",
            "ingest-contaminated-land",
            "ingest-tpo",
            "ingest-culverts",
            "ingest-conservation-areas",
            "ingest-greenbelt",
            "ingest-os-topography",
            "ingest-os-places",
            "ingest-os-features",
            "ingest-settlement-boundaries",
        ):
            self.assertIn(f"- {command}", WORKFLOW)
            self.assertIn(command, RUNNER)

    def test_workflow_routes_expansion_commands_to_paged_expansion_runner(self) -> None:
        self.assertIn("src/source_expansion_runner.py", WORKFLOW)
        self.assertIn("src/source_expansion_runner_wfs_paging.py", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging \"$SELECTED_COMMAND\"", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-source-expansion", WORKFLOW)
        self.assertIn("is_source_expansion_ingest()", WORKFLOW)
        self.assertIn("HLA is supporting evidence only", WORKFLOW)

    def test_paged_runner_bounds_spatialhub_wfs_reads(self) -> None:
        self.assertIn("class PagedWfsSourceExpansionRunner(SourceExpansionRunner)", PAGED_RUNNER)
        self.assertIn('"maxFeatures": str(batch_limit)', PAGED_RUNNER)
        self.assertIn('params["startIndex"] = str(offset)', PAGED_RUNNER)
        self.assertIn("SOURCE_EXPANSION_PAGE_SIZE", WORKFLOW)
        self.assertIn("No usable WFS features returned", PAGED_RUNNER)

    def test_paged_runner_uses_capabilities_as_vdl_layer_authority(self) -> None:
        self.assertIn("def _wfs_feature_types", PAGED_RUNNER)
        self.assertIn("GetCapabilities", PAGED_RUNNER)
        self.assertIn('source.get("source_family") == "vdl"', PAGED_RUNNER)
        self.assertIn("return names", PAGED_RUNNER)
        self.assertIn("Capabilities is the authority here", PAGED_RUNNER)

    def test_sepa_arcgis_ingest_is_clipped_to_canonical_site_aoi(self) -> None:
        self.assertIn('source.get("source_family") != "sepa_flood"', PAGED_RUNNER)
        self.assertIn("_canonical_site_envelopes", PAGED_RUNNER)
        self.assertIn('"geometryType": "esriGeometryEnvelope"', PAGED_RUNNER)
        self.assertIn('"spatialRel": "esriSpatialRelIntersects"', PAGED_RUNNER)
        self.assertIn("SOURCE_EXPANSION_ARCGIS_MAX_FEATURES_PER_LAYER", PAGED_RUNNER)
        self.assertIn("sepa_layer_feature_cap_reached", PAGED_RUNNER)

    def test_paged_runner_handles_empty_arcgis_tiles_without_geopandas_crash(self) -> None:
        self.assertIn("def _empty_geo_frame", PAGED_RUNNER)
        self.assertIn('gpd.GeoDataFrame({"geometry": []}', PAGED_RUNNER)
        self.assertIn("def _feature_collection_to_gdf", PAGED_RUNNER)
        self.assertIn('if not payload.get("features")', PAGED_RUNNER)
        self.assertIn('if "Unknown column geometry" in str(exc)', PAGED_RUNNER)
        self.assertNotIn('gpd.GeoDataFrame([], geometry="geometry"', PAGED_RUNNER)

    def test_constraint_ingest_is_internally_staged_and_budget_gated(self) -> None:
        self.assertIn("def _ingest_constraint_family", PAGED_RUNNER)
        self.assertIn("constraint_layer_gate", PAGED_RUNNER)
        self.assertIn('if gate["measurement_approved"]', PAGED_RUNNER)
        self.assertIn("constraint_loaded_measurement_deferred", PAGED_RUNNER)
        self.assertIn("SOURCE_EXPANSION_CONSTRAINT_MEASURE_MODE", PAGED_RUNNER)
        self.assertIn("MAX_MEASURE_FEATURES", PAGED_RUNNER)
        self.assertIn("MAX_MEASURE_LAYERS", PAGED_RUNNER)
        self.assertIn("measurement_statement_timeout", PAGED_RUNNER)
        self.assertIn("constraint_measurement_deferred", PAGED_RUNNER)
        self.assertIn("source_family in CONSTRAINT_FAMILIES", PAGED_RUNNER)
        self.assertIn('return "load_only"', PAGED_RUNNER)
        self.assertIn("SOURCE_EXPANSION_COMMAND_TIMEOUT", WORKFLOW)
        self.assertIn('timeout "$SOURCE_EXPANSION_COMMAND_TIMEOUT" python -m src.source_expansion_runner_wfs_paging', WORKFLOW)
        self.assertIn('SOURCE_EXPANSION_ARCGIS_MAX_TILES: "75"', WORKFLOW)
        self.assertIn('SOURCE_EXPANSION_ARCGIS_MAX_FEATURES_PER_LAYER: "10000"', WORKFLOW)
        self.assertIn('SOURCE_EXPANSION_MAX_MEASURE_FEATURES: "2500"', WORKFLOW)
        self.assertIn('SOURCE_EXPANSION_MAX_MEASURE_LAYERS: "1"', WORKFLOW)
        self.assertNotIn("probe-sepa-flood", WORKFLOW)
        self.assertNotIn("load-sepa-flood", WORKFLOW)
        self.assertNotIn("gate-sepa-flood", WORKFLOW)
        self.assertNotIn("measure-sepa-flood", WORKFLOW)

    def test_hes_uses_arcgis_rest_and_handles_non_paginated_layers(self) -> None:
        self.assertIn("source_key: hes_designations", MANIFEST)
        self.assertIn("HES_Designations/MapServer/WFSServer", MANIFEST)
        self.assertIn("def _normalise_arcgis_endpoint_url", PAGED_RUNNER)
        self.assertIn('/arcgis/rest/services/', PAGED_RUNNER)
        self.assertIn('/wfsserver', PAGED_RUNNER)
        self.assertIn("arcgis_pagination_unsupported", PAGED_RUNNER)
        self.assertIn("returnIdsOnly", PAGED_RUNNER)
        self.assertIn("objectIds", PAGED_RUNNER)
        self.assertIn("self.client.post", PAGED_RUNNER)

    def test_paged_runner_flattens_3d_source_geometries_before_storage(self) -> None:
        self.assertIn("from shapely import force_2d", PAGED_RUNNER)
        self.assertIn("def _force_2d_frame", PAGED_RUNNER)
        self.assertIn("force_2d(geometry)", PAGED_RUNNER)
        self.assertIn("return self._force_2d_frame(frame)", PAGED_RUNNER)

    def test_canonical_constraint_anchor_has_no_legacy_site_dependency(self) -> None:
        anchor_sql = MIGRATION.split("create or replace function public.constraints_site_anchor()", 1)[1]
        anchor_sql = anchor_sql.split("insert into public.constraint_layer_registry", 1)[0]

        self.assertIn("from landintel.canonical_sites as site", anchor_sql)
        self.assertIn("site.id::text as site_id", anchor_sql)
        self.assertIn("site.id::text as site_location_id", anchor_sql)
        self.assertNotIn("public.sites", anchor_sql)
        self.assertNotIn("public.site_locations", anchor_sql)

    def test_expansion_schema_proves_live_population_not_repo_only(self) -> None:
        for object_name in (
            "landintel.ela_site_records",
            "landintel.vdl_site_records",
            "landintel.source_expansion_events",
            "landintel.site_signals",
            "landintel.site_change_events",
            "analytics.v_phase_one_source_expansion_readiness",
            "analytics.v_phase_one_control_policy_priority",
            "public.refresh_constraint_measurements_for_layer",
        ):
            self.assertIn(object_name, MIGRATION if object_name != "analytics.v_phase_one_control_policy_priority" else (APP_DIR / "sql" / "046_phase_one_control_policy_priority.sql").read_text(encoding="utf-8"))

        for proof_column in (
            "raw_or_feature_rows",
            "linked_or_measured_rows",
            "evidence_rows",
            "signal_rows",
            "change_event_rows",
            "review_output_rows",
            "live_wired_proven",
        ):
            self.assertIn(proof_column, MIGRATION)

    def test_constraint_source_families_are_seeded_and_measured(self) -> None:
        for source_family in (
            "sepa_flood",
            "coal_authority",
            "hes",
            "naturescot",
            "contaminated_land",
            "tpo",
            "culverts",
            "conservation_areas",
            "greenbelt",
        ):
            self.assertIn(source_family, MIGRATION)
            self.assertIn(source_family, RUNNER)

        self.assertIn("public.constraint_source_features", RUNNER)
        self.assertIn("public.refresh_constraint_measurements_for_layer", RUNNER)
        self.assertIn("source_expansion_constraint", MIGRATION)

    def test_ela_vdl_are_not_hla_or_planning_loop_commands(self) -> None:
        self.assertIn("FUTURE_CONTEXT_FAMILIES", RUNNER)
        self.assertIn("_publish_future_context", RUNNER)
        self.assertIn("Surfaced from {source_dataset} evidence", RUNNER)
        self.assertNotIn("reconcile-catchup-scan --source-family ela", WORKFLOW)
        self.assertNotIn("reconcile-catchup-scan --source-family vdl", WORKFLOW)

    def test_os_sources_are_registered_without_local_storage(self) -> None:
        for source_key in ("os_downloads_terrain50", "os_places_api", "os_features_api"):
            self.assertIn(source_key, RUNNER)
        self.assertIn("https://api.os.uk/downloads/v1/products/Terrain50/downloads", RUNNER)
        self.assertIn('"auth_env_vars": []', RUNNER)
        self.assertIn("https://api.os.uk/search/places/v1/find", RUNNER)
        self.assertIn("https://api.os.uk/features/v1/wfs", RUNNER)
        self.assertIn("secrets.OS_PLACES_API_KEY", WORKFLOW)
        self.assertIn("secrets.OS_PLACES_API", WORKFLOW)
        self.assertNotIn("TEMP_STORAGE_PATH", RUNNER)

    def test_control_policy_spine_prioritises_title_ldp_and_settlement(self) -> None:
        priority_migration = (APP_DIR / "sql" / "046_phase_one_control_policy_priority.sql").read_text(encoding="utf-8")

        self.assertIn("source_family: title_number", MANIFEST)
        self.assertIn("source_family: ldp", MANIFEST)
        self.assertIn("source_family: settlement", MANIFEST)
        self.assertIn("source_status: live_internal_validation", MANIFEST)
        self.assertIn("source_key: ldp_spatialhub_package", MANIFEST)
        self.assertIn("local_development_plans-is", MANIFEST)
        self.assertIn("spatialhub_ckan_package_zips", MANIFEST)
        self.assertIn("commercial_use_licence_not_confirmed", MANIFEST)
        self.assertIn("source_key: nrs_settlement_boundaries", MANIFEST)
        self.assertIn("nrs_arcgis_geojson", MANIFEST)
        self.assertIn("NRS:SettlementBoundaries", MANIFEST)
        self.assertIn("maps.gov.scot/server/rest/services/NRS/NRS/MapServer/5", MANIFEST)
        self.assertIn("auth_env_vars: [BOUNDARY_AUTHKEY]", MANIFEST)
        self.assertIn("e457f123-09df-4d67-ac81-d7bb2e470499", MANIFEST)
        self.assertIn("settlement_position_overlay_not_promoted", MANIFEST)
        self.assertIn("def _ingest_settlement_boundary_family", RUNNER)
        self.assertIn("def _fetch_settlement_boundary_frame", RUNNER)
        self.assertIn("def _fetch_settlement_boundary_arcgis_frame", RUNNER)
        self.assertIn("nrs_settlement_wfs_forbidden_using_arcgis_rest", RUNNER)
        self.assertIn("NRS_REQUEST_HEADERS", RUNNER)
        self.assertIn('if "BOUNDARY_AUTHKEY" in auth_vars', RUNNER)
        self.assertIn("params.update(self._auth_params(source))", RUNNER)
        self.assertIn("def _replace_settlement_boundary_rows", RUNNER)
        self.assertIn("(1, 'title_number'", priority_migration)
        self.assertIn("'resolve-title-numbers'", priority_migration)
        self.assertIn("(2, 'ldp', 'ingest-ldp'", priority_migration)
        self.assertIn("(3, 'settlement', 'ingest-settlement-boundaries'", priority_migration)
        self.assertIn("core_policy_storage_proven_licence_gated", priority_migration)
        self.assertIn("core_policy_storage_proven_interpreter_gated", priority_migration)
        self.assertIn("landintel_ldp_site_records_source_record_uidx", priority_migration)
        self.assertIn("landintel_settlement_boundary_records_source_record_uidx", priority_migration)
        self.assertIn("control_wired_proven", RUNNER)

    def test_ckan_package_fetches_preserve_query_string_ids(self) -> None:
        registry_runner = (APP_DIR / "src" / "source_estate_registry.py").read_text(encoding="utf-8")

        self.assertIn("self.client.get(url, params=params or None)", RUNNER)
        self.assertIn("self.client.get(url, params=params or None)", registry_runner)

    def test_title_resolution_bridge_is_candidate_first_and_api_safe(self) -> None:
        self.assertIn("public.site_title_resolution_candidates", TITLE_BRIDGE_MIGRATION)
        self.assertIn("public.refresh_site_title_resolution_bridge", TITLE_BRIDGE_MIGRATION)
        self.assertIn("public.refresh_site_title_resolution_bridge_for_sites", TITLE_BRIDGE_MIGRATION)
        self.assertIn("public.extract_ros_title_number_candidate", TITLE_BRIDGE_MIGRATION)
        self.assertIn("add column if not exists title_number text", TITLE_BRIDGE_MIGRATION)
        self.assertIn("ros_cadastral_parcels_title_number_idx", TITLE_BRIDGE_MIGRATION)
        self.assertIn("parcel.title_number", TITLE_BRIDGE_MIGRATION)
        self.assertIn("site_or_toid_geometry_to_ros_cadastral", TITLE_BRIDGE_MIGRATION)
        self.assertIn("set search_path = pg_catalog, public, landintel, extensions", TITLE_BRIDGE_MIGRATION)
        self.assertIn("set_config('statement_timeout', '15min', true)", TITLE_BRIDGE_MIGRATION)
        self.assertIn("parcel.authority_name = anchor.authority_name", TITLE_BRIDGE_MIGRATION)
        self.assertIn("parcel.geometry OPERATOR(extensions.&&) anchor.geometry", TITLE_BRIDGE_MIGRATION)
        self.assertIn("'^[A-Z]{3}[0-9]{1,10}$'", TITLE_BRIDGE_MIGRATION)
        self.assertIn("'site_geometry_to_ros_cadastral'::text as match_method", TITLE_BRIDGE_MIGRATION)
        self.assertIn("site.primary_ros_parcel_id", TITLE_BRIDGE_MIGRATION)
        self.assertIn("primary_ros_parcel_candidate", TITLE_BRIDGE_MIGRATION)
        self.assertIn("needs_licensed_bridge", TITLE_BRIDGE_MIGRATION)
        self.assertIn("RoS Land Register API is title-number-first", TITLE_BRIDGE_MIGRATION)
        self.assertIn("enable row level security", TITLE_BRIDGE_MIGRATION)
        self.assertIn("revoke all on function public.refresh_site_title_resolution_bridge", TITLE_BRIDGE_MIGRATION)
        self.assertIn("def resolve_title_numbers", RUNNER)
        self.assertIn("refresh_site_title_resolution_bridge_for_sites", RUNNER)
        self.assertIn("_refresh_ros_parcel_title_numbers", RUNNER)
        self.assertIn("MIN_OPERATIONAL_AREA_ACRES", RUNNER)
        self.assertIn("min_operational_area_acres", RUNNER)
        self.assertIn("TITLE_RESOLUTION_PARCEL_TITLE_BATCH_SIZE", RUNNER)
        self.assertIn("TITLE_RESOLUTION_SITE_BATCH_SIZE", RUNNER)
        self.assertIn("ros_parcel_title_batch_completed", RUNNER)
        self.assertIn("order by id desc limit 1", RUNNER)
        self.assertNotIn("max(id)", RUNNER)
        self.assertIn("title_resolution_batch_completed", RUNNER)
        self.assertIn("json.dumps(site_location_ids)", RUNNER)
        self.assertIn("cast(:max_candidates_per_site as integer)", RUNNER)
        self.assertIn("cast(:min_overlap_sqm as numeric)", RUNNER)
        self.assertIn("cast(:site_location_ids as jsonb)", RUNNER)
        self.assertIn("title_bridge_candidates_need_licensed_bridge", RUNNER)
        self.assertIn("title_bridge_probable_titles_promoted", RUNNER)
        self.assertIn("TITLE_RESOLUTION_MAX_CANDIDATES_PER_SITE", WORKFLOW)
        self.assertIn("TITLE_RESOLUTION_MIN_OVERLAP_SQM", WORKFLOW)
        self.assertIn("TITLE_RESOLUTION_PARCEL_TITLE_BATCH_SIZE", WORKFLOW)
        self.assertIn('TITLE_RESOLUTION_PARCEL_TITLE_BATCH_SIZE: "0"', WORKFLOW)
        self.assertIn("TITLE_RESOLUTION_SITE_BATCH_SIZE", WORKFLOW)
        self.assertIn('TITLE_RESOLUTION_SITE_BATCH_SIZE: "500"', WORKFLOW)
        self.assertIn('elif [ "$SELECTED_COMMAND" = "resolve-title-numbers" ]; then', WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-title-number-control", WORKFLOW)
        self.assertIn("title_number,", LOADER)
        self.assertIn("normalized_title_number,", LOADER)
        self.assertIn("public.extract_ros_title_number_candidate(cast(:raw_attributes as jsonb), :ros_inspire_id)", LOADER)
        self.assertIn("ingest-ros-cadastral", WORKFLOW)
        self.assertIn('elif [ "$SELECTED_COMMAND" = "ingest-ros-cadastral" ]; then', WORKFLOW)
        self.assertIn("python -m src.main ingest-ros-cadastral", WORKFLOW)
        self.assertIn("src/main.py", WORKFLOW)
        self.assertIn("ros_cadastral_spatial_bridge", MANIFEST)
        self.assertIn("TOID/site geometry -> RoS cadastral parcel -> title-number candidate", MANIFEST)

    def test_site_to_ros_parcel_linking_is_batched_and_audited(self) -> None:
        self.assertIn("public.site_ros_parcel_link_candidates", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("public.refresh_site_ros_parcel_link_candidates_for_sites", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("parcel.centroid OPERATOR(extensions.&&)", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("OPERATOR(extensions.<->)", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("st_intersection", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("primary_ros_parcel_id", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("ros_cadastral_site_parcel_link", SITE_PARCEL_LINK_MIGRATION)
        self.assertIn("def link_sites_to_ros_parcels", RUNNER)
        self.assertIn("def audit_site_parcel_links", RUNNER)
        self.assertIn("refresh_site_ros_parcel_link_candidates_for_sites", RUNNER)
        self.assertIn("SITE_PARCEL_LINK_SITE_BATCH_SIZE", RUNNER)
        self.assertIn("site_parcel_link_batch_completed", RUNNER)
        self.assertIn("site_parcel_link_bridge", RUNNER)
        self.assertIn("site_parcel_link_audit", RUNNER)
        self.assertIn("- link-sites-to-ros-parcels", WORKFLOW)
        self.assertIn("- audit-site-parcel-links", WORKFLOW)
        self.assertIn('elif [ "$SELECTED_COMMAND" = "link-sites-to-ros-parcels" ]; then', WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-site-parcel-links", WORKFLOW)
        self.assertIn('SITE_PARCEL_LINK_SITE_BATCH_SIZE: "250"', WORKFLOW)

    def test_priority_zero_constraint_measurement_engine_is_batched_and_delta_based(self) -> None:
        self.assertIn("def measure_constraints", RUNNER)
        self.assertIn("def measure_constraints_debug_all_layers", RUNNER)
        self.assertIn("def audit_constraint_measurements", RUNNER)
        self.assertIn("refresh_constraint_measurements_for_layer_sites", RUNNER)
        self.assertIn("CONSTRAINT_MEASURE_SITE_BATCH_SIZE", RUNNER)
        self.assertIn("CONSTRAINT_MEASURE_MAX_BATCHES_PER_LAYER", RUNNER)
        self.assertIn("measure-constraints-debug-all-layers", RUNNER)
        self.assertIn("CONSTRAINT_MATERIAL_OVERLAP_DELTA_PCT", RUNNER)
        self.assertIn("CONSTRAINT_MATERIAL_DISTANCE_DELTA_M", RUNNER)
        self.assertIn("constraint_measurement_batch_completed", RUNNER)
        self.assertIn("constraint_measurement_debug_all_layers_completed", RUNNER)
        self.assertIn("only for material evidence-state changes", RUNNER)
        self.assertIn("cast(:layer_key_filter as text) is null", RUNNER)
        self.assertIn("cast(:source_family_filter as text) is null", RUNNER)
        self.assertIn("cast(:authority_filter as text) is null", RUNNER)
        self.assertIn("cast(:after_site_location_id as text) is null", RUNNER)
        self.assertIn("refresh_constraint_measurements_for_layer_sites", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("tmp_constraint_changed_sites", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("constraint_relationship_added", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("constraint_relationship_removed", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("overlap_pct_changed", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("nearest_distance_changed", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("site_constraint_measurement_scan_state", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("has_constraint_relationship", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("site_constraint_measurement_scan_state", RUNNER)
        self.assertIn("OPERATOR(extensions.&&)", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("st_dwithin", CONSTRAINT_ENGINE_MIGRATION.lower())
        self.assertIn("st_intersects", CONSTRAINT_ENGINE_MIGRATION.lower())
        self.assertIn("analytics.v_constraint_measurement_coverage", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("analytics.v_constraint_measurement_layer_coverage", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("missing_overlap_character_count", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("scanned_site_layer_pair_pct", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("landintel.flood_records", CONSTRAINT_ENGINE_MIGRATION)
        self.assertIn("- measure-constraints", WORKFLOW)
        self.assertIn("- measure-constraints-debug-all-layers", WORKFLOW)
        self.assertIn("- audit-constraint-measurements", WORKFLOW)
        self.assertIn('elif [ "$SELECTED_COMMAND" = "measure-constraints" ]; then', WORKFLOW)
        self.assertIn('elif [ "$SELECTED_COMMAND" = "measure-constraints-debug-all-layers" ]; then', WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-constraint-measurements", WORKFLOW)
        self.assertIn("constraint_measure_max_batches", WORKFLOW)
        self.assertIn("CONSTRAINT_MEASURE_SITE_BATCH_SIZE: ${{ inputs.constraint_measure_site_batch_size || '25' }}", WORKFLOW)
        self.assertIn("CONSTRAINT_MEASURE_MAX_BATCHES: ${{ inputs.constraint_measure_max_batches || '4' }}", WORKFLOW)
        self.assertIn('CONSTRAINT_MEASURE_MAX_BATCHES_PER_LAYER: "1"', WORKFLOW)


if __name__ == "__main__":
    unittest.main()

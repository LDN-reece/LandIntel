from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
WORKFLOW = (APP_DIR.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8")
RUNNER = (APP_DIR / "src" / "phase2_source_runner.py").read_text(encoding="utf-8")
MIGRATION = "\n".join(
    (APP_DIR / "sql" / filename).read_text(encoding="utf-8")
    for filename in (
        "051_phase2_source_estate_framework.sql",
        "052_live_proof_workflow_gates.sql",
        "054_site_prove_it_conviction_layer.sql",
        "054z_urgent_site_address_title_tables.sql",
        "055_ldn_candidate_control_screen.sql",
        "056_urgent_site_address_title_pack.sql",
    )
)
MANIFEST = (APP_DIR / "config" / "phase2_source_estate.yaml").read_text(encoding="utf-8")
CATALOG_SYNC = (APP_DIR / "src" / "source_catalog_sync.py").read_text(encoding="utf-8")
DB_HELPER = (APP_DIR / "src" / "db.py").read_text(encoding="utf-8")


class Phase2SourceEstateContractTests(unittest.TestCase):
    def test_workflow_exposes_phase2_commands_and_inputs(self) -> None:
        for command in (
            "discover-phase2-sources",
            "ingest-planning-appeals",
            "ingest-power-infrastructure",
            "ingest-amenities",
            "ingest-demographics",
            "ingest-market-context",
            "ingest-planning-documents",
            "ingest-intelligence-events",
            "ingest-companies-house",
            "ingest-fca-entities",
            "refresh-title-reviews",
            "refresh-planning-decisions",
            "audit-planning-decisions",
            "refresh-title-readiness",
            "refresh-site-market-context",
            "refresh-site-amenity-context",
            "refresh-site-demographic-context",
            "refresh-site-power-context",
            "refresh-site-abnormal-risk",
            "refresh-site-assessments",
            "refresh-site-prove-it-assessments",
            "audit-site-prove-it-assessments",
            "refresh-ldn-candidate-screen",
            "audit-ldn-candidate-screen",
            "refresh-urgent-address-title-pack",
            "audit-urgent-address-title-pack",
            "audit-full-source-estate",
        ):
            self.assertIn(f"- {command}", WORKFLOW)
            self.assertIn(command, RUNNER)
        for input_name in (
            "phase2_source_family",
            "phase2_authority",
            "phase2_batch_size",
            "phase2_runtime_minutes",
            "phase2_dry_run",
            "phase2_force_refresh",
            "phase2_audit_only",
        ):
            self.assertIn(input_name, WORKFLOW)
        self.assertIn("src/phase2_source_runner.py", WORKFLOW)
        self.assertIn("python -m src.phase2_source_runner \"$SELECTED_COMMAND\"", WORKFLOW)

    def test_source_runs_and_migrations_are_serialized(self) -> None:
        self.assertIn("concurrency:", WORKFLOW)
        self.assertEqual(WORKFLOW.count("concurrency:"), 1)
        self.assertIn("landintel-sources-${{ github.ref }}", WORKFLOW)
        self.assertIn("cancel-in-progress: false", WORKFLOW)
        self.assertIn("pg_advisory_lock", DB_HELPER)
        self.assertIn("landintel.run_migrations", DB_HELPER)
        self.assertIn("pg_advisory_unlock", DB_HELPER)

    def test_manifest_registers_all_phase2_modules_with_lifecycle_statuses(self) -> None:
        for module_key in (
            "planning_appeals",
            "planning_decisions",
            "address_property_base",
            "open_location_spine",
            "title_control",
            "power_infrastructure",
            "terrain_abnormal",
            "market_context",
            "amenities",
            "demographics",
            "planning_documents",
            "local_intelligence",
            "site_assessment",
            "site_conviction",
        ):
            self.assertIn(f"module_key: {module_key}", MANIFEST)
        for lifecycle_stage in (
            "source_registered",
            "access_confirmed",
            "raw_data_landed",
            "normalised",
            "linked_to_site",
            "measured",
            "evidence_generated",
            "signals_generated",
            "assessment_ready",
            "trusted_for_review",
        ):
            self.assertIn(f"- {lifecycle_stage}", MANIFEST)
            self.assertIn(lifecycle_stage, MIGRATION)
        for catalyst_source in (
            "source_key: overture_buildings_open",
            "source_key: overture_places_open",
            "source_key: overture_transportation_open",
            "source_key: geolytix_supermarket_points",
            "source_key: geolytix_bank_points",
            "source_key: retail_centre_boundaries_open",
            "source_key: nls_historic_os_maps",
            "source_key: forest_research_trees_outside_woodland",
            "source_key: meta_tree_canopy_height",
            "source_key: os_open_zoomstack",
            "source_key: os_open_toid",
            "source_key: os_open_built_up_areas",
        ):
            self.assertIn(catalyst_source, MANIFEST)

    def test_phase2_schema_creates_required_tables_and_views(self) -> None:
        for table_name in (
            "landintel.planning_appeal_records",
            "landintel.planning_appeal_documents",
            "landintel.site_planning_appeal_links",
            "landintel.appeal_issue_tags",
            "landintel.planning_decision_facts",
            "landintel.site_planning_decision_context",
            "landintel.title_order_workflow",
            "landintel.title_review_records",
            "landintel.ownership_control_signals",
            "landintel.corporate_owner_links",
            "landintel.corporate_entity_enrichments",
            "landintel.corporate_charge_records",
            "landintel.known_controlled_sites",
            "landintel.power_assets",
            "landintel.power_capacity_zones",
            "landintel.site_power_context",
            "landintel.infrastructure_friction_facts",
            "landintel.site_terrain_metrics",
            "landintel.site_slope_profiles",
            "landintel.site_cut_fill_risk",
            "landintel.site_ground_risk_context",
            "landintel.abnormal_cost_benchmarks",
            "landintel.site_abnormal_cost_flags",
            "landintel.market_transactions",
            "landintel.epc_property_attributes",
            "landintel.market_area_metrics",
            "landintel.site_market_context",
            "landintel.internal_comparable_evidence",
            "landintel.buyer_bid_evidence",
            "landintel.amenity_assets",
            "landintel.site_amenity_context",
            "landintel.location_strength_facts",
            "landintel.open_location_spine_features",
            "landintel.site_open_location_spine_context",
            "landintel.demographic_area_metrics",
            "landintel.site_demographic_context",
            "landintel.housing_demand_context",
            "landintel.planning_document_records",
            "landintel.planning_document_extractions",
            "landintel.section75_obligation_records",
            "landintel.site_planning_document_links",
            "landintel.intelligence_event_records",
            "landintel.site_intelligence_links",
            "landintel.settlement_intelligence_links",
            "landintel.site_prove_it_assessments",
            "landintel.site_urgent_address_candidates",
            "landintel.site_urgent_address_title_pack",
            "landintel.site_register_status_facts",
            "landintel.site_ldn_candidate_screen",
        ):
            self.assertIn(table_name, MIGRATION)
        for view_name in (
            "analytics.v_planning_appeal_coverage",
            "analytics.v_site_planning_appeal_context",
            "analytics.v_planning_decision_coverage",
            "analytics.v_site_planning_decision_context",
            "analytics.v_title_readiness",
            "analytics.v_site_control_signals",
            "analytics.v_site_power_context",
            "analytics.v_site_abnormal_risk_context",
            "analytics.v_site_market_context",
            "analytics.v_site_amenity_context",
            "analytics.v_open_location_spine_coverage",
            "analytics.v_site_open_location_spine_context",
            "analytics.v_site_demographic_context",
            "analytics.v_site_planning_document_context",
            "analytics.v_site_intelligence_events",
            "analytics.v_site_assessment_context",
            "analytics.v_site_prove_it_coverage",
            "analytics.v_site_prove_it_assessments",
            "analytics.v_urgent_site_address_title_pack",
            "analytics.v_urgent_site_address_candidates",
            "analytics.v_urgent_address_title_coverage",
            "analytics.v_register_site_development_status",
            "analytics.v_ldn_candidate_screen",
            "analytics.v_true_ldn_sites",
            "analytics.v_ldn_review_candidates",
            "analytics.v_ldn_candidate_screen_coverage",
            "analytics.v_site_register_evidence_balance",
            "analytics.v_register_origin_overconfidence",
            "analytics.v_register_sourced_sites_needing_corroboration",
            "analytics.v_landintel_source_estate_matrix",
            "analytics.v_landintel_source_lifecycle_stage_counts",
        ):
            self.assertIn(view_name, MIGRATION)
            self.assertIn("with (security_invoker = true)", MIGRATION)

    def test_source_estate_matrix_blocks_unproven_trust(self) -> None:
        trust_gate = MIGRATION.split("create or replace view analytics.v_landintel_source_estate_matrix", 1)[1]
        self.assertIn("row_count > 0", trust_gate)
        self.assertIn("linked_site_count > 0", trust_gate)
        self.assertIn("evidence_count > 0", trust_gate)
        self.assertIn("signal_count > 0", trust_gate)
        self.assertIn("assessment_ready_count > 0", trust_gate)
        self.assertIn("freshness_record_count > 0", trust_gate)
        self.assertIn("critical_limitation_blocking_review", trust_gate)
        self.assertIn("trust_block_reason", trust_gate)
        self.assertIn("freshness_status not in ('failed', 'stale', 'access_required', 'gated')", trust_gate)
        self.assertIn("current_lifecycle_stage", trust_gate)
        self.assertIn("linked_rollup.source_key = registry.source_key", trust_gate)
        self.assertIn("evidence_rollup.source_key = registry.source_key", trust_gate)
        self.assertIn("signal_rollup.source_key = registry.source_key", trust_gate)
        self.assertIn("metadata ->> 'source_key'", trust_gate)
        self.assertIn("open_location_spine_features", trust_gate)
        self.assertIn("site_open_location_spine_context", trust_gate)
        self.assertIn("source_scope_key like 'source_expansion:%%'", trust_gate)

    def test_live_proof_gate_counts_open_location_spine_rows(self) -> None:
        live_gate = (APP_DIR / "sql" / "052_live_proof_workflow_gates.sql").read_text(encoding="utf-8")
        self.assertIn(
            "union all select source_key, source_family, count(*)::bigint from landintel.open_location_spine_features",
            live_gate,
        )
        self.assertIn(
            "union all select source_key, source_family, count(*)::bigint from landintel.site_open_location_spine_context",
            live_gate,
        )
        self.assertIn(
            "union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context",
            live_gate,
        )
        self.assertIn("source_scope_key like 'source_expansion:%%'", live_gate)

    def test_prove_it_conviction_layer_gates_review_and_pursuit(self) -> None:
        self.assertIn("source_key: prove_it_conviction_layer", MANIFEST)
        self.assertIn("create table if not exists landintel.site_prove_it_assessments", MIGRATION)
        self.assertIn("site_prove_it_assessments_review_ready_check", MIGRATION)
        self.assertIn("site_prove_it_assessments_pursue_gate_check", MIGRATION)
        self.assertIn(
            "union all select source_key, source_family, count(*)::bigint from landintel.site_prove_it_assessments",
            MIGRATION,
        )
        self.assertIn(
            "union all select source_key, source_family, canonical_site_id from landintel.site_prove_it_assessments",
            MIGRATION,
        )
        self.assertIn("where review_ready_flag = true", MIGRATION)
        self.assertIn("jsonb_array_length(proof_points) > 0", MIGRATION)
        self.assertIn("cardinality(prove_it_drivers) > 0", MIGRATION)
        self.assertIn("cardinality(top_warnings) > 0", MIGRATION)
        self.assertIn("cardinality(missing_critical_evidence) > 0", MIGRATION)
        self.assertIn("title_spend_recommendation is not null", MIGRATION)
        self.assertIn("planning_journey_type is distinct from 'no_clear_journey'", MIGRATION)
        self.assertIn("constraint_position is distinct from 'terminal'", MIGRATION)
        self.assertIn("constraint_position is distinct from 'unknown'", MIGRATION)
        self.assertIn("gate_downgrade_reason", MIGRATION)
        self.assertIn("pursue_requires_measured_non_terminal_constraint_position", MIGRATION)
        self.assertIn("control_position is distinct from 'known_blocked'", MIGRATION)
        self.assertIn("def refresh_site_prove_it_assessments", RUNNER)
        self.assertIn("def audit_site_prove_it_assessments", RUNNER)
        self.assertIn("analytics.v_site_prove_it_coverage", RUNNER)
        self.assertIn("hla_ela_vdl_are_discovery_context_not_commercial_proof", RUNNER)
        self.assertIn("Register/context source requires independent corroboration", RUNNER)
        self.assertIn("commercial_weight', 'low_to_medium'", RUNNER)
        self.assertIn("corroboration_required', true", RUNNER)
        self.assertIn("coalesce(area_acres, 0) >= 4", RUNNER)
        self.assertIn("ldn_candidate_status = 'true_ldn_candidate'", RUNNER)

    def test_ldn_candidate_screen_targets_private_no_builder_without_title_certainty(self) -> None:
        self.assertIn("source_key: ldn_candidate_screen", MANIFEST)
        self.assertIn("create table if not exists landintel.site_ldn_candidate_screen", MIGRATION)
        self.assertIn("create table if not exists landintel.site_register_status_facts", MIGRATION)
        self.assertIn("ownership_not_confirmed_until_title_review", MIGRATION)
        self.assertIn("register_owner_or_developer_signal_not_legal_title", MIGRATION)
        self.assertIn("ldn_target_private_no_builder", MIGRATION)
        self.assertIn("source_role text", MIGRATION)
        self.assertIn("evidence_role text", MIGRATION)
        self.assertIn("commercial_weight text", MIGRATION)
        self.assertIn("corroboration_required boolean", MIGRATION)
        self.assertIn("register_origin_site", MIGRATION)
        self.assertIn("independent_corroboration_count", MIGRATION)
        self.assertIn("register_corroboration_status", MIGRATION)
        self.assertIn("register_origin_overconfidence_count", MIGRATION)
        self.assertIn("analytics.v_site_register_evidence_balance", MIGRATION)
        self.assertIn("analytics.v_register_origin_overconfidence", MIGRATION)
        self.assertIn("analytics.v_register_sourced_sites_needing_corroboration", MIGRATION)
        self.assertIn("HLA, ELA and VDL are discovery/context layers", MIGRATION)
        self.assertIn("Register/context source requires independent corroboration", MIGRATION)
        self.assertIn("area_acres < 4", MIGRATION)
        self.assertIn("development_progress_status in ('not_started', 'stalled', 'uneconomic', 'incomplete')", MIGRATION)
        self.assertIn("public_sector_signal", MIGRATION)
        self.assertIn("rsl_lha_charity_signal", MIGRATION)
        self.assertIn("housebuilder_developer_signal", MIGRATION)
        self.assertIn("build_started_indicator", MIGRATION)
        self.assertIn("stalled_indicator", MIGRATION)
        self.assertIn("unregistered_opportunity_signal", MIGRATION)
        self.assertIn("analytics.v_true_ldn_sites", MIGRATION)
        self.assertIn("control_blocker_type is null", MIGRATION)
        self.assertIn("def refresh_ldn_candidate_screen", RUNNER)
        self.assertIn("def audit_ldn_candidate_screen", RUNNER)
        self.assertIn("landintel.refresh_ldn_candidate_screen", RUNNER)
        self.assertIn("analytics.v_ldn_candidate_screen_coverage", RUNNER)
        self.assertIn("prove_it_coverage", RUNNER)
        self.assertIn("claim_statement", RUNNER)
        self.assertIn("jsonb_array_length(proof_points) as proof_point_count", RUNNER)
        self.assertIn("missing_critical_evidence", RUNNER)

    def test_urgent_address_title_pack_links_addresses_and_title_candidates_without_ownership_certainty(self) -> None:
        self.assertIn("source_key: urgent_address_title_pack", MANIFEST)
        self.assertIn("create table if not exists landintel.site_urgent_address_candidates", MIGRATION)
        self.assertIn("create table if not exists landintel.site_urgent_address_title_pack", MIGRATION)
        self.assertIn("landintel.refresh_urgent_site_address_title_pack", MIGRATION)
        self.assertIn("analytics.v_urgent_site_address_title_pack", MIGRATION)
        self.assertIn("analytics.v_urgent_address_title_coverage", MIGRATION)
        self.assertIn("order_title_urgently", MIGRATION)
        self.assertIn("true_ldn_candidate", MIGRATION)
        self.assertIn("possible_title_reference_identified", MIGRATION)
        self.assertIn("ownership_not_confirmed_until_title_review", MIGRATION)
        self.assertIn("title_number_candidate_not_ownership_confirmation", MIGRATION)
        self.assertIn("OS Places API radius search", RUNNER)
        self.assertIn("def refresh_urgent_address_title_pack", RUNNER)
        self.assertIn("def audit_urgent_address_title_pack", RUNNER)
        self.assertIn("def _fetch_os_places_for_urgent_sites", RUNNER)
        self.assertIn("def _os_places_endpoint_query_params", RUNNER)
        self.assertIn("endpoint_url", RUNNER)
        self.assertIn("os_places_address_fetch_failed", RUNNER)
        self.assertIn("parse_qsl", RUNNER)
        self.assertIn("OS_PLACES_API", RUNNER)
        self.assertIn("OS_PROJECT_API", RUNNER)
        self.assertIn("review_forgotten_soul", RUNNER)
        self.assertIn("review_private_candidate", RUNNER)
        self.assertIn("constraint_review_required", RUNNER)
        self.assertIn("min(self.batch_size, 25)", RUNNER)
        self.assertIn("prove_it.review_ready_flag = true", MIGRATION)
        self.assertIn("ldn_candidate_review_queue", MIGRATION)
        self.assertIn("coalesce(site.area_acres, 0) desc", MIGRATION)
        self.assertIn("refresh-urgent-address-title-pack", WORKFLOW)
        self.assertIn("audit-urgent-address-title-pack", WORKFLOW)

    def test_source_catalog_sync_uses_upserts_without_reload_deletes(self) -> None:
        self.assertNotIn("delete from landintel.source_endpoint_catalog", CATALOG_SYNC)
        self.assertNotIn("delete from landintel.entity_blueprint_catalog", CATALOG_SYNC)
        self.assertNotIn("delete from landintel.source_catalog", CATALOG_SYNC)
        self.assertGreaterEqual(CATALOG_SYNC.count("on conflict"), 3)

    def test_runner_dry_run_has_no_mutation_paths(self) -> None:
        self.assertIn("self.dry_run", RUNNER)
        self.assertIn("if not self.dry_run and not self.audit_only", RUNNER)
        self.assertIn("candidate_site_count", RUNNER)
        self.assertIn("PHASE2_DRY_RUN", RUNNER)
        self.assertIn("PHASE2_AUDIT_ONLY", RUNNER)

    def test_planning_decision_refresh_prioritises_site_linked_records(self) -> None:
        refresh_sql = RUNNER.split("def refresh_planning_decisions", 1)[1]
        refresh_sql = refresh_sql.split("def audit_planning_decisions", 1)[0]

        self.assertIn("coalesce(planning.canonical_site_id, state_row.current_canonical_site_id)", refresh_sql)
        self.assertIn(
            "(coalesce(planning.canonical_site_id, state_row.current_canonical_site_id) is not null) desc",
            refresh_sql,
        )
        self.assertIn("(existing.source_record_signature is null) desc", refresh_sql)
        self.assertIn("context_source_all", refresh_sql)
        self.assertIn("where facts.canonical_site_id is not null", refresh_sql)
        self.assertIn("where previous_signature is distinct from current_signature", refresh_sql)
        self.assertIn("insert into landintel.evidence_references", refresh_sql)
        self.assertIn("insert into landintel.site_signals", refresh_sql)

    def test_public_phase2_adapters_land_rows_before_context(self) -> None:
        for snippet in (
            "DEFAULT_UK_HPI_AVERAGE_PRICE_URL",
            "DEFAULT_SIMD_URL",
            "DEFAULT_NAPTAN_URL",
            "def ingest_market_context",
            "def ingest_demographics",
            "def _ingest_naptan_amenity_assets",
            "landintel.market_area_metrics",
            "landintel.site_market_context",
            "landintel.demographic_area_metrics",
            "landintel.site_demographic_context",
            "landintel.amenity_assets",
            "st_transform(st_setsrid(st_makepoint(:longitude, :latitude), 4326), 27700)",
            "source_limitation', 'area_level_market_context_not_site_value'",
            "source_limitation', 'area_level_context_not_buyer_demand_certainty'",
        ):
            self.assertIn(snippet, RUNNER)

        amenities_sql = RUNNER.split("def ingest_amenities", 1)[1]
        amenities_sql = amenities_sql.split("def ingest_planning_documents", 1)[0]
        self.assertIn("'constraint_source_feature_key'", amenities_sql)
        self.assertIn("'constraint_feature_metadata'", amenities_sql)
        self.assertIn("site.geometry OPERATOR(extensions.<->) asset.geometry", amenities_sql)
        self.assertIn("deleted_evidence as", amenities_sql)
        self.assertIn("deleted_signals as", amenities_sql)
        self.assertNotIn("feature.raw_payload", amenities_sql)

    def test_power_ingest_does_not_turn_metadata_into_asset_rows(self) -> None:
        power_sql = RUNNER.split("def ingest_power_infrastructure", 1)[1]
        power_sql = power_sql.split("def ingest_intelligence_events", 1)[0]
        self.assertIn("DEFAULT_SPEN_METADATA_URL", RUNNER)
        self.assertIn("metadata_catalog_record_count", power_sql)
        self.assertIn("asset_row_count", power_sql)
        self.assertIn("gated_until_asset_geometry_access_is_proven", power_sql)
        self.assertNotIn("insert into landintel.power_assets", power_sql)

    def test_proof_tests_include_counts_by_lifecycle_stage(self) -> None:
        self.assertIn("v_landintel_source_lifecycle_stage_counts", MIGRATION)
        self.assertIn("lifecycle_counts", RUNNER)
        for stage_name in (
            "source_registered",
            "access_confirmed",
            "raw_data_landed",
            "normalised",
            "linked_to_site",
            "measured",
            "evidence_generated",
            "signals_generated",
            "assessment_ready",
            "trusted_for_review",
        ):
            self.assertIn(stage_name, RUNNER)

    def test_forbidden_decision_words_are_not_used_as_output_labels(self) -> None:
        output_text = "\n".join((MANIFEST, MIGRATION, RUNNER)).lower()
        for word in ("pass", "fail", "red", "amber", "viable", "unviable", "safe", "fatal"):
            self.assertIsNone(re.search(rf"\b{word}\b", output_text), word)
        self.assertIsNone(re.search(r"\bgreen\b", output_text))


if __name__ == "__main__":
    unittest.main()

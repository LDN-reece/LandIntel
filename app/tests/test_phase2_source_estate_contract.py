from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
WORKFLOW = (APP_DIR.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8")
RUNNER = (APP_DIR / "src" / "phase2_source_runner.py").read_text(encoding="utf-8")
MIGRATION = (APP_DIR / "sql" / "051_phase2_source_estate_framework.sql").read_text(encoding="utf-8")
MANIFEST = (APP_DIR / "config" / "phase2_source_estate.yaml").read_text(encoding="utf-8")
CATALOG_SYNC = (APP_DIR / "src" / "source_catalog_sync.py").read_text(encoding="utf-8")


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
            "refresh-title-readiness",
            "refresh-site-market-context",
            "refresh-site-amenity-context",
            "refresh-site-demographic-context",
            "refresh-site-power-context",
            "refresh-site-abnormal-risk",
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

    def test_manifest_registers_all_phase2_modules_with_lifecycle_statuses(self) -> None:
        for module_key in (
            "planning_appeals",
            "title_control",
            "power_infrastructure",
            "terrain_abnormal",
            "market_context",
            "amenities",
            "demographics",
            "planning_documents",
            "local_intelligence",
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

    def test_phase2_schema_creates_required_tables_and_views(self) -> None:
        for table_name in (
            "landintel.planning_appeal_records",
            "landintel.planning_appeal_documents",
            "landintel.site_planning_appeal_links",
            "landintel.appeal_issue_tags",
            "landintel.title_order_workflow",
            "landintel.title_review_records",
            "landintel.ownership_control_signals",
            "landintel.corporate_owner_links",
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
        ):
            self.assertIn(table_name, MIGRATION)
        for view_name in (
            "analytics.v_planning_appeal_coverage",
            "analytics.v_site_planning_appeal_context",
            "analytics.v_title_readiness",
            "analytics.v_site_control_signals",
            "analytics.v_site_power_context",
            "analytics.v_site_abnormal_risk_context",
            "analytics.v_site_market_context",
            "analytics.v_site_amenity_context",
            "analytics.v_site_demographic_context",
            "analytics.v_site_planning_document_context",
            "analytics.v_site_intelligence_events",
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
        self.assertIn("freshness_status not in ('failed', 'stale', 'access_required', 'gated')", trust_gate)
        self.assertIn("current_lifecycle_stage", trust_gate)
        self.assertIn("linked_rollup.source_key = registry.source_key", trust_gate)
        self.assertIn("evidence_rollup.source_key = registry.source_key", trust_gate)
        self.assertIn("signal_rollup.source_key = registry.source_key", trust_gate)
        self.assertIn("metadata ->> 'source_key'", trust_gate)

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

from __future__ import annotations

import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
SQL_VIEWS = (APP_DIR / "sql" / "033_landintel_live_audit_views.sql").read_text(encoding="utf-8")
SQL_COMMENTS = (APP_DIR / "sql" / "034_landintel_live_audit_comments.sql").read_text(encoding="utf-8")
SQL_SMOKE = (APP_DIR / "sql_checks" / "live_audit_smoke.sql").read_text(encoding="utf-8")
RUNBOOK = (APP_DIR / "docs" / "live-source-audit-runbook.md").read_text(encoding="utf-8")
SUPABASE_DEPLOY_WORKFLOW = (
    APP_DIR.parent / ".github" / "workflows" / "codex-mcp-supabase-deploy.yml"
).read_text(encoding="utf-8")


class LiveAuditViewsContractTests(unittest.TestCase):
    def test_declares_all_required_live_views(self) -> None:
        for view_name in (
            "analytics.v_live_source_coverage",
            "analytics.v_live_site_summary",
            "analytics.v_live_site_sources",
            "analytics.v_live_ingest_audit",
            "analytics.v_live_site_readiness",
        ):
            self.assertIn(view_name, SQL_VIEWS)

    def test_live_source_coverage_contains_required_columns(self) -> None:
        for column_name in (
            "authority_name",
            "source_family",
            "source_dataset",
            "raw_record_count",
            "linked_canonical_site_count",
            "linked_source_record_count",
            "unlinked_raw_record_count",
            "last_ingest_run_id",
            "last_ingest_started_at",
            "last_ingest_finished_at",
            "last_ingest_status",
            "latest_source_update_at",
        ):
            self.assertIn(column_name, SQL_VIEWS)

    def test_live_site_summary_contains_required_columns_and_labels(self) -> None:
        for column_name in (
            "canonical_site_id",
            "site_code",
            "site_name",
            "authority_name",
            "settlement_name",
            "area_acres",
            "workflow_status",
            "surfaced_reason",
            "primary_parcel_id",
            "planning_record_count",
            "hla_record_count",
            "bgs_record_count",
            "constraint_record_count",
            "evidence_count",
            "source_families_present",
            "unresolved_alias_count",
            "latest_source_update_at",
            "latest_ingest_status",
            "data_completeness_status",
            "traceability_status",
            "site_stage",
            "review_ready_flag",
            "commercial_ready_flag",
            "missing_core_inputs",
            "why_not_ready",
        ):
            self.assertIn(column_name, SQL_VIEWS)

        for label in (
            "raw_only",
            "linked_partial",
            "linked_core",
            "linked_enriched",
            "clear",
            "review_needed",
            "unresolved_links",
            "planning_only",
            "hla_only",
            "planning_hla_linked",
            "planning_hla_bgs_linked",
        ):
            self.assertIn(label, SQL_VIEWS)

    def test_live_readiness_contains_required_columns_and_bands(self) -> None:
        for column_name in (
            "minimum_readiness_band",
            "review_ready_flag",
            "commercial_ready_flag",
            "missing_core_inputs",
            "why_not_ready",
        ):
            self.assertIn(column_name, SQL_VIEWS)

        for label in ("not_ready", "review_ready", "commercial_ready"):
            self.assertIn(label, SQL_VIEWS)

    def test_comments_cover_required_live_objects(self) -> None:
        for object_name in (
            "landintel.canonical_sites",
            "landintel.site_reference_aliases",
            "landintel.site_source_links",
            "landintel.planning_application_records",
            "landintel.hla_site_records",
            "landintel.bgs_records",
            "landintel.evidence_references",
            "landintel.v_site_traceability",
            "landintel.v_source_ingest_summary",
            "analytics.v_live_source_coverage",
            "analytics.v_live_site_summary",
            "analytics.v_live_site_sources",
            "analytics.v_live_ingest_audit",
            "analytics.v_live_site_readiness",
        ):
            self.assertIn(object_name, SQL_COMMENTS)

    def test_comments_mark_legacy_surfaces(self) -> None:
        self.assertIn("legacy", SQL_COMMENTS.lower())
        for object_name in (
            "analytics.v_frontend_authority_summary",
            "analytics.v_frontend_authority_size_summary",
            "analytics.v_ros_parcels_summary_by_authority_size",
            "analytics.v_ingest_run_summary",
        ):
            self.assertIn(object_name, SQL_COMMENTS)

    def test_smoke_sql_checks_views_columns_and_consistency(self) -> None:
        for object_name in (
            "analytics.v_live_source_coverage",
            "analytics.v_live_site_summary",
            "analytics.v_live_site_sources",
            "analytics.v_live_ingest_audit",
            "analytics.v_live_site_readiness",
            "information_schema.columns",
            "landintel.canonical_sites",
            "landintel.site_source_links",
        ):
            self.assertIn(object_name, SQL_SMOKE)

        for snippet in (
            "raw_record_count < linked_source_record_count",
            "unlinked_raw_record_count <>",
            "minimum_readiness_band",
        ):
            self.assertIn(snippet, SQL_SMOKE)

    def test_runbook_uses_supported_browse_path(self) -> None:
        for view_name in (
            "analytics.v_live_source_coverage",
            "analytics.v_live_site_summary",
            "analytics.v_live_site_sources",
            "analytics.v_live_site_readiness",
            "landintel.v_site_traceability",
        ):
            self.assertIn(view_name, RUNBOOK)

    def test_supabase_deploy_workflow_warns_when_schema_audit_is_missing(self) -> None:
        self.assertIn("path: /tmp/landintel-schema-audit", SUPABASE_DEPLOY_WORKFLOW)
        self.assertIn("if-no-files-found: warn", SUPABASE_DEPLOY_WORKFLOW)
        self.assertNotIn("if-no-files-found: error", SUPABASE_DEPLOY_WORKFLOW)

    def test_supabase_deploy_workflow_passes_boundary_authkey(self) -> None:
        self.assertIn("BOUNDARY_AUTHKEY: ${{ secrets.BOUNDARY_AUTHKEY }}", SUPABASE_DEPLOY_WORKFLOW)
        self.assertIn('echo "Missing GitHub secret: BOUNDARY_AUTHKEY"', SUPABASE_DEPLOY_WORKFLOW)


if __name__ == "__main__":
    unittest.main()

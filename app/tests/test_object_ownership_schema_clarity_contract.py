from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "062_object_ownership_schema_clarity.sql").read_text(encoding="utf-8")
MIGRATION_LOWER = MIGRATION.lower()
CLEANUP_DOC = (APP_DIR / "docs" / "schema" / "landintel_cleanup_structure_plan.md").read_text(
    encoding="utf-8"
)
CLEANUP_DOC_LOWER = CLEANUP_DOC.lower()
BGS_MANIFEST = (APP_DIR / "docs" / "schema" / "bgs_borehole_master_upload_manifest.md").read_text(
    encoding="utf-8"
)
BGS_CONTRACT = (APP_DIR / "docs" / "schema" / "bgs_borehole_master_schema_contract.md").read_text(
    encoding="utf-8"
)


class ObjectOwnershipSchemaClarityContractTests(unittest.TestCase):
    def test_migration_creates_target_schemas_idempotently(self) -> None:
        for schema_name in ("landintel_store", "landintel_sourced", "landintel_reporting"):
            self.assertIn(f"create schema if not exists {schema_name};", MIGRATION_LOWER)

    def test_migration_creates_object_ownership_registry_idempotently(self) -> None:
        self.assertIn("create table if not exists landintel_store.object_ownership_registry", MIGRATION_LOWER)
        self.assertIn("id uuid primary key default extensions.uuid_generate_v4()", MIGRATION_LOWER)
        self.assertIn("to_regprocedure('extensions.uuid_generate_v4()') is null", MIGRATION_LOWER)
        self.assertIn("schema_name text not null", MIGRATION_LOWER)
        self.assertIn("object_name text not null", MIGRATION_LOWER)
        self.assertIn("object_type text not null default 'table'", MIGRATION_LOWER)
        self.assertIn("current_status text not null", MIGRATION_LOWER)
        self.assertIn("owner_layer text not null", MIGRATION_LOWER)
        self.assertIn("metadata jsonb default '{}'::jsonb", MIGRATION_LOWER)
        self.assertIn("(schema_name, object_name, object_type)", MIGRATION_LOWER)

    def test_migration_uses_create_or_replace_view_for_compatibility_views(self) -> None:
        self.assertIn("create or replace view %s with (security_invoker = true)", MIGRATION_LOWER)
        self.assertIn("to_regclass(view_pair[2]) is not null", MIGRATION_LOWER)
        for view_name in (
            "planning_application_records",
            "planning_decision_facts",
            "hla_site_records",
            "ela_site_records",
            "vdl_site_records",
            "ldp_site_records",
            "settlement_boundary_records",
            "evidence_references",
            "site_signals",
            "ros_cadastral_parcels",
            "land_objects",
            "constraint_layer_registry",
            "constraint_source_features",
            "site_constraint_measurements",
            "site_constraint_group_summaries",
            "site_commercial_friction_facts",
            "site_title_validation",
            "site_ros_parcel_link_candidates",
            "site_title_resolution_candidates",
            "bgs_records",
            "bgs_borehole_master",
            "open_location_spine_features",
            "site_open_location_spine_context",
        ):
            self.assertIn(f"landintel_store.{view_name}", MIGRATION_LOWER)
        self.assertIn("create or replace view landintel_reporting.v_object_ownership_matrix", MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"alter\s+table\s+[^;]+\s+rename\s+", re.IGNORECASE))
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+landintel\.", re.IGNORECASE))
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+public\.", re.IGNORECASE))

    def test_migration_seeds_required_statuses_and_major_objects(self) -> None:
        for status in (
            "current_keep",
            "current_but_expensive_scale_risk",
            "duplicate_candidate",
            "known_origin_manual_bulk_upload",
            "legacy_candidate_retire",
            "repo_defined_empty_stub",
            "stub_future_module",
        ):
            self.assertIn(status, MIGRATION_LOWER)
        for object_name in (
            "canonical_sites",
            "evidence_references",
            "site_signals",
            "site_prove_it_assessments",
            "site_ldn_candidate_screen",
            "source_estate_registry",
            "planning_application_records",
            "constraint_source_features",
            "ros_cadastral_parcels",
            "land_objects",
            "site_title_validation",
            "title_review_records",
            "bgs_borehole_master",
            "open_location_spine_features",
        ):
            self.assertIn(object_name, MIGRATION_LOWER)

    def test_cleanup_doc_describes_target_schema_and_legacy_public_model(self) -> None:
        for required_phrase in (
            "landintel_store",
            "landintel_sourced",
            "landintel_reporting",
            "public",
            "legacy compatibility",
            "object ownership registry",
            "status taxonomy",
        ):
            self.assertIn(required_phrase, CLEANUP_DOC_LOWER)

    def test_cleanup_doc_records_key_audit_risks(self) -> None:
        for required_phrase in (
            "land_objects",
            "duplicate candidate",
            "bgs_borehole_master",
            "off-repo/uncontrolled risk label is corrected",
            "known-origin manual bulk upload",
            "title_review_records=0",
            "ownership remains unconfirmed",
            "constraint coverage",
            "layer-by-layer",
            "empty phase 2 tables",
        ):
            self.assertIn(required_phrase, CLEANUP_DOC_LOWER)

    def test_bgs_manifest_and_contract_capture_safe_use_and_refresh_policy(self) -> None:
        manifest_lower = BGS_MANIFEST.lower()
        contract_lower = BGS_CONTRACT.lower()
        for required_phrase in (
            "bgs single onshore borehole index",
            "manual refresh only",
            "do not",
            "not safe as final ground condition interpretation",
            "site-to-borehole proximity",
            "evidence reference",
            "site_ground_risk_context",
            "site_abnormal_cost_flags",
        ):
            self.assertIn(required_phrase, manifest_lower)
        for required_phrase in (
            "bgs_id",
            "source_upload_id",
            "geom_27700",
            "depth_m",
            "api_raw_payload",
            "bgs_borehole_master_uploads",
            "no output may claim safe ground",
        ):
            self.assertIn(required_phrase, contract_lower)


if __name__ == "__main__":
    unittest.main()

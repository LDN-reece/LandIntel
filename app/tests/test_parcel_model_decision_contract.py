from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "066_parcel_model_decision.sql").read_text(encoding="utf-8")
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "parcel_model_decision.md").read_text(encoding="utf-8")
DOC_LOWER = DOC.lower()


class ParcelModelDecisionContractTests(unittest.TestCase):
    def test_migration_creates_reporting_views(self) -> None:
        for view_name in (
            "landintel_reporting.v_parcel_model_status",
            "landintel_reporting.v_parcel_model_lightweight_overlap_audit",
        ):
            self.assertIn(f"create or replace view {view_name}", MIGRATION_LOWER)

    def test_migration_does_not_create_or_move_physical_parcel_data(self) -> None:
        self.assertNotIn("create table", MIGRATION_LOWER)
        self.assertNotIn("insert into public.ros_cadastral_parcels", MIGRATION_LOWER)
        self.assertNotIn("insert into public.land_objects", MIGRATION_LOWER)
        self.assertNotIn("update public.ros_cadastral_parcels", MIGRATION_LOWER)
        self.assertNotIn("update public.land_objects", MIGRATION_LOWER)

    def test_migration_contains_no_destructive_sql(self) -> None:
        self.assertNotIn("drop table", MIGRATION_LOWER)
        self.assertNotIn("truncate", MIGRATION_LOWER)
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"alter\s+table\s+[^;]+\s+rename\s+", re.IGNORECASE))
        self.assertNotRegex(MIGRATION_LOWER, re.compile(r"delete\s+from\s+", re.IGNORECASE))

    def test_migration_labels_parcel_truth_and_duplicate_candidate(self) -> None:
        for required_phrase in (
            "public",
            "ros_cadastral_parcels",
            "canonical ros cadastral parcel source",
            "land_objects",
            "duplicate_candidate",
            "legacy normalised parcel/object cache",
            "land_parcels",
            "legacy_candidate_retire",
            "recommended_canonical_parcel_source",
            "safe_to_retire",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_migration_documents_active_land_object_dependencies(self) -> None:
        for required_phrase in (
            "app/src/loaders/supabase_loader.py::upsert_land_objects",
            "app/src/main.py::ingest-ros-cadastral calls loader.upsert_land_objects",
            "app/sql/056_urgent_site_address_title_pack.sql",
            "public.land_object_address_links",
            "do not retire yet",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_migration_avoids_broad_spatial_overlap_audit(self) -> None:
        self.assertIn("no broad spatial overlap query is run", MIGRATION_LOWER)
        self.assertIn("exact retirement proof should use bounded source-key checks", MIGRATION_LOWER)
        self.assertNotIn("st_intersects", MIGRATION_LOWER)
        self.assertNotIn("st_intersection", MIGRATION_LOWER)
        self.assertNotIn("st_area(st_intersection", MIGRATION_LOWER)

    def test_object_registry_updates_are_non_retirement_approval(self) -> None:
        self.assertIn("landintel_store.object_ownership_registry", MIGRATION_LOWER)
        self.assertIn("duplicate_candidate_not_retire_now", MIGRATION_LOWER)
        self.assertIn("retirement-readiness pr", MIGRATION_LOWER)

    def test_docs_explain_parcel_model_decision_and_caveats(self) -> None:
        for required_phrase in (
            "public.ros_cadastral_parcels remains the canonical ros parcel source",
            "public.land_objects remains a duplicate_candidate",
            "not safe to retire now",
            "public.land_parcels remains a legacy_candidate_retire object",
            "no data is deleted",
            "no data is moved",
            "no broad spatial join is run",
            "ros parcel references are not title numbers",
            "ros parcel linkage is not ownership proof",
            "title_review_records remains the human ownership confirmation layer",
        ):
            self.assertIn(required_phrase, DOC_LOWER)

    def test_docs_answer_required_dependency_questions(self) -> None:
        for required_phrase in (
            "which table is the parcel source of truth",
            "upsert_land_objects",
            "ingest-ros-cadastral",
            "urgent_site_address_title_pack",
            "does land_objects contain anything not represented by ros parcels",
            "bounded source-key dependency check",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

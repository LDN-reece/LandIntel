import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (ROOT / "sql" / "075_drive_source_dedupe_enrichment.sql").read_text(encoding="utf-8").lower()
DOC = (ROOT / "docs" / "source_completion" / "drive_source_file_sync.md").read_text(encoding="utf-8").lower()


class DriveSourceDedupeEnrichmentContractTests(unittest.TestCase):
    def test_migration_creates_readable_use_case_and_dedupe_surfaces(self) -> None:
        self.assertIn("create table if not exists landintel_store.drive_source_use_case_catalog", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_drive_source_use_case_schema", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_drive_source_dedupe_enrichment", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_drive_source_duplicate_review_queue", MIGRATION)
        self.assertIn("create or replace view landintel_reporting.v_drive_source_enrichment_queue", MIGRATION)

    def test_dedupe_uses_existing_database_surfaces(self) -> None:
        for required_reference in (
            "landintel_store.drive_source_file_registry",
            "landintel.source_corpus_assets",
            "landintel.source_estate_registry",
            "landintel_reporting.v_source_completion_matrix",
            "drive_name_counts",
            "corpus_asset_match_count",
            "exact_drive_file_match",
            "file_name_match",
            "same_name_in_drive_count",
        ):
            self.assertIn(required_reference, MIGRATION)

    def test_duplicate_and_enrichment_statuses_are_explicit(self) -> None:
        for required_status in (
            "exact_drive_asset_duplicate",
            "file_name_matches_existing_source_asset",
            "duplicate_name_inside_drive_registry",
            "source_family_has_existing_database_coverage_review_before_upload",
            "not_known_duplicate_ready_for_enrichment",
            "known_origin_manual_upload_governance_only",
            "not_upload_ready_loose_component",
            "manual_review_required",
            "safe_to_enrich_flag",
            "do_not_upload_duplicate_without_review",
            "enrich_existing_source_family_do_not_create_new_truth",
            "create_bounded_source_completion_adapter",
        ):
            self.assertIn(required_status, MIGRATION)

    def test_use_case_catalog_links_material_drive_families_to_landintel_use_cases(self) -> None:
        for required_family in (
            "'greenbelt'",
            "'contaminated_land'",
            "'culverts'",
            "'tpo'",
            "'conservation_areas'",
            "'council_assets'",
            "'landscape'",
            "'school_catchments'",
            "'naturescot'",
            "'ros_cadastral'",
            "'hla'",
            "'ela'",
            "'vdl'",
            "'bgs'",
        ):
            self.assertIn(required_family, MIGRATION)

        for required_phrase in (
            "public.constraint_source_features / public.site_constraint_measurements",
            "landintel.ownership_control_signals",
            "public.ros_cadastral_parcels",
            "landintel.hla_site_records",
            "landintel.ela_site_records",
            "landintel.vdl_site_records",
            "known-origin manual bulk upload",
        ):
            self.assertIn(required_phrase, MIGRATION)

    def test_migration_contains_no_destructive_sql(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bdelete\s+from\s+(landintel|public|analytics|landintel_store)\b",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION), pattern)

    def test_docs_explain_duplicate_limits_and_use_case_linking(self) -> None:
        for required_phrase in (
            "duplicate and enrichment control",
            "metadata-level",
            "does not claim content-level duplicate certainty",
            "enrich the existing source family or use case",
            "must not create a duplicate truth table",
            "v_drive_source_dedupe_enrichment",
            "v_drive_source_duplicate_review_queue",
            "v_drive_source_enrichment_queue",
            "council asset register enriches public ownership exclusion",
            "bgs stays paused",
        ):
            self.assertIn(required_phrase, DOC)


if __name__ == "__main__":
    unittest.main()

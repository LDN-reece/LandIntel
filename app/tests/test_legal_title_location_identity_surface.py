from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "082_legal_title_location_identity_surface.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "legal_title_location_identity_surface.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class LegalTitleLocationIdentitySurfaceTests(unittest.TestCase):
    def test_view_is_created_as_operator_surface(self) -> None:
        self.assertIn(
            "create or replace view landintel_sourced.v_site_legal_title_location_identity",
            MIGRATION_LOWER,
        )
        for field_name in (
            "legal_title_number",
            "address",
            "local_area_or_settlement_name",
            "local_authority",
            "local_council",
        ):
            self.assertIn(field_name, MIGRATION_LOWER)

    def test_existing_sources_are_read_without_title_workflow_truth(self) -> None:
        for relation in (
            "landintel.canonical_sites",
            "landintel.site_ldn_candidate_screen",
            "landintel.site_urgent_address_title_pack",
            "landintel.site_urgent_address_candidates",
            "public.site_title_validation",
            "public.site_title_resolution_candidates",
            "public.site_ros_parcel_link_candidates",
            "public.ros_cadastral_parcels",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

        self.assertNotIn("landintel.title_order_workflow", MIGRATION_LOWER)
        self.assertNotIn("landintel.title_review_records", MIGRATION_LOWER)

    def test_title_number_is_legal_shape_only_and_sct_excluded(self) -> None:
        self.assertIn("public.is_scottish_title_number_candidate", MIGRATION_LOWER)
        self.assertIn("validation.validation_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("candidate.resolution_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("parcel_link.link_status <> 'rejected'", MIGRATION_LOWER)
        self.assertIn("legal title number not held", MIGRATION_LOWER)
        self.assertIn("sct parcel references and rejected records are excluded", MIGRATION_LOWER)

    def test_external_focus_area_is_filtered(self) -> None:
        self.assertIn("external_focus_area", MIGRATION_LOWER)
        self.assertIn("source_route", MIGRATION_LOWER)

    def test_no_destructive_sql_or_physical_table(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\bdrop\s+view\b",
            r"\btruncate\b",
            r"\bdelete\s+from\s+(landintel|public|analytics|landintel_store|landintel_sourced)\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bcreate\s+table\b",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, MIGRATION_LOWER), pattern)

    def test_docs_are_clear_and_non_workflow_framed(self) -> None:
        for required_phrase in (
            "legal title number",
            "address",
            "local area / settlement name",
            "local authority",
            "local council",
            "does not infer ownership",
            "sct references are excluded",
            "roS parcel references are not title numbers".lower(),
            "does not create a physical table",
            "external focus-area records are filtered",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

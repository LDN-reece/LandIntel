from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "083_title_area_access_context_surface.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "title_area_access_context_surface.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class TitleAreaAccessContextSurfaceTests(unittest.TestCase):
    def test_view_is_created_with_required_operator_fields(self) -> None:
        self.assertIn(
            "create or replace view landintel_sourced.v_site_title_area_access_context",
            MIGRATION_LOWER,
        )
        for field_name in (
            "title_area_acres",
            "site_area_acres",
            "developable_geometry_status",
            "developable_geometry_flag",
            "nearest_road_name",
            "nearest_road_distance_m",
            "road_access_context_status",
            "landlocked_context_risk",
        ):
            self.assertIn(field_name, MIGRATION_LOWER)

    def test_view_reuses_existing_identity_parcel_and_road_context(self) -> None:
        for relation in (
            "landintel_sourced.v_site_legal_title_location_identity",
            "landintel.canonical_sites",
            "public.site_ros_parcel_link_candidates",
            "public.ros_cadastral_parcels",
            "landintel.site_open_location_spine_context",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

        self.assertIn("public.is_scottish_title_number_candidate", MIGRATION_LOWER)
        self.assertIn("parcel_link.link_status <> 'rejected'", MIGRATION_LOWER)

    def test_access_and_developable_geometry_are_caveated(self) -> None:
        for required_phrase in (
            "road_context_not_measured",
            "potentially_landlocked_or_remote_from_road_context",
            "possible_landlocked_or_field_isolated",
            "not legal access proof",
            "not_net_developable_area",
            "not a net developable area",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_percent_literals_are_escaped_for_psycopg(self) -> None:
        self.assertIn("like '%%road%%'", MIGRATION_LOWER)
        self.assertIn("like '%%street%%'", MIGRATION_LOWER)
        self.assertIn("like '%%highway%%'", MIGRATION_LOWER)
        self.assertNotIn("like '%road%'", MIGRATION_LOWER)
        self.assertNotIn("like '%street%'", MIGRATION_LOWER)
        self.assertNotIn("like '%highway%'", MIGRATION_LOWER)

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

    def test_docs_explain_access_limits_and_next_action(self) -> None:
        for required_phrase in (
            "title area in acres",
            "site area in acres",
            "measurement-ready",
            "nearest road",
            "contextual landlocked/access risk",
            "does not prove adopted road access",
            "bounded os open roads/open-location context refresh",
            "does not replace manual access review",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

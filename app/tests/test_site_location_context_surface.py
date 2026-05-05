from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "084_site_location_context_surface.sql").read_text(
    encoding="utf-8"
)
MIGRATION_LOWER = MIGRATION.lower()
DOC = (APP_DIR / "docs" / "schema" / "site_location_context_surface.md").read_text(
    encoding="utf-8"
)
DOC_LOWER = DOC.lower()


class SiteLocationContextSurfaceTests(unittest.TestCase):
    def test_view_is_created_with_required_operator_fields(self) -> None:
        self.assertIn(
            "create or replace view landintel_sourced.v_site_location_context",
            MIGRATION_LOWER,
        )
        for field_name in (
            "legal_title_number",
            "address",
            "local_area_or_settlement_name",
            "local_authority",
            "site_area_acres",
            "title_area_acres",
            "location_context_status",
            "npf4_service_anchor_context_status",
            "service_anchor_count_within_1600m",
            "nearest_service_anchor_distance_m",
        ):
            self.assertIn(field_name, MIGRATION_LOWER)

    def test_view_reuses_existing_context_surfaces_and_open_location_spine(self) -> None:
        for relation in (
            "landintel_sourced.v_site_title_area_access_context",
            "landintel.site_open_location_spine_context",
            "landintel_store.object_ownership_registry",
        ):
            self.assertIn(relation, MIGRATION_LOWER)

    def test_location_families_and_statuses_are_operator_safe(self) -> None:
        for required_phrase in (
            "road_access",
            "education",
            "healthcare",
            "transport",
            "open_space",
            "water",
            "authority_boundary",
            "location_context_not_measured",
            "partial_location_context_measured",
            "core_location_context_measured",
            "service_anchor_context_present",
            "service_anchor_context_weak_or_absent",
        ):
            self.assertIn(required_phrase, MIGRATION_LOWER)

    def test_percent_literals_are_escaped_for_psycopg(self) -> None:
        for pattern in (
            "road",
            "school",
            "health",
            "transport",
            "greenspace",
            "water",
            "boundary",
        ):
            self.assertIn(f"like '%%{pattern}%%'", MIGRATION_LOWER)
            self.assertNotIn(f"like '%{pattern}%'", MIGRATION_LOWER)

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

    def test_docs_explain_context_limits_and_next_action(self) -> None:
        for required_phrase in (
            "legal title",
            "road/access context",
            "education",
            "healthcare",
            "transport",
            "1600m",
            "does not prove",
            "npf4 compliance",
            "bounded open-location context refresh",
            "not a commercial rejection",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
SOURCE_EXPANSION_MIGRATION = (APP_DIR / "sql" / "044_phase_one_source_expansion.sql").read_text(encoding="utf-8")


class PhaseOneMigrationSafetyContractTests(unittest.TestCase):
    def test_source_expansion_migration_avoids_psycopg_percent_placeholders(self) -> None:
        """Regression guard for the live run-migrations failure caused by raw SQL percent placeholders."""
        self.assertNotIn("Unknown constraint layer key: %", SOURCE_EXPANSION_MIGRATION)
        self.assertNotRegex(SOURCE_EXPANSION_MIGRATION, re.compile(r"raise\s+exception\s+'[^']*%", re.IGNORECASE))

    def test_source_expansion_migration_keeps_constraint_anchor_canonical(self) -> None:
        anchor_sql = SOURCE_EXPANSION_MIGRATION.split(
            "create or replace function public.constraints_site_anchor()", 1
        )[1].split("insert into public.constraint_layer_registry", 1)[0]

        self.assertIn("from landintel.canonical_sites as site", anchor_sql)
        self.assertNotIn("public.sites", anchor_sql)
        self.assertNotIn("public.site_locations", anchor_sql)


if __name__ == "__main__":
    unittest.main()

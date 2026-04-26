from __future__ import annotations

from pathlib import Path
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
ORIGINAL_MIGRATION = (APP_DIR / "sql" / "046_phase_one_site_signal_compatibility.sql").read_text(encoding="utf-8")
FIX_MIGRATION = (APP_DIR / "sql" / "047_phase_one_site_signal_trigger_fix.sql").read_text(encoding="utf-8")
CONFIDENCE_MIGRATION = (APP_DIR / "sql" / "048_phase_one_site_signal_confidence_domain.sql").read_text(encoding="utf-8")


class PhaseOneSiteSignalCompatibilityContractTests(unittest.TestCase):
    def test_phase_one_signals_generate_legacy_signal_key(self) -> None:
        self.assertIn("ensure_phase_one_site_signal_compatibility", FIX_MIGRATION)
        self.assertIn("new.signal_key", FIX_MIGRATION)
        self.assertIn("new.signal_family", FIX_MIGRATION)
        self.assertIn("new.signal_name", FIX_MIGRATION)
        self.assertIn("md5(v_key_basis)", FIX_MIGRATION)

    def test_trigger_runs_before_signal_insert_or_update(self) -> None:
        self.assertIn("before insert or update on landintel.site_signals", ORIGINAL_MIGRATION)
        self.assertIn("site_signals_phase_one_compatibility_trigger", ORIGINAL_MIGRATION)
        self.assertIn("create or replace function landintel.ensure_phase_one_site_signal_compatibility", FIX_MIGRATION)

    def test_trigger_fix_is_live_schema_safe(self) -> None:
        self.assertIn("Live-schema-safe", FIX_MIGRATION)
        self.assertNotIn("new.signal_payload", FIX_MIGRATION)
        self.assertNotIn("new.signal_source", FIX_MIGRATION)

    def test_original_optional_column_defaults_are_overridden(self) -> None:
        self.assertIn("new.signal_payload", ORIGINAL_MIGRATION)
        self.assertIn("new.signal_source", ORIGINAL_MIGRATION)
        self.assertIn("deliberately avoids optional columns", FIX_MIGRATION)

    def test_phase_one_normalized_signal_confidence_is_allowed(self) -> None:
        self.assertIn("drop constraint if exists site_signals_confidence_check", CONFIDENCE_MIGRATION)
        self.assertIn("or confidence between 0 and 1", CONFIDENCE_MIGRATION)
        self.assertIn("or confidence in (2, 3, 4, 5)", CONFIDENCE_MIGRATION)
        self.assertIn("Phase One normalized confidence", CONFIDENCE_MIGRATION)


if __name__ == "__main__":
    unittest.main()

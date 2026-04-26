from __future__ import annotations

from pathlib import Path
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "046_phase_one_site_signal_compatibility.sql").read_text(encoding="utf-8")


class PhaseOneSiteSignalCompatibilityContractTests(unittest.TestCase):
    def test_phase_one_signals_generate_legacy_signal_key(self) -> None:
        self.assertIn("ensure_phase_one_site_signal_compatibility", MIGRATION)
        self.assertIn("new.signal_key", MIGRATION)
        self.assertIn("new.signal_family", MIGRATION)
        self.assertIn("new.signal_name", MIGRATION)
        self.assertIn("md5(v_key_basis)", MIGRATION)

    def test_trigger_runs_before_signal_insert_or_update(self) -> None:
        self.assertIn("before insert or update on landintel.site_signals", MIGRATION)
        self.assertIn("site_signals_phase_one_compatibility_trigger", MIGRATION)

    def test_signal_payload_and_source_legacy_defaults_are_preserved(self) -> None:
        self.assertIn("new.signal_payload := '{}'::jsonb", MIGRATION)
        self.assertIn("new.signal_source := 'derived'", MIGRATION)


if __name__ == "__main__":
    unittest.main()

"""Unit tests for Scottish signal generation."""

from __future__ import annotations

import unittest

from src.site_engine.signal_engine import build_site_signals
from src.site_engine.source_normalisers import normalise_site_evidence

try:
    from helpers import snapshot_from_scenario
except ImportError:  # pragma: no cover - depends on unittest discovery root
    from tests.helpers import snapshot_from_scenario


class SignalEngineTest(unittest.TestCase):
    """Keep atomic Scottish signals explicit and deterministic."""

    def test_builds_source_aware_signals_for_clean_strategic_site(self) -> None:
        snapshot = snapshot_from_scenario("SC-A-001")
        evidence = normalise_site_evidence(snapshot)

        signals = {signal.key: signal for signal in build_site_signals(snapshot, evidence)}

        self.assertEqual(signals["canonical_match_confidence"].text_value, "high")
        self.assertGreaterEqual(signals["matched_reference_count"].numeric_value or 0, 3)
        self.assertEqual(signals["settlement_boundary_position"].text_value, "just_outside")
        self.assertEqual(signals["allocation_status"].text_value, "emerging")
        self.assertEqual(signals["access_status"].text_value, "confirmed")
        self.assertEqual(signals["overall_utility_burden"].text_value, "low")
        self.assertEqual(signals["buyer_depth_estimate"].text_value, "broad")
        self.assertEqual(signals["new_build_comparable_strength"].text_value, "high")

    def test_surfaces_brownfield_and_vdl_signals(self) -> None:
        snapshot = snapshot_from_scenario("SC-D-004")
        evidence = normalise_site_evidence(snapshot)

        signals = {signal.key: signal for signal in build_site_signals(snapshot, evidence)}

        self.assertTrue(signals["vdl_register_status"].bool_value)
        self.assertEqual(signals["previous_use_type"].text_value, "industrial")
        self.assertEqual(signals["bgs_investigation_intensity"].text_value, "high")
        self.assertEqual(signals["flood_risk"].text_value, "medium")


if __name__ == "__main__":
    unittest.main()

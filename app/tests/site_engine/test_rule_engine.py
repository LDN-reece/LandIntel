"""Unit tests for Scottish scoring and bucket routing."""

from __future__ import annotations

import unittest

from src.site_engine.rule_engine import apply_interpretation_rules, build_site_assessment
from src.site_engine.signal_engine import build_site_signals
from src.site_engine.source_normalisers import normalise_site_evidence

try:
    from helpers import snapshot_from_scenario
except ImportError:  # pragma: no cover - depends on unittest discovery root
    from tests.helpers import snapshot_from_scenario


class RuleEngineTest(unittest.TestCase):
    """Verify deterministic Scottish bucket routing."""

    def test_routes_stalled_re_entry_correctly(self) -> None:
        snapshot = snapshot_from_scenario("SC-C-003")
        evidence = normalise_site_evidence(snapshot)
        assessment = build_site_assessment(snapshot, evidence)

        self.assertEqual(assessment.bucket_code, "C")
        self.assertEqual(assessment.monetisation_horizon, "Short Term")
        self.assertGreaterEqual(assessment.scores["R"].value, 4)
        self.assertGreaterEqual(assessment.scores["F"].value, 3)
        self.assertNotIn("reference_reconciliation_review", assessment.review_flags)

    def test_routes_infrastructure_locked_site_correctly(self) -> None:
        snapshot = snapshot_from_scenario("SC-E-005")
        evidence = normalise_site_evidence(snapshot)
        signals = {signal.key: signal for signal in build_site_signals(snapshot, evidence)}

        assessment = build_site_assessment(snapshot, evidence)
        interpretations = apply_interpretation_rules(assessment, signals)

        self.assertEqual(assessment.bucket_code, "E")
        self.assertEqual(assessment.monetisation_horizon, "Medium Term")
        self.assertLessEqual(assessment.scores["I"].value, 2)
        self.assertIn("Dominant blocker", [item.title for item in interpretations])

    def test_triggers_hard_fail_for_dead_site(self) -> None:
        snapshot = snapshot_from_scenario("SC-F-006")
        evidence = normalise_site_evidence(snapshot)
        assessment = build_site_assessment(snapshot, evidence)

        self.assertEqual(assessment.bucket_code, "F")
        self.assertTrue(assessment.hard_fail_flags)
        self.assertTrue(assessment.human_review_required)
        self.assertIn("planning_fatality", [flag.gate for flag in assessment.hard_fail_flags])


if __name__ == "__main__":
    unittest.main()

"""Scenario tests for the six Scottish portfolio buckets."""

from __future__ import annotations

import unittest

from src.site_engine.rule_engine import build_site_assessment
from src.site_engine.signal_engine import build_site_signals
from src.site_engine.source_normalisers import normalise_site_evidence

try:
    from helpers import snapshot_from_scenario
except ImportError:  # pragma: no cover - depends on unittest discovery root
    from tests.helpers import snapshot_from_scenario


EXPECTED_BUCKETS = {
    "SC-A-001": "A",
    "SC-B-002": "B",
    "SC-C-003": "C",
    "SC-D-004": "D",
    "SC-E-005": "E",
    "SC-F-006": "F",
}


class ScottishSiteScenarioTest(unittest.TestCase):
    """Validate the seeded Scottish routing scenarios end to end."""

    def test_each_seeded_scenario_routes_to_the_expected_bucket(self) -> None:
        for site_code, expected_bucket in EXPECTED_BUCKETS.items():
            with self.subTest(site_code=site_code):
                snapshot = snapshot_from_scenario(site_code)
                evidence = normalise_site_evidence(snapshot)
                signals = {signal.key: signal for signal in build_site_signals(snapshot, evidence)}
                assessment = build_site_assessment(snapshot, evidence)
                self.assertEqual(assessment.bucket_code, expected_bucket)


if __name__ == "__main__":
    unittest.main()

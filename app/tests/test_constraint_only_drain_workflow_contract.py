import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / ".github" / "workflows" / "run-landintel-constraint-drain.yml").read_text(
    encoding="utf-8"
)
WORKFLOW_LOWER = WORKFLOW.lower()


class ConstraintOnlyDrainWorkflowContractTests(unittest.TestCase):
    def test_workflow_is_constraint_only_and_bounded(self) -> None:
        self.assertIn("Run LandIntel Constraint Drain", WORKFLOW)
        self.assertIn("constraint-measurement-drain-source-family", WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_MAX_PAIR_BATCH_SIZE: \"250\"", WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_DRAIN_MAX_BATCHES", WORKFLOW)
        self.assertIn("drain_cycles is capped at 48", WORKFLOW)
        self.assertIn("serial bounded constraint drain only", WORKFLOW)
        self.assertIn("no broad all-site/all-layer scan", WORKFLOW)
        self.assertIn("BOUNDARY_AUTHKEY: ${{ secrets.BOUNDARY_AUTHKEY }}", WORKFLOW)

    def test_workflow_supports_cohort_source_and_layer_exclusions(self) -> None:
        self.assertIn("priority_bands:", WORKFLOW)
        self.assertIn("source_families:", WORKFLOW)
        self.assertIn("exclude_layer_keys:", WORKFLOW)
        self.assertIn("CONSTRAINT_MEASURE_EXCLUDE_LAYER_KEYS", WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_SITE_PRIORITY_BAND", WORKFLOW)
        self.assertIn("CONSTRAINT_MEASURE_SOURCE_FAMILY", WORKFLOW)

    def test_workflow_avoids_unrelated_ingestion_and_context_refresh(self) -> None:
        forbidden = (
            "complete-open-data-universe",
            "refresh-site-bgs-borehole-context",
            "ingest-planning-history",
            "ingest-sepa-flood",
            "site-title-traceability-proof",
            "process-reconcile-queue",
            "refresh-site-market-context",
        )
        for phrase in forbidden:
            self.assertNotIn(phrase, WORKFLOW_LOWER)


if __name__ == "__main__":
    unittest.main()

import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parents[0]
RUNNER = (ROOT / "src" / "constraint_measurement_execution_proof.py").read_text(encoding="utf-8")
RUNNER_LOWER = RUNNER.lower()
DOC = (
    ROOT / "docs" / "constraints" / "bounded_constraint_measurement_execution_proof.md"
).read_text(encoding="utf-8")
DOC_LOWER = DOC.lower()
WORKFLOW = (REPO_ROOT / ".github" / "workflows" / "run-landintel-sources.yml").read_text(
    encoding="utf-8"
)
WORKFLOW_LOWER = WORKFLOW.lower()


class BoundedConstraintMeasurementExecutionProofContractTests(unittest.TestCase):
    def test_runner_compiles_and_exposes_command(self) -> None:
        ast.parse(RUNNER)
        self.assertIn("constraint-measurement-proof-flood-title-spend", RUNNER)
        self.assertIn("constraint_measurement_proof_flood_title_spend", RUNNER)

    def test_runner_uses_existing_truth_tables_and_finalizer_only(self) -> None:
        self.assertIn("landintel_reporting.v_constraint_priority_measurement_queue", RUNNER)
        self.assertIn("public.refresh_constraint_measurements_for_layer_sites", RUNNER)
        self.assertIn("public.site_constraint_measurements", RUNNER)
        self.assertIn("public.site_constraint_measurement_scan_state", RUNNER)
        self.assertNotIn("create table", RUNNER_LOWER)
        self.assertNotIn("site_constraint_measurements_new", RUNNER_LOWER)
        self.assertNotIn("constraint_measurement_truth", RUNNER_LOWER)

    def test_runner_is_flood_title_spend_bounded(self) -> None:
        self.assertIn("site_priority_band = 'title_spend_candidates'", RUNNER)
        self.assertIn("constraint_priority_family = 'flood'", RUNNER)
        self.assertIn("MAX_PROOF_PAIR_BATCH_SIZE = 25", RUNNER)
        self.assertIn("DEFAULT_PROOF_PAIR_BATCH_SIZE = 10", RUNNER)
        self.assertIn("limit :batch_size", RUNNER_LOWER)

    def test_runner_contains_no_destructive_sql(self) -> None:
        forbidden_patterns = (
            r"\bdrop\s+table\b",
            r"\btruncate\b",
            r"\balter\s+table\s+\S+\s+rename\b",
            r"\bdelete\s+from\s+(landintel|public|analytics|landintel_store)\b",
            r"\bcreate\s+table\s+if\s+not\s+exists\s+public\.site_constraint",
        )
        for pattern in forbidden_patterns:
            self.assertIsNone(re.search(pattern, RUNNER_LOWER), pattern)

    def test_docs_explain_operational_bounds(self) -> None:
        for required_phrase in (
            "flood only",
            "title_spend_candidates only",
            "bounded batch",
            "hard cap",
            "no broad all-site/all-layer scan",
            "scan state",
            "before/after proof",
            "does not add rag scoring",
            "pass/fail",
            "not a new constraint engine",
        ):
            self.assertIn(required_phrase, DOC_LOWER)

    def test_workflow_exposes_guarded_command(self) -> None:
        self.assertIn("- constraint-measurement-proof-flood-title-spend", WORKFLOW)
        self.assertIn('CONSTRAINT_PROOF_PAIR_BATCH_SIZE: "10"', WORKFLOW)
        self.assertIn("constraint_measurement_execution_proof.py", WORKFLOW)
        self.assertIn(
            "python -m src.constraint_measurement_execution_proof constraint-measurement-proof-flood-title-spend",
            WORKFLOW,
        )
        self.assertIn("python -m src.constraint_scaler_proof print-constraint-scaler-proof", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-constraint-measurements", WORKFLOW)


if __name__ == "__main__":
    unittest.main()

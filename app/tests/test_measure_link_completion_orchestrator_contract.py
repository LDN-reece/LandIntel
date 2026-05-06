from __future__ import annotations

from pathlib import Path
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "run-landintel-measure-link-completion.yml"
WORKFLOW_TEXT = WORKFLOW_PATH.read_text(encoding="utf-8")
WORKFLOW_LOWER = WORKFLOW_TEXT.lower()
DOC = (
    ROOT / "app" / "docs" / "source_completion" / "measure_link_completion_orchestrator.md"
).read_text(encoding="utf-8")
DOC_LOWER = DOC.lower()


class MeasureLinkCompletionOrchestratorContractTests(unittest.TestCase):
    def test_workflow_is_valid_yaml_and_dispatchable(self) -> None:
        parsed = yaml.safe_load(WORKFLOW_TEXT)
        self.assertEqual(parsed["name"], "Run LandIntel Measure Link Completion")
        self.assertIn("workflow_dispatch", parsed[True])
        inputs = parsed[True]["workflow_dispatch"]["inputs"]
        for input_name in (
            "completion_cycles",
            "title_traceability_site_batch_size",
            "constraint_site_priority_bands",
            "constraint_source_families",
            "constraint_pair_batch_size",
            "constraint_drain_max_batches",
            "constraint_drain_runtime_minutes",
            "include_open_location_context",
            "include_phase2_context_refresh",
        ):
            self.assertIn(input_name, inputs)

    def test_workflow_runs_existing_link_measure_and_audit_commands(self) -> None:
        for required_command in (
            "python -m src.source_phase_runner run-migrations",
            "python -m src.source_estate_registry register-source-estate",
            "site-title-traceability-proof",
            "site-title-traceability-proof-outside-registers",
            "reconcile-catchup-scan",
            "process-reconcile-queue",
            "refresh-affected-sites",
            "complete-open-data-universe",
            "constraint-measurement-drain-source-family",
            "audit-constraint-measurements",
            "refresh-site-amenity-context",
            "refresh-site-demographic-context",
            "refresh-site-market-context",
            "refresh-site-power-context",
            "refresh-site-abnormal-risk",
            "audit-site-dd-orchestration",
            "audit-source-completion-matrix",
        ):
            self.assertIn(required_command, WORKFLOW_TEXT)

    def test_constraint_loop_is_source_family_and_priority_band_bounded(self) -> None:
        for source_family in (
            "sepa_flood",
            "coal_authority",
            "greenbelt",
            "contaminated_land",
            "culverts",
            "hes",
            "conservation_areas",
            "naturescot",
            "tpo",
        ):
            self.assertIn(source_family, WORKFLOW_LOWER)

        for priority_band in (
            "title_spend_candidates",
            "review_queue",
            "ldn_candidate_screen",
        ):
            self.assertIn(priority_band, WORKFLOW_LOWER)

        self.assertIn('constraint_proof_site_priority_band="$priority_band"', WORKFLOW_LOWER)
        self.assertIn('constraint_measure_source_family="$source_family"', WORKFLOW_LOWER)
        self.assertIn("constraint_proof_pair_batch_size", WORKFLOW_LOWER)
        self.assertIn("constraint_proof_drain_max_batches", WORKFLOW_LOWER)
        self.assertIn("constraint_proof_drain_runtime_minutes", WORKFLOW_LOWER)
        self.assertIn("constraint_proof_allow_layer_errors", WORKFLOW_LOWER)

    def test_workflow_avoids_unbounded_or_destructive_paths(self) -> None:
        forbidden_phrases = (
            "full-ingest-planning-history",
            "full-reconcile-canonical-sites",
            "measure-constraints-debug-all-layers",
            "truncate ",
            "drop table",
            "delete from ",
            "rm -rf",
        )
        for phrase in forbidden_phrases:
            self.assertNotIn(phrase, WORKFLOW_LOWER)

        self.assertIn("timeout \"$source_expansion_command_timeout\"", WORKFLOW_LOWER)
        self.assertIn("completion_cycles is capped at 24", WORKFLOW_LOWER)
        self.assertIn("no unbounded all-site/all-layer scan", WORKFLOW_LOWER)

    def test_docs_explain_commercial_purpose_and_completion_rule(self) -> None:
        for required_phrase in (
            "legal title/location identity",
            "measured constraints",
            "open-location context",
            "source completion matrix",
            "bounded",
            "zero priority backlog",
            "not one heroic unsafe full-table scan",
            "does not create a second constraint engine",
            "does not confirm ownership",
            "continue past isolated layer timeouts",
        ):
            self.assertIn(required_phrase, DOC_LOWER)


if __name__ == "__main__":
    unittest.main()

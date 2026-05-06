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
MEASURE_LINK_WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "run-landintel-measure-link-completion.yml"
).read_text(encoding="utf-8")
FINALIZER_MIGRATION = (ROOT / "sql" / "085_constraint_finalizer_requested_anchor.sql").read_text(
    encoding="utf-8"
)
FINALIZER_MIGRATION_LOWER = FINALIZER_MIGRATION.lower()


class BoundedConstraintMeasurementExecutionProofContractTests(unittest.TestCase):
    def test_runner_compiles_and_exposes_command(self) -> None:
        ast.parse(RUNNER)
        self.assertIn("constraint-measurement-proof-flood-title-spend", RUNNER)
        self.assertIn("constraint_measurement_proof_flood_title_spend", RUNNER)
        self.assertIn("constraint-measurement-proof-title-spend-source-family", RUNNER)
        self.assertIn("constraint_measurement_proof_title_spend_source_family", RUNNER)
        self.assertIn("constraint-measurement-drain-source-family", RUNNER)
        self.assertIn("constraint_measurement_drain_source_family", RUNNER)

    def test_runner_uses_existing_truth_tables_and_finalizer_only(self) -> None:
        self.assertIn("landintel_reporting.v_constraint_priority_sites", RUNNER)
        self.assertIn("landintel_reporting.v_constraint_priority_layers", RUNNER)
        self.assertIn("public.refresh_constraint_measurements_for_layer_sites", RUNNER)
        self.assertIn("public.site_constraint_measurements", RUNNER)
        self.assertIn("public.site_constraint_measurement_scan_state", RUNNER)
        self.assertIn("exact_spatial_no_hit_prefilter", RUNNER)
        self.assertNotIn("from landintel_reporting.v_constraint_priority_measurement_queue", RUNNER)
        self.assertNotIn("create table", RUNNER_LOWER)
        self.assertNotIn("site_constraint_measurements_new", RUNNER_LOWER)
        self.assertNotIn("constraint_measurement_truth", RUNNER_LOWER)

    def test_runner_is_flood_title_spend_bounded(self) -> None:
        self.assertIn("site_priority_band = 'title_spend_candidates'", RUNNER)
        self.assertIn("constraint_priority_family = 'flood'", RUNNER)
        self.assertIn("DEFAULT_MAX_PROOF_PAIR_BATCH_SIZE = 25", RUNNER)
        self.assertIn("ABSOLUTE_MAX_PROOF_PAIR_BATCH_SIZE = 250", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_MAX_PAIR_BATCH_SIZE", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_LAYER_SITE_BATCH_SIZE", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_HEAVY_LAYER_SITE_BATCH_SIZE", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_HEAVY_LAYER_KEYS", RUNNER)
        self.assertIn("naturescot:protectedareas_sac", RUNNER)
        self.assertIn("naturescot:protectedareas_spa", RUNNER)
        self.assertIn("_measure_layer_site_chunks", RUNNER)
        self.assertIn("chunk_index", RUNNER)
        self.assertIn("DEFAULT_PROOF_PAIR_BATCH_SIZE = 10", RUNNER)
        self.assertIn("limit :batch_size", RUNNER_LOWER)

    def test_source_family_runner_fails_closed_without_filter(self) -> None:
        self.assertIn("CONSTRAINT_MEASURE_SOURCE_FAMILY", RUNNER)
        self.assertIn("CONSTRAINT_MEASURE_LAYER_KEY", RUNNER)
        self.assertIn("requires source family or layer key", RUNNER_LOWER)
        self.assertIn("This guard prevents broad all-layer runs.", RUNNER)
        self.assertIn("priority_layers.source_family = :source_family", RUNNER)
        self.assertIn("priority_layers.layer_key = :layer_key", RUNNER)
        self.assertIn("priority_sites.site_priority_band = :site_priority_band", RUNNER)
        self.assertIn("from public.constraint_source_features as feature", RUNNER)
        self.assertIn("where feature.constraint_layer_id = priority_layers.constraint_layer_id::uuid", RUNNER)

    def test_finalizer_scan_state_anchor_is_requested_site_only(self) -> None:
        self.assertIn(
            "create or replace function public.refresh_constraint_measurements_for_layer_sites",
            FINALIZER_MIGRATION_LOWER,
        )
        scan_state_sql = FINALIZER_MIGRATION_LOWER.split(
            "insert into public.site_constraint_measurement_scan_state", 1
        )[1]
        self.assertIn("from landintel.canonical_sites as site", scan_state_sql)
        self.assertIn("from unnest(p_site_location_ids) as input(site_location_id)", scan_state_sql)
        self.assertIn("requested.site_location_id = site.id::text", scan_state_sql)
        self.assertNotIn("from public.constraints_site_anchor() as anchor", scan_state_sql)
        self.assertIn("does not execute measurement", FINALIZER_MIGRATION_LOWER)

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

    def test_runner_logs_layer_errors_without_reserved_message_collision(self) -> None:
        self.assertIn("LOG_RECORD_RESERVED_KEYS", RUNNER)
        self.assertIn('"message"', RUNNER)
        self.assertIn("_safe_log_extra(proof)", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_ALLOW_LAYER_ERRORS", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_DRAIN_MAX_BATCHES", RUNNER)
        self.assertIn("CONSTRAINT_PROOF_DRAIN_RUNTIME_MINUTES", RUNNER)
        self.assertIn("ABSOLUTE_DRAIN_MAX_BATCHES = 25", RUNNER)
        self.assertIn("single_site_retry_after_chunk_timeout", RUNNER)
        self.assertIn("parent_chunk_error", RUNNER)
        self.assertIn("CONSTRAINT_MEASURE_EXCLUDE_LAYER_KEYS", RUNNER)
        self.assertIn("excluded_layer_keys", RUNNER)

    def test_docs_explain_operational_bounds(self) -> None:
        for required_phrase in (
            "flood only",
            "title_spend_candidates only",
            "source-family",
            "coal_authority",
            "bounded batch",
            "hard cap",
            "no broad all-site/all-layer scan",
            "fails closed",
            "scan state",
            "before/after proof",
            "queue correction",
            "caps it per source family",
            "heavy layer safeguard",
            "drain command",
            "constraint-measurement-drain-source-family",
            "constraint_proof_drain_max_batches",
            "constraint_proof_drain_runtime_minutes",
            "protectedareas_sac",
            "protectedareas_spa",
            "exact_spatial_no_hit_prefilter",
            "does not add rag scoring",
            "pass/fail",
            "not a new constraint engine",
        ):
            self.assertIn(required_phrase, DOC_LOWER)

    def test_workflow_exposes_guarded_command(self) -> None:
        self.assertIn("- constraint-measurement-proof-flood-title-spend", WORKFLOW)
        self.assertIn("- constraint-measurement-proof-title-spend-source-family", WORKFLOW)
        self.assertIn("- constraint-measurement-drain-source-family", WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_PAIR_BATCH_SIZE: ${{ inputs.constraint_measure_site_batch_size || '10' }}", WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_SITE_PRIORITY_BAND: ${{ inputs.constraint_measure_authority || 'title_spend_candidates' }}", WORKFLOW)
        self.assertIn('CONSTRAINT_PROOF_MAX_PAIR_BATCH_SIZE: "250"', WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_DRAIN_MAX_BATCHES: ${{ inputs.constraint_measure_max_batches || '4' }}", WORKFLOW)
        self.assertIn("CONSTRAINT_PROOF_DRAIN_RUNTIME_MINUTES: ${{ inputs.constraint_measure_runtime_minutes || '10' }}", WORKFLOW)
        self.assertIn("constraint_measure_exclude_layer_keys", WORKFLOW)
        self.assertIn("CONSTRAINT_MEASURE_EXCLUDE_LAYER_KEYS", WORKFLOW)
        self.assertIn('CONSTRAINT_PROOF_HEAVY_LAYER_SITE_BATCH_SIZE: "1"', WORKFLOW)
        self.assertIn('CONSTRAINT_PROOF_MAX_PAIR_BATCH_SIZE: "250"', MEASURE_LINK_WORKFLOW)
        self.assertIn('CONSTRAINT_PROOF_HEAVY_LAYER_SITE_BATCH_SIZE: "1"', MEASURE_LINK_WORKFLOW)
        self.assertIn(
            'CONSTRAINT_PROOF_HEAVY_LAYER_KEYS: "naturescot:protectedareas_sac,naturescot:protectedareas_spa"',
            MEASURE_LINK_WORKFLOW,
        )
        self.assertIn("constraint_measurement_execution_proof.py", WORKFLOW)
        self.assertIn(
            "python -m src.constraint_measurement_execution_proof constraint-measurement-proof-flood-title-spend",
            WORKFLOW,
        )
        self.assertIn(
            "python -m src.constraint_measurement_execution_proof constraint-measurement-proof-title-spend-source-family",
            WORKFLOW,
        )
        self.assertIn(
            "python -m src.constraint_measurement_execution_proof constraint-measurement-drain-source-family",
            WORKFLOW,
        )
        self.assertIn("python -m src.constraint_scaler_proof print-constraint-scaler-proof", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-constraint-measurements", WORKFLOW)

    def test_status_report_freezes_current_source_completion_state(self) -> None:
        status_doc = (
            ROOT / "docs" / "source_completion" / "source_completion_programme_status_2026_05_04.md"
        ).read_text(encoding="utf-8").lower()
        self.assertIn("live_complete`: 0", status_doc)
        self.assertIn("pr #2", status_doc)
        self.assertIn("pr #3", status_doc)
        self.assertIn("stale/superseded", status_doc)
        self.assertIn("constraint_measure_source_family", status_doc)
        self.assertIn("no bgs, apex, source ingestion or planning extraction", status_doc)


if __name__ == "__main__":
    unittest.main()

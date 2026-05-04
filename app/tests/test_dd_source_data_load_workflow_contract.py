import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = (ROOT.parent / ".github" / "workflows" / "run-landintel-dd-source-data-load.yml").read_text(
    encoding="utf-8"
).lower()
DOC = (ROOT / "docs" / "source_completion" / "dd_source_data_load_workflow.md").read_text(encoding="utf-8").lower()


class DDSourceDataLoadWorkflowContractTests(unittest.TestCase):
    def test_workflow_exists_and_is_data_first(self) -> None:
        self.assertIn("name: run landintel dd source data load", WORKFLOW)
        self.assertIn("dd_source_scope", WORKFLOW)
        self.assertIn("source_expansion_constraint_measure_mode: \"off\"", WORKFLOW)
        self.assertIn("source_expansion_max_measure_features: \"0\"", WORKFLOW)
        self.assertIn("open_location_spine_context_refresh_mode: \"disabled\"", WORKFLOW)
        self.assertIn("interpretation/measurement: disabled", WORKFLOW)

    def test_workflow_syncs_drive_registry_but_does_not_claim_drive_truth_ingest(self) -> None:
        self.assertIn("sync drive source registry", WORKFLOW)
        self.assertIn("sync-drive-source-manifest", WORKFLOW)
        self.assertIn("download bounded drive-ready source files as artifact", WORKFLOW)
        self.assertIn("does not ingest them into source truth tables", WORKFLOW)

    def test_workflow_covers_priority_dd_source_loaders(self) -> None:
        for command in (
            "ingest-ldp",
            "ingest-settlement-boundaries",
            "ingest-hla",
            "ingest-ela",
            "ingest-vdl",
            "ingest-sepa-flood",
            "ingest-coal-authority",
            "ingest-greenbelt",
            "ingest-contaminated-land",
            "ingest-culverts",
            "ingest-tpo",
            "ingest-conservation-areas",
            "ingest-hes-designations",
            "ingest-naturescot",
            "ingest-os-boundary-line",
            "ingest-os-open-roads",
            "ingest-os-open-rivers",
            "ingest-os-open-names",
            "ingest-os-open-greenspace",
            "ingest-os-open-built-up-areas",
            "ingest-naptan",
            "ingest-statistics-gov-scot",
            "ingest-opentopography-srtm",
            "ingest-amenities",
            "ingest-demographics",
            "ingest-market-context",
            "ingest-power-infrastructure",
            "ingest-planning-appeals",
            "ingest-planning-documents",
            "ingest-companies-house",
            "ingest-fca-entities",
        ):
            self.assertIn(command, WORKFLOW)

    def test_workflow_runs_audits_and_uploads_status(self) -> None:
        self.assertIn("audit-open-location-spine-completion", WORKFLOW)
        self.assertIn("audit-source-expansion", WORKFLOW)
        self.assertIn("audit-full-source-estate", WORKFLOW)
        self.assertIn("audit-source-completion-matrix", WORKFLOW)
        self.assertIn("dd-source-data-load-status", WORKFLOW)

    def test_docs_explain_scope_and_remaining_phases(self) -> None:
        for phrase in (
            "data-first reset",
            "registered -> data_loaded -> geometry_ready -> measured -> interpreted",
            "does not score sites",
            "do not by themselves create source truth tables",
            "layer-by-layer site measurements",
            "yes/no/affected area/percentage outputs",
        ):
            self.assertIn(phrase, DOC)


if __name__ == "__main__":
    unittest.main()

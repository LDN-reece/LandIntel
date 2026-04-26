from __future__ import annotations

from pathlib import Path
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
WORKFLOW = (APP_DIR.parent / ".github" / "workflows" / "run-landintel-sources.yml").read_text(encoding="utf-8")
RUNNER = (APP_DIR / "src" / "source_expansion_runner.py").read_text(encoding="utf-8")
PAGED_RUNNER = (APP_DIR / "src" / "source_expansion_runner_wfs_paging.py").read_text(encoding="utf-8")
MIGRATION = (APP_DIR / "sql" / "044_phase_one_source_expansion.sql").read_text(encoding="utf-8")
MANIFEST = (APP_DIR / "config" / "phase_one_source_estate.yaml").read_text(encoding="utf-8")


class SourceExpansionContractTests(unittest.TestCase):
    def test_workflow_exposes_missing_source_universe_commands(self) -> None:
        for command in (
            "audit-source-expansion",
            "ingest-ela",
            "ingest-vdl",
            "ingest-sepa-flood",
            "ingest-coal-authority",
            "ingest-hes-designations",
            "ingest-naturescot",
            "ingest-contaminated-land",
            "ingest-tpo",
            "ingest-culverts",
            "ingest-conservation-areas",
            "ingest-greenbelt",
            "ingest-os-topography",
            "ingest-os-places",
            "ingest-os-features",
            "promote-ldp-authority-source",
            "promote-settlement-authority-source",
        ):
            self.assertIn(f"- {command}", WORKFLOW)
            self.assertIn(command, RUNNER)

    def test_workflow_routes_expansion_commands_to_paged_expansion_runner(self) -> None:
        self.assertIn("src/source_expansion_runner.py", WORKFLOW)
        self.assertIn("src/source_expansion_runner_wfs_paging.py", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging \"$SELECTED_COMMAND\"", WORKFLOW)
        self.assertIn("python -m src.source_expansion_runner_wfs_paging audit-source-expansion", WORKFLOW)
        self.assertIn("is_source_expansion_ingest()", WORKFLOW)
        self.assertIn("HLA is supporting evidence only", WORKFLOW)

    def test_paged_runner_bounds_spatialhub_wfs_reads(self) -> None:
        self.assertIn("class PagedWfsSourceExpansionRunner(SourceExpansionRunner)", PAGED_RUNNER)
        self.assertIn('"maxFeatures": str(batch_limit)', PAGED_RUNNER)
        self.assertIn('params["startIndex"] = str(offset)', PAGED_RUNNER)
        self.assertIn("SOURCE_EXPANSION_PAGE_SIZE", WORKFLOW)
        self.assertIn("No usable WFS features returned", PAGED_RUNNER)

    def test_canonical_constraint_anchor_has_no_legacy_site_dependency(self) -> None:
        anchor_sql = MIGRATION.split("create or replace function public.constraints_site_anchor()", 1)[1]
        anchor_sql = anchor_sql.split("insert into public.constraint_layer_registry", 1)[0]

        self.assertIn("from landintel.canonical_sites as site", anchor_sql)
        self.assertIn("site.id::text as site_id", anchor_sql)
        self.assertIn("site.id::text as site_location_id", anchor_sql)
        self.assertNotIn("public.sites", anchor_sql)
        self.assertNotIn("public.site_locations", anchor_sql)

    def test_expansion_schema_proves_live_population_not_repo_only(self) -> None:
        for object_name in (
            "landintel.ela_site_records",
            "landintel.vdl_site_records",
            "landintel.source_expansion_events",
            "landintel.site_signals",
            "landintel.site_change_events",
            "analytics.v_phase_one_source_expansion_readiness",
            "public.refresh_constraint_measurements_for_layer",
        ):
            self.assertIn(object_name, MIGRATION)

        for proof_column in (
            "raw_or_feature_rows",
            "linked_or_measured_rows",
            "evidence_rows",
            "signal_rows",
            "change_event_rows",
            "review_output_rows",
            "live_wired_proven",
        ):
            self.assertIn(proof_column, MIGRATION)

    def test_constraint_source_families_are_seeded_and_measured(self) -> None:
        for source_family in (
            "sepa_flood",
            "coal_authority",
            "hes",
            "naturescot",
            "contaminated_land",
            "tpo",
            "culverts",
            "conservation_areas",
            "greenbelt",
        ):
            self.assertIn(source_family, MIGRATION)
            self.assertIn(source_family, RUNNER)

        self.assertIn("public.constraint_source_features", RUNNER)
        self.assertIn("public.refresh_constraint_measurements_for_layer", RUNNER)
        self.assertIn("source_expansion_constraint", MIGRATION)

    def test_ela_vdl_are_not_hla_or_planning_loop_commands(self) -> None:
        self.assertIn("FUTURE_CONTEXT_FAMILIES", RUNNER)
        self.assertIn("_publish_future_context", RUNNER)
        self.assertIn("Surfaced from {source_dataset} evidence", RUNNER)
        self.assertNotIn("reconcile-catchup-scan --source-family ela", WORKFLOW)
        self.assertNotIn("reconcile-catchup-scan --source-family vdl", WORKFLOW)

    def test_os_sources_are_registered_without_local_storage(self) -> None:
        for source_key in ("os_downloads_terrain50", "os_places_api", "os_features_api"):
            self.assertIn(source_key, RUNNER)
        self.assertIn("https://api.os.uk/downloads/v1/products", RUNNER)
        self.assertIn("https://api.os.uk/search/places/v1/find", RUNNER)
        self.assertIn("https://api.os.uk/features/v1/wfs", RUNNER)
        self.assertNotIn("TEMP_STORAGE_PATH", RUNNER)

    def test_manifest_still_declares_ldp_and_settlement_deferred(self) -> None:
        self.assertIn("source_family: ldp", MANIFEST)
        self.assertIn("source_family: settlement", MANIFEST)
        self.assertIn("source_status: explicitly_deferred", MANIFEST)
        self.assertIn("authority_adapter_not_validated", MANIFEST)
        self.assertIn("explicitly_deferred_until_authority_adapter_validated", MIGRATION)


if __name__ == "__main__":
    unittest.main()

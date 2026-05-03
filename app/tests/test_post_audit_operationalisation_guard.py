from __future__ import annotations

from pathlib import Path
import re
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
MIGRATION = (APP_DIR / "sql" / "062_object_ownership_schema_clarity.sql").read_text(encoding="utf-8")
MIGRATION_LOWER = MIGRATION.lower()
RUNBOOK = (
    APP_DIR / "docs" / "schema" / "landintel_post_audit_operationalisation_runbook.md"
).read_text(encoding="utf-8")
RUNBOOK_LOWER = RUNBOOK.lower()


class PostAuditOperationalisationGuardTests(unittest.TestCase):
    def test_runbook_keeps_post_audit_sequence_non_destructive(self) -> None:
        for required_phrase in (
            "clean, classify, prove and expose before scaling",
            "run-migrations",
            "audit-full-source-estate",
            "landintel_reporting.v_object_ownership_matrix",
            "do not re-upload",
            "ownership remains unconfirmed",
            "do not run broad all-layer scans",
            "do not move data",
            "do not retire legacy tables",
            "do not treat stubs as implemented",
        ):
            self.assertIn(required_phrase, RUNBOOK_LOWER)

    def test_runbook_preserves_business_interpretation_caveats(self) -> None:
        for required_phrase in (
            "bgs_borehole_master",
            "known_origin_manual_bulk_upload",
            "high_value_governance_incomplete",
            "not safe for piling",
            "not safe for",
            "hla, ela and vdl remain discovery/context layers",
            "register-origin site still needs corroboration",
        ):
            self.assertIn(required_phrase, RUNBOOK_LOWER)

    def test_all_compatibility_view_source_objects_are_registered(self) -> None:
        source_relations = re.findall(r"array\['landintel_store\.[^']+',\s*'([^']+)'\]", MIGRATION)
        self.assertGreater(source_relations, [])

        for source_relation in source_relations:
            schema_name, object_name = source_relation.split(".", 1)
            registry_tuple_start = f"('{schema_name}', '{object_name}',"
            self.assertIn(
                registry_tuple_start.lower(),
                MIGRATION_LOWER,
                f"{source_relation} has a compatibility view but is not seeded in object_ownership_registry.",
            )

    def test_registry_does_not_create_second_truth_system(self) -> None:
        self.assertIn("it is not a source of commercial truth", RUNBOOK_LOWER)
        self.assertIn("governance map", RUNBOOK_LOWER)
        self.assertNotIn("object_ownership_registry is the source of truth", RUNBOOK_LOWER)

    def test_bgs_status_is_locked_as_known_origin_not_orphaned(self) -> None:
        self.assertIn("'landintel', 'bgs_borehole_master'", MIGRATION_LOWER)
        self.assertIn("'known_origin_manual_bulk_upload'", MIGRATION_LOWER)
        self.assertIn("high_value_governance_incomplete", MIGRATION_LOWER)

        bgs_seed_line = next(
            (
                line
                for line in MIGRATION.splitlines()
                if "'bgs_borehole_master', 'table'" in line
            ),
            "",
        )
        self.assertTrue(bgs_seed_line)
        self.assertNotIn("orphaned_in_supabase", bgs_seed_line.lower())

    def test_legacy_and_duplicate_labels_are_not_retirement_approval(self) -> None:
        for object_name in ("land_objects", "land_parcels", "site_spatial_links"):
            self.assertIn(object_name, RUNBOOK_LOWER)
        for required_phrase in (
            "not approved for deletion",
            "dependency mapping",
            "human decision",
        ):
            self.assertIn(required_phrase, RUNBOOK_LOWER)


if __name__ == "__main__":
    unittest.main()

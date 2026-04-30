from __future__ import annotations

from pathlib import Path
import unittest


APP_DIR = Path(__file__).resolve().parents[1]
LINK_METHOD_MIGRATION = (APP_DIR / "sql" / "045_phase_one_source_link_methods.sql").read_text(encoding="utf-8")
RUNNER = (APP_DIR / "src" / "source_expansion_runner.py").read_text(encoding="utf-8")


class PhaseOneSourceLinkMethodContractTests(unittest.TestCase):
    def test_ela_vdl_direct_publish_link_method_is_allowed(self) -> None:
        self.assertIn("spatial_overlap_or_seed", RUNNER)
        self.assertIn("spatial_overlap_or_seed", LINK_METHOD_MIGRATION)
        self.assertIn("site_source_links_link_method_check", LINK_METHOD_MIGRATION)

    def test_existing_reconcile_link_methods_remain_allowed(self) -> None:
        for method in (
            "legacy_link",
            "continuity",
            "direct_reference",
            "trusted_alias",
            "spatial_dominance",
            "balanced_auto_create",
            "dominant_spatial_overlap",
            "new_source_geometry",
        ):
            self.assertIn(method, LINK_METHOD_MIGRATION)

    def test_constraint_is_not_replaced_with_unbounded_free_text(self) -> None:
        self.assertIn("link_method = any (array[", LINK_METHOD_MIGRATION)
        self.assertNotIn("btrim(link_method) <> ''", LINK_METHOD_MIGRATION)


if __name__ == "__main__":
    unittest.main()

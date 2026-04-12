"""Regression tests for loader SQL generation."""

from __future__ import annotations

import logging
import unittest
from types import SimpleNamespace

from src.loaders.supabase_loader import SupabaseLoader
from src.models.source_registry import SourceRegistryRecord


class _FakeDatabase:
    """Capture SQL calls without touching a real database."""

    def __init__(self) -> None:
        self.sql: str | None = None
        self.params_list: list[dict[str, object]] | None = None

    def execute_many(self, sql: str, params_list: list[dict[str, object]]) -> None:
        self.sql = sql
        self.params_list = params_list

    def dispose(self) -> None:
        pass


class SupabaseLoaderTest(unittest.TestCase):
    """Keep critical upsert SQL stable."""

    def setUp(self) -> None:
        settings = SimpleNamespace(
            http_timeout_seconds=5,
            supabase_service_role_key=None,
            supabase_url="https://example.supabase.co",
            supabase_audit_bucket_name="landintel-ingest-audit",
            batch_size=1000,
        )
        self.database = _FakeDatabase()
        self.loader = SupabaseLoader(settings, self.database, logging.getLogger("test.loader"))

    def tearDown(self) -> None:
        self.loader.close()

    def test_source_registry_upsert_uses_typed_null_geometry(self) -> None:
        count = self.loader.upsert_source_registry(
            [
                SourceRegistryRecord(
                    source_name="Boundary dataset",
                    source_type="dataset",
                    metadata_uuid="test:null-geometry",
                    record_json={"ok": True},
                    geographic_extent=None,
                )
            ]
        )

        self.assertEqual(count, 1)
        self.assertIsNotNone(self.database.sql)
        self.assertIsNotNone(self.database.params_list)
        assert self.database.sql is not None
        assert self.database.params_list is not None
        self.assertIn("cast(:geographic_extent_wkb as text) is null", self.database.sql)
        self.assertIn("null::geometry(multipolygon, 4326)", self.database.sql)
        self.assertIsNone(self.database.params_list[0]["geographic_extent_wkb"])


if __name__ == "__main__":
    unittest.main()

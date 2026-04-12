"""Regression tests for loader SQL generation."""

from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx

from src.loaders.supabase_loader import SupabaseLoader, SupabaseStorageClient
from src.models.source_registry import SourceRegistryRecord


class _FakeDatabase:
    """Capture SQL calls without touching a real database."""

    def __init__(self) -> None:
        self.sql: str | None = None
        self.params_list: list[dict[str, object]] | None = None
        self.executed_sql: list[str] = []
        self.executed_params: list[dict[str, object]] = []

    def execute_many(self, sql: str, params_list: list[dict[str, object]]) -> None:
        self.sql = sql
        self.params_list = params_list

    def execute(self, sql: str, params: dict[str, object] | None = None) -> None:
        self.executed_sql.append(sql)
        self.executed_params.append(params or {})

    def dispose(self) -> None:
        pass


class _FakeResponse:
    """Minimal response stub for upload tests."""

    def __init__(self, status_code: int = 200, text: str = "") -> None:
        self.status_code = status_code
        self.text = text
        self.request = httpx.Request("POST", "https://example.supabase.co/storage/v1/object")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "upload failed",
                request=self.request,
                response=httpx.Response(self.status_code, text=self.text, request=self.request),
            )


class _FakeStorageHttpClient:
    """Capture outgoing storage upload requests."""

    def __init__(self, response: _FakeResponse | None = None) -> None:
        self.response = response or _FakeResponse()
        self.calls: list[dict[str, object]] = []

    def post(self, url: str, **kwargs: object) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response

    def close(self) -> None:
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

    def test_storage_bucket_is_created_via_sql(self) -> None:
        storage = SupabaseStorageClient(
            SimpleNamespace(
                http_timeout_seconds=5,
                supabase_service_role_key="service-role",
                supabase_url="https://example.supabase.co",
                supabase_audit_bucket_name="landintel-ingest-audit",
            ),
            logging.getLogger("test.storage"),
            self.database,
        )
        try:
            storage._ensure_bucket()
        finally:
            storage.close()

        self.assertTrue(self.database.executed_sql)
        self.assertIn("insert into storage.buckets", self.database.executed_sql[0])
        self.assertEqual(
            self.database.executed_params[0],
            {"bucket_name": "landintel-ingest-audit"},
        )

    def test_storage_upload_uses_multipart_form_data(self) -> None:
        storage = SupabaseStorageClient(
            SimpleNamespace(
                http_timeout_seconds=5,
                supabase_service_role_key="service-role",
                supabase_url="https://example.supabase.co",
                supabase_audit_bucket_name="landintel-ingest-audit",
            ),
            logging.getLogger("test.storage"),
            self.database,
        )
        fake_client = _FakeStorageHttpClient()
        storage.client = fake_client

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = Path(tmp_dir) / "artifact.zip"
            local_path.write_bytes(b"PK\x03\x04test")

            try:
                uploaded_path = storage.upload_file(local_path, "ros_cadastral/run-123/downloads/artifact.zip")
            finally:
                storage.close()

        self.assertEqual(uploaded_path, "ros_cadastral/run-123/downloads/artifact.zip")
        self.assertEqual(len(fake_client.calls), 1)
        call = fake_client.calls[0]
        self.assertIn("/storage/v1/object/landintel-ingest-audit/ros_cadastral/run-123/downloads/artifact.zip", call["url"])
        self.assertIn("files", call)
        self.assertNotIn("content", call)
        headers = call["headers"]
        assert isinstance(headers, dict)
        self.assertEqual(headers["x-upsert"], "true")
        self.assertNotIn("Content-Type", headers)

    def test_storage_upload_failure_is_non_fatal(self) -> None:
        storage = SupabaseStorageClient(
            SimpleNamespace(
                http_timeout_seconds=5,
                supabase_service_role_key="service-role",
                supabase_url="https://example.supabase.co",
                supabase_audit_bucket_name="landintel-ingest-audit",
            ),
            logging.getLogger("test.storage"),
            self.database,
        )
        fake_client = _FakeStorageHttpClient(_FakeResponse(status_code=400, text="Asset Already Exists"))
        storage.client = fake_client

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = Path(tmp_dir) / "artifact.zip"
            local_path.write_bytes(b"PK\x03\x04test")

            try:
                uploaded_path = storage.upload_file(local_path, "ros_cadastral/run-123/downloads/artifact.zip")
            finally:
                storage.close()

        self.assertIsNone(uploaded_path)
        self.assertEqual(len(fake_client.calls), 1)


if __name__ == "__main__":
    unittest.main()

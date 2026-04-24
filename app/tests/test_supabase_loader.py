"""Regression tests for loader SQL generation."""

from __future__ import annotations

import logging
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - depends on local test environment
    class _HttpxRequest:
        def __init__(self, method: str, url: str) -> None:
            self.method = method
            self.url = url

    class _HttpxResponse:
        def __init__(self, status_code: int, text: str = "", request: object | None = None) -> None:
            self.status_code = status_code
            self.text = text
            self.request = request

    class _HttpxHTTPStatusError(Exception):
        def __init__(self, message: str, *, request: object | None = None, response: object | None = None) -> None:
            super().__init__(message)
            self.request = request
            self.response = response

    class _HttpxShim:
        Request = _HttpxRequest
        Response = _HttpxResponse
        HTTPError = Exception
        HTTPStatusError = _HttpxHTTPStatusError

    httpx = _HttpxShim()
try:
    import geopandas as gpd
except ModuleNotFoundError:  # pragma: no cover - depends on local test environment
    gpd = None

from src.loaders.supabase_loader import SupabaseLoader, SupabaseStorageClient
from src.models.source_registry import SourceRegistryRecord


class _FakeDatabase:
    """Capture SQL calls without touching a real database."""

    def __init__(self) -> None:
        self.sql: str | None = None
        self.params_list: list[dict[str, object]] | None = None
        self.executed_sql: list[str] = []
        self.executed_params: list[dict[str, object]] = []
        self.fetch_one_result: dict[str, object] | None = None
        self.fetch_one_results: list[dict[str, object] | None] = []
        self.fetch_all_result: list[dict[str, object]] = []

    def execute_many(self, sql: str, params_list: list[dict[str, object]]) -> None:
        self.sql = sql
        self.params_list = params_list

    def execute(self, sql: str, params: dict[str, object] | None = None) -> None:
        self.executed_sql.append(sql)
        self.executed_params.append(params or {})

    def fetch_one(self, sql: str, params: dict[str, object] | None = None) -> dict[str, object] | None:
        self.executed_sql.append(sql)
        self.executed_params.append(params or {})
        if self.fetch_one_results:
            return self.fetch_one_results.pop(0)
        return self.fetch_one_result

    def fetch_all(self, sql: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.executed_sql.append(sql)
        self.executed_params.append(params or {})
        return self.fetch_all_result

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

    def delete(self, url: str, **kwargs: object) -> _FakeResponse:
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
            audit_artifact_backend="none",
            supabase_audit_bucket_name="landintel-ingest-audit",
            supabase_working_bucket_name="landintel-working",
            supabase_archive_bucket_name="landintel-ingest-audit",
            persist_staging_rows=False,
            staging_retention_days=14,
            artifact_working_retention_days=30,
            artifact_archive_retention_days=365,
            minimum_operational_area_acres=4.0,
            mirror_land_objects=False,
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
                audit_artifact_backend="supabase",
                supabase_audit_bucket_name="landintel-ingest-audit",
                supabase_working_bucket_name="landintel-working",
                supabase_archive_bucket_name="landintel-ingest-audit",
            ),
            logging.getLogger("test.storage"),
            self.database,
        )
        try:
            storage._ensure_bucket("landintel-ingest-audit")
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
                audit_artifact_backend="supabase",
                supabase_audit_bucket_name="landintel-ingest-audit",
                supabase_working_bucket_name="landintel-working",
                supabase_archive_bucket_name="landintel-ingest-audit",
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
                uploaded_path = storage.upload_file(
                    local_path,
                    "landintel-ingest-audit",
                    "ros_cadastral/run-123/downloads/artifact.zip",
                )
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
                audit_artifact_backend="supabase",
                supabase_audit_bucket_name="landintel-ingest-audit",
                supabase_working_bucket_name="landintel-working",
                supabase_archive_bucket_name="landintel-ingest-audit",
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
                uploaded_path = storage.upload_file(
                    local_path,
                    "landintel-ingest-audit",
                    "ros_cadastral/run-123/downloads/artifact.zip",
                )
            finally:
                storage.close()

        self.assertIsNone(uploaded_path)
        self.assertEqual(len(fake_client.calls), 1)

    def test_storage_upload_is_disabled_when_backend_is_none(self) -> None:
        storage = SupabaseStorageClient(
            SimpleNamespace(
                http_timeout_seconds=5,
                supabase_service_role_key="service-role",
                supabase_url="https://example.supabase.co",
                audit_artifact_backend="none",
                supabase_audit_bucket_name="landintel-ingest-audit",
                supabase_working_bucket_name="landintel-working",
                supabase_archive_bucket_name="landintel-ingest-audit",
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
                uploaded_path = storage.upload_file(local_path, "landintel-working", "ignored/path.zip")
            finally:
                storage.close()

        self.assertIsNone(uploaded_path)
        self.assertEqual(len(fake_client.calls), 0)
        self.assertFalse(self.database.executed_sql)

    def test_refresh_cached_outputs_calls_analytics_function(self) -> None:
        self.loader.refresh_cached_outputs()

        self.assertTrue(self.database.executed_sql)
        self.assertIn("select analytics.refresh_cached_outputs()", self.database.executed_sql[0])

    def test_prune_staging_data_returns_deleted_counts(self) -> None:
        self.database.fetch_one_result = {"raw_deleted": 3, "clean_deleted": 5}

        result = self.loader.prune_staging_data()

        self.assertEqual(result, {"raw_deleted": 3, "clean_deleted": 5})
        self.assertTrue(self.database.executed_sql)
        self.assertIn("delete from staging.ros_cadastral_parcels_raw", self.database.executed_sql[0])
        self.assertEqual(self.database.executed_params[0], {"retention_days": 14})

    def test_upload_audit_artifact_registers_manifest_even_when_storage_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = Path(tmp_dir) / "artifact.zip"
            local_path.write_bytes(b"PK\x03\x04test")

            uploaded_path = self.loader.upload_audit_artifact(
                local_path,
                "ros_cadastral/run-123/downloads/artifact.zip",
                run_id="11111111-1111-1111-1111-111111111111",
                source_name="RoS",
                artifact_role="source_download",
                retention_class="archive",
            )

        self.assertIsNone(uploaded_path)
        self.assertTrue(any("insert into public.source_artifacts" in sql for sql in self.database.executed_sql))
        manifest_params = next(
            params
            for sql, params in zip(self.database.executed_sql, self.database.executed_params)
            if "insert into public.source_artifacts" in sql
        )
        self.assertEqual(manifest_params["storage_backend"], "none")
        self.assertEqual(manifest_params["retention_class"], "archive")

    def test_prune_expired_artifacts_marks_manifest_deleted(self) -> None:
        self.database.fetch_all_result = [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "storage_bucket": "landintel-working",
                "storage_path": "ros_cadastral/run-1/file.zip",
            }
        ]
        fake_client = _FakeStorageHttpClient()
        self.loader.storage.client = fake_client
        self.loader.storage.enabled = True

        result = self.loader.prune_expired_artifacts(limit=10)

        self.assertEqual(result, {"deleted": 1, "failed": 0})
        self.assertEqual(len(fake_client.calls), 1)
        self.assertIn("/storage/v1/object/landintel-working/ros_cadastral/run-1/file.zip", fake_client.calls[0]["url"])
        self.assertTrue(any("update public.source_artifacts" in sql for sql in self.database.executed_sql))

    def test_audit_operational_footprint_returns_summary(self) -> None:
        self.database.fetch_one_result = {
            "authority_count": 20,
            "source_registry_count": 18,
            "ingest_run_count": 4,
            "parcel_count": 100,
            "parcel_under_min_count": 80,
            "parcel_over_min_count": 20,
            "land_object_count": 90,
            "canonical_site_count": 35_733,
            "live_site_summary_count": 35_733,
            "live_site_readiness_count": 35_733,
            "live_site_sources_count": 2_864,
        }
        self.database.fetch_all_result = [
            {"authority_name": "Dundee City", "parcel_count": 10, "parcel_under_min_count": 8, "parcel_over_min_count": 2, "total_area_acres": 123.4}
        ]

        result = self.loader.audit_operational_footprint(minimum_area_acres=4.0)

        self.assertEqual(result["summary"]["parcel_count"], 100)
        self.assertEqual(result["summary"]["parcel_under_min_count"], 80)
        self.assertEqual(result["summary"]["canonical_site_count"], 35_733)
        self.assertEqual(result["authority_rows"][0]["authority_name"], "Dundee City")

    def test_cleanup_operational_footprint_deletes_parcels_and_land_objects(self) -> None:
        self.database.fetch_one_results = [
            {"deleted_count": 25},
            {"deleted_count": 80, "deleted_area_acres": 143.5},
        ]

        result = self.loader.cleanup_operational_footprint(
            minimum_area_acres=4.0,
            drop_land_object_mirror=True,
        )

        self.assertEqual(result["deleted_land_object_rows"], 25)
        self.assertEqual(result["deleted_parcel_rows"], 80)
        self.assertEqual(result["deleted_parcel_area_acres"], 143.5)
        self.assertTrue(any("delete from public.land_objects" in sql for sql in self.database.executed_sql))
        self.assertTrue(any("delete from public.ros_cadastral_parcels" in sql for sql in self.database.executed_sql))
        self.assertTrue(any("select analytics.refresh_cached_outputs()" in sql for sql in self.database.executed_sql))

    @unittest.skipIf(gpd is None, "geopandas is not installed in this test environment")
    def test_raw_staging_insert_is_disabled_by_default(self) -> None:
        gdf = gpd.GeoDataFrame(
            [
                {
                    "run_id": "run-1",
                    "source_name": "RoS",
                    "source_file": "county.zip",
                    "source_county": "Aberdeen",
                    "ros_inspire_id": "id-1",
                    "raw_attributes": {"ok": True},
                    "geometry": None,
                }
            ],
            geometry="geometry",
            crs="EPSG:27700",
        )

        count = self.loader.insert_raw_parcels(gdf)

        self.assertEqual(count, 0)
        self.assertFalse(self.database.sql)


if __name__ == "__main__":
    unittest.main()

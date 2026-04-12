"""Supabase Storage uploads and Postgres upserts."""

from __future__ import annotations

import json
import logging
import mimetypes
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import geopandas as gpd
import httpx

from config.settings import Settings
from src.db import Database, chunked
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.models.source_registry import SourceRegistryRecord


class SupabaseStorageClient:
    """Upload raw source artifacts into Supabase Storage."""

    def __init__(self, settings: Settings, logger: logging.Logger, database: Database) -> None:
        self.settings = settings
        self.database = database
        self.logger = logger.getChild("storage")
        self.enabled = (
            settings.audit_artifact_backend == "supabase"
            and bool(settings.supabase_service_role_key)
        )
        self._bucket_ready = False
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)

    def close(self) -> None:
        """Close the HTTP client."""

        self.client.close()

    def upload_file(self, local_path: Path, remote_path: str) -> str | None:
        """Upload a local artifact into the audit bucket."""

        if not self.enabled:
            reason = (
                "backend_disabled"
                if self.settings.audit_artifact_backend != "supabase"
                else "missing_service_role_key"
            )
            self.logger.warning(
                "storage_upload_skipped",
                extra={"reason": reason, "local_path": str(local_path)},
            )
            return None

        self._ensure_bucket()
        content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
        object_url = (
            f"{self.settings.supabase_url.rstrip('/')}/storage/v1/object/"
            f"{self.settings.supabase_audit_bucket_name}/{remote_path.lstrip('/')}"
        )
        try:
            with local_path.open("rb") as handle:
                response = self.client.post(
                    object_url,
                    headers={
                        "apikey": self.settings.supabase_service_role_key or "",
                        "Authorization": f"Bearer {self.settings.supabase_service_role_key}",
                        "x-upsert": "true",
                    },
                    files={"file": (local_path.name, handle, content_type)},
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            self.logger.warning(
                "storage_upload_failed",
                extra={
                    "local_path": str(local_path),
                    "remote_path": remote_path,
                    "status_code": exc.response.status_code,
                    "response_text": exc.response.text[:500],
                },
            )
            return None
        except (httpx.HTTPError, OSError) as exc:
            self.logger.warning(
                "storage_upload_failed",
                extra={
                    "local_path": str(local_path),
                    "remote_path": remote_path,
                    "error": str(exc),
                },
            )
            return None
        return remote_path

    def _ensure_bucket(self) -> None:
        """Create the audit bucket if needed."""

        if self._bucket_ready:
            return

        bucket_name = self.settings.supabase_audit_bucket_name
        self.database.execute(
            """
                insert into storage.buckets (id, name, public)
                values (:bucket_name, :bucket_name, false)
                on conflict (id) do nothing
            """,
            {"bucket_name": bucket_name},
        )
        self._bucket_ready = True


class SupabaseLoader:
    """Persist pipeline outputs into Supabase Postgres and Storage."""

    def __init__(self, settings: Settings, database: Database, logger: logging.Logger) -> None:
        self.settings = settings
        self.database = database
        self.logger = logger.getChild("loader")
        self.storage = SupabaseStorageClient(settings, logger, database)

    def close(self) -> None:
        """Release any client resources."""

        self.storage.close()
        self.database.dispose()

    def run_migrations(self) -> None:
        """Run the raw SQL migration set."""

        self.database.run_migrations()

    def create_ingest_run(self, record: IngestRunRecord) -> str:
        """Create an ingest run and return its UUID."""

        sql = """
            insert into public.ingest_runs (run_type, source_name, status, metadata)
            values (:run_type, :source_name, :status, cast(:metadata as jsonb))
            returning id
        """
        return str(
            self.database.scalar(
                sql,
                {
                    "run_type": record.run_type,
                    "source_name": record.source_name,
                    "status": record.status,
                    "metadata": self._json_dumps(record.metadata),
                },
            )
        )

    def update_ingest_run(self, run_id: str, update: IngestRunUpdate) -> None:
        """Update progress for an ingest run."""

        assignments: list[str] = []
        params: dict[str, Any] = {"run_id": run_id}

        if update.status is not None:
            assignments.append("status = :status")
            params["status"] = update.status
        if update.records_fetched is not None:
            assignments.append("records_fetched = :records_fetched")
            params["records_fetched"] = update.records_fetched
        if update.records_loaded is not None:
            assignments.append("records_loaded = :records_loaded")
            params["records_loaded"] = update.records_loaded
        if update.records_retained is not None:
            assignments.append("records_retained = :records_retained")
            params["records_retained"] = update.records_retained
        if update.error_message is not None:
            assignments.append("error_message = :error_message")
            params["error_message"] = update.error_message
        if update.metadata is not None:
            assignments.append("metadata = cast(:metadata as jsonb)")
            params["metadata"] = self._json_dumps(update.metadata)
        if update.finished:
            assignments.append("finished_at = now()")

        if not assignments:
            return

        sql = f"update public.ingest_runs set {', '.join(assignments)} where id = :run_id"
        self.database.execute(sql, params)

    def upload_audit_artifact(self, local_path: Path, remote_path: str) -> str | None:
        """Upload a downloaded or derived artifact to Supabase Storage."""

        return self.storage.upload_file(local_path, remote_path)

    def refresh_cached_outputs(self) -> None:
        """Refresh the precomputed analytics surfaces the frontend should read from."""

        self.database.execute("select analytics.refresh_cached_outputs();")

    def prune_staging_data(self) -> None:
        """Delete old staging rows so debug tables do not grow forever."""

        row = self.database.fetch_one(
            """
                with deleted_raw as (
                    delete from staging.ros_cadastral_parcels_raw
                    where loaded_at < now() - make_interval(days => :retention_days)
                    returning 1
                ),
                deleted_clean as (
                    delete from staging.ros_cadastral_parcels_clean
                    where cleaned_at < now() - make_interval(days => :retention_days)
                    returning 1
                )
                select
                    (select count(*) from deleted_raw) as raw_deleted,
                    (select count(*) from deleted_clean) as clean_deleted
            """,
            {"retention_days": self.settings.staging_retention_days},
        )
        return {
            "raw_deleted": int((row or {}).get("raw_deleted", 0)),
            "clean_deleted": int((row or {}).get("clean_deleted", 0)),
        }

    def upsert_source_registry(self, records: list[SourceRegistryRecord]) -> int:
        """Upsert discovered source registry entries."""

        sql = """
            insert into public.source_registry (
                source_name,
                source_type,
                publisher,
                metadata_uuid,
                endpoint_url,
                download_url,
                record_json,
                geographic_extent,
                last_seen_at
            )
            values (
                :source_name,
                :source_type,
                :publisher,
                :metadata_uuid,
                :endpoint_url,
                :download_url,
                cast(:record_json as jsonb),
                case
                    when cast(:geographic_extent_wkb as text) is null then null::geometry(multipolygon, 4326)
                    else ST_Multi(ST_GeomFromWKB(decode(cast(:geographic_extent_wkb as text), 'hex'), 4326))
                end,
                :last_seen_at
            )
            on conflict (metadata_uuid)
            where metadata_uuid is not null
            do update set
                source_name = excluded.source_name,
                source_type = excluded.source_type,
                publisher = excluded.publisher,
                endpoint_url = excluded.endpoint_url,
                download_url = excluded.download_url,
                record_json = excluded.record_json,
                geographic_extent = excluded.geographic_extent,
                last_seen_at = excluded.last_seen_at,
                updated_at = now()
        """

        payload = [
            {
                "source_name": record.source_name,
                "source_type": record.source_type,
                "publisher": record.publisher,
                "metadata_uuid": record.metadata_uuid,
                "endpoint_url": record.endpoint_url,
                "download_url": record.download_url,
                "record_json": self._json_dumps(record.record_json),
                "geographic_extent_wkb": self._geometry_hex(record.geographic_extent),
                "last_seen_at": record.last_seen_at or datetime.now(timezone.utc),
            }
            for record in records
        ]
        self.database.execute_many(sql, payload)
        return len(payload)

    def upsert_authority_aoi(self, gdf: gpd.GeoDataFrame) -> int:
        """Upsert the canonical authority AOI geometries."""

        sql = """
            insert into public.authority_aoi (
                authority_name,
                active,
                geometry,
                geometry_simplified
            )
            values (
                :authority_name,
                :active,
                ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                case
                    when cast(:geometry_simplified_wkb as text) is null then null::geometry(multipolygon, 27700)
                    else ST_Multi(ST_GeomFromWKB(decode(cast(:geometry_simplified_wkb as text), 'hex'), 27700))
                end
            )
            on conflict (authority_name)
            do update set
                active = excluded.active,
                geometry = excluded.geometry,
                geometry_simplified = excluded.geometry_simplified,
                updated_at = now()
        """

        payload = [
            {
                "authority_name": row["authority_name"],
                "active": bool(row.get("active", True)),
                "geometry_wkb": self._geometry_hex(row["geometry"]),
                "geometry_simplified_wkb": self._geometry_hex(row.get("geometry_simplified")),
            }
            for row in gdf.to_dict(orient="records")
        ]
        self.database.execute_many(sql, payload)
        return len(payload)

    def insert_raw_parcels(self, gdf: gpd.GeoDataFrame) -> int:
        """Append raw staging parcels."""

        if not self.settings.persist_staging_rows:
            self.logger.info("staging_insert_skipped", extra={"table": "staging.ros_cadastral_parcels_raw"})
            return 0

        sql = """
            insert into staging.ros_cadastral_parcels_raw (
                run_id,
                source_name,
                source_file,
                source_county,
                ros_inspire_id,
                raw_attributes,
                geometry
            )
            values (
                :run_id,
                :source_name,
                :source_file,
                :source_county,
                :ros_inspire_id,
                cast(:raw_attributes as jsonb),
                case
                    when cast(:geometry_wkb as text) is null then null::geometry(multipolygon, 27700)
                    else ST_Multi(ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700))
                end
            )
        """

        payload = [
            {
                "run_id": row["run_id"],
                "source_name": row["source_name"],
                "source_file": row.get("source_file"),
                "source_county": row.get("source_county"),
                "ros_inspire_id": row.get("ros_inspire_id"),
                "raw_attributes": self._json_dumps(row.get("raw_attributes", {})),
                "geometry_wkb": self._geometry_hex(row.get("geometry")),
            }
            for row in gdf.to_dict(orient="records")
        ]
        for batch in chunked(payload, self.settings.batch_size):
            self.database.execute_many(sql, batch)
        return len(payload)

    def insert_clean_parcels(self, gdf: gpd.GeoDataFrame) -> int:
        """Append clean staging parcels."""

        if not self.settings.persist_staging_rows:
            self.logger.info(
                "staging_insert_skipped",
                extra={"table": "staging.ros_cadastral_parcels_clean"},
            )
            return 0

        sql = """
            insert into staging.ros_cadastral_parcels_clean (
                run_id,
                source_name,
                source_file,
                source_county,
                ros_inspire_id,
                raw_attributes,
                geometry
            )
            values (
                :run_id,
                :source_name,
                :source_file,
                :source_county,
                :ros_inspire_id,
                cast(:raw_attributes as jsonb),
                case
                    when cast(:geometry_wkb as text) is null then null::geometry(multipolygon, 27700)
                    else ST_Multi(ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700))
                end
            )
        """

        payload = [
            {
                "run_id": row["run_id"],
                "source_name": row["source_name"],
                "source_file": row.get("source_file"),
                "source_county": row.get("source_county"),
                "ros_inspire_id": row.get("ros_inspire_id"),
                "raw_attributes": self._json_dumps(row.get("raw_attributes", {})),
                "geometry_wkb": self._geometry_hex(row.get("geometry")),
            }
            for row in gdf.to_dict(orient="records")
        ]
        for batch in chunked(payload, self.settings.batch_size):
            self.database.execute_many(sql, batch)
        return len(payload)

    def upsert_processed_parcels(self, gdf: gpd.GeoDataFrame) -> int:
        """Upsert processed parcels into the production table."""

        sql = """
            insert into public.ros_cadastral_parcels (
                ros_inspire_id,
                authority_name,
                source_county,
                geometry,
                centroid,
                area_sqm,
                area_ha,
                area_acres,
                size_bucket,
                size_bucket_label,
                source_name,
                source_file,
                raw_attributes
            )
            values (
                :ros_inspire_id,
                :authority_name,
                :source_county,
                ST_Multi(ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700)),
                case
                    when cast(:centroid_wkb as text) is null then null::geometry(point, 27700)
                    else ST_GeomFromWKB(decode(cast(:centroid_wkb as text), 'hex'), 27700)
                end,
                :area_sqm,
                :area_ha,
                :area_acres,
                :size_bucket,
                :size_bucket_label,
                :source_name,
                :source_file,
                cast(:raw_attributes as jsonb)
            )
            on conflict (ros_inspire_id, authority_name)
            where ros_inspire_id is not null
            do update set
                source_county = excluded.source_county,
                geometry = excluded.geometry,
                centroid = excluded.centroid,
                area_sqm = excluded.area_sqm,
                area_ha = excluded.area_ha,
                area_acres = excluded.area_acres,
                size_bucket = excluded.size_bucket,
                size_bucket_label = excluded.size_bucket_label,
                source_name = excluded.source_name,
                source_file = excluded.source_file,
                raw_attributes = excluded.raw_attributes,
                updated_at = now()
        """

        payload = [
            self._processed_row_to_mapping(row)
            for row in gdf.to_dict(orient="records")
            if row.get("ros_inspire_id")
        ]
        for batch in chunked(payload, self.settings.batch_size):
            self.database.execute_many(sql, batch)
        return len(payload)

    def upsert_land_objects(self, gdf: gpd.GeoDataFrame) -> int:
        """Upsert normalised cross-source land objects."""

        sql = """
            insert into public.land_objects (
                object_type,
                source_system,
                source_key,
                authority_name,
                geometry,
                area_sqm,
                area_ha,
                area_acres,
                size_bucket,
                size_bucket_label,
                metadata
            )
            values (
                :object_type,
                :source_system,
                :source_key,
                :authority_name,
                ST_GeomFromWKB(decode(:geometry_wkb, 'hex'), 27700),
                :area_sqm,
                :area_ha,
                :area_acres,
                :size_bucket,
                :size_bucket_label,
                cast(:metadata as jsonb)
            )
            on conflict (source_system, source_key, authority_name)
            do update set
                geometry = excluded.geometry,
                area_sqm = excluded.area_sqm,
                area_ha = excluded.area_ha,
                area_acres = excluded.area_acres,
                size_bucket = excluded.size_bucket,
                size_bucket_label = excluded.size_bucket_label,
                metadata = excluded.metadata,
                updated_at = now()
        """

        payload = []
        for row in gdf.to_dict(orient="records"):
            ros_inspire_id = row.get("ros_inspire_id")
            authority_name = row.get("authority_name")
            source_key = f"{ros_inspire_id}:{authority_name}" if ros_inspire_id else None
            if not source_key:
                continue
            payload.append(
                {
                    "object_type": "ros_cadastral_parcel",
                    "source_system": "ros_inspire",
                    "source_key": source_key,
                    "authority_name": authority_name,
                    "geometry_wkb": self._geometry_hex(row["geometry"]),
                    "area_sqm": float(row["area_sqm"]),
                    "area_ha": float(row["area_ha"]),
                    "area_acres": float(row["area_acres"]),
                    "size_bucket": row["size_bucket"],
                    "size_bucket_label": row["size_bucket_label"],
                    "metadata": self._json_dumps(
                        {
                            "ros_inspire_id": ros_inspire_id,
                            "source_name": row["source_name"],
                            "source_file": row.get("source_file"),
                            "source_county": row.get("source_county"),
                            "raw_attributes": row.get("raw_attributes", {}),
                        }
                    ),
                }
            )

        for batch in chunked(payload, self.settings.batch_size):
            self.database.execute_many(sql, batch)
        return len(payload)

    def fetch_active_authorities(self) -> gpd.GeoDataFrame:
        """Return active authority AOIs from PostGIS."""

        sql = """
            select authority_name, active, geometry, geometry_simplified
            from public.authority_aoi
            where active = true
            order by authority_name
        """
        return self.database.read_geodataframe(sql)

    def _processed_row_to_mapping(self, row: dict[str, Any]) -> dict[str, Any]:
        """Convert a processed row into a database insert mapping."""

        return {
            "ros_inspire_id": row["ros_inspire_id"],
            "authority_name": row["authority_name"],
            "source_county": row.get("source_county"),
            "geometry_wkb": self._geometry_hex(row["geometry"]),
            "centroid_wkb": self._geometry_hex(row["centroid"]),
            "area_sqm": float(row["area_sqm"]),
            "area_ha": float(row["area_ha"]),
            "area_acres": float(row["area_acres"]),
            "size_bucket": row["size_bucket"],
            "size_bucket_label": row["size_bucket_label"],
            "source_name": row["source_name"],
            "source_file": row.get("source_file"),
            "raw_attributes": self._json_dumps(row.get("raw_attributes", {})),
        }

    def _geometry_hex(self, geometry: Any) -> str | None:
        """Return geometry WKB hex or None."""

        if geometry is None or getattr(geometry, "is_empty", True):
            return None
        return geometry.wkb_hex

    def _json_dumps(self, payload: Any) -> str:
        """Serialise JSON payloads for SQL inserts."""

        return json.dumps(payload, default=self._json_default, ensure_ascii=False)

    def _json_default(self, value: Any) -> Any:
        """Handle common non-JSON Python types."""

        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, Path):
            return str(value)
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return str(value)
        return str(value)

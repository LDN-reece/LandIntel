"""Supabase Storage uploads and Postgres upserts."""

from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

try:
    import geopandas as gpd
except ModuleNotFoundError:  # pragma: no cover - depends on local runtime image
    gpd = None

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - depends on local runtime image
    httpx = None

from config.settings import Settings
from src.db import Database, chunked
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.models.source_artifacts import SourceArtifactRecord
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
        self._ready_buckets: set[str] = set()
        self.client = (
            httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)
            if httpx is not None
            else None
        )

    def close(self) -> None:
        """Close the HTTP client."""

        if self.client is not None:
            self.client.close()

    def upload_file(self, local_path: Path, bucket_name: str, remote_path: str) -> str | None:
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

        if self.client is None:
            self.logger.warning(
                "storage_upload_skipped",
                extra={"reason": "http_client_unavailable", "local_path": str(local_path)},
            )
            return None

        self._ensure_bucket(bucket_name)
        content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
        object_url = (
            f"{self.settings.supabase_url.rstrip('/')}/storage/v1/object/"
            f"{bucket_name}/{remote_path.lstrip('/')}"
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

    def delete_file(self, bucket_name: str, remote_path: str) -> bool:
        """Delete a stored artifact once its retention window has expired."""

        if not self.enabled:
            return False
        if self.client is None:
            self.logger.warning(
                "storage_delete_skipped",
                extra={"reason": "http_client_unavailable", "bucket_name": bucket_name, "remote_path": remote_path},
            )
            return False

        object_url = (
            f"{self.settings.supabase_url.rstrip('/')}/storage/v1/object/"
            f"{bucket_name}/{remote_path.lstrip('/')}"
        )
        try:
            response = self.client.delete(
                object_url,
                headers={
                    "apikey": self.settings.supabase_service_role_key or "",
                    "Authorization": f"Bearer {self.settings.supabase_service_role_key}",
                },
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            self.logger.warning(
                "storage_delete_failed",
                extra={
                    "bucket_name": bucket_name,
                    "remote_path": remote_path,
                    "status_code": exc.response.status_code,
                    "response_text": exc.response.text[:500],
                },
            )
            return False
        except httpx.HTTPError as exc:
            self.logger.warning(
                "storage_delete_failed",
                extra={
                    "bucket_name": bucket_name,
                    "remote_path": remote_path,
                    "error": str(exc),
                },
            )
            return False
        return True

    def _ensure_bucket(self, bucket_name: str) -> None:
        """Create the required bucket if needed."""

        if bucket_name in self._ready_buckets:
            return

        self.database.execute(
            """
                insert into storage.buckets (id, name, public)
                values (:bucket_name, :bucket_name, false)
                on conflict (id) do nothing
            """,
            {"bucket_name": bucket_name},
        )
        self._ready_buckets.add(bucket_name)


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

    def upload_audit_artifact(
        self,
        local_path: Path,
        remote_path: str,
        *,
        run_id: str | None = None,
        source_name: str = "unknown_source",
        artifact_role: str = "source_download",
        authority_name: str | None = None,
        retention_class: str = "working",
        source_url: str | None = None,
        source_reference: str | None = None,
        metadata: dict[str, Any] | None = None,
        storage_bucket: str | None = None,
    ) -> str | None:
        """Upload a downloaded or derived artifact and always register its audit manifest."""

        bucket_name = storage_bucket or self._resolve_bucket_name(retention_class)
        uploaded_path = self.storage.upload_file(local_path, bucket_name, remote_path)
        record = SourceArtifactRecord(
            ingest_run_id=run_id,
            source_name=source_name,
            authority_name=authority_name,
            artifact_role=artifact_role,
            artifact_format=self._infer_artifact_format(local_path),
            local_path=str(local_path),
            source_url=source_url,
            source_reference=source_reference,
            storage_backend="supabase" if uploaded_path else "none",
            storage_bucket=bucket_name if uploaded_path else None,
            storage_path=uploaded_path,
            content_sha256=self._hash_file(local_path),
            size_bytes=local_path.stat().st_size if local_path.exists() else None,
            retention_class=retention_class,
            expires_at=self._calculate_artifact_expiry(retention_class),
            metadata={
                "storage_upload_requested": self.settings.audit_artifact_backend == "supabase",
                "storage_upload_succeeded": bool(uploaded_path),
                **(metadata or {}),
            },
        )
        self.record_source_artifact(record)
        return uploaded_path

    def record_source_artifact(self, record: SourceArtifactRecord) -> None:
        """Persist metadata about a raw or derived artifact without storing the file in Postgres."""

        self.database.execute(
            """
                insert into public.source_artifacts (
                    ingest_run_id,
                    source_name,
                    authority_name,
                    artifact_role,
                    artifact_format,
                    local_path,
                    source_url,
                    source_reference,
                    storage_backend,
                    storage_bucket,
                    storage_path,
                    content_sha256,
                    size_bytes,
                    row_count_estimate,
                    retention_class,
                    expires_at,
                    metadata
                )
                values (
                    cast(:ingest_run_id as uuid),
                    :source_name,
                    :authority_name,
                    :artifact_role,
                    :artifact_format,
                    :local_path,
                    :source_url,
                    :source_reference,
                    :storage_backend,
                    :storage_bucket,
                    :storage_path,
                    :content_sha256,
                    :size_bytes,
                    :row_count_estimate,
                    :retention_class,
                    :expires_at,
                    cast(:metadata as jsonb)
                )
            """,
            {
                "ingest_run_id": record.ingest_run_id,
                "source_name": record.source_name,
                "authority_name": record.authority_name,
                "artifact_role": record.artifact_role,
                "artifact_format": record.artifact_format,
                "local_path": record.local_path,
                "source_url": record.source_url,
                "source_reference": record.source_reference,
                "storage_backend": record.storage_backend,
                "storage_bucket": record.storage_bucket,
                "storage_path": record.storage_path,
                "content_sha256": record.content_sha256,
                "size_bytes": record.size_bytes,
                "row_count_estimate": record.row_count_estimate,
                "retention_class": record.retention_class,
                "expires_at": record.expires_at,
                "metadata": self._json_dumps(record.metadata),
            },
        )

    def refresh_cached_outputs(self) -> None:
        """Refresh the precomputed analytics surfaces the frontend should read from."""

        self.database.execute("select analytics.refresh_cached_outputs();")

    def prune_staging_data(self) -> dict[str, int]:
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

    def prune_expired_artifacts(self, limit: int = 200) -> dict[str, int]:
        """Delete expired Supabase Storage objects while keeping manifest rows for audit."""

        candidates = self.database.fetch_all(
            """
                select id, storage_bucket, storage_path
                from public.source_artifacts
                where deleted_at is null
                  and storage_backend = 'supabase'
                  and storage_bucket is not null
                  and storage_path is not null
                  and expires_at is not null
                  and expires_at <= now()
                order by expires_at asc
                limit :limit
            """,
            {"limit": limit},
        )
        deleted = 0
        failed = 0
        for row in candidates:
            bucket_name = str(row["storage_bucket"])
            storage_path = str(row["storage_path"])
            if self.storage.delete_file(bucket_name, storage_path):
                deleted += 1
                self.database.execute(
                    """
                        update public.source_artifacts
                        set deleted_at = now(),
                            metadata = coalesce(metadata, '{}'::jsonb) || cast(:metadata as jsonb)
                        where id = cast(:artifact_id as uuid)
                    """,
                    {
                        "artifact_id": row["id"],
                        "metadata": self._json_dumps({"deleted_reason": "retention_expired"}),
                    },
                )
            else:
                failed += 1
        return {"deleted": deleted, "failed": failed}

    def audit_operational_footprint(self, *, minimum_area_acres: float) -> dict[str, Any]:
        """Return a lightweight snapshot of the live operational footprint."""

        summary = self.database.fetch_one(
            """
                select
                    (select count(*) from public.authority_aoi where active = true) as authority_count,
                    (select count(*) from public.source_registry) as source_registry_count,
                    (select count(*) from public.ingest_runs) as ingest_run_count,
                    (select count(*) from public.ros_cadastral_parcels) as parcel_count,
                    (select count(*) from public.ros_cadastral_parcels where coalesce(area_acres, 0) < :minimum_area_acres) as parcel_under_min_count,
                    (select count(*) from public.ros_cadastral_parcels where coalesce(area_acres, 0) >= :minimum_area_acres) as parcel_over_min_count,
                    (select count(*) from public.land_objects) as land_object_count,
                    (select count(*) from landintel.canonical_sites) as canonical_site_count,
                    (select count(*) from analytics.v_live_site_summary) as live_site_summary_count,
                    (select count(*) from analytics.v_live_site_readiness) as live_site_readiness_count,
                    (select count(*) from analytics.v_live_site_sources) as live_site_sources_count
            """,
            {"minimum_area_acres": minimum_area_acres},
        ) or {}
        authority_rows = self.database.fetch_all(
            """
                select
                    authority_name,
                    count(*)::bigint as parcel_count,
                    count(*) filter (where coalesce(area_acres, 0) < :minimum_area_acres)::bigint as parcel_under_min_count,
                    count(*) filter (where coalesce(area_acres, 0) >= :minimum_area_acres)::bigint as parcel_over_min_count,
                    round(coalesce(sum(area_acres), 0), 3) as total_area_acres
                from public.ros_cadastral_parcels
                group by authority_name
                order by parcel_count desc, authority_name asc
            """,
            {"minimum_area_acres": minimum_area_acres},
        )
        return {
            "minimum_area_acres": minimum_area_acres,
            "summary": {
                key: int(value) if isinstance(value, (int, bool)) else value
                for key, value in summary.items()
            },
            "authority_rows": authority_rows,
        }

    def cleanup_operational_footprint(
        self,
        *,
        minimum_area_acres: float,
        drop_land_object_mirror: bool,
    ) -> dict[str, Any]:
        """Delete legacy low-value operational rows from older parcel-led runs."""

        deleted_land_objects = 0
        if drop_land_object_mirror:
            land_object_row = self.database.fetch_one(
                """
                    with deleted as (
                        delete from public.land_objects as lo
                        where lo.source_system in ('ros_cadastral', 'ros_inspire')
                          and not exists (
                              select 1
                              from landintel.canonical_sites as s
                              where s.metadata ->> 'primary_land_object_id' = lo.id::text
                          )
                        returning 1
                    )
                    select count(*) as deleted_count
                    from deleted
                """
            ) or {}
            deleted_land_objects = int(land_object_row.get("deleted_count", 0))

        parcel_row = self.database.fetch_one(
            """
                with deleted as (
                    delete from public.ros_cadastral_parcels as rp
                    where coalesce(rp.area_acres, 0) < :minimum_area_acres
                      and not exists (
                          select 1 from landintel.canonical_sites as s where s.primary_ros_parcel_id = rp.id
                      )
                      and not exists (
                          select 1
                          from public.site_spatial_links as spatial
                          where spatial.linked_record_table = 'public.ros_cadastral_parcels'
                            and spatial.linked_record_id = rp.id::text
                      )
                    returning area_acres
                )
                select
                    count(*) as deleted_count,
                    coalesce(sum(area_acres), 0) as deleted_area_acres
                from deleted
            """,
            {"minimum_area_acres": minimum_area_acres},
        ) or {}

        self.refresh_cached_outputs()

        return {
            "minimum_area_acres": minimum_area_acres,
            "drop_land_object_mirror": drop_land_object_mirror,
            "deleted_parcel_rows": int(parcel_row.get("deleted_count", 0)),
            "deleted_parcel_area_acres": float(parcel_row.get("deleted_area_acres", 0) or 0),
            "deleted_land_object_rows": deleted_land_objects,
        }

    def _resolve_bucket_name(self, retention_class: str) -> str:
        """Route short-lived artifacts to a working bucket and long-lived copies to archive."""

        if retention_class == "archive":
            return getattr(
                self.settings,
                "supabase_archive_bucket_name",
                getattr(self.settings, "supabase_audit_bucket_name", "landintel-ingest-audit"),
            )
        return getattr(self.settings, "supabase_working_bucket_name", "landintel-working")

    def _calculate_artifact_expiry(self, retention_class: str) -> datetime | None:
        """Return the expiry timestamp for an artifact retention class."""

        now = datetime.now(timezone.utc)
        if retention_class == "permanent":
            return None
        if retention_class == "archive":
            days = getattr(self.settings, "artifact_archive_retention_days", 365)
        else:
            days = getattr(self.settings, "artifact_working_retention_days", 30)
        return now + timedelta(days=max(days, 0))

    def _infer_artifact_format(self, local_path: Path) -> str | None:
        """Infer a coarse artifact format from the file suffix."""

        suffix = local_path.suffix.lower().lstrip(".")
        return suffix or None

    def _hash_file(self, local_path: Path) -> str | None:
        """Compute a SHA-256 digest so audit manifests survive even if the file is later deleted."""

        if not local_path.exists():
            return None

        digest = hashlib.sha256()
        with local_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

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
                title_number,
                normalized_title_number,
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
                public.extract_ros_title_number_candidate(cast(:raw_attributes as jsonb), :ros_inspire_id),
                public.normalize_site_title_number(public.extract_ros_title_number_candidate(cast(:raw_attributes as jsonb), :ros_inspire_id)),
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
                title_number = excluded.title_number,
                normalized_title_number = excluded.normalized_title_number,
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
            if not ros_inspire_id:
                continue
            payload.append(
                {
                    "object_type": "ros_cadastral_parcel",
                    "source_system": "ros_cadastral",
                    "source_key": ros_inspire_id,
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

    def insert_bgs_boreholes_raw_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        ingest_run_id: str,
        source_archive_name: str,
        source_file_name: str,
        source_snapshot_date: date,
    ) -> int:
        """Append raw BGS borehole rows in chunked batches."""

        if not rows:
            return 0

        sql = """
            insert into public.bgs_boreholes_raw (
                ingest_run_id,
                source_row_number,
                source_archive_name,
                source_file_name,
                source_snapshot_date,
                qs,
                numb,
                bsuff,
                regno,
                rt,
                grid_refer,
                easting,
                northing,
                x,
                y,
                confidenti,
                strtheight,
                name,
                length,
                bgs_id,
                date_known,
                date_k_typ,
                date_enter,
                ags_log_ur,
                raw_record,
                geom
            )
            values (
                cast(:ingest_run_id as uuid),
                :source_row_number,
                :source_archive_name,
                :source_file_name,
                :source_snapshot_date,
                :qs,
                :numb,
                :bsuff,
                :regno,
                :rt,
                :grid_refer,
                :easting,
                :northing,
                :x,
                :y,
                :confidenti,
                :strtheight,
                :name,
                :length,
                :bgs_id,
                :date_known,
                :date_k_typ,
                :date_enter,
                :ags_log_ur,
                cast(:raw_record as jsonb),
                case
                    when cast(:geometry_wkb as text) is null then null::geometry(point, 27700)
                    else ST_GeomFromWKB(decode(cast(:geometry_wkb as text), 'hex'), 27700)
                end
            )
            on conflict (ingest_run_id, source_row_number)
            do update set
                source_archive_name = excluded.source_archive_name,
                source_file_name = excluded.source_file_name,
                source_snapshot_date = excluded.source_snapshot_date,
                qs = excluded.qs,
                numb = excluded.numb,
                bsuff = excluded.bsuff,
                regno = excluded.regno,
                rt = excluded.rt,
                grid_refer = excluded.grid_refer,
                easting = excluded.easting,
                northing = excluded.northing,
                x = excluded.x,
                y = excluded.y,
                confidenti = excluded.confidenti,
                strtheight = excluded.strtheight,
                name = excluded.name,
                length = excluded.length,
                bgs_id = excluded.bgs_id,
                date_known = excluded.date_known,
                date_k_typ = excluded.date_k_typ,
                date_enter = excluded.date_enter,
                ags_log_ur = excluded.ags_log_ur,
                raw_record = excluded.raw_record,
                geom = excluded.geom,
                imported_at = now()
        """

        payload = [
            {
                "ingest_run_id": ingest_run_id,
                "source_row_number": row["source_row_number"],
                "source_archive_name": source_archive_name,
                "source_file_name": source_file_name,
                "source_snapshot_date": source_snapshot_date,
                "qs": row.get("qs"),
                "numb": row.get("numb"),
                "bsuff": row.get("bsuff"),
                "regno": row.get("regno"),
                "rt": row.get("rt"),
                "grid_refer": row.get("grid_refer"),
                "easting": row.get("easting"),
                "northing": row.get("northing"),
                "x": row.get("x"),
                "y": row.get("y"),
                "confidenti": row.get("confidenti"),
                "strtheight": row.get("strtheight"),
                "name": row.get("name"),
                "length": row.get("length"),
                "bgs_id": row.get("bgs_id"),
                "date_known": row.get("date_known"),
                "date_k_typ": row.get("date_k_typ"),
                "date_enter": row.get("date_enter"),
                "ags_log_ur": row.get("ags_log_ur"),
                "raw_record": self._json_dumps(row.get("raw_record", {})),
                "geometry_wkb": self._geometry_hex(row.get("geom")),
            }
            for row in rows
        ]
        for batch in chunked(payload, self.settings.batch_size):
            self.database.execute_many(sql, batch)
        return len(payload)

    def refresh_bgs_boreholes(self, ingest_run_id: str) -> dict[str, Any]:
        """Merge a raw BGS ingest run into the authoritative master table."""

        row = self.database.fetch_one(
            """
            select *
            from public.refresh_bgs_boreholes(cast(:ingest_run_id as uuid))
            """,
            {"ingest_run_id": ingest_run_id},
        )
        return row or {}

    def refresh_bgs_site_constraints(
        self,
        *,
        source_ingest_run_id: str,
        site_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Refresh borehole-derived site constraints from the master table."""

        row = self.database.fetch_one(
            """
            select *
            from public.refresh_bgs_site_constraints(
                cast(:source_ingest_run_id as uuid),
                cast(:site_ids as uuid[])
            )
            """,
            {
                "source_ingest_run_id": source_ingest_run_id,
                "site_ids": site_ids,
            },
        )
        return row or {}

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

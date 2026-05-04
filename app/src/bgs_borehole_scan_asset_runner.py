"""Bounded BGS borehole scan/log asset manifest runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from config.settings import Settings, get_settings
from src.db import Database
from src.loaders.supabase_loader import SupabaseStorageClient
from src.logging_config import configure_logging


SOURCE_KEY = "bgs_borehole_scan_assets"
SOURCE_FAMILY = "bgs"
SAFE_USE_CAVEAT = (
    "BGS scan/log asset rows are manifest records only. Files are not stored in Postgres and "
    "asset availability is not ground-condition interpretation."
)


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=str)


def _env_int(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(1, min(value, maximum))


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


class BgsBoreholeScanAssetRunner:
    """Create bounded BGS asset manifest rows and optionally store files outside Postgres."""

    def __init__(self, database: Database, settings: Settings) -> None:
        self.database = database
        self.settings = settings
        self.max_assets_per_run = _env_int("BGS_SCAN_ASSET_MAX_PER_RUN", 5, 50)
        self.max_download_bytes = _env_int("BGS_SCAN_ASSET_MAX_DOWNLOAD_BYTES", 15_000_000, 100_000_000)
        self.enable_downloads = _env_bool("BGS_SCAN_FETCH_ENABLE_DOWNLOADS", False)
        self.authority_filter = os.getenv("BGS_SCAN_QUEUE_AUTHORITY") or os.getenv("PHASE2_AUTHORITY") or ""
        self.priority_band_filter = os.getenv("BGS_SCAN_QUEUE_PRIORITY_BAND", "")

    def fetch_assets(self) -> dict[str, Any]:
        if not self._has_queue_prerequisites():
            return self._record_event(
                {
                    "selected_queue_count": 0,
                    "asset_manifest_rows": 0,
                    "downloaded_asset_count": 0,
                    "linked_not_downloaded_count": 0,
                    "source_blocker": "bgs_scan_asset_prerequisites_missing",
                },
                status="blocked",
            )

        queue_rows = self.database.fetch_all(
            """
            select
                queue.id as queue_id,
                queue.canonical_site_id,
                queue.registry_id,
                queue.bgs_id,
                queue.site_priority_band,
                queue.site_priority_rank,
                site.authority_name,
                registry.ags_log_url as source_url,
                registry.registration_number
            from landintel_store.bgs_borehole_scan_fetch_queue as queue
            join landintel_store.bgs_borehole_scan_registry as registry
              on registry.id = queue.registry_id
            join landintel.canonical_sites as site
              on site.id = queue.canonical_site_id
            left join landintel_store.bgs_borehole_scan_assets as asset
              on asset.queue_id = queue.id
            where queue.queue_status = 'queued'
              and queue.fetch_status = 'linked_not_downloaded'
              and nullif(registry.ags_log_url, '') is not null
              and (:authority_name = '' or site.authority_name ilike :authority_name_like)
              and (:priority_band = '' or queue.site_priority_band = :priority_band)
              and (
                    asset.queue_id is null
                    or asset.asset_status in ('linked_not_downloaded', 'fetch_failed')
              )
            order by
                asset.updated_at nulls first,
                queue.site_priority_rank,
                queue.borehole_distance_m nulls last,
                queue.id
            limit :max_assets
            """,
            {
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
                "priority_band": self.priority_band_filter,
                "max_assets": self.max_assets_per_run,
            },
        )

        asset_rows: list[dict[str, Any]] = []
        downloaded_asset_count = 0
        linked_not_downloaded_count = 0
        for row in queue_rows:
            asset_row = self._build_asset_manifest(row)
            if self._downloads_are_enabled():
                asset_row = self._download_and_store_asset(row, asset_row)
            else:
                linked_not_downloaded_count += 1
            if asset_row["asset_status"] == "stored_in_supabase_storage":
                downloaded_asset_count += 1
            asset_rows.append(asset_row)

        self._upsert_asset_rows(asset_rows)
        self._mark_queue_rows(asset_rows)
        proof = {
            "selected_queue_count": len(queue_rows),
            "asset_manifest_rows": len(asset_rows),
            "downloaded_asset_count": downloaded_asset_count,
            "linked_not_downloaded_count": linked_not_downloaded_count,
            "storage_backend": self.settings.audit_artifact_backend,
            "downloads_enabled": self.enable_downloads,
            "max_assets_per_run": self.max_assets_per_run,
        }
        return self._record_event(proof, status="success")

    def audit(self) -> dict[str, Any]:
        return {
            "message": "bgs_borehole_scan_assets_proof",
            "asset_counts": self.database.fetch_one(
                """
                select
                    count(*)::integer as asset_row_count,
                    count(distinct canonical_site_id)::integer as asset_site_count,
                    count(distinct bgs_id)::integer as asset_bgs_record_count,
                    count(*) filter (where asset_status = 'linked_not_downloaded')::integer as linked_not_downloaded_count,
                    count(*) filter (where asset_status = 'stored_in_supabase_storage')::integer as stored_asset_count,
                    max(updated_at) as latest_asset_updated_at
                from landintel_store.bgs_borehole_scan_assets
                """
            ),
            "asset_status_counts": self.database.fetch_all(
                """
                select
                    asset_status,
                    fetch_status,
                    count(*)::integer as row_count
                from landintel_store.bgs_borehole_scan_assets
                group by asset_status, fetch_status
                order by asset_status, fetch_status
                """
            ),
            "operator_sample": self.database.fetch_all(
                """
                select
                    canonical_site_id,
                    site_label,
                    authority_name,
                    site_priority_band,
                    bgs_id,
                    registration_number,
                    borehole_name,
                    source_url,
                    asset_status,
                    fetch_status,
                    storage_path,
                    safe_use_caveat
                from landintel_reporting.v_bgs_scan_assets
                order by site_priority_rank, bgs_id
                limit 20
                """
            ),
        }

    def _build_asset_manifest(self, row: dict[str, Any]) -> dict[str, Any]:
        fetch_status = "download_disabled"
        if self.enable_downloads and self.settings.audit_artifact_backend != "supabase":
            fetch_status = "storage_not_configured"
        return {
            "queue_id": str(row["queue_id"]),
            "canonical_site_id": str(row["canonical_site_id"]),
            "registry_id": str(row["registry_id"]),
            "bgs_id": row["bgs_id"],
            "source_url": row["source_url"],
            "asset_status": "linked_not_downloaded",
            "fetch_status": fetch_status,
            "storage_bucket": None,
            "storage_path": None,
            "source_content_type": None,
            "source_content_length": None,
            "source_http_status": None,
            "source_sha256": None,
            "fetch_attempted_at": datetime.now(timezone.utc),
            "fetched_at": None,
            "last_error": None,
            "safe_use_caveat": SAFE_USE_CAVEAT,
            "metadata": _json_dumps(
                {
                    "phase": "G3",
                    "source_key": SOURCE_KEY,
                    "source_family": SOURCE_FAMILY,
                    "download_assets": self._downloads_are_enabled(),
                    "pdf_blob_in_postgres": False,
                    "registration_number": row.get("registration_number"),
                }
            ),
        }

    def _downloads_are_enabled(self) -> bool:
        return (
            self.enable_downloads
            and self.settings.audit_artifact_backend == "supabase"
            and bool(self.settings.supabase_service_role_key)
        )

    def _download_and_store_asset(self, queue_row: dict[str, Any], asset_row: dict[str, Any]) -> dict[str, Any]:
        logger = configure_logging(self.settings)
        storage = SupabaseStorageClient(self.settings, logger, self.database)
        parsed = urlparse(str(queue_row["source_url"]))
        filename = Path(parsed.path).name or f"bgs_{queue_row['bgs_id']}.bin"
        local_path = self.settings.temp_storage_path / "bgs_scan_assets" / f"{queue_row['queue_id']}_{filename}"
        remote_path = f"bgs_scan_assets/{queue_row['canonical_site_id']}/{queue_row['bgs_id']}/{filename}"
        hasher = hashlib.sha256()
        bytes_written = 0
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with httpx.stream("GET", str(queue_row["source_url"]), timeout=self.settings.http_timeout_seconds) as response:
                asset_row["source_http_status"] = response.status_code
                asset_row["source_content_type"] = response.headers.get("content-type")
                content_length = response.headers.get("content-length")
                asset_row["source_content_length"] = int(content_length) if content_length and content_length.isdigit() else None
                response.raise_for_status()
                if asset_row["source_content_length"] and asset_row["source_content_length"] > self.max_download_bytes:
                    asset_row["fetch_status"] = "download_skipped_too_large"
                    asset_row["last_error"] = f"content_length_exceeds_{self.max_download_bytes}"
                    return asset_row
                with local_path.open("wb") as handle:
                    for chunk in response.iter_bytes():
                        bytes_written += len(chunk)
                        if bytes_written > self.max_download_bytes:
                            asset_row["fetch_status"] = "download_skipped_too_large"
                            asset_row["last_error"] = f"download_exceeds_{self.max_download_bytes}"
                            return asset_row
                        hasher.update(chunk)
                        handle.write(chunk)
            uploaded_path = storage.upload_file(local_path, remote_path)
            if uploaded_path:
                asset_row["asset_status"] = "stored_in_supabase_storage"
                asset_row["fetch_status"] = "asset_stored"
                asset_row["storage_bucket"] = self.settings.supabase_audit_bucket_name
                asset_row["storage_path"] = uploaded_path
                asset_row["source_content_length"] = bytes_written
                asset_row["source_sha256"] = hasher.hexdigest()
                asset_row["fetched_at"] = datetime.now(timezone.utc)
            else:
                asset_row["fetch_status"] = "storage_upload_failed"
        except (httpx.HTTPError, OSError, ValueError) as exc:
            asset_row["asset_status"] = "fetch_failed"
            asset_row["fetch_status"] = "fetch_failed"
            asset_row["last_error"] = str(exc)[:500]
        finally:
            storage.close()
            try:
                local_path.unlink(missing_ok=True)
            except OSError:
                pass
        return asset_row

    def _upsert_asset_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self.database.execute_many(
            """
            insert into landintel_store.bgs_borehole_scan_assets (
                queue_id,
                canonical_site_id,
                registry_id,
                bgs_id,
                source_key,
                source_family,
                source_url,
                asset_status,
                fetch_status,
                storage_bucket,
                storage_path,
                source_content_type,
                source_content_length,
                source_http_status,
                source_sha256,
                fetch_attempted_at,
                fetched_at,
                last_error,
                safe_use_caveat,
                metadata,
                updated_at
            ) values (
                :queue_id,
                :canonical_site_id,
                :registry_id,
                :bgs_id,
                :source_key,
                :source_family,
                :source_url,
                :asset_status,
                :fetch_status,
                :storage_bucket,
                :storage_path,
                :source_content_type,
                :source_content_length,
                :source_http_status,
                :source_sha256,
                :fetch_attempted_at,
                :fetched_at,
                :last_error,
                :safe_use_caveat,
                cast(:metadata as jsonb),
                now()
            )
            on conflict (queue_id) do update set
                source_url = excluded.source_url,
                asset_status = excluded.asset_status,
                fetch_status = excluded.fetch_status,
                storage_bucket = excluded.storage_bucket,
                storage_path = excluded.storage_path,
                source_content_type = excluded.source_content_type,
                source_content_length = excluded.source_content_length,
                source_http_status = excluded.source_http_status,
                source_sha256 = excluded.source_sha256,
                fetch_attempted_at = excluded.fetch_attempted_at,
                fetched_at = excluded.fetched_at,
                last_error = excluded.last_error,
                safe_use_caveat = excluded.safe_use_caveat,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            [
                {
                    **row,
                    "source_key": SOURCE_KEY,
                    "source_family": SOURCE_FAMILY,
                }
                for row in rows
            ],
        )

    def _mark_queue_rows(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self.database.execute_many(
            """
            update landintel_store.bgs_borehole_scan_fetch_queue
            set fetch_status = :fetch_status,
                updated_at = now()
            where id = :queue_id
            """,
            [
                {
                    "queue_id": row["queue_id"],
                    "fetch_status": "asset_stored"
                    if row["asset_status"] == "stored_in_supabase_storage"
                    else "linked_not_downloaded",
                }
                for row in rows
            ],
        )

    def _has_queue_prerequisites(self) -> bool:
        return bool(
            self.database.scalar(
                """
                select
                    to_regclass('landintel_store.bgs_borehole_scan_fetch_queue') is not null
                    and to_regclass('landintel_store.bgs_borehole_scan_registry') is not null
                    and to_regclass('landintel_store.bgs_borehole_scan_assets') is not null
                """
            )
        )

    def _record_event(self, proof: dict[str, Any], *, status: str) -> dict[str, Any]:
        observed_rows = int(proof.get("asset_manifest_rows") or proof.get("selected_queue_count") or 0)
        self.database.execute(
            """
            insert into landintel.source_expansion_events (
                command_name,
                source_key,
                source_family,
                status,
                raw_rows,
                linked_rows,
                measured_rows,
                evidence_rows,
                signal_rows,
                change_event_rows,
                summary,
                metadata
            ) values (
                'fetch-bgs-borehole-scans',
                :source_key,
                :source_family,
                :status,
                :raw_rows,
                :linked_rows,
                0,
                0,
                0,
                0,
                :summary,
                cast(:metadata as jsonb)
            )
            """,
            {
                "source_key": SOURCE_KEY,
                "source_family": SOURCE_FAMILY,
                "status": status,
                "raw_rows": observed_rows,
                "linked_rows": observed_rows,
                "summary": "BGS scan/log asset manifests refreshed. Files are not stored in Postgres.",
                "metadata": _json_dumps(proof),
            },
        )
        self.database.execute(
            """
            insert into landintel.source_freshness_states (
                source_scope_key,
                source_family,
                source_dataset,
                source_name,
                source_access_mode,
                refresh_cadence,
                max_staleness_days,
                source_observed_at,
                last_checked_at,
                last_success_at,
                next_refresh_due_at,
                freshness_status,
                live_access_status,
                ranking_eligible,
                review_output_eligible,
                stale_reason_code,
                check_summary,
                records_observed,
                metadata,
                updated_at
            ) values (
                'phase_g3:bgs_borehole_scan_assets',
                :source_family,
                'BGS borehole scan/log asset manifests',
                'BGS scan/log asset manifests',
                'known_origin_manual_bulk_upload',
                'manual',
                90,
                now(),
                now(),
                :last_success_at,
                now() + interval '90 days',
                :freshness_status,
                :live_access_status,
                false,
                true,
                :stale_reason_code,
                :check_summary,
                :records_observed,
                cast(:metadata as jsonb),
                now()
            )
            on conflict (source_scope_key) do update set
                source_family = excluded.source_family,
                source_dataset = excluded.source_dataset,
                source_name = excluded.source_name,
                source_access_mode = excluded.source_access_mode,
                source_observed_at = excluded.source_observed_at,
                last_checked_at = excluded.last_checked_at,
                last_success_at = excluded.last_success_at,
                next_refresh_due_at = excluded.next_refresh_due_at,
                freshness_status = excluded.freshness_status,
                live_access_status = excluded.live_access_status,
                stale_reason_code = excluded.stale_reason_code,
                check_summary = excluded.check_summary,
                records_observed = excluded.records_observed,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            {
                "source_family": SOURCE_FAMILY,
                "last_success_at": datetime.now(timezone.utc) if status == "success" else None,
                "freshness_status": "current" if status == "success" else "blocked",
                "live_access_status": "asset_manifest_live",
                "stale_reason_code": None if status == "success" else proof.get("source_blocker"),
                "check_summary": "BGS scan/log asset manifest workflow completed.",
                "records_observed": observed_rows,
                "metadata": _json_dumps({"pdf_blob_in_postgres": False, **proof}),
            },
        )
        return {
            "message": "bgs_borehole_scan_assets_proof",
            "source_key": SOURCE_KEY,
            "source_family": SOURCE_FAMILY,
            "status": status,
            "max_assets_per_run": self.max_assets_per_run,
            "downloads_enabled": self.enable_downloads,
            "storage_backend": self.settings.audit_artifact_backend,
            **proof,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch or audit bounded BGS borehole scan/log asset manifests.")
    parser.add_argument("command", choices=("fetch-bgs-borehole-scans", "audit-bgs-borehole-scan-assets"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    runner = BgsBoreholeScanAssetRunner(database, settings)
    try:
        if args.command == "fetch-bgs-borehole-scans":
            result = runner.fetch_assets()
        else:
            result = runner.audit()
        print(json.dumps(result, default=str, ensure_ascii=False), flush=True)
        logger.info(
            "bgs_borehole_scan_asset_command_completed",
            extra={key: value for key, value in result.items() if key != "message"},
        )
        return 0
    except Exception:
        logger.exception(
            "bgs_borehole_scan_asset_command_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

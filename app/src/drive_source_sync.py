"""Keep the Google Drive-held Scotland source file estate auditable.

This runner deliberately does not ingest source datasets into LandIntel truth
tables. It records file metadata and ready-upload status so source completion
work can be planned without relying on memory or ad hoc Drive browsing.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any

import httpx
import yaml

from config.settings import Settings, get_settings
from src.db import Database, chunked
from src.logging_config import configure_logging


GOOGLE_APPS_PREFIX = "application/vnd.google-apps."
GOOGLE_DRIVE_API_FILES_URL = "https://www.googleapis.com/drive/v3/files"


@dataclass(slots=True)
class DriveSourceFile:
    root_folder_id: str
    root_folder_name: str
    folder_path: str
    folder_id: str | None
    parent_folder_id: str | None
    file_id: str
    file_name: str
    file_or_folder: str
    mime_type: str | None
    file_extension: str | None
    drive_url: str
    source_family: str | None
    asset_role: str | None
    ready_to_upload_flag: bool
    ready_to_upload_reason: str | None
    upload_status: str
    download_status: str
    size_bytes: int | None
    md5_checksum: str | None
    drive_created_at: str | None
    drive_modified_at: str | None
    metadata: dict[str, Any]

    def as_params(self) -> dict[str, Any]:
        return {
            "root_folder_id": self.root_folder_id,
            "root_folder_name": self.root_folder_name,
            "folder_path": self.folder_path,
            "folder_id": self.folder_id,
            "parent_folder_id": self.parent_folder_id,
            "file_id": self.file_id,
            "file_name": self.file_name,
            "file_or_folder": self.file_or_folder,
            "mime_type": self.mime_type,
            "file_extension": self.file_extension,
            "drive_url": self.drive_url,
            "source_family": self.source_family,
            "asset_role": self.asset_role,
            "ready_to_upload_flag": self.ready_to_upload_flag,
            "ready_to_upload_reason": self.ready_to_upload_reason,
            "upload_status": self.upload_status,
            "download_status": self.download_status,
            "size_bytes": self.size_bytes,
            "md5_checksum": self.md5_checksum,
            "drive_created_at": self.drive_created_at,
            "drive_modified_at": self.drive_modified_at,
            "metadata": json.dumps(self.metadata, sort_keys=True),
        }


class DriveSourceSyncRunner:
    """Synchronise the curated Drive source manifest into Supabase."""

    def __init__(self, settings: Settings, logger: Any) -> None:
        self.settings = settings
        self.logger = logger.getChild("drive_source_sync")
        self.database = Database(settings)
        self.client = httpx.Client(timeout=settings.http_timeout_seconds, follow_redirects=True)
        self.manifest_path = settings.drive_source_manifest_path
        self.manifest = yaml.safe_load(self.manifest_path.read_text(encoding="utf-8")) or {}

    def close(self) -> None:
        self.client.close()
        self.database.dispose()

    def audit_manifest(self) -> dict[str, Any]:
        files = self._manifest_rows()
        source_family_counts = Counter(row.source_family or "unknown" for row in files if row.file_or_folder == "file")
        asset_role_counts = Counter(row.asset_role or "unknown" for row in files if row.file_or_folder == "file")
        ready_by_family: dict[str, int] = defaultdict(int)
        for row in files:
            if row.file_or_folder == "file" and row.ready_to_upload_flag:
                ready_by_family[row.source_family or "unknown"] += 1

        payload = {
            "manifest_path": str(self.manifest_path),
            "root_folder_id": self._root().get("folder_id"),
            "root_folder_name": self._root().get("folder_name"),
            "folder_count": sum(1 for row in files if row.file_or_folder == "folder"),
            "file_count": sum(1 for row in files if row.file_or_folder == "file"),
            "ready_to_upload_count": sum(1 for row in files if row.ready_to_upload_flag),
            "source_family_counts": dict(sorted(source_family_counts.items())),
            "asset_role_counts": dict(sorted(asset_role_counts.items())),
            "ready_to_upload_by_family": dict(sorted(ready_by_family.items())),
            "downloads_enabled": self.settings.drive_source_sync_enable_downloads,
            "live_drive_api_configured": bool(self.settings.google_drive_api_key),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_summary(payload)
        self.logger.info("drive_source_manifest_audit", extra=payload)
        return payload

    def sync_manifest(self, *, refresh_live_metadata: bool = True) -> dict[str, Any]:
        rows = self._manifest_rows()
        if refresh_live_metadata and self.settings.google_drive_api_key:
            rows = self._with_live_metadata(rows)
        self._upsert_rows(rows)
        payload = self.audit_manifest() | {
            "synced_rows": len(rows),
            "synced_file_rows": sum(1 for row in rows if row.file_or_folder == "file"),
            "synced_folder_rows": sum(1 for row in rows if row.file_or_folder == "folder"),
            "refresh_live_metadata": refresh_live_metadata,
        }
        self._write_summary(payload)
        self.logger.info("drive_source_manifest_synced", extra=payload)
        return payload

    def sync_ready_upload_files(self) -> dict[str, Any]:
        sync_payload = self.sync_manifest(refresh_live_metadata=True)
        ready_rows = [row for row in self._manifest_rows() if row.file_or_folder == "file" and row.ready_to_upload_flag]
        if not self.settings.drive_source_sync_enable_downloads:
            payload = sync_payload | {
                "download_status": "disabled",
                "downloaded_files": 0,
                "download_note": "Ready files were registered only. Set DRIVE_SOURCE_SYNC_ENABLE_DOWNLOADS=true to download binary files in a bounded workflow run.",
            }
            self._mark_ready_download_status("disabled")
            self._write_summary(payload)
            self.logger.info("drive_source_ready_upload_sync_metadata_only", extra=payload)
            return payload
        if not self.settings.google_drive_api_key:
            payload = sync_payload | {
                "download_status": "blocked",
                "downloaded_files": 0,
                "download_note": "GOOGLE_DRIVE_API_KEY is required for workflow-based Drive downloads.",
            }
            self._write_summary(payload)
            self.logger.warning("drive_source_ready_upload_sync_blocked", extra=payload)
            return payload

        downloaded = self._download_ready_rows(ready_rows)
        payload = sync_payload | {
            "download_status": "downloaded" if downloaded else "skipped",
            "downloaded_files": len(downloaded),
            "download_directory": str(self._download_directory()),
            "downloaded_file_ids": [item["file_id"] for item in downloaded],
        }
        self._write_summary(payload)
        self.logger.info("drive_source_ready_upload_sync_downloaded", extra=payload)
        return payload

    def _root(self) -> dict[str, Any]:
        return dict(self.manifest.get("root") or {})

    def _manifest_rows(self) -> list[DriveSourceFile]:
        root = self._root()
        root_id = str(root.get("folder_id") or self.settings.drive_source_root_folder_id)
        root_name = str(root.get("folder_name") or "Scotland")
        rows: list[DriveSourceFile] = []
        for folder in self.manifest.get("folders") or []:
            folder_path = str(folder.get("folder_path") or "root")
            folder_id = folder.get("folder_id")
            source_family = folder.get("source_family")
            rows.append(
                DriveSourceFile(
                    root_folder_id=root_id,
                    root_folder_name=root_name,
                    folder_path=folder_path,
                    folder_id=folder_id,
                    parent_folder_id=root_id if folder_path != "root" else None,
                    file_id=str(folder_id or folder_path),
                    file_name=folder_path,
                    file_or_folder="folder",
                    mime_type="application/vnd.google-apps.folder",
                    file_extension=None,
                    drive_url=self._folder_url(str(folder_id or root_id)),
                    source_family=source_family,
                    asset_role="drive_folder",
                    ready_to_upload_flag=False,
                    ready_to_upload_reason="Folder row for inventory/navigation only.",
                    upload_status="metadata_only",
                    download_status="not_requested",
                    size_bytes=None,
                    md5_checksum=None,
                    drive_created_at=None,
                    drive_modified_at=None,
                    metadata={"manifest_folder": folder},
                )
            )
            for file_payload in folder.get("files") or []:
                rows.append(self._file_row(root_id, root_name, folder, file_payload))
        return rows

    def _file_row(
        self,
        root_id: str,
        root_name: str,
        folder: dict[str, Any],
        file_payload: dict[str, Any],
    ) -> DriveSourceFile:
        file_name = str(file_payload["file_name"])
        mime_type = file_payload.get("mime_type")
        ready_to_upload = bool(file_payload.get("ready_to_upload"))
        asset_role = file_payload.get("asset_role") or folder.get("asset_role")
        upload_status = self._upload_status(asset_role, ready_to_upload)
        return DriveSourceFile(
            root_folder_id=root_id,
            root_folder_name=root_name,
            folder_path=str(folder.get("folder_path") or "root"),
            folder_id=folder.get("folder_id"),
            parent_folder_id=folder.get("folder_id"),
            file_id=str(file_payload["file_id"]),
            file_name=file_name,
            file_or_folder="file",
            mime_type=mime_type,
            file_extension=self._extension(file_name),
            drive_url=self._file_url(str(file_payload["file_id"])),
            source_family=file_payload.get("source_family") or folder.get("source_family"),
            asset_role=asset_role,
            ready_to_upload_flag=ready_to_upload,
            ready_to_upload_reason=file_payload.get("ready_to_upload_reason"),
            upload_status=upload_status,
            download_status="not_requested",
            size_bytes=self._as_int(file_payload.get("size_bytes")),
            md5_checksum=file_payload.get("md5_checksum"),
            drive_created_at=file_payload.get("drive_created_at"),
            drive_modified_at=file_payload.get("drive_modified_at"),
            metadata={"manifest_file": file_payload, "manifest_folder_path": folder.get("folder_path")},
        )

    def _with_live_metadata(self, rows: list[DriveSourceFile]) -> list[DriveSourceFile]:
        enriched: list[DriveSourceFile] = []
        for row in rows:
            if row.file_or_folder != "file":
                enriched.append(row)
                continue
            metadata = self._fetch_drive_metadata(row.file_id)
            if not metadata:
                enriched.append(row)
                continue
            enriched.append(
                replace(
                    row,
                    size_bytes=self._as_int(metadata.get("size")),
                    md5_checksum=metadata.get("md5Checksum") or row.md5_checksum,
                    drive_created_at=metadata.get("createdTime") or row.drive_created_at,
                    drive_modified_at=metadata.get("modifiedTime") or row.drive_modified_at,
                    mime_type=metadata.get("mimeType") or row.mime_type,
                    metadata={**row.metadata, "live_drive_metadata": metadata},
                )
            )
        return enriched

    def _fetch_drive_metadata(self, file_id: str) -> dict[str, Any] | None:
        response = self.client.get(
            f"{GOOGLE_DRIVE_API_FILES_URL}/{file_id}",
            params={
                "key": self.settings.google_drive_api_key,
                "fields": "id,name,mimeType,createdTime,modifiedTime,size,md5Checksum,webViewLink",
                "supportsAllDrives": "true",
            },
        )
        if response.status_code >= 400:
            self.logger.warning(
                "drive_metadata_refresh_failed",
                extra={"file_id": file_id, "status_code": response.status_code},
            )
            return None
        return dict(response.json())

    def _upsert_rows(self, rows: list[DriveSourceFile]) -> None:
        sql = """
            insert into landintel_store.drive_source_file_registry (
                root_folder_id,
                root_folder_name,
                folder_path,
                folder_id,
                parent_folder_id,
                file_id,
                file_name,
                file_or_folder,
                mime_type,
                file_extension,
                drive_url,
                source_family,
                asset_role,
                ready_to_upload_flag,
                ready_to_upload_reason,
                upload_status,
                download_status,
                size_bytes,
                md5_checksum,
                drive_created_at,
                drive_modified_at,
                manifest_seen_at,
                live_seen_at,
                last_synced_at,
                metadata,
                updated_at
            )
            values (
                :root_folder_id,
                :root_folder_name,
                :folder_path,
                :folder_id,
                :parent_folder_id,
                :file_id,
                :file_name,
                :file_or_folder,
                :mime_type,
                :file_extension,
                :drive_url,
                :source_family,
                :asset_role,
                :ready_to_upload_flag,
                :ready_to_upload_reason,
                :upload_status,
                :download_status,
                :size_bytes,
                :md5_checksum,
                cast(:drive_created_at as timestamptz),
                cast(:drive_modified_at as timestamptz),
                now(),
                case when :drive_modified_at is not null then now() else null end,
                now(),
                cast(:metadata as jsonb),
                now()
            )
            on conflict (root_folder_id, file_id) do update set
                root_folder_name = excluded.root_folder_name,
                folder_path = excluded.folder_path,
                folder_id = excluded.folder_id,
                parent_folder_id = excluded.parent_folder_id,
                file_name = excluded.file_name,
                file_or_folder = excluded.file_or_folder,
                mime_type = excluded.mime_type,
                file_extension = excluded.file_extension,
                drive_url = excluded.drive_url,
                source_family = excluded.source_family,
                asset_role = excluded.asset_role,
                ready_to_upload_flag = excluded.ready_to_upload_flag,
                ready_to_upload_reason = excluded.ready_to_upload_reason,
                upload_status = excluded.upload_status,
                download_status = excluded.download_status,
                size_bytes = excluded.size_bytes,
                md5_checksum = excluded.md5_checksum,
                drive_created_at = excluded.drive_created_at,
                drive_modified_at = excluded.drive_modified_at,
                manifest_seen_at = now(),
                live_seen_at = coalesce(excluded.live_seen_at, landintel_store.drive_source_file_registry.live_seen_at),
                last_synced_at = now(),
                metadata = excluded.metadata,
                updated_at = now()
        """
        for batch in chunked([row.as_params() for row in rows], 100):
            self.database.execute_many(sql, batch)

    def _mark_ready_download_status(self, status: str) -> None:
        self.database.execute(
            """
                update landintel_store.drive_source_file_registry
                set download_status = :status,
                    updated_at = now()
                where root_folder_id = :root_folder_id
                  and ready_to_upload_flag is true
            """,
            {"status": status, "root_folder_id": self._root().get("folder_id")},
        )

    def _download_ready_rows(self, ready_rows: list[DriveSourceFile]) -> list[dict[str, Any]]:
        downloaded: list[dict[str, Any]] = []
        download_dir = self._download_directory()
        download_dir.mkdir(parents=True, exist_ok=True)
        byte_budget = self.settings.drive_source_sync_max_download_bytes
        max_files = self.settings.drive_source_sync_max_files_per_run
        consumed_bytes = 0
        for row in ready_rows[:max_files]:
            if row.mime_type and row.mime_type.startswith(GOOGLE_APPS_PREFIX):
                self.logger.info("drive_download_skipped_google_workspace_file", extra={"file_id": row.file_id})
                continue
            if row.size_bytes and consumed_bytes + row.size_bytes > byte_budget:
                self.logger.info("drive_download_budget_reached", extra={"file_id": row.file_id})
                continue
            target = download_dir / f"{row.file_id}_{self._safe_filename(row.file_name)}"
            try:
                response = self.client.get(
                    f"{GOOGLE_DRIVE_API_FILES_URL}/{row.file_id}",
                    params={"key": self.settings.google_drive_api_key, "alt": "media", "supportsAllDrives": "true"},
                )
                response.raise_for_status()
                target.write_bytes(response.content)
                consumed_bytes += target.stat().st_size
                downloaded.append({"file_id": row.file_id, "file_name": row.file_name, "path": str(target)})
            except Exception as exc:  # pragma: no cover - network path is intentionally optional.
                self.logger.warning("drive_download_failed", extra={"file_id": row.file_id, "error": str(exc)})
        return downloaded

    def _write_summary(self, payload: dict[str, Any]) -> None:
        summary_path = self.settings.temp_storage_path / "drive-source-sync-summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _download_directory(self) -> Path:
        return self.settings.temp_storage_path / "drive_source_ready_files"

    @staticmethod
    def _file_url(file_id: str) -> str:
        return f"https://drive.google.com/file/d/{file_id}/view"

    @staticmethod
    def _folder_url(folder_id: str) -> str:
        return f"https://drive.google.com/drive/folders/{folder_id}"

    @staticmethod
    def _extension(file_name: str) -> str | None:
        suffix = Path(file_name).suffix.lower().lstrip(".")
        return suffix or None

    @staticmethod
    def _upload_status(asset_role: str | None, ready_to_upload: bool) -> str:
        if asset_role == "known_origin_manual_bulk_upload":
            return "paused"
        if ready_to_upload:
            return "ready_for_controlled_upload"
        return "not_ready"

    @staticmethod
    def _safe_filename(file_name: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", file_name).strip("_") or "drive_file"

    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronise LandIntel's Google Drive source file registry.")
    parser.add_argument(
        "command",
        choices=(
            "audit-drive-source-manifest",
            "sync-drive-source-manifest",
            "sync-drive-ready-upload-files",
        ),
    )
    args = parser.parse_args()

    logger = configure_logging("landintel")
    settings = get_settings()
    runner = DriveSourceSyncRunner(settings, logger)
    try:
        if args.command == "audit-drive-source-manifest":
            payload = runner.audit_manifest()
        elif args.command == "sync-drive-source-manifest":
            payload = runner.sync_manifest(refresh_live_metadata=True)
        else:
            payload = runner.sync_ready_upload_files()
        print(json.dumps(payload, indent=2, sort_keys=True))
    finally:
        runner.close()


if __name__ == "__main__":
    main()

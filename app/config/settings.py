"""Runtime settings for the LandIntel Scotland ingestion worker."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


APP_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    """Environment-backed application settings."""

    model_config = SettingsConfigDict(
        env_file=APP_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    supabase_db_url: str = Field(..., alias="SUPABASE_DB_URL")
    supabase_url: str = Field(..., alias="SUPABASE_URL")
    supabase_service_role_key: str | None = Field(default=None, alias="SUPABASE_SERVICE_ROLE_KEY")

    geonetwork_base_url: str = Field(
        default="https://spatialdata.gov.scot/geonetwork",
        alias="GEONETWORK_BASE_URL",
    )
    geonetwork_search_path: str = Field(
        default="/srv/api/search/records/_search",
        alias="GEONETWORK_SEARCH_PATH",
    )
    geonetwork_record_path: str = Field(
        default="/srv/api/records",
        alias="GEONETWORK_RECORD_PATH",
    )

    ros_download_base_url: str = Field(
        default="https://ros-inspire.themapcloud.com/maps/download/ros-cp.cadastralparcel",
        alias="ROS_DOWNLOAD_BASE_URL",
    )
    ros_view_service_url: str = Field(
        default="https://ros-inspire.themapcloud.com/maps/wms?service=WMS&request=GetCapabilities&version=1.3.0",
        alias="ROS_VIEW_SERVICE_URL",
    )
    ros_api_base_url: str = Field(
        default="https://api.scotlis.ros.gov.uk",
        alias="ROS_API_BASE_URL",
    )
    ros_client_id: str | None = Field(default=None, alias="ROS_CLIENT_ID")
    ros_client_secret: str | None = Field(default=None, alias="ROS_CLIENT_SECRET")

    boundary_package_show_url: str = Field(
        default="https://data.spatialhub.scot/api/3/action/package_show?id=local_authority_boundaries-is",
        alias="BOUNDARY_PACKAGE_SHOW_URL",
    )
    boundary_geojson_url: str | None = Field(default=None, alias="BOUNDARY_GEOJSON_URL")
    boundary_authkey: str | None = Field(default=None, alias="BOUNDARY_AUTHKEY")
    boundary_simplify_tolerance: float = Field(
        default=10.0,
        alias="BOUNDARY_SIMPLIFY_TOLERANCE",
    )

    temp_storage_path: Path = Field(default=APP_DIR / ".tmp", alias="TEMP_STORAGE_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_json: bool = Field(default=True, alias="LOG_JSON")
    log_file_path: Path = Field(default=APP_DIR / "logs" / "landintel_ingest.log", alias="LOG_FILE_PATH")

    audit_artifact_backend: Literal["none", "supabase"] = Field(
        default="none",
        alias="AUDIT_ARTIFACT_BACKEND",
    )
    supabase_audit_bucket_name: str = Field(
        default="landintel-ingest-audit",
        alias="SUPABASE_AUDIT_BUCKET_NAME",
    )
    persist_staging_rows: bool = Field(default=False, alias="PERSIST_STAGING_ROWS")
    staging_retention_days: int = Field(default=14, alias="STAGING_RETENTION_DAYS")
    batch_size: int = Field(default=1_000, alias="BATCH_SIZE")
    http_timeout_seconds: int = Field(default=120, alias="HTTP_TIMEOUT_SECONDS")

    enable_internal_scheduler: bool = Field(default=False, alias="ENABLE_INTERNAL_SCHEDULER")
    startup_command: str = Field(default="none", alias="STARTUP_COMMAND")
    quarterly_cron: str = Field(default="0 6 2 3,6,9,12 *", alias="QUARTERLY_CRON")

    councils_config_path: Path = Field(
        default=CONFIG_DIR / "councils.yaml",
        alias="COUNCILS_CONFIG_PATH",
    )

    @property
    def sqlalchemy_database_url(self) -> str:
        """Return a SQLAlchemy URL that always uses the psycopg driver."""

        url = self.supabase_db_url
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg://", 1)
        elif url.startswith("postgresql://") and "+psycopg" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url

    @property
    def sql_dir(self) -> Path:
        """Directory containing ordered SQL migrations."""

        return APP_DIR / "sql"

    def ensure_local_directories(self) -> None:
        """Create runtime directories needed by the worker."""

        self.temp_storage_path.mkdir(parents=True, exist_ok=True)
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)

    def load_target_councils(self) -> list[str]:
        """Return the canonical authority names from YAML configuration."""

        with self.councils_config_path.open("r", encoding="utf-8") as handle:
            payload: dict[str, Any] = yaml.safe_load(handle) or {}
        councils = payload.get("target_councils", [])
        if not councils:
            raise ValueError("No target councils were found in councils.yaml.")
        return [str(item) for item in councils]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings object."""

    settings = Settings()
    settings.ensure_local_directories()
    return settings

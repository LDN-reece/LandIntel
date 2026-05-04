"""Runtime settings for the LandIntel Scotland ingestion worker."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - depends on local runtime image
    yaml = None
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
    supabase_working_bucket_name: str = Field(
        default="landintel-working",
        alias="SUPABASE_WORKING_BUCKET_NAME",
    )
    supabase_archive_bucket_name: str = Field(
        default="landintel-ingest-audit",
        alias="SUPABASE_ARCHIVE_BUCKET_NAME",
    )
    persist_staging_rows: bool = Field(default=False, alias="PERSIST_STAGING_ROWS")
    staging_retention_days: int = Field(default=14, alias="STAGING_RETENTION_DAYS")
    artifact_working_retention_days: int = Field(
        default=30,
        alias="ARTIFACT_WORKING_RETENTION_DAYS",
    )
    artifact_archive_retention_days: int = Field(
        default=365,
        alias="ARTIFACT_ARCHIVE_RETENTION_DAYS",
    )
    minimum_operational_area_acres: float = Field(
        default=4.0,
        alias="MIN_OPERATIONAL_AREA_ACRES",
    )
    mirror_land_objects: bool = Field(default=False, alias="MIRROR_LAND_OBJECTS")
    batch_size: int = Field(default=1_000, alias="BATCH_SIZE")
    planning_new_site_min_area_acres: float = Field(
        default=4.0,
        alias="PLANNING_NEW_SITE_MIN_AREA_ACRES",
    )
    reconcile_queue_batch_limit: int = Field(default=500, alias="RECONCILE_QUEUE_BATCH_LIMIT")
    reconcile_refresh_batch_limit: int = Field(default=250, alias="RECONCILE_REFRESH_BATCH_LIMIT")
    reconcile_runtime_minutes: int = Field(default=45, alias="RECONCILE_RUNTIME_MINUTES")
    reconcile_lease_seconds: int = Field(default=1_800, alias="RECONCILE_LEASE_SECONDS")
    reconcile_refresh_lease_seconds: int = Field(
        default=1_200,
        alias="RECONCILE_REFRESH_LEASE_SECONDS",
    )
    reconcile_max_attempts: int = Field(default=3, alias="RECONCILE_MAX_ATTEMPTS")
    http_timeout_seconds: int = Field(default=120, alias="HTTP_TIMEOUT_SECONDS")

    enable_internal_scheduler: bool = Field(default=False, alias="ENABLE_INTERNAL_SCHEDULER")
    startup_command: str = Field(default="none", alias="STARTUP_COMMAND")
    quarterly_cron: str = Field(default="0 6 2 3,6,9,12 *", alias="QUARTERLY_CRON")
    planning_review_cron: str = Field(default="0 7 * * 1", alias="PLANNING_REVIEW_CRON")
    policy_review_cron: str = Field(default="0 8 * * 1", alias="POLICY_REVIEW_CRON")
    refresh_queue_cron: str = Field(default="0 9 * * *", alias="REFRESH_QUEUE_CRON")
    bgs_borehole_archive_path: Path | None = Field(default=None, alias="BGS_BOREHOLE_ARCHIVE_PATH")
    process_site_refresh_queue_after_bgs: bool = Field(
        default=False,
        alias="PROCESS_SITE_REFRESH_QUEUE_AFTER_BGS",
    )
    bgs_site_refresh_limit: int = Field(default=200, alias="BGS_SITE_REFRESH_LIMIT")
    workflow_source_input_path: Path | None = Field(default=None, alias="LANDINTEL_SOURCE_INPUT_PATH")
    workflow_source_input_dir: Path | None = Field(default=None, alias="LANDINTEL_SOURCE_INPUT_DIR")
    workflow_source_input_extracted_dir: Path | None = Field(
        default=None,
        alias="LANDINTEL_SOURCE_INPUT_EXTRACTED_DIR",
    )
    workflow_source_input_filename: str | None = Field(default=None, alias="LANDINTEL_SOURCE_INPUT_FILENAME")

    councils_config_path: Path = Field(
        default=CONFIG_DIR / "councils.yaml",
        alias="COUNCILS_CONFIG_PATH",
    )
    buyer_profiles_config_path: Path = Field(
        default=CONFIG_DIR / "buyer_profiles.yaml",
        alias="BUYER_PROFILES_CONFIG_PATH",
    )
    commercial_defaults_config_path: Path = Field(
        default=CONFIG_DIR / "commercial_defaults.yaml",
        alias="COMMERCIAL_DEFAULTS_CONFIG_PATH",
    )
    investor_entities_config_path: Path = Field(
        default=CONFIG_DIR / "investor_entities.yaml",
        alias="INVESTOR_ENTITIES_CONFIG_PATH",
    )
    investor_evidence_config_path: Path = Field(
        default=CONFIG_DIR / "investor_evidence.yaml",
        alias="INVESTOR_EVIDENCE_CONFIG_PATH",
    )
    strategy_rules_config_path: Path = Field(
        default=CONFIG_DIR / "strategy_rules.yaml",
        alias="STRATEGY_RULES_CONFIG_PATH",
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

        if yaml is None:
            raise RuntimeError("PyYAML is required to load the councils configuration.")

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

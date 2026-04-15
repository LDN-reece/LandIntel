"""CLI entry points for the Scotland land ingestion worker."""

from __future__ import annotations

import argparse
import logging
import traceback
from typing import Callable

from config.settings import Settings, get_settings
from src.db import Database
from src.fetchers.boundaries import BoundaryFetcher
from src.fetchers.geonetwork import GeoNetworkClient
from src.fetchers.ros_cadastral import ROS_SOURCE_NAME, RoSCadastralFetcher
from src.loaders.supabase_loader import SupabaseLoader
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.processors.calculate_area import calculate_area_metrics
from src.processors.classify_size import classify_size_buckets
from src.processors.clip_to_authorities import clip_parcels_to_authorities
from src.processors.extract import choose_preferred_candidate, extract_archive
from src.processors.normalise import load_preferred_spatial_frame, normalise_ros_cadastral_frame
from src.url_safety import redact_sensitive_query_params
from src.processors.validate_geometry import repair_invalid_geometries


class LandIntelPipeline:
    """Run Stage 1 discovery, boundary ingestion, and RoS parcel processing."""

    def __init__(self, settings: Settings, logger: logging.Logger) -> None:
        self.settings = settings
        self.logger = logger
        self.database = Database(settings)
        self.loader = SupabaseLoader(settings, self.database, logger)
        self.geonetwork = GeoNetworkClient(settings, logger)
        self.boundaries = BoundaryFetcher(settings, logger)
        self.ros = RoSCadastralFetcher(settings, logger)
        self.target_authorities = settings.load_target_councils()

    def close(self) -> None:
        """Close any network and database resources."""

        self.geonetwork.close()
        self.boundaries.close()
        self.ros.close()
        self.loader.close()

    def prepare_database(self) -> None:
        """Ensure the schema is present before running any stage."""

        self.loader.run_migrations()

    def discover_sources(self) -> None:
        """Discover Stage 1 metadata records and persist them."""

        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="discover_sources",
                source_name="spatialdata.gov.scot",
                status="running",
                metadata={"queries": "stage_1_default"},
            )
        )
        try:
            records = self.geonetwork.discover_stage_one_sources()
            boundary_record = self.boundaries.discover_source_metadata()
            records.append(boundary_record)
            loaded = self.loader.upsert_source_registry(records)
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=len(records),
                    records_loaded=loaded,
                    records_retained=loaded,
                    metadata={"source_names": [record.source_name for record in records]},
                    finished=True,
                ),
            )
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="failed",
                    error_message=str(exc),
                    metadata={"traceback": traceback.format_exc()},
                    finished=True,
                ),
            )
            raise

    def load_boundaries(self) -> None:
        """Download, standardise, and upsert authority boundaries."""

        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="load_boundaries",
                source_name="Local Authority Areas - Scotland",
                status="running",
                metadata={"target_authority_count": len(self.target_authorities)},
            )
        )
        try:
            download = self.boundaries.download_boundaries(
                self.settings.temp_storage_path / "boundaries" / run_id
            )
            self.loader.upload_audit_artifact(
                download.local_path,
                f"boundaries/{run_id}/{download.local_path.name}",
            )
            authority_gdf = self.boundaries.load_target_authorities(
                download.local_path,
                self.target_authorities,
            )
            loaded = self.loader.upsert_authority_aoi(authority_gdf)
            self.loader.upsert_source_registry([download.source_record])
            self.loader.refresh_cached_outputs()

            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=len(authority_gdf),
                    records_loaded=loaded,
                    records_retained=loaded,
                    metadata={
                        "download_url": redact_sensitive_query_params(download.download_url),
                        "cache_refreshed": True,
                    },
                    finished=True,
                ),
            )
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="failed",
                    error_message=str(exc),
                    metadata={"traceback": traceback.format_exc()},
                    finished=True,
                ),
            )
            raise

    def ingest_ros_cadastral(self) -> None:
        """Run the full RoS parcel ingestion and clipping flow."""

        authority_gdf = self.loader.fetch_active_authorities()
        if authority_gdf.empty:
            raise RuntimeError(
                "No authority AOIs were found in public.authority_aoi. Run load-boundaries first."
            )

        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="ingest_ros_cadastral",
                source_name=ROS_SOURCE_NAME,
                status="running",
                metadata={"target_authority_count": len(self.target_authorities)},
            )
        )

        records_fetched = 0
        records_loaded = 0
        records_retained = 0
        county_failures: list[dict[str, str]] = []
        cache_refreshed = False
        staging_pruned = {"raw_deleted": 0, "clean_deleted": 0}

        try:
            self.loader.upsert_source_registry([self.ros.build_source_registry_record()])
            archives = self.ros.download_county_archives(
                self.settings.temp_storage_path / "ros_cadastral" / run_id / "downloads"
            )

            for archive in archives:
                try:
                    self.loader.upload_audit_artifact(
                        archive.local_path,
                        f"ros_cadastral/{run_id}/downloads/{archive.local_path.name}",
                    )

                    extracted_dir = (
                        self.settings.temp_storage_path
                        / "ros_cadastral"
                        / run_id
                        / "extracted"
                        / archive.county_code
                    )
                    candidates = extract_archive(archive.local_path, extracted_dir)
                    source_path = choose_preferred_candidate(candidates)

                    raw_frame = load_preferred_spatial_frame(source_path)
                    raw_gdf = normalise_ros_cadastral_frame(
                        raw_frame,
                        run_id=run_id,
                        source_name=ROS_SOURCE_NAME,
                        source_file=archive.local_path.name,
                        source_county=archive.county_name,
                    )
                    records_fetched += len(raw_gdf)
                    raw_loaded = self.loader.insert_raw_parcels(raw_gdf)

                    clean_gdf = repair_invalid_geometries(raw_gdf, self.logger)
                    missing_ids = int(clean_gdf["ros_inspire_id"].isna().sum()) if not clean_gdf.empty else 0
                    if missing_ids:
                        self.logger.warning(
                            "ros_rows_missing_inspire_id",
                            extra={"county_code": archive.county_code, "row_count": missing_ids},
                        )
                    clean_loaded = self.loader.insert_clean_parcels(clean_gdf)
                    records_loaded += raw_loaded + clean_loaded

                    clipped = clip_parcels_to_authorities(clean_gdf, authority_gdf, self.logger)
                    enriched = classify_size_buckets(calculate_area_metrics(clipped))
                    retained = self.loader.upsert_processed_parcels(enriched)
                    land_objects = self.loader.upsert_land_objects(enriched)
                    records_retained += retained
                    records_loaded += retained + land_objects

                    self.loader.update_ingest_run(
                        run_id,
                        IngestRunUpdate(
                            status="running",
                            records_fetched=records_fetched,
                            records_loaded=records_loaded,
                            records_retained=records_retained,
                            metadata={
                                "last_completed_county": archive.county_name,
                                "county_failures": county_failures,
                            },
                        ),
                    )
                except Exception as county_exc:
                    county_failures.append(
                        {
                            "county_code": archive.county_code,
                            "county_name": archive.county_name,
                            "error": str(county_exc),
                        }
                    )
                    self.logger.exception(
                        "ros_county_processing_failed",
                        extra={
                            "county_code": archive.county_code,
                            "county_name": archive.county_name,
                        },
                    )

            if records_retained == 0 and county_failures:
                status = "failed"
                error_message = "All county parcel ingests failed."
            elif county_failures:
                status = "partial_success"
                error_message = f"{len(county_failures)} county archives failed."
            else:
                status = "success"
                error_message = None

            if records_retained > 0:
                self.loader.refresh_cached_outputs()
                cache_refreshed = True

            if self.settings.staging_retention_days > 0:
                staging_pruned = self.loader.prune_staging_data()

            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status=status,
                    records_fetched=records_fetched,
                    records_loaded=records_loaded,
                    records_retained=records_retained,
                    error_message=error_message,
                    metadata={
                        "county_failures": county_failures,
                        "cache_refreshed": cache_refreshed,
                        "staging_pruned": staging_pruned,
                        "persist_staging_rows": self.settings.persist_staging_rows,
                    },
                    finished=True,
                ),
            )

            if status == "failed":
                raise RuntimeError(error_message or "RoS parcel ingestion failed.")
        except Exception:
            raise

    def full_refresh(self) -> None:
        """Run the full Stage 1 refresh sequence."""

        stage_failures: list[dict[str, str]] = []
        for stage_name, func in (
            ("discover_sources", self.discover_sources),
            ("load_boundaries", self.load_boundaries),
            ("ingest_ros_cadastral", self.ingest_ros_cadastral),
        ):
            try:
                func()
            except Exception as exc:
                stage_failures.append({"stage": stage_name, "error": str(exc)})
                self.logger.exception("full_refresh_stage_failed", extra={"stage": stage_name})

        if stage_failures:
            raise RuntimeError(f"Full refresh completed with failures: {stage_failures}")


def run_full_refresh(settings: Settings, logger: logging.Logger) -> None:
    """Convenience wrapper for scheduler entry points."""

    pipeline = LandIntelPipeline(settings, logger)
    try:
        pipeline.prepare_database()
        pipeline.full_refresh()
    finally:
        pipeline.close()


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""

    parser = argparse.ArgumentParser(description="LandIntel Scotland ingestion worker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("run-migrations", help="Execute SQL migrations")
    subparsers.add_parser("discover-sources", help="Discover source metadata records")
    subparsers.add_parser("load-boundaries", help="Load target authority boundaries")
    subparsers.add_parser("ingest-ros-cadastral", help="Ingest and clip RoS parcels")
    subparsers.add_parser("full-refresh", help="Run the full Stage 1 refresh sequence")

    return parser


def main() -> int:
    """CLI entry point."""

    settings = get_settings()
    logger = configure_logging(settings)
    args = build_parser().parse_args()

    pipeline = LandIntelPipeline(settings, logger)
    try:
        def run_stage(stage: Callable[[], None]) -> None:
            pipeline.prepare_database()
            stage()

        command_map: dict[str, Callable[[], None]] = {
            "run-migrations": pipeline.prepare_database,
            "discover-sources": lambda: run_stage(pipeline.discover_sources),
            "load-boundaries": lambda: run_stage(pipeline.load_boundaries),
            "ingest-ros-cadastral": lambda: run_stage(pipeline.ingest_ros_cadastral),
            "full-refresh": lambda: run_stage(pipeline.full_refresh),
        }
        command_map[args.command]()
        return 0
    except Exception:
        logger.exception("pipeline_command_failed", extra={"command": args.command})
        return 1
    finally:
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())

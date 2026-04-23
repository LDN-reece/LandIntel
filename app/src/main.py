"""CLI entry points for the Scotland land ingestion worker."""

from __future__ import annotations

import argparse
import logging
import traceback
from pathlib import Path
from typing import Callable

from config.settings import Settings, get_settings
from src.db import Database
from src.fetchers.boundaries import BoundaryFetcher
from src.fetchers.geonetwork import GeoNetworkClient
from src.fetchers.ros_cadastral import ROS_SOURCE_NAME, RoSCadastralFetcher
from src.loaders.supabase_loader import SupabaseLoader
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.processors.bgs_boreholes import (
    BGS_BOREHOLE_SOURCE_NAME,
    inspect_bgs_borehole_archive,
    iter_bgs_borehole_batches,
)
from src.processors.calculate_area import calculate_area_metrics
from src.processors.classify_size import classify_size_buckets
from src.processors.clip_to_authorities import clip_parcels_to_authorities
from src.processors.extract import choose_preferred_candidate, extract_archive
from src.processors.filter_operational_candidates import filter_operational_candidates
from src.processors.normalise import load_preferred_spatial_frame, normalise_ros_cadastral_frame
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
                run_id=run_id,
                source_name="Local Authority Areas - Scotland",
                artifact_role="source_download",
                retention_class="archive",
                source_url=download.download_url,
                metadata={"dataset": "authority_boundaries"},
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
                        "download_url": download.download_url,
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
        artifacts_pruned = {"deleted": 0, "failed": 0}
        cleanup_summary = {"deleted_parcel_rows": 0, "deleted_land_object_rows": 0, "deleted_parcel_area_acres": 0.0}
        filtered_out_rows = 0
        filtered_out_area_acres = 0.0

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
                        run_id=run_id,
                        source_name=ROS_SOURCE_NAME,
                        authority_name=archive.county_name,
                        artifact_role="source_download",
                        retention_class="archive",
                        source_url=archive.download_url,
                        metadata={"county_code": archive.county_code},
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
                    operational, operational_summary = filter_operational_candidates(
                        enriched,
                        minimum_area_acres=self.settings.minimum_operational_area_acres,
                    )
                    filtered_out_rows += int(operational_summary["filtered_out_rows"])
                    filtered_out_area_acres += float(operational_summary["filtered_out_area_acres"])

                    retained = self.loader.upsert_processed_parcels(operational)
                    land_objects = (
                        self.loader.upsert_land_objects(operational)
                        if self.settings.mirror_land_objects
                        else 0
                    )
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
                                "minimum_operational_area_acres": self.settings.minimum_operational_area_acres,
                                "mirror_land_objects": self.settings.mirror_land_objects,
                                "filtered_out_rows": filtered_out_rows,
                                "filtered_out_area_acres": round(filtered_out_area_acres, 3),
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

            if records_retained > 0 or not self.settings.mirror_land_objects:
                cleanup_summary = self.loader.cleanup_operational_footprint(
                    minimum_area_acres=self.settings.minimum_operational_area_acres,
                    drop_land_object_mirror=not self.settings.mirror_land_objects,
                )
                cache_refreshed = True

            if self.settings.staging_retention_days > 0:
                staging_pruned = self.loader.prune_staging_data()
            artifacts_pruned = self.loader.prune_expired_artifacts()

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
                        "artifacts_pruned": artifacts_pruned,
                        "cleanup_summary": cleanup_summary,
                        "minimum_operational_area_acres": self.settings.minimum_operational_area_acres,
                        "mirror_land_objects": self.settings.mirror_land_objects,
                        "filtered_out_rows": filtered_out_rows,
                        "filtered_out_area_acres": round(filtered_out_area_acres, 3),
                        "persist_staging_rows": self.settings.persist_staging_rows,
                    },
                    finished=True,
                ),
            )

            if status == "failed":
                raise RuntimeError(error_message or "RoS parcel ingestion failed.")
        except Exception:
            raise

    def ingest_bgs_boreholes(self, archive_path: Path) -> dict[str, object]:
        """Load the BGS borehole archive into raw, master, and compatibility layers."""

        archive_path = archive_path.expanduser()
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="ingest_bgs_boreholes",
                source_name=BGS_BOREHOLE_SOURCE_NAME,
                status="running",
                metadata={"archive_path": str(archive_path), "archive_name": archive_path.name},
            )
        )

        try:
            source_info = inspect_bgs_borehole_archive(archive_path)
            self.loader.upload_audit_artifact(
                archive_path,
                f"bgs_boreholes/{run_id}/{archive_path.name}",
                run_id=run_id,
                source_name=BGS_BOREHOLE_SOURCE_NAME,
                artifact_role="source_download",
                retention_class="archive",
                metadata={
                    "dataset_key": "bgs_single_onshore_borehole_index",
                    "source_snapshot_date": source_info.source_snapshot_date.isoformat(),
                    "feature_count": source_info.feature_count,
                    "internal_member_path": source_info.internal_member_path,
                    "crs": source_info.crs,
                },
            )

            loaded_raw_rows = 0
            progress_step = max(self.settings.batch_size * 25, self.settings.batch_size)
            for batch in iter_bgs_borehole_batches(source_info, batch_size=self.settings.batch_size):
                loaded_raw_rows += self.loader.insert_bgs_boreholes_raw_rows(
                    batch.rows,
                    ingest_run_id=run_id,
                    source_archive_name=archive_path.name,
                    source_file_name=source_info.source_file_name,
                    source_snapshot_date=source_info.source_snapshot_date,
                )
                if loaded_raw_rows % progress_step == 0 or loaded_raw_rows == source_info.feature_count:
                    self.loader.update_ingest_run(
                        run_id,
                        IngestRunUpdate(
                            status="running",
                            records_fetched=loaded_raw_rows,
                            records_loaded=loaded_raw_rows,
                            records_retained=0,
                            metadata={
                                "archive_name": archive_path.name,
                                "source_snapshot_date": source_info.source_snapshot_date.isoformat(),
                                "feature_count": source_info.feature_count,
                                "loaded_raw_rows": loaded_raw_rows,
                            },
                        ),
                    )

            master_summary = self.loader.refresh_bgs_boreholes(run_id)
            compatibility_summary = self.loader.refresh_bgs_site_constraints(source_ingest_run_id=run_id)
            normalised_rows = int(master_summary.get("normalised_rows", 0) or 0)
            borehole_rows = int(compatibility_summary.get("borehole_rows_refreshed", 0) or 0)
            site_investigation_rows = int(compatibility_summary.get("site_investigation_rows_refreshed", 0) or 0)
            summary: dict[str, object] = {
                "run_id": run_id,
                "archive_name": archive_path.name,
                "archive_path": str(archive_path),
                "source_file_name": source_info.source_file_name,
                "source_snapshot_date": source_info.source_snapshot_date.isoformat(),
                "feature_count": source_info.feature_count,
                "bounds": source_info.bounds,
                "raw_rows_loaded": loaded_raw_rows,
                **master_summary,
                **compatibility_summary,
            }
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=loaded_raw_rows,
                    records_loaded=loaded_raw_rows + normalised_rows + borehole_rows + site_investigation_rows,
                    records_retained=normalised_rows,
                    metadata=summary,
                    finished=True,
                ),
            )
            self.logger.info("bgs_borehole_ingest_completed", extra=summary)
            return summary
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="failed",
                    error_message=str(exc),
                    metadata={"archive_path": str(archive_path), "traceback": traceback.format_exc()},
                    finished=True,
                ),
            )
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
    bgs_parser = subparsers.add_parser(
        "ingest-bgs-boreholes",
        help="Load the authoritative BGS borehole archive and refresh borehole-derived site evidence",
    )
    bgs_parser.add_argument("--archive-path", required=True, help="Absolute path to the BGS borehole ZIP archive")
    bgs_parser.add_argument(
        "--process-site-refresh-queue",
        action="store_true",
        help="Immediately process queued site recalculations after the borehole evidence refresh completes",
    )
    bgs_parser.add_argument(
        "--site-refresh-limit",
        type=int,
        default=200,
        help="Maximum queued site refreshes to process when --process-site-refresh-queue is used",
    )
    subparsers.add_parser("full-refresh", help="Run the full Stage 1 refresh sequence")
    subparsers.add_parser(
        "audit-operational-footprint",
        help="Summarise the live parcel and site footprint currently stored in Supabase",
    )
    cleanup_parser = subparsers.add_parser(
        "cleanup-operational-footprint",
        help="Delete legacy low-value parcel rows and the optional duplicate land-object mirror",
    )
    cleanup_parser.add_argument(
        "--min-area-acres",
        type=float,
        default=None,
        help="Minimum acreage to keep in public.ros_cadastral_parcels; defaults to MIN_OPERATIONAL_AREA_ACRES",
    )
    cleanup_parser.add_argument(
        "--keep-land-objects",
        action="store_true",
        help="Keep the duplicate public.land_objects mirror instead of deleting unreferenced mirror rows",
    )
    prune_parser = subparsers.add_parser(
        "prune-audit-artifacts",
        help="Delete expired audit artifacts from Supabase Storage while keeping manifest rows",
    )
    prune_parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum expired stored artifacts to delete in one run",
    )

    seed_parser = subparsers.add_parser(
        "seed-mvp-sites",
        help="Create or refresh seeded site aggregates for the qualification MVP",
    )
    seed_parser.add_argument("--limit", type=int, default=6, help="Number of curated Scottish portfolio scenarios to seed")

    refresh_parser = subparsers.add_parser(
        "refresh-site-qualifications",
        help="Compatibility alias for Phase One opportunity refresh",
    )
    refresh_parser.add_argument("--site-id", action="append", help="Specific site UUID to refresh")
    refresh_parser.add_argument("--site-code", action="append", help="Specific site code to refresh")
    refresh_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum queued refresh requests to process when no explicit sites are supplied",
    )
    refresh_phase_one_parser = subparsers.add_parser(
        "refresh-opportunities",
        help="Reprocess queued canonical opportunities or refresh explicitly selected ids",
    )
    refresh_phase_one_parser.add_argument("--site-id", action="append", help="Specific canonical site UUID to refresh")
    refresh_phase_one_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum queued refresh requests to process when no explicit sites are supplied",
    )

    publish_planning_parser = subparsers.add_parser(
        "publish-planning-links",
        help="Publish resolved planning reconcile links back into the live canonical site layer",
    )
    publish_planning_parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum resolved planning rows to publish",
    )

    weekly_planning_parser = subparsers.add_parser(
        "weekly-planning-review",
        help="Run the weekly planning publish, change event, and queue refresh cycle",
    )
    weekly_planning_parser.add_argument("--publish-limit", type=int, default=1000)
    weekly_planning_parser.add_argument("--refresh-limit", type=int, default=200)

    weekly_policy_parser = subparsers.add_parser(
        "weekly-policy-review",
        help="Run the weekly policy, HLA, ELA, VDL, and settlement review cycle",
    )
    weekly_policy_parser.add_argument("--refresh-limit", type=int, default=200)

    serve_parser = subparsers.add_parser(
        "serve-review-ui",
        help="Run the internal site search and review UI",
    )
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)

    return parser


def main() -> int:
    """CLI entry point."""

    settings = get_settings()
    logger = configure_logging(settings)
    args = build_parser().parse_args()

    if args.command == "serve-review-ui":
        try:
            from src.web.app import serve

            serve(settings, host=args.host, port=args.port)
            return 0
        except Exception:
            logger.exception("pipeline_command_failed", extra={"command": args.command})
            return 1

    pipeline = LandIntelPipeline(settings, logger)
    site_service = None
    try:
        def run_stage(stage: Callable[[], None]) -> None:
            pipeline.prepare_database()
            stage()

        if args.command in {
            "run-migrations",
            "discover-sources",
            "load-boundaries",
            "ingest-ros-cadastral",
            "full-refresh",
            "audit-operational-footprint",
            "cleanup-operational-footprint",
            "prune-audit-artifacts",
            "ingest-bgs-boreholes",
        }:
            if args.command == "ingest-bgs-boreholes":
                pipeline.prepare_database()
                bgs_summary = pipeline.ingest_bgs_boreholes(Path(args.archive_path))
                logger.info("bgs_borehole_ingest_summary", extra=bgs_summary)
                if args.process_site_refresh_queue:
                    from src.opportunity_engine.service import OpportunityService

                    site_service = OpportunityService(settings, logger)
                    refresh_summary = site_service.process_pending_refresh_requests(limit=args.site_refresh_limit)
                    logger.info("bgs_borehole_site_refresh_summary", extra=refresh_summary)
            else:
                command_map: dict[str, Callable[[], None]] = {
                    "run-migrations": pipeline.prepare_database,
                    "discover-sources": lambda: run_stage(pipeline.discover_sources),
                    "load-boundaries": lambda: run_stage(pipeline.load_boundaries),
                    "ingest-ros-cadastral": lambda: run_stage(pipeline.ingest_ros_cadastral),
                    "full-refresh": lambda: run_stage(pipeline.full_refresh),
                    "audit-operational-footprint": lambda: run_stage(
                        lambda: logger.info(
                            "operational_footprint_audit",
                            extra=pipeline.loader.audit_operational_footprint(
                                minimum_area_acres=settings.minimum_operational_area_acres,
                            ),
                        )
                    ),
                    "cleanup-operational-footprint": lambda: run_stage(
                        lambda: logger.info(
                            "operational_footprint_cleanup",
                            extra=pipeline.loader.cleanup_operational_footprint(
                                minimum_area_acres=(
                                    args.min_area_acres
                                    if args.min_area_acres is not None
                                    else settings.minimum_operational_area_acres
                                ),
                                drop_land_object_mirror=not args.keep_land_objects,
                            ),
                        )
                    ),
                    "prune-audit-artifacts": lambda: run_stage(
                        lambda: pipeline.loader.prune_expired_artifacts(limit=args.limit)
                    ),
                }
                command_map[args.command]()
        else:
            pipeline.prepare_database()
            if args.command == "seed-mvp-sites":
                logger.error(
                    "seed_mvp_sites_retired",
                    extra={
                        "command": args.command,
                        "message": "The old MVP site seeding command has been retired in Phase One. Use the live canonical-site pipeline instead.",
                    },
                )
                return 1
            else:
                from src.opportunity_engine.service import OpportunityService

                site_service = OpportunityService(settings, logger)
                if args.command in {"refresh-site-qualifications", "refresh-opportunities"}:
                    explicit_site_ids = list(args.site_id or [])
                    if args.command == "refresh-site-qualifications" and getattr(args, "site_code", None):
                        logger.warning(
                            "site_code_refresh_no_longer_supported",
                            extra={"site_codes": list(args.site_code or [])},
                        )
                    if explicit_site_ids:
                        logger.info(
                            "phase_one_refresh_explicit_summary",
                            extra=site_service.refresh_explicit_sites(explicit_site_ids),
                        )
                    else:
                        logger.info(
                            "phase_one_refresh_queue_summary",
                            extra=site_service.process_pending_refresh_requests(limit=args.limit),
                        )
                elif args.command == "publish-planning-links":
                    logger.info(
                        "phase_one_publish_planning_summary",
                        extra=site_service.publish_planning_links(limit=args.limit),
                    )
                elif args.command == "weekly-planning-review":
                    logger.info(
                        "phase_one_weekly_planning_summary",
                        extra=site_service.run_weekly_planning_review(
                            publish_limit=args.publish_limit,
                            refresh_limit=args.refresh_limit,
                        ),
                    )
                elif args.command == "weekly-policy-review":
                    logger.info(
                        "phase_one_weekly_policy_summary",
                        extra=site_service.run_weekly_policy_review(refresh_limit=args.refresh_limit),
                    )
        return 0
    except Exception:
        logger.exception("pipeline_command_failed", extra={"command": args.command})
        return 1
    finally:
        if site_service is not None:
            site_service.close()
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())

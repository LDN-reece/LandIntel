"""Lean GitHub Actions entrypoints for operational parcel control."""

from __future__ import annotations

import argparse
import os
import traceback
from typing import Any, Callable

from config.settings import Settings, get_settings
from src.logging_config import configure_logging
from src.main import LandIntelPipeline
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.processors.calculate_area import calculate_area_metrics
from src.processors.classify_size import classify_size_buckets
from src.processors.clip_to_authorities import clip_parcels_to_authorities
from src.processors.extract import choose_preferred_candidate, extract_archive
from src.processors.filter_operational_candidates import filter_operational_candidates
from src.processors.normalise import load_preferred_spatial_frame, normalise_ros_cadastral_frame
from src.processors.validate_geometry import repair_invalid_geometries


def _default_min_area_acres() -> float:
    try:
        return float(os.getenv("MIN_OPERATIONAL_AREA_ACRES", "4"))
    except ValueError:
        return 4.0


def _default_mirror_land_objects() -> bool:
    return os.getenv("MIRROR_LAND_OBJECTS", "false").strip().lower() in {"1", "true", "yes", "on"}


def _safe_refresh_cached_outputs(pipeline: LandIntelPipeline) -> None:
    try:
        pipeline.loader.refresh_cached_outputs()
    except Exception:
        pipeline.logger.warning(
            "cached_output_refresh_failed",
            extra={"traceback": traceback.format_exc()},
        )


def audit_operational_footprint(
    pipeline: LandIntelPipeline,
    *,
    minimum_area_acres: float,
) -> dict[str, Any]:
    """Report the current parcel and land object footprint in Supabase."""

    summary = pipeline.loader.database.fetch_one(
        """
            select
                (select count(*) from public.authority_aoi) as authority_count,
                (select count(*) from public.source_registry) as source_registry_count,
                (select count(*) from public.ingest_runs) as ingest_run_count,
                (select count(*) from public.ros_cadastral_parcels) as parcel_count,
                (select count(*) from public.ros_cadastral_parcels where coalesce(area_acres, 0) < :min_area_acres) as parcel_under_min_count,
                (select count(*) from public.ros_cadastral_parcels where coalesce(area_acres, 0) >= :min_area_acres) as parcel_over_min_count,
                (select count(*) from public.land_objects) as land_object_count
        """,
        {"min_area_acres": minimum_area_acres},
    ) or {}
    authority_rows = pipeline.loader.database.fetch_all(
        """
            select
                authority_name,
                count(*) as parcel_count,
                count(*) filter (where coalesce(area_acres, 0) < :min_area_acres) as under_min_count,
                count(*) filter (where coalesce(area_acres, 0) >= :min_area_acres) as over_min_count,
                round(coalesce(sum(area_acres), 0)::numeric, 2) as total_area_acres
            from public.ros_cadastral_parcels
            group by authority_name
            order by authority_name
        """,
        {"min_area_acres": minimum_area_acres},
    )
    payload = {
        "minimum_area_acres": minimum_area_acres,
        **summary,
        "authority_rows": authority_rows,
    }
    pipeline.logger.info("operational_footprint_audit", extra=payload)
    return payload


def cleanup_operational_footprint(
    pipeline: LandIntelPipeline,
    *,
    minimum_area_acres: float,
    drop_land_object_mirror: bool,
) -> dict[str, Any]:
    """Delete low-value operational rows from the live Supabase footprint."""

    deleted_land_objects = {"deleted_rows": 0, "deleted_area_acres": 0.0}
    if drop_land_object_mirror:
        deleted_land_objects = pipeline.loader.database.fetch_one(
            """
                with deleted as (
                    delete from public.land_objects
                    where object_type = 'ros_cadastral_parcel'
                      and source_system in ('ros_inspire', 'ros_cadastral')
                    returning coalesce(area_acres, 0)::double precision as area_acres
                )
                select
                    count(*) as deleted_rows,
                    coalesce(sum(area_acres), 0) as deleted_area_acres
                from deleted
            """
        ) or deleted_land_objects

    deleted_parcels = pipeline.loader.database.fetch_one(
        """
            with deleted as (
                delete from public.ros_cadastral_parcels
                where coalesce(area_acres, 0) < :min_area_acres
                returning coalesce(area_acres, 0)::double precision as area_acres
            )
            select
                count(*) as deleted_rows,
                coalesce(sum(area_acres), 0) as deleted_area_acres
            from deleted
        """,
        {"min_area_acres": minimum_area_acres},
    ) or {"deleted_rows": 0, "deleted_area_acres": 0.0}

    _safe_refresh_cached_outputs(pipeline)

    payload = {
        "minimum_area_acres": minimum_area_acres,
        "drop_land_object_mirror": drop_land_object_mirror,
        "deleted_parcel_rows": int(deleted_parcels.get("deleted_rows", 0)),
        "deleted_parcel_area_acres": float(deleted_parcels.get("deleted_area_acres", 0.0)),
        "deleted_land_object_rows": int(deleted_land_objects.get("deleted_rows", 0)),
        "deleted_land_object_area_acres": float(deleted_land_objects.get("deleted_area_acres", 0.0)),
    }
    pipeline.logger.info("operational_footprint_cleanup", extra=payload)
    return payload


def ingest_ros_cadastral_lean(
    pipeline: LandIntelPipeline,
    *,
    minimum_area_acres: float,
    mirror_land_objects: bool,
) -> None:
    """Run a lean parcel ingest that only persists operational candidates."""

    authority_gdf = pipeline.loader.fetch_active_authorities()
    if authority_gdf.empty:
        raise RuntimeError(
            "No authority AOIs were found in public.authority_aoi. Run load-boundaries first."
        )

    run_id = pipeline.loader.create_ingest_run(
        IngestRunRecord(
            run_type="ingest_ros_cadastral_lean",
            source_name=pipeline.ros.build_source_registry_record().source_name,
            status="running",
            metadata={
                "target_authority_count": len(pipeline.target_authorities),
                "minimum_operational_area_acres": minimum_area_acres,
                "mirror_land_objects": mirror_land_objects,
            },
        )
    )

    records_fetched = 0
    records_loaded = 0
    records_retained = 0
    filtered_out_rows = 0
    filtered_out_area_acres = 0.0
    county_failures: list[dict[str, str]] = []
    cache_refreshed = False
    staging_pruned = {"raw_deleted": 0, "clean_deleted": 0}
    cleanup_summary = {"deleted_parcel_rows": 0, "deleted_land_object_rows": 0}

    try:
        pipeline.loader.upsert_source_registry([pipeline.ros.build_source_registry_record()])
        archives = pipeline.ros.download_county_archives(
            pipeline.settings.temp_storage_path / "ros_cadastral" / run_id / "downloads"
        )

        for archive in archives:
            try:
                pipeline.loader.upload_audit_artifact(
                    archive.local_path,
                    f"ros_cadastral/{run_id}/downloads/{archive.local_path.name}",
                )
                extracted_dir = (
                    pipeline.settings.temp_storage_path
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
                    source_name=pipeline.ros.build_source_registry_record().source_name,
                    source_file=archive.local_path.name,
                    source_county=archive.county_name,
                )
                records_fetched += len(raw_gdf)
                raw_loaded = pipeline.loader.insert_raw_parcels(raw_gdf)

                clean_gdf = repair_invalid_geometries(raw_gdf, pipeline.logger)
                clean_loaded = pipeline.loader.insert_clean_parcels(clean_gdf)
                records_loaded += raw_loaded + clean_loaded

                clipped = clip_parcels_to_authorities(clean_gdf, authority_gdf, pipeline.logger)
                enriched = classify_size_buckets(calculate_area_metrics(clipped))
                operational, summary = filter_operational_candidates(
                    enriched,
                    minimum_area_acres=minimum_area_acres,
                )
                filtered_out_rows += int(summary["filtered_out_rows"])
                filtered_out_area_acres += float(summary["filtered_out_area_acres"])

                retained = pipeline.loader.upsert_processed_parcels(operational)
                land_objects = (
                    pipeline.loader.upsert_land_objects(operational)
                    if mirror_land_objects
                    else 0
                )
                records_retained += retained
                records_loaded += retained + land_objects

                pipeline.loader.update_ingest_run(
                    run_id,
                    IngestRunUpdate(
                        status="running",
                        records_fetched=records_fetched,
                        records_loaded=records_loaded,
                        records_retained=records_retained,
                        metadata={
                            "last_completed_county": archive.county_name,
                            "county_failures": county_failures,
                            "minimum_operational_area_acres": minimum_area_acres,
                            "mirror_land_objects": mirror_land_objects,
                            "filtered_out_rows": filtered_out_rows,
                            "filtered_out_area_acres": round(filtered_out_area_acres, 6),
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
                pipeline.logger.exception(
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
            _safe_refresh_cached_outputs(pipeline)
            cache_refreshed = True

        if pipeline.settings.staging_retention_days > 0:
            staging_pruned = pipeline.loader.prune_staging_data()

        cleanup_summary = cleanup_operational_footprint(
            pipeline,
            minimum_area_acres=minimum_area_acres,
            drop_land_object_mirror=not mirror_land_objects,
        )

        pipeline.loader.update_ingest_run(
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
                    "persist_staging_rows": pipeline.settings.persist_staging_rows,
                    "minimum_operational_area_acres": minimum_area_acres,
                    "mirror_land_objects": mirror_land_objects,
                    "filtered_out_rows": filtered_out_rows,
                    "filtered_out_area_acres": round(filtered_out_area_acres, 6),
                    "cleanup_summary": cleanup_summary,
                },
                finished=True,
            ),
        )

        if status == "failed":
            raise RuntimeError(error_message or "Lean RoS parcel ingestion failed.")
    except Exception as exc:
        pipeline.loader.update_ingest_run(
            run_id,
            IngestRunUpdate(
                status="failed",
                error_message=str(exc),
                metadata={"traceback": traceback.format_exc()},
                finished=True,
            ),
        )
        raise


def full_refresh_lean(
    settings: Settings,
    *,
    minimum_area_acres: float,
    mirror_land_objects: bool,
) -> None:
    """Run the lean GitHub-first refresh path."""

    logger = configure_logging(settings)
    pipeline = LandIntelPipeline(settings, logger)
    try:
        pipeline.prepare_database()
        pipeline.discover_sources()
        pipeline.load_boundaries()
        ingest_ros_cadastral_lean(
            pipeline,
            minimum_area_acres=minimum_area_acres,
            mirror_land_objects=mirror_land_objects,
        )
    finally:
        pipeline.close()


def build_parser() -> argparse.ArgumentParser:
    """Build the lean operations CLI parser."""

    parser = argparse.ArgumentParser(description="LandIntel lean GitHub Actions runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("audit-operational-footprint", "Report the current Supabase operational footprint"),
        ("cleanup-operational-footprint", "Delete low-value parcel rows and parcel mirror rows"),
        ("ingest-ros-cadastral-lean", "Ingest parcels with a minimum operational acreage filter"),
        ("full-refresh-lean", "Run discover, boundaries, and lean parcel ingest"),
    ):
        command = subparsers.add_parser(name, help=help_text)
        command.add_argument("--min-area-acres", type=float, default=_default_min_area_acres())
        command.add_argument(
            "--mirror-land-objects",
            action="store_true",
            default=_default_mirror_land_objects(),
            help="Persist the parcel mirror rows into public.land_objects",
        )

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
            "audit-operational-footprint": lambda: run_stage(
                lambda: audit_operational_footprint(
                    pipeline,
                    minimum_area_acres=args.min_area_acres,
                )
            ),
            "cleanup-operational-footprint": lambda: run_stage(
                lambda: cleanup_operational_footprint(
                    pipeline,
                    minimum_area_acres=args.min_area_acres,
                    drop_land_object_mirror=not bool(args.mirror_land_objects),
                )
            ),
            "ingest-ros-cadastral-lean": lambda: run_stage(
                lambda: ingest_ros_cadastral_lean(
                    pipeline,
                    minimum_area_acres=args.min_area_acres,
                    mirror_land_objects=bool(args.mirror_land_objects),
                )
            ),
            "full-refresh-lean": lambda: full_refresh_lean(
                settings,
                minimum_area_acres=args.min_area_acres,
                mirror_land_objects=bool(args.mirror_land_objects),
            ),
        }
        command_map[args.command]()
        return 0
    except Exception:
        logger.exception("lean_pipeline_command_failed", extra={"command": args.command})
        return 1
    finally:
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())

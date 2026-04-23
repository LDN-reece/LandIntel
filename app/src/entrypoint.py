"""Railway-friendly process entrypoint."""

from __future__ import annotations

from config.settings import get_settings
from src.logging_config import configure_logging
from src.main import LandIntelPipeline, run_full_refresh
from src.scheduler import start_scheduler


def run_startup_command(command: str, logger) -> int:
    """Run an explicit startup command when requested."""

    settings = get_settings()

    if command == "full-refresh":
        run_full_refresh(settings, logger)
        return 0

    pipeline = LandIntelPipeline(settings, logger)
    site_service = None
    try:
        if command == "run-migrations":
            pipeline.prepare_database()
        elif command == "discover-sources":
            pipeline.prepare_database()
            pipeline.discover_sources()
        elif command == "load-boundaries":
            pipeline.prepare_database()
            pipeline.load_boundaries()
        elif command == "ingest-ros-cadastral":
            pipeline.prepare_database()
            pipeline.ingest_ros_cadastral()
        elif command == "ingest-bgs-boreholes":
            if settings.bgs_borehole_archive_path is None:
                raise ValueError("BGS_BOREHOLE_ARCHIVE_PATH must be set when STARTUP_COMMAND=ingest-bgs-boreholes.")
            pipeline.prepare_database()
            logger.info(
                "bgs_borehole_ingest_summary",
                extra=pipeline.ingest_bgs_boreholes(settings.bgs_borehole_archive_path),
            )
            if settings.process_site_refresh_queue_after_bgs:
                from src.opportunity_engine.service import OpportunityService

                site_service = OpportunityService(settings, logger)
                logger.info(
                    "bgs_borehole_site_refresh_summary",
                    extra=site_service.process_pending_refresh_requests(limit=settings.bgs_site_refresh_limit),
                )
        elif command == "audit-operational-footprint":
            pipeline.prepare_database()
            logger.info(
                "operational_footprint_audit",
                extra=pipeline.loader.audit_operational_footprint(
                    minimum_area_acres=settings.minimum_operational_area_acres,
                ),
            )
        elif command == "cleanup-operational-footprint":
            pipeline.prepare_database()
            logger.info(
                "operational_footprint_cleanup",
                extra=pipeline.loader.cleanup_operational_footprint(
                    minimum_area_acres=settings.minimum_operational_area_acres,
                    drop_land_object_mirror=not settings.mirror_land_objects,
                ),
            )
        elif command in {"refresh-opportunities", "publish-planning-links", "weekly-planning-review", "weekly-policy-review"}:
            from src.opportunity_engine.service import OpportunityService

            pipeline.prepare_database()
            site_service = OpportunityService(settings, logger)
            if command == "refresh-opportunities":
                logger.info(
                    "phase_one_refresh_queue_summary",
                    extra=site_service.process_pending_refresh_requests(limit=settings.bgs_site_refresh_limit),
                )
            elif command == "publish-planning-links":
                logger.info(
                    "phase_one_publish_planning_summary",
                    extra=site_service.publish_planning_links(),
                )
            elif command == "weekly-planning-review":
                logger.info(
                    "phase_one_weekly_planning_summary",
                    extra=site_service.run_weekly_planning_review(refresh_limit=settings.bgs_site_refresh_limit),
                )
            elif command == "weekly-policy-review":
                logger.info(
                    "phase_one_weekly_policy_summary",
                    extra=site_service.run_weekly_policy_review(refresh_limit=settings.bgs_site_refresh_limit),
                )
        else:
            raise ValueError(f"Unsupported STARTUP_COMMAND: {command}")
        return 0
    finally:
        if site_service is not None:
            site_service.close()
        pipeline.close()


def main() -> int:
    """Start safely by default, or run a requested command."""

    settings = get_settings()
    logger = configure_logging(settings)

    if settings.enable_internal_scheduler:
        logger.info("worker_mode_selected", extra={"mode": "scheduler"})
        start_scheduler(settings=settings, logger=logger)
        return 0

    if settings.startup_command == "none":
        logger.info("worker_mode_selected", extra={"mode": "idle"})
        return 0

    logger.info(
        "worker_mode_selected",
        extra={"mode": "startup_command", "command": settings.startup_command},
    )
    run_startup_command(settings.startup_command, logger)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Internal scheduler for recurring Phase One review jobs."""

from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config.settings import Settings, get_settings
from src.logging_config import configure_logging
from src.opportunity_engine.service import OpportunityService


def start_scheduler(
    settings: Settings | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Run weekly planning and policy review jobs plus queue catch-up."""

    settings = settings or get_settings()
    logger = logger or configure_logging(settings)
    scheduler = BlockingScheduler(timezone="Europe/London")

    def run_planning_review() -> None:
        service = OpportunityService(settings, logger)
        try:
            logger.info("weekly_planning_review_started")
            logger.info("weekly_planning_review_completed", extra=service.run_weekly_planning_review())
        finally:
            service.close()

    def run_policy_review() -> None:
        service = OpportunityService(settings, logger)
        try:
            logger.info("weekly_policy_review_started")
            logger.info("weekly_policy_review_completed", extra=service.run_weekly_policy_review())
        finally:
            service.close()

    def run_refresh_queue() -> None:
        service = OpportunityService(settings, logger)
        try:
            logger.info("refresh_queue_catchup_started")
            logger.info("refresh_queue_catchup_completed", extra=service.process_pending_refresh_requests(limit=200))
        finally:
            service.close()

    scheduler.add_job(
        run_planning_review,
        CronTrigger.from_crontab(settings.planning_review_cron, timezone="Europe/London"),
        id="weekly-planning-review",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_policy_review,
        CronTrigger.from_crontab(settings.policy_review_cron, timezone="Europe/London"),
        id="weekly-policy-review",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        run_refresh_queue,
        CronTrigger.from_crontab(settings.refresh_queue_cron, timezone="Europe/London"),
        id="refresh-queue-catchup",
        max_instances=1,
        coalesce=True,
    )

    logger.info(
        "scheduler_started",
        extra={
            "planning_review_cron": settings.planning_review_cron,
            "policy_review_cron": settings.policy_review_cron,
            "refresh_queue_cron": settings.refresh_queue_cron,
        },
    )
    scheduler.start()


if __name__ == "__main__":
    start_scheduler()

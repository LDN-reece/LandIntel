"""Optional internal scheduler for long-running worker mode."""

from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config.settings import Settings, get_settings
from src.logging_config import configure_logging
from src.main import run_full_refresh


def start_scheduler(
    settings: Settings | None = None,
    logger: logging.Logger | None = None,
) -> None:
    """Start a quarterly scheduler loop.

    Railway cron jobs are the preferred production trigger. This module exists so the
    same image can also run as a persistent scheduler when needed.
    """

    settings = settings or get_settings()
    logger = logger or configure_logging(settings)

    scheduler = BlockingScheduler(timezone="Europe/London")
    scheduler.add_job(
        lambda: run_full_refresh(settings, logger),
        CronTrigger.from_crontab(settings.quarterly_cron, timezone="Europe/London"),
        id="quarterly-landintel-refresh",
        max_instances=1,
        coalesce=True,
    )

    logger.info("scheduler_started", extra={"cron": settings.quarterly_cron})
    scheduler.start()


if __name__ == "__main__":
    start_scheduler()

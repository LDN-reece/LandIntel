"""Railway-friendly process entrypoint."""

from __future__ import annotations

from config.settings import get_settings
from src.logging_config import configure_logging
from src.main import run_full_refresh
from src.scheduler import start_scheduler


def main() -> int:
    """Run once by default, or start the internal scheduler when enabled."""

    settings = get_settings()
    logger = configure_logging(settings)

    if settings.enable_internal_scheduler:
        logger.info("worker_mode_selected", extra={"mode": "scheduler"})
        start_scheduler(settings=settings, logger=logger)
        return 0

    logger.info("worker_mode_selected", extra={"mode": "run_once"})
    run_full_refresh(settings, logger)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

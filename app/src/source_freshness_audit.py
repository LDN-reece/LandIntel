"""Live source freshness audit for LandIntel Phase One."""

from __future__ import annotations

import argparse

from config.settings import get_settings
from src.logging_config import configure_logging
from src.source_phase_runner import SourcePhaseRunner


class SourceFreshnessAuditRunner(SourcePhaseRunner):
    """Audit whether Phase One source families are current enough to trust."""

    def audit_source_freshness(self) -> dict[str, object]:
        rows = self.database.fetch_all(
            """
                select *
                from analytics.v_source_freshness_matrix
                order by
                    case phase_one_role
                        when 'critical' then 1
                        when 'target_live' then 2
                        else 3
                    end,
                    source_family asc
            """
        )
        blocked_rows = [row for row in rows if (row.get("ranking_freshness_gate") or "").startswith("block_")]
        stale_rows = [row for row in rows if row.get("freshness_status") == "stale"]
        failed_rows = [row for row in rows if row.get("freshness_status") == "failed"]
        unknown_rows = [row for row in rows if row.get("freshness_status") == "unknown"]
        payload = {
            "source_count": len(rows),
            "blocked_count": len(blocked_rows),
            "stale_count": len(stale_rows),
            "failed_count": len(failed_rows),
            "unknown_count": len(unknown_rows),
            "blocked_sources": [row.get("source_family") for row in blocked_rows],
            "stale_sources": [row.get("source_family") for row in stale_rows],
            "failed_sources": [row.get("source_family") for row in failed_rows],
            "unknown_sources": [row.get("source_family") for row in unknown_rows],
            "matrix": rows,
        }
        self.logger.info("source_freshness_audit", extra=payload)
        return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LandIntel live source freshness audit.")
    parser.add_argument("command", choices=("audit-source-freshness",))
    return parser


def main() -> int:
    parser = build_parser()
    parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = SourceFreshnessAuditRunner(settings, logger)
    try:
        runner.audit_source_freshness()
        runner.logger.info("source_freshness_command_completed", extra={"command": "audit-source-freshness"})
        return 0
    except Exception:
        runner.logger.exception("source_freshness_command_failed", extra={"command": "audit-source-freshness"})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

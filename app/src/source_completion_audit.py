"""Read-only proof for the LandIntel source completion matrix."""

from __future__ import annotations

import argparse
import json
import traceback
from typing import Any

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


def collect_source_completion_proof(database: Database) -> dict[str, Any]:
    """Return bounded proof rows from the Phase F source completion surface."""

    return {
        "message": "source_completion_matrix_workflow_proof",
        "status_counts": database.fetch_all(
            """
            select
                current_status,
                count(*)::integer as source_count
            from landintel_reporting.v_source_completion_matrix
            group by current_status
            order by
                case current_status
                    when 'live_complete' then 1
                    when 'live_partial' then 2
                    when 'manual_only' then 3
                    when 'discovery_only' then 4
                    when 'registered_only' then 5
                    when 'blocked' then 6
                    when 'retired_or_replaced' then 7
                    else 8
                end,
                current_status
            """
        ),
        "priority_status_counts": database.fetch_all(
            """
            select
                priority,
                current_status,
                count(*)::integer as source_count
            from landintel_reporting.v_source_completion_matrix
            group by priority, current_status
            order by priority, current_status
            """
        ),
        "workflow_gap_sample": database.fetch_all(
            """
            select
                source_key,
                source_name,
                source_family,
                current_status,
                workflow_command,
                github_actions_command_available,
                known_blocker,
                next_action,
                priority
            from landintel_reporting.v_source_completion_matrix
            where current_status in ('registered_only', 'blocked', 'discovery_only')
               or github_actions_command_available is false
               or known_blocker is not null
            order by priority, current_status, source_family, source_key
            limit 30
            """
        ),
        "completion_ready_sample": database.fetch_all(
            """
            select
                source_key,
                source_name,
                source_family,
                current_status,
                row_count,
                linked_site_count,
                measured_site_count,
                evidence_count,
                signal_count,
                freshness_record_count,
                workflow_command,
                priority
            from landintel_reporting.v_source_completion_matrix
            where current_status in ('live_complete', 'live_partial', 'manual_only')
            order by
                case current_status
                    when 'live_complete' then 1
                    when 'live_partial' then 2
                    else 3
                end,
                priority,
                source_family,
                source_key
            limit 30
            """
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print bounded live proof for the source completion matrix.")
    parser.add_argument("command", choices=("audit-source-completion-matrix",))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    try:
        proof = collect_source_completion_proof(database)
        print(json.dumps(proof, default=str, ensure_ascii=False), flush=True)
        logger.info("source_completion_matrix_workflow_proof_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception(
            "source_completion_matrix_workflow_proof_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

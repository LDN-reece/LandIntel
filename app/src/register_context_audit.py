"""Read-only proof for the LandIntel register/context clean surface."""

from __future__ import annotations

import argparse
import json
import traceback
from typing import Any

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


def collect_register_context_proof(database: Database) -> dict[str, Any]:
    """Return bounded proof rows for HLA/ELA/VDL/LDP/settlement context."""

    return {
        "message": "register_context_merge_proof",
        "merge_status": database.fetch_all(
            """
            select
                source_family,
                source_key,
                raw_row_count,
                current_row_count,
                linked_site_count,
                invalid_geometry_row_count,
                possible_duplicate_row_count,
                latest_source_updated_at,
                drive_file_count,
                drive_ready_file_count,
                matrix_current_status,
                matrix_row_count,
                matrix_linked_site_count,
                source_completion_alignment_status,
                recommended_action
            from landintel_reporting.v_register_context_merge_status
            order by source_family
            """
        ),
        "source_completion_overlay": database.fetch_all(
            """
            select
                source_key,
                source_family,
                matrix_current_status,
                target_status,
                workflow_command,
                github_actions_command_available,
                matrix_row_count,
                actual_register_row_count,
                matrix_linked_site_count,
                actual_linked_site_count,
                invalid_geometry_row_count,
                possible_duplicate_row_count,
                corrected_status_hint,
                source_completion_alignment_status,
                recommended_action
            from landintel_reporting.v_register_context_source_completion_overlay
            order by source_family, source_key
            """
        ),
        "freshness": database.fetch_all(
            """
            select
                source_family,
                source_key,
                raw_row_count,
                latest_source_updated_at,
                latest_drive_synced_at,
                freshness_status,
                last_checked_at,
                last_success_at,
                register_freshness_status,
                recommended_refresh_workflow
            from landintel_reporting.v_register_context_freshness
            order by source_family
            """
        ),
        "duplicate_sample": database.fetch_all(
            """
            select
                source_family,
                record_dedupe_key,
                duplicate_group_size,
                row_count,
                distinct_source_record_ids,
                distinct_geometry_hashes,
                latest_updated_at,
                caveat
            from landintel_reporting.v_register_context_duplicate_diagnostics
            order by duplicate_group_size desc, source_family, record_dedupe_key
            limit 30
            """
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print bounded live proof for register/context sources.")
    parser.add_argument("command", choices=("audit-register-context",))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    try:
        proof = collect_register_context_proof(database)
        print(json.dumps(proof, default=str, ensure_ascii=False), flush=True)
        logger.info("register_context_audit_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception(
            "register_context_audit_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

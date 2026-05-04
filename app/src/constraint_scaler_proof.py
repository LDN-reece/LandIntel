"""Read-only proof for the constraint coverage scaler reporting views."""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from typing import Any

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


def collect_constraint_scaler_proof(database: Database) -> dict[str, Any]:
    """Return bounded proof rows from the Phase E constraint scaler surfaces."""

    return {
        "message": "constraint_scaler_workflow_proof",
        "coverage_by_layer_sample": database.fetch_all(
            """
            select
                constraint_priority_rank,
                constraint_priority_family,
                layer_key,
                layer_name,
                source_family,
                source_feature_count,
                measured_site_count,
                measured_row_count,
                commercial_friction_fact_count
            from landintel_reporting.v_constraint_coverage_by_layer
            order by constraint_priority_rank, source_family, layer_key
            limit 20
            """
        ),
        "coverage_by_site_priority": database.fetch_all(
            """
            select
                site_priority_rank,
                site_priority_band,
                site_count,
                sites_with_measurements,
                sites_with_scan_state,
                sites_without_scan_state
            from landintel_reporting.v_constraint_coverage_by_site_priority
            order by site_priority_rank
            """
        ),
        "measurement_backlog_sample": database.fetch_all(
            """
            select
                site_priority_rank,
                site_priority_band,
                constraint_priority_rank,
                constraint_priority_family,
                layer_key,
                target_site_layer_pairs,
                scanned_site_layer_pairs,
                backlog_site_layer_pairs,
                recommended_workflow_command
            from landintel_reporting.v_constraint_measurement_backlog
            order by site_priority_rank, constraint_priority_rank, layer_key
            limit 20
            """
        ),
        "priority_measurement_queue_sample": database.fetch_all(
            """
            select
                queue_rank,
                site_priority_band,
                constraint_priority_family,
                layer_key,
                authority_name,
                area_acres,
                recommended_workflow_command,
                recommended_layer_key
            from landintel_reporting.v_constraint_priority_measurement_queue
            order by queue_rank
            limit 20
            """
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print bounded live proof for constraint scaler reporting views.")
    parser.add_argument("command", choices=("print-constraint-scaler-proof",))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    try:
        proof = collect_constraint_scaler_proof(database)
        print(json.dumps(proof, default=str, ensure_ascii=False), flush=True)
        logger.info("constraint_scaler_workflow_proof_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception(
            "constraint_scaler_workflow_proof_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

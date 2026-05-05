"""Read-only proof for site title traceability and DD measurement orchestration."""

from __future__ import annotations

import argparse
import json
import traceback
from typing import Any

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


def collect_site_dd_orchestration_proof(database: Database) -> dict[str, Any]:
    """Return bounded proof rows from the site DD orchestration surfaces."""

    return {
        "message": "site_dd_orchestration_workflow_proof",
        "traceability_status_counts": database.fetch_all(
            """
            select
                title_traceability_status,
                count(*)::integer as site_count
            from landintel_reporting.v_site_title_traceability_matrix
            group by title_traceability_status
            order by title_traceability_status
            """
        ),
        "measurement_status_counts": database.fetch_all(
            """
            select
                coalesce(site_priority_band, 'unprioritised') as site_priority_band,
                measurement_readiness_status,
                count(*)::integer as site_count
            from landintel_reporting.v_site_measurement_readiness_matrix
            group by coalesce(site_priority_band, 'unprioritised'), measurement_readiness_status
            order by
                case coalesce(site_priority_band, 'unprioritised')
                    when 'title_spend_candidates' then 1
                    when 'review_queue' then 2
                    when 'ldn_candidate_screen' then 3
                    when 'prove_it_candidates' then 4
                    when 'wider_canonical_sites' then 5
                    else 9
                end,
                measurement_readiness_status
            """
        ),
        "orchestration_summary": database.fetch_all(
            """
            select
                site_priority_band,
                title_traceability_status,
                measurement_readiness_status,
                site_count,
                sites_with_safe_title_candidate,
                sites_with_ros_parcel_candidate,
                sites_with_constraint_scan_state,
                sites_with_constraint_measurements,
                unscanned_priority_pair_count
            from landintel_reporting.v_site_dd_orchestration_summary
            order by
                case site_priority_band
                    when 'title_spend_candidates' then 1
                    when 'review_queue' then 2
                    when 'ldn_candidate_screen' then 3
                    when 'prove_it_candidates' then 4
                    when 'wider_canonical_sites' then 5
                    else 9
                end,
                title_traceability_status,
                measurement_readiness_status
            limit 50
            """
        ),
        "next_queue_sample": database.fetch_all(
            """
            select
                orchestration_queue_rank,
                canonical_site_id,
                site_label,
                authority_name,
                gross_area_acres,
                site_priority_band,
                title_traceability_status,
                measurement_readiness_status,
                orchestration_step,
                recommended_workflow_command,
                recommended_workflow_input_hint,
                next_constraint_source_family,
                next_constraint_layer_key,
                orchestration_reason
            from landintel_reporting.v_site_dd_orchestration_queue
            order by orchestration_queue_rank
            limit 30
            """
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print bounded live proof for site DD orchestration.")
    parser.add_argument("command", choices=("audit-site-dd-orchestration",))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    try:
        proof = collect_site_dd_orchestration_proof(database)
        print(json.dumps(proof, default=str, ensure_ascii=False), flush=True)
        logger.info("site_dd_orchestration_workflow_proof_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception(
            "site_dd_orchestration_workflow_proof_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

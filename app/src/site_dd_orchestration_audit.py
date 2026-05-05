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

    database.fetch_one("select set_config('statement_timeout', '30s', false) as statement_timeout")

    return {
        "message": "site_dd_orchestration_workflow_proof",
        "reporting_view_presence": database.fetch_all(
            """
            select
                view_name,
                to_regclass(view_name)::text as resolved_relation
            from (
                values
                    ('landintel_reporting.v_site_title_traceability_matrix'),
                    ('landintel_reporting.v_site_measurement_readiness_matrix'),
                    ('landintel_reporting.v_site_dd_orchestration_queue'),
                    ('landintel_reporting.v_site_dd_orchestration_summary')
            ) as required(view_name)
            """
        ),
        "direct_title_traceability_counts": database.fetch_one(
            """
            select
                (select count(*)::integer from landintel.canonical_sites) as canonical_site_count,
                (select count(*)::integer from landintel.canonical_sites where geometry is not null) as sites_with_geometry,
                (
                    select count(distinct site_id)::integer
                    from public.site_ros_parcel_link_candidates
                    where link_status <> 'rejected'
                ) as sites_with_ros_parcel_candidate,
                (
                    select count(*)::integer
                    from public.site_ros_parcel_link_candidates
                    where link_status <> 'rejected'
                ) as ros_parcel_candidate_rows,
                (
                    select count(distinct site_id)::integer
                    from public.site_title_resolution_candidates
                    where resolution_status <> 'rejected'
                ) as sites_with_title_resolution_candidate,
                (
                    select count(*)::integer
                    from public.site_title_resolution_candidates
                    where resolution_status = 'needs_licensed_bridge'
                ) as licensed_bridge_required_rows,
                (
                    select count(distinct canonical_site_id)::integer
                    from landintel.title_review_records
                ) as sites_with_human_title_review,
                (
                    select count(*)::integer
                    from landintel.title_order_workflow
                ) as title_order_workflow_rows
            """
        ),
        "direct_measurement_counts": database.fetch_one(
            """
            select
                (
                    select count(distinct site_location_id)::integer
                    from public.site_constraint_measurement_scan_state
                    where scan_scope = 'canonical_site_geometry'
                ) as sites_with_constraint_scan_state,
                (
                    select count(*)::integer
                    from public.site_constraint_measurement_scan_state
                    where scan_scope = 'canonical_site_geometry'
                ) as constraint_scan_state_rows,
                (
                    select count(distinct site_location_id)::integer
                    from public.site_constraint_measurements
                ) as sites_with_constraint_measurements,
                (
                    select count(*)::integer
                    from public.site_constraint_measurements
                ) as constraint_measurement_rows,
                (
                    select count(*)::integer
                    from public.site_commercial_friction_facts
                ) as commercial_friction_fact_rows
            """
        ),
        "constraint_priority_queue_sample": database.fetch_all(
            """
            select
                canonical_site_id,
                site_label,
                authority_name,
                site_priority_band,
                constraint_priority_family,
                source_family,
                layer_key,
                layer_name,
                queue_rank,
                recommended_workflow_command,
                bounded_run_guidance
            from landintel_reporting.v_constraint_priority_measurement_queue
            order by queue_rank
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

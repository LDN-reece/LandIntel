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
                    ('landintel_reporting.v_site_title_traceability_scan_state'),
                    ('landintel_reporting.v_site_title_no_candidate_diagnostics'),
                    ('landintel_reporting.v_site_title_no_candidate_diagnostic_summary'),
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
        "title_traceability_scan_state_counts": database.fetch_all(
            """
            select
                scan_scope,
                scan_status,
                count(*)::integer as row_count,
                count(distinct canonical_site_id)::integer as site_count,
                max(scanned_at) as latest_scanned_at
            from landintel_store.site_title_traceability_scan_state
            group by scan_scope, scan_status
            order by scan_scope, scan_status
            """
        ),
        "title_no_candidate_diagnostic_summary": database.fetch_all(
            """
            select *
            from landintel_reporting.v_site_title_no_candidate_diagnostic_summary
            order by site_count desc, scan_scope, site_priority_band, diagnostic_reason
            limit 20
            """
        ),
        "latest_title_no_candidate_diagnostics": database.fetch_all(
            """
            select
                canonical_site_id,
                site_label,
                scan_scope,
                site_priority_band,
                authority_name,
                area_acres,
                nearest_centroid_distance_m,
                nearest_geometry_distance_m,
                parcel_centroid_bbox_hits_250m,
                parcel_geometry_within_250m,
                diagnostic_reason,
                recommended_action
            from landintel_reporting.v_site_title_no_candidate_diagnostics
            order by scanned_at desc, site_priority_rank nulls last, area_acres desc nulls last
            limit 20
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
        "constraint_source_family_direct_counts": database.fetch_all(
            """
            with feature_counts as (
                select
                    constraint_layer_id,
                    count(*)::integer as source_feature_count
                from public.constraint_source_features
                group by constraint_layer_id
            ),
            scan_counts as (
                select
                    constraint_layer_id,
                    count(*)::integer as scan_state_rows,
                    count(distinct site_location_id)::integer as scanned_site_count
                from public.site_constraint_measurement_scan_state
                where scan_scope = 'canonical_site_geometry'
                group by constraint_layer_id
            ),
            measurement_counts as (
                select
                    constraint_layer_id,
                    count(*)::integer as measurement_rows,
                    count(distinct site_location_id)::integer as measured_site_count
                from public.site_constraint_measurements
                group by constraint_layer_id
            )
            select
                layer.source_family,
                count(*)::integer as active_layer_count,
                coalesce(sum(feature_counts.source_feature_count), 0)::integer as source_feature_count,
                coalesce(sum(scan_counts.scan_state_rows), 0)::integer as scan_state_rows,
                coalesce(sum(scan_counts.scanned_site_count), 0)::integer as scanned_site_count,
                coalesce(sum(measurement_counts.measurement_rows), 0)::integer as measurement_rows,
                coalesce(sum(measurement_counts.measured_site_count), 0)::integer as measured_site_count
            from public.constraint_layer_registry as layer
            left join feature_counts
              on feature_counts.constraint_layer_id = layer.id
            left join scan_counts
              on scan_counts.constraint_layer_id = layer.id
            left join measurement_counts
              on measurement_counts.constraint_layer_id = layer.id
            where layer.is_active = true
            group by layer.source_family
            order by layer.source_family
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

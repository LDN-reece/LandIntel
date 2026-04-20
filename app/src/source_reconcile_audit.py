"""Live audit runner for incremental reconcile visibility."""

from __future__ import annotations

import argparse

from config.settings import get_settings
from src.logging_config import configure_logging
from src.source_phase_runner import SourcePhaseRunner


class IncrementalReconcileAuditRunner(SourcePhaseRunner):
    """Show raw source counts alongside reconcile status and queue health."""

    def audit_source_footprint(self) -> dict[str, object]:
        summary = self.database.fetch_one(
            """
                select
                    (select count(*) from landintel.canonical_sites) as canonical_site_count,
                    (select count(*) from landintel.site_reference_aliases) as alias_count,
                    (select count(*) from landintel.planning_application_records) as planning_record_count,
                    (select count(*) from landintel.hla_site_records) as hla_record_count,
                    (select count(*) from landintel.bgs_records) as bgs_record_count,
                    (select count(*) from landintel.site_source_links) as site_source_link_count,
                    (select count(*) from landintel.evidence_references) as evidence_reference_count
            """
        ) or {}
        planning_by_authority = self.database.fetch_all(
            """
                select authority_name,
                       count(*)::bigint as planning_records,
                       count(*) filter (where canonical_site_id is not null)::bigint as linked_planning_records
                from landintel.planning_application_records
                group by authority_name
                order by planning_records desc, authority_name asc
            """
        )
        reconcile_by_family = self.database.fetch_all(
            """
                select
                    source_family,
                    count(*)::bigint as state_count,
                    count(*) filter (where active_flag = true)::bigint as active_state_count,
                    count(*) filter (where lifecycle_status = 'retired')::bigint as retired_state_count,
                    count(*) filter (where current_canonical_site_id is not null)::bigint as states_with_site_id,
                    count(*) filter (where publish_state = 'published')::bigint as published_state_count,
                    count(*) filter (where publish_state = 'provisional')::bigint as provisional_state_count,
                    count(*) filter (where publish_state = 'blocked')::bigint as blocked_state_count,
                    count(*) filter (where review_required = true)::bigint as review_required_state_count,
                    count(*) filter (where last_processed_at is null)::bigint as unprocessed_state_count,
                    count(*) filter (
                        where publish_state = 'published'
                          and not exists (
                              select 1
                              from landintel.site_source_links as link_row
                              where link_row.reconcile_state_id = landintel.source_reconcile_state.id
                                and link_row.active_flag = true
                          )
                    )::bigint as published_without_live_link_count
                from landintel.source_reconcile_state
                group by source_family
                order by source_family asc
            """
        )
        linkage_by_family = self.database.fetch_all(
            """
                with families as (
                    select 'hla'::text as source_family
                    union all
                    select 'planning'::text as source_family
                )
                select
                    families.source_family,
                    case
                        when families.source_family = 'planning' then (
                            select count(*)::bigint
                            from landintel.planning_application_records
                            where canonical_site_id is not null
                        )
                        else (
                            select count(*)::bigint
                            from landintel.hla_site_records
                            where canonical_site_id is not null
                        )
                    end as source_rows_with_site_id,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_state as state_row
                        where state_row.source_family = families.source_family
                          and state_row.current_canonical_site_id is not null
                    ) as states_with_site_id,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_state as state_row
                        where state_row.source_family = families.source_family
                          and state_row.publish_state = 'published'
                    ) as published_state_count,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_state as state_row
                        where state_row.source_family = families.source_family
                          and state_row.publish_state = 'provisional'
                    ) as provisional_state_count,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_state as state_row
                        where state_row.source_family = families.source_family
                          and state_row.publish_state = 'blocked'
                    ) as blocked_state_count,
                    (
                        select count(*)::bigint
                        from landintel.site_source_links as link_row
                        where link_row.source_family = families.source_family
                          and coalesce(link_row.active_flag, true) = true
                    ) as active_live_link_count,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_queue as queue_row
                        where queue_row.source_family = families.source_family
                          and queue_row.status = 'pending'
                    ) as pending_queue_count,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_queue as queue_row
                        where queue_row.source_family = families.source_family
                          and queue_row.status = 'review_required'
                    ) as review_queue_count,
                    (
                        select count(*)::bigint
                        from landintel.source_reconcile_queue as queue_row
                        where queue_row.source_family = families.source_family
                          and queue_row.status = 'dead_letter'
                    ) as dead_letter_count
                from families
                order by families.source_family asc
            """
        )
        queue_health = self.database.fetch_all(
            """
                select *
                from analytics.v_reconcile_queue_health
                order by source_family asc
            """
        )
        drift_summary = self.database.fetch_one(
            """
                select *
                from analytics.v_reconcile_drift_summary
            """
        ) or {}
        planning_review_reasons = self.database.fetch_all(
            """
                select
                    review_reason_code,
                    count(*)::bigint as state_count
                from landintel.source_reconcile_state
                where source_family = 'planning'
                  and review_reason_code is not null
                group by review_reason_code
                order by state_count desc, review_reason_code asc
            """
        )
        payload = {
            "summary": summary,
            "planning_by_authority": planning_by_authority,
            "reconcile_by_family": reconcile_by_family,
            "linkage_by_family": linkage_by_family,
            "queue_health": queue_health,
            "drift_summary": drift_summary,
            "planning_review_reasons": planning_review_reasons,
        }
        self.logger.info("source_phase_audit", extra=payload)
        return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LandIntel live source and reconcile audit.")
    parser.add_argument("command", choices=("audit-source-footprint",))
    return parser


def main() -> int:
    parser = build_parser()
    parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = IncrementalReconcileAuditRunner(settings, logger)
    try:
        runner.audit_source_footprint()
        runner.logger.info("incremental_reconcile_command_completed", extra={"command": "audit-source-footprint"})
        return 0
    except Exception:
        runner.logger.exception("incremental_reconcile_command_failed", extra={"command": "audit-source-footprint"})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

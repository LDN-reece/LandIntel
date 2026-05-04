"""Database access for the Phase One opportunity engine."""

from __future__ import annotations

import json
from typing import Any

from src.db import Database
from src.opportunity_engine.types import OpportunitySearchFilters, OpportunitySignal, OpportunitySnapshot


class OpportunityRepository:
    """Keep Phase One database access explicit and live-baseline aware."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def search_opportunities(self, filters: OpportunitySearchFilters) -> list[dict[str, Any]]:
        sql = "select * from analytics.v_live_opportunity_queue where 1 = 1"
        params: dict[str, Any] = {"limit": filters.limit}

        if filters.query:
            params["query"] = f"%{filters.query.strip()}%"
            sql += """
                and (
                    site_name ilike :query
                    or site_code ilike :query
                    or coalesce(authority_name, '') ilike :query
                    or coalesce(last_change_summary, '') ilike :query
                )
            """
        if filters.queue_name:
            sql += " and queue_name = :queue_name"
            params["queue_name"] = filters.queue_name
        if filters.authority_name:
            sql += " and authority_name = :authority_name"
            params["authority_name"] = filters.authority_name
        if filters.source_route:
            sql += " and source_route = :source_route"
            params["source_route"] = filters.source_route
        if filters.size_band:
            sql += " and size_rank = :size_band"
            params["size_band"] = filters.size_band
        if filters.planning_context_band:
            sql += " and planning_context_band = :planning_context_band"
            params["planning_context_band"] = filters.planning_context_band
        if filters.settlement_position:
            sql += " and coalesce(settlement_position, 'unknown') = :settlement_position"
            params["settlement_position"] = filters.settlement_position
        if filters.location_band:
            sql += " and coalesce(location_band, 'unknown') = :location_band"
            params["location_band"] = filters.location_band
        if filters.constraint_severity:
            sql += " and coalesce(constraint_severity, 'unknown') = :constraint_severity"
            params["constraint_severity"] = filters.constraint_severity
        if filters.access_strength:
            sql += " and coalesce(access_strength, 'unknown') = :access_strength"
            params["access_strength"] = filters.access_strength
        if filters.geometry_quality:
            sql += " and coalesce(geometry_quality, 'unknown') = :geometry_quality"
            params["geometry_quality"] = filters.geometry_quality
        if filters.ownership_control_state:
            sql += " and coalesce(ownership_control_state, 'unknown') = :ownership_control_state"
            params["ownership_control_state"] = filters.ownership_control_state
        if filters.title_state:
            sql += " and coalesce(title_state, 'commercial_inference') = :title_state"
            params["title_state"] = filters.title_state
        if filters.review_status:
            sql += " and review_status = :review_status"
            params["review_status"] = filters.review_status
        if filters.resurfaced_only is not None:
            sql += " and coalesce(resurfaced_flag, false) = :resurfaced_only"
            params["resurfaced_only"] = filters.resurfaced_only

        sql += """
            order by
                case queue_name
                    when 'Strong Candidates' then 1
                    when 'Needs Review' then 2
                    when 'New Candidates' then 3
                    when 'Watchlist / Resurfaced' then 4
                    else 5
                end,
                queue_position asc,
                site_name asc
            limit :limit
        """
        return self.database.fetch_all(sql, params)

    def fetch_filter_options(self) -> dict[str, list[str]]:
        select_distinct = """
            select distinct {column_name}
            from analytics.v_live_opportunity_queue
            where {column_name} is not null
            order by {column_name}
        """
        return {
            "queues": [row["queue_name"] for row in self.database.fetch_all(select_distinct.format(column_name="queue_name"))],
            "authorities": [row["authority_name"] for row in self.database.fetch_all(select_distinct.format(column_name="authority_name"))],
            "source_routes": [row["source_route"] for row in self.database.fetch_all(select_distinct.format(column_name="source_route"))],
            "size_bands": [row["size_rank"] for row in self.database.fetch_all(select_distinct.format(column_name="size_rank"))],
            "planning_context_bands": [
                row["planning_context_band"]
                for row in self.database.fetch_all(select_distinct.format(column_name="planning_context_band"))
            ],
            "settlement_positions": [
                row["settlement_position"]
                for row in self.database.fetch_all(select_distinct.format(column_name="settlement_position"))
            ],
            "location_bands": [row["location_band"] for row in self.database.fetch_all(select_distinct.format(column_name="location_band"))],
            "constraint_severities": [
                row["constraint_severity"]
                for row in self.database.fetch_all(select_distinct.format(column_name="constraint_severity"))
            ],
            "access_strengths": [
                row["access_strength"] for row in self.database.fetch_all(select_distinct.format(column_name="access_strength"))
            ],
            "geometry_qualities": [
                row["geometry_quality"] for row in self.database.fetch_all(select_distinct.format(column_name="geometry_quality"))
            ],
            "ownership_control_states": [
                row["ownership_control_state"]
                for row in self.database.fetch_all(select_distinct.format(column_name="ownership_control_state"))
            ],
            "title_states": [row["title_state"] for row in self.database.fetch_all(select_distinct.format(column_name="title_state"))],
            "review_statuses": [row["review_status"] for row in self.database.fetch_all(select_distinct.format(column_name="review_status"))],
        }

    def fetch_opportunity_detail(self, canonical_site_id: str) -> dict[str, Any] | None:
        summary = self.database.fetch_one(
            "select * from analytics.v_live_site_summary where canonical_site_id = cast(:site_id as uuid)",
            {"site_id": canonical_site_id},
        )
        if not summary:
            return None

        return {
            "summary": summary,
            "readiness": self.database.fetch_one(
                "select * from analytics.v_live_site_readiness where canonical_site_id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "title": self.database.fetch_one(
                "select * from analytics.v_live_site_title where canonical_site_id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "constraints": self.database.fetch_one(
                "select * from analytics.v_live_site_constraints where canonical_site_id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "assessment": self.database.fetch_one(
                "select * from analytics.v_live_site_assessment where canonical_site_id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "review_state": self.database.fetch_one(
                "select * from analytics.v_live_site_review_state where canonical_site_id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "source_rows": self.database.fetch_all(
                """
                select *
                from analytics.v_live_site_sources
                where canonical_site_id = cast(:site_id as uuid)
                order by source_family, source_dataset
                """,
                {"site_id": canonical_site_id},
            ),
            "change_log": self.database.fetch_all(
                """
                select *
                from analytics.v_live_site_change_log
                where canonical_site_id = cast(:site_id as uuid)
                order by created_at desc, change_event_id desc
                limit 50
                """,
                {"site_id": canonical_site_id},
            ),
            "canonical_site": self.database.fetch_one(
                "select * from landintel.canonical_sites where id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "planning_records": self.database.fetch_all(
                """
                select *
                from landintel.planning_application_records
                where canonical_site_id = cast(:site_id as uuid)
                order by coalesce(decision_date, lodged_date) desc nulls last, updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "hla_records": self.database.fetch_all(
                """
                select *
                from landintel.hla_site_records
                where canonical_site_id = cast(:site_id as uuid)
                order by updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "ldp_records": self.database.fetch_all(
                """
                select *
                from landintel.ldp_site_records
                where canonical_site_id = cast(:site_id as uuid)
                order by updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "settlement_boundary_records": self.database.fetch_all(
                """
                select *
                from landintel.settlement_boundary_records
                where authority_name = (
                    select authority_name
                    from landintel.canonical_sites
                    where id = cast(:site_id as uuid)
                )
                order by updated_at desc
                limit 25
                """,
                {"site_id": canonical_site_id},
            ),
            "bgs_records": self.database.fetch_all(
                """
                select *
                from landintel.bgs_records
                where canonical_site_id = cast(:site_id as uuid)
                order by updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "flood_records": self.database.fetch_all(
                """
                select *
                from landintel.flood_records
                where canonical_site_id = cast(:site_id as uuid)
                order by overlap_pct desc nulls last, updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "ela_records": self.database.fetch_all(
                """
                select *
                from landintel.ela_site_records
                where canonical_site_id = cast(:site_id as uuid)
                order by updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "vdl_records": self.database.fetch_all(
                """
                select *
                from landintel.vdl_site_records
                where canonical_site_id = cast(:site_id as uuid)
                order by updated_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "parcel_rows": self.fetch_linked_parcels(canonical_site_id),
            "title_links": self.database.fetch_all(
                """
                select *
                from public.site_spatial_links
                where site_id = :site_id
                order by updated_at desc, created_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "title_validations": self.database.fetch_all(
                """
                select *
                from public.site_title_validation
                where site_id = :site_id
                order by updated_at desc, created_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            "geometry_diagnostics": self.database.fetch_one(
                "select * from landintel.site_geometry_diagnostics where canonical_site_id = cast(:site_id as uuid)",
                {"site_id": canonical_site_id},
            ),
            "constraint_overview": self.database.fetch_one(
                "select * from analytics.v_constraints_tab_overview where site_id = :site_id",
                {"site_id": canonical_site_id},
            ),
            "constraint_group_summaries": self.database.fetch_all(
                """
                select *
                from analytics.v_constraints_tab_group_summaries
                where site_id = :site_id
                order by constraint_group, layer_name
                """,
                {"site_id": canonical_site_id},
            ),
            "constraint_measurements": self.database.fetch_all(
                """
                select *
                from analytics.v_constraints_tab_measurements
                where site_id = :site_id
                order by layer_name, feature_name
                """,
                {"site_id": canonical_site_id},
            ),
            "constraint_friction_facts": self.database.fetch_all(
                """
                select *
                from analytics.v_constraints_tab_commercial_friction
                where site_id = :site_id
                order by layer_name, fact_label
                """,
                {"site_id": canonical_site_id},
            ),
            "review_events": self.database.fetch_all(
                """
                select *
                from landintel.site_review_events
                where canonical_site_id = cast(:site_id as uuid)
                order by created_at desc, id desc
                """,
                {"site_id": canonical_site_id},
            ),
            "manual_overrides": self.database.fetch_all(
                """
                select *
                from landintel.site_manual_overrides
                where canonical_site_id = cast(:site_id as uuid)
                order by created_at desc, id desc
                """,
                {"site_id": canonical_site_id},
            ),
            "signal_rows": self.database.fetch_all(
                """
                select *
                from landintel.site_signals
                where canonical_site_id = cast(:site_id as uuid)
                order by signal_group, signal_key
                """,
                {"site_id": canonical_site_id},
            ),
        }

    def fetch_linked_parcels(self, canonical_site_id: str) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            with linked_parcels as (
                select site.primary_ros_parcel_id as parcel_id, true as is_primary
                from landintel.canonical_sites as site
                where site.id = cast(:site_id as uuid)
                  and site.primary_ros_parcel_id is not null
                union
                select cast(spatial.linked_record_id as uuid) as parcel_id, false as is_primary
                from public.site_spatial_links as spatial
                where spatial.site_id = :site_id
                  and spatial.linked_record_table = 'public.ros_cadastral_parcels'
                  and spatial.linked_record_id ~* '^[0-9a-f-]{36}$'
            )
            select
                parcel.*,
                max(linked_parcels.is_primary::int)::boolean as is_primary
            from linked_parcels
            join public.ros_cadastral_parcels as parcel
              on parcel.id = linked_parcels.parcel_id
            group by parcel.id
            order by is_primary desc, parcel.area_acres desc, parcel.id
            """,
            {"site_id": canonical_site_id},
        )

    def fetch_site_snapshot(self, canonical_site_id: str) -> OpportunitySnapshot:
        detail = self.fetch_opportunity_detail(canonical_site_id)
        if not detail:
            raise ValueError(f"Unknown canonical site: {canonical_site_id}")

        return OpportunitySnapshot(
            summary=detail["summary"],
            readiness=detail["readiness"],
            sources=detail["source_rows"],
            canonical_site=detail["canonical_site"],
            planning_records=detail["planning_records"],
            hla_records=detail["hla_records"],
            ldp_records=detail["ldp_records"],
            settlement_boundary_records=detail["settlement_boundary_records"],
            bgs_records=detail["bgs_records"],
            flood_records=detail["flood_records"],
            ela_records=detail["ela_records"],
            vdl_records=detail["vdl_records"],
            site_source_links=self.database.fetch_all(
                """
                select *
                from landintel.site_source_links
                where canonical_site_id = cast(:site_id as uuid)
                  and active_flag = true
                order by updated_at desc, created_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            site_reference_aliases=self.database.fetch_all(
                """
                select *
                from landintel.site_reference_aliases
                where canonical_site_id = cast(:site_id as uuid)
                order by updated_at desc, created_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            evidence_references=self.database.fetch_all(
                """
                select *
                from landintel.evidence_references
                where canonical_site_id = cast(:site_id as uuid)
                  and active_flag = true
                order by created_at desc
                """,
                {"site_id": canonical_site_id},
            ),
            parcel_rows=detail["parcel_rows"],
            title_links=detail["title_links"],
            title_validations=detail["title_validations"],
            geometry_metrics=self.fetch_geometry_metrics(canonical_site_id),
            geometry_diagnostics=detail["geometry_diagnostics"],
            constraint_overview=detail["constraint_overview"],
            constraint_group_summaries=detail["constraint_group_summaries"],
            constraint_measurements=detail["constraint_measurements"],
            constraint_friction_facts=detail["constraint_friction_facts"],
            review_events=detail["review_events"],
            manual_overrides=detail["manual_overrides"],
            change_events=detail["change_log"],
            latest_assessment=detail["assessment"],
        )

    def fetch_geometry_metrics(self, canonical_site_id: str) -> dict[str, Any] | None:
        return self.database.fetch_one(
            """
            select
                site.id as canonical_site_id,
                coalesce(site.area_acres, public.calculate_area_acres(st_area(site.geometry)::numeric)) as original_area_acres,
                st_numgeometries(site.geometry) as component_count,
                coalesce(
                    (
                        select count(distinct parcel.id)
                        from public.site_spatial_links as spatial
                        join public.ros_cadastral_parcels as parcel
                          on parcel.id = cast(spatial.linked_record_id as uuid)
                        where spatial.site_id = site.id::text
                          and spatial.linked_record_table = 'public.ros_cadastral_parcels'
                          and spatial.linked_record_id ~* '^[0-9a-f-]{36}$'
                    ),
                    case when site.primary_ros_parcel_id is not null then 1 else 0 end
                ) as parcel_count,
                round(greatest(st_xmax(st_envelope(site.geometry)) - st_xmin(st_envelope(site.geometry)), 0)::numeric, 2) as bbox_width_m,
                round(greatest(st_ymax(st_envelope(site.geometry)) - st_ymin(st_envelope(site.geometry)), 0)::numeric, 2) as bbox_height_m,
                round(
                    coalesce(
                        (4 * pi() * st_area(site.geometry) / nullif(power(st_perimeter(site.geometry), 2), 0))::numeric,
                        0
                    ),
                    6
                ) as shape_compactness
            from landintel.canonical_sites as site
            where site.id = cast(:site_id as uuid)
              and site.geometry is not null
            """,
            {"site_id": canonical_site_id},
        )

    def fetch_pending_refresh_requests(self, limit: int) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            select *
            from landintel.canonical_site_refresh_queue
            where (
                    status = 'pending'
                 or (status = 'failed' and coalesce(next_attempt_at, now()) <= now())
                 or (status = 'processing' and lease_expires_at is not null and lease_expires_at <= now())
            )
            order by
                case status
                    when 'pending' then 1
                    when 'failed' then 2
                    when 'processing' then 3
                    else 4
                end,
                created_at asc
            limit :limit
            """,
            {"limit": limit},
        )

    def claim_refresh_request(self, request_id: str, worker_name: str) -> bool:
        row = self.database.fetch_one(
            """
            update landintel.canonical_site_refresh_queue
            set status = 'processing',
                claimed_by = :worker_name,
                claimed_at = now(),
                lease_expires_at = now() + interval '15 minutes',
                updated_at = now()
            where id = cast(:request_id as uuid)
              and status in ('pending', 'failed', 'processing')
            returning id::text as id
            """,
            {"request_id": request_id, "worker_name": worker_name},
        )
        return row is not None

    def complete_refresh_request(self, request_id: str) -> None:
        self.database.execute(
            """
            update landintel.canonical_site_refresh_queue
            set status = 'completed',
                processed_at = now(),
                lease_expires_at = null,
                error_message = null,
                updated_at = now()
            where id = cast(:request_id as uuid)
            """,
            {"request_id": request_id},
        )

    def fail_refresh_request(self, request_id: str, error_message: str) -> None:
        self.database.execute(
            """
            update landintel.canonical_site_refresh_queue
            set status = 'failed',
                attempt_count = coalesce(attempt_count, 0) + 1,
                next_attempt_at = now() + interval '30 minutes',
                lease_expires_at = null,
                error_message = :error_message,
                updated_at = now()
            where id = cast(:request_id as uuid)
            """,
            {"request_id": request_id, "error_message": error_message[:2000]},
        )

    def upsert_geometry_diagnostics(self, canonical_site_id: str, diagnostics: dict[str, Any]) -> None:
        self.database.execute(
            """
            insert into landintel.site_geometry_diagnostics (
                canonical_site_id,
                original_area_acres,
                component_count,
                parcel_count,
                bbox_width_m,
                bbox_height_m,
                shape_compactness,
                indicative_clean_area_acres,
                indicative_usable_area_ratio,
                sliver_flag,
                fragmentation_flag,
                width_depth_warning,
                access_only_warning,
                infrastructure_heavy_warning,
                metadata
            )
            values (
                cast(:site_id as uuid),
                :original_area_acres,
                :component_count,
                :parcel_count,
                :bbox_width_m,
                :bbox_height_m,
                :shape_compactness,
                :indicative_clean_area_acres,
                :indicative_usable_area_ratio,
                :sliver_flag,
                :fragmentation_flag,
                :width_depth_warning,
                :access_only_warning,
                :infrastructure_heavy_warning,
                cast(:metadata as jsonb)
            )
            on conflict (canonical_site_id) do update set
                original_area_acres = excluded.original_area_acres,
                component_count = excluded.component_count,
                parcel_count = excluded.parcel_count,
                bbox_width_m = excluded.bbox_width_m,
                bbox_height_m = excluded.bbox_height_m,
                shape_compactness = excluded.shape_compactness,
                indicative_clean_area_acres = excluded.indicative_clean_area_acres,
                indicative_usable_area_ratio = excluded.indicative_usable_area_ratio,
                sliver_flag = excluded.sliver_flag,
                fragmentation_flag = excluded.fragmentation_flag,
                width_depth_warning = excluded.width_depth_warning,
                access_only_warning = excluded.access_only_warning,
                infrastructure_heavy_warning = excluded.infrastructure_heavy_warning,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            {
                "site_id": canonical_site_id,
                "original_area_acres": diagnostics.get("original_area_acres"),
                "component_count": diagnostics.get("component_count"),
                "parcel_count": diagnostics.get("parcel_count"),
                "bbox_width_m": diagnostics.get("bbox_width_m"),
                "bbox_height_m": diagnostics.get("bbox_height_m"),
                "shape_compactness": diagnostics.get("shape_compactness"),
                "indicative_clean_area_acres": diagnostics.get("indicative_clean_area_acres"),
                "indicative_usable_area_ratio": diagnostics.get("indicative_usable_area_ratio"),
                "sliver_flag": diagnostics.get("sliver_flag", False),
                "fragmentation_flag": diagnostics.get("fragmentation_flag", False),
                "width_depth_warning": diagnostics.get("width_depth_warning", False),
                "access_only_warning": diagnostics.get("access_only_warning", False),
                "infrastructure_heavy_warning": diagnostics.get("infrastructure_heavy_warning", False),
                "metadata": _json_dumps(diagnostics.get("metadata", {})),
            },
        )

    def upsert_signals(self, canonical_site_id: str, signals: list[OpportunitySignal]) -> None:
        for signal in signals:
            self.database.execute(
                """
                insert into landintel.site_signals (
                    canonical_site_id,
                    signal_key,
                    signal_value,
                    signal_status,
                    source_family,
                    confidence,
                    metadata,
                    signal_label,
                    signal_group,
                    fact_label,
                    reasoning
                )
                values (
                    cast(:site_id as uuid),
                    :signal_key,
                    cast(:signal_value as jsonb),
                    :signal_status,
                    :source_family,
                    :confidence,
                    cast(:metadata as jsonb),
                    :signal_label,
                    :signal_group,
                    :fact_label,
                    :reasoning
                )
                on conflict (canonical_site_id, signal_key) do update set
                    signal_value = excluded.signal_value,
                    signal_status = excluded.signal_status,
                    source_family = excluded.source_family,
                    confidence = excluded.confidence,
                    metadata = excluded.metadata,
                    signal_label = excluded.signal_label,
                    signal_group = excluded.signal_group,
                    fact_label = excluded.fact_label,
                    reasoning = excluded.reasoning,
                    updated_at = now()
                """,
                {
                    "site_id": canonical_site_id,
                    "signal_key": signal.signal_key,
                    "signal_value": _json_dumps(signal.signal_value),
                    "signal_status": signal.signal_status,
                    "source_family": signal.source_family,
                    "confidence": signal.confidence,
                    "metadata": _json_dumps(signal.metadata),
                    "signal_label": signal.signal_label,
                    "signal_group": signal.signal_group,
                    "fact_label": signal.fact_label,
                    "reasoning": signal.reasoning,
                },
            )

    def insert_assessment(
        self,
        canonical_site_id: str,
        assessment: dict[str, Any],
        *,
        source_registry_id: str | None = None,
        ingest_run_id: str | None = None,
    ) -> str:
        next_version = int(
            self.database.scalar(
                """
                select coalesce(max(assessment_version), 0) + 1
                from landintel.site_assessments
                where canonical_site_id = cast(:site_id as uuid)
                """,
                {"site_id": canonical_site_id},
            )
        )
        return str(
            self.database.scalar(
                """
                insert into landintel.site_assessments (
                    canonical_site_id,
                    assessment_version,
                    bucket,
                    monetisation_horizon,
                    dominant_blocker,
                    scores,
                    score_confidence,
                    human_review_required,
                    explanation_text,
                    source_registry_id,
                    ingest_run_id,
                    metadata,
                    overall_tier,
                    overall_rank_score,
                    queue_recommendation,
                    why_it_surfaced,
                    why_it_survived,
                    good_items,
                    bad_items,
                    ugly_items,
                    subrank_summary,
                    title_state,
                    ownership_control_fact_label,
                    resurfaced_reason,
                    latest_assessment_at
                )
                values (
                    cast(:site_id as uuid),
                    :assessment_version,
                    :bucket,
                    :monetisation_horizon,
                    :dominant_blocker,
                    cast(:scores as jsonb),
                    cast(:score_confidence as jsonb),
                    :human_review_required,
                    :explanation_text,
                    cast(:source_registry_id as uuid),
                    cast(:ingest_run_id as uuid),
                    cast(:metadata as jsonb),
                    :overall_tier,
                    :overall_rank_score,
                    :queue_recommendation,
                    :why_it_surfaced,
                    :why_it_survived,
                    cast(:good_items as jsonb),
                    cast(:bad_items as jsonb),
                    cast(:ugly_items as jsonb),
                    cast(:subrank_summary as jsonb),
                    :title_state,
                    :ownership_control_fact_label,
                    :resurfaced_reason,
                    now()
                )
                returning id
                """,
                {
                    "site_id": canonical_site_id,
                    "assessment_version": next_version,
                    "bucket": assessment.get("bucket"),
                    "monetisation_horizon": assessment.get("monetisation_horizon"),
                    "dominant_blocker": assessment.get("dominant_blocker"),
                    "scores": _json_dumps(assessment.get("scores", {})),
                    "score_confidence": _json_dumps(assessment.get("score_confidence", {})),
                    "human_review_required": assessment.get("human_review_required", True),
                    "explanation_text": assessment.get("explanation_text"),
                    "source_registry_id": source_registry_id,
                    "ingest_run_id": ingest_run_id,
                    "metadata": _json_dumps(assessment.get("metadata", {})),
                    "overall_tier": assessment.get("overall_tier"),
                    "overall_rank_score": assessment.get("overall_rank_score"),
                    "queue_recommendation": assessment.get("queue_recommendation"),
                    "why_it_surfaced": assessment.get("why_it_surfaced"),
                    "why_it_survived": assessment.get("why_it_survived"),
                    "good_items": _json_dumps(assessment.get("good_items", [])),
                    "bad_items": _json_dumps(assessment.get("bad_items", [])),
                    "ugly_items": _json_dumps(assessment.get("ugly_items", [])),
                    "subrank_summary": _json_dumps(assessment.get("subrank_summary", {})),
                    "title_state": assessment.get("title_state"),
                    "ownership_control_fact_label": assessment.get("ownership_control_fact_label"),
                    "resurfaced_reason": assessment.get("resurfaced_reason"),
                },
            )
        )

    def record_review_status(
        self,
        canonical_site_id: str,
        review_status: str,
        actor_name: str,
        reason_text: str | None = None,
    ) -> None:
        self.database.execute(
            """
            insert into landintel.site_review_events (
                canonical_site_id,
                event_type,
                review_status,
                actor_name,
                reason_text,
                metadata
            )
            values (
                cast(:site_id as uuid),
                'status_change',
                :review_status,
                :actor_name,
                :reason_text,
                '{}'::jsonb
            )
            """,
            {
                "site_id": canonical_site_id,
                "review_status": review_status,
                "actor_name": actor_name,
                "reason_text": reason_text,
            },
        )

    def record_review_note(
        self,
        canonical_site_id: str,
        actor_name: str,
        note_text: str,
    ) -> None:
        self.database.execute(
            """
            insert into landintel.site_review_events (
                canonical_site_id,
                event_type,
                actor_name,
                note_text,
                metadata
            )
            values (
                cast(:site_id as uuid),
                'note',
                :actor_name,
                :note_text,
                '{}'::jsonb
            )
            """,
            {
                "site_id": canonical_site_id,
                "actor_name": actor_name,
                "note_text": note_text,
            },
        )

    def record_manual_override(
        self,
        canonical_site_id: str,
        actor_name: str,
        override_key: str,
        override_value: dict[str, Any],
        reason_text: str | None = None,
    ) -> None:
        self.database.execute(
            """
            insert into landintel.site_manual_overrides (
                canonical_site_id,
                override_key,
                override_value,
                actor_name,
                reason_text,
                metadata
            )
            values (
                cast(:site_id as uuid),
                :override_key,
                cast(:override_value as jsonb),
                :actor_name,
                :reason_text,
                '{}'::jsonb
            )
            """,
            {
                "site_id": canonical_site_id,
                "override_key": override_key,
                "override_value": _json_dumps(override_value),
                "actor_name": actor_name,
                "reason_text": reason_text,
            },
        )
        self.database.execute(
            """
            insert into landintel.site_review_events (
                canonical_site_id,
                event_type,
                actor_name,
                reason_text,
                metadata
            )
            values (
                cast(:site_id as uuid),
                'manual_override',
                :actor_name,
                :reason_text,
                cast(:metadata as jsonb)
            )
            """,
            {
                "site_id": canonical_site_id,
                "actor_name": actor_name,
                "reason_text": reason_text or f"Manual override recorded for {override_key}.",
                "metadata": _json_dumps({"override_key": override_key}),
            },
        )

    def record_title_action(
        self,
        canonical_site_id: str,
        action: str,
        actor_name: str,
        reason_text: str | None = None,
        title_number: str | None = None,
    ) -> None:
        self.database.execute(
            """
            select landintel.record_title_review_event(
                cast(:site_id as uuid),
                :event_type,
                :actor_name,
                :reason_text,
                :title_number,
                '{}'::jsonb
            )
            """,
            {
                "site_id": canonical_site_id,
                "event_type": action,
                "actor_name": actor_name,
                "reason_text": reason_text,
                "title_number": title_number,
            },
        )

    def publish_reconciled_planning_links(self, limit: int = 1000) -> int:
        return int(
            self.database.scalar(
                "select landintel.publish_reconciled_planning_links(:limit)",
                {"limit": limit},
            )
        )

    def fetch_recent_planning_changes(self, days_back: int = 8) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            select
                planning.canonical_site_id::text as canonical_site_id,
                planning.source_record_id,
                planning.planning_reference,
                planning.application_status,
                planning.decision,
                planning.updated_at
            from landintel.planning_application_records as planning
            where planning.canonical_site_id is not null
              and planning.updated_at >= now() - make_interval(days => :days_back)
            order by planning.updated_at desc
            """,
            {"days_back": days_back},
        )

    def fetch_recent_policy_changes(self, days_back: int = 8) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            """
            with policy_rows as (
                select
                    ldp.canonical_site_id::text as canonical_site_id,
                    ldp.source_record_id,
                    ldp.support_level as headline_value,
                    ldp.updated_at,
                    'ldp'::text as source_family
                from landintel.ldp_site_records as ldp
                where ldp.canonical_site_id is not null
                union all
                select
                    hla.canonical_site_id::text as canonical_site_id,
                    hla.source_record_id,
                    hla.effectiveness_status as headline_value,
                    hla.updated_at,
                    'hla'::text as source_family
                from landintel.hla_site_records as hla
                where hla.canonical_site_id is not null
                union all
                select
                    ela.canonical_site_id::text as canonical_site_id,
                    ela.source_record_id,
                    ela.employment_status as headline_value,
                    ela.updated_at,
                    'ela'::text as source_family
                from landintel.ela_site_records as ela
                where ela.canonical_site_id is not null
                union all
                select
                    vdl.canonical_site_id::text as canonical_site_id,
                    vdl.source_record_id,
                    coalesce(vdl.derelict_status, vdl.vacancy_status) as headline_value,
                    vdl.updated_at,
                    'vdl'::text as source_family
                from landintel.vdl_site_records as vdl
                where vdl.canonical_site_id is not null
            )
            select *
            from policy_rows
            where updated_at >= now() - make_interval(days => :days_back)
            order by updated_at desc
            """,
            {"days_back": days_back},
        )

    def record_change_event(
        self,
        canonical_site_id: str,
        source_family: str,
        change_category: str,
        event_type: str,
        event_summary: str,
        source_record_id: str | None = None,
        alert_priority: str = "normal",
        resurfaced_flag: bool = False,
        metadata: dict[str, Any] | None = None,
        enqueue_refresh: bool = True,
    ) -> None:
        self.database.execute(
            """
            select landintel.record_site_change_event(
                cast(:site_id as uuid),
                :source_family,
                :change_category,
                :event_type,
                :event_summary,
                :source_record_id,
                :alert_priority,
                :resurfaced_flag,
                cast(:metadata as jsonb),
                :enqueue_refresh
            )
            """,
            {
                "site_id": canonical_site_id,
                "source_family": source_family,
                "change_category": change_category,
                "event_type": event_type,
                "event_summary": event_summary,
                "source_record_id": source_record_id,
                "alert_priority": alert_priority,
                "resurfaced_flag": resurfaced_flag,
                "metadata": _json_dumps(metadata or {}),
                "enqueue_refresh": enqueue_refresh,
            },
        )


def _json_dumps(value: Any) -> str:
    return json.dumps(value, default=str)

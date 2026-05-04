"""Bounded site-to-BGS-borehole context refresh and proof."""

from __future__ import annotations

import argparse
import json
import os
import traceback
from datetime import datetime, timezone
from typing import Any

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


SOURCE_KEY = "bgs_borehole_context"
SOURCE_FAMILY = "bgs"
SAFE_USE_CAVEAT = (
    "BGS borehole index context is safe for proximity, density and log-availability intelligence only. "
    "It is not final ground-condition interpretation, piling, grouting, remediation or abnormal-cost evidence."
)


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=str)


def _env_int(name: str, default: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(1, min(value, maximum))


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


class BgsBoreholeContextRunner:
    """Refresh site-level BGS borehole context in small candidate-site batches."""

    def __init__(self, database: Database) -> None:
        self.database = database
        self.batch_size = _env_int("BGS_BOREHOLE_CONTEXT_BATCH_SIZE", 25, 100)
        self.max_age_days = _env_int("BGS_BOREHOLE_CONTEXT_MAX_AGE_DAYS", 30, 365)
        self.authority_filter = os.getenv("BGS_BOREHOLE_CONTEXT_AUTHORITY") or os.getenv("PHASE2_AUTHORITY") or ""
        self.priority_band_filter = os.getenv("BGS_BOREHOLE_CONTEXT_PRIORITY_BAND", "")
        self.force_refresh = _env_bool("BGS_BOREHOLE_CONTEXT_FORCE_REFRESH", False)

    def refresh(self) -> dict[str, Any]:
        if not self._has_clean_borehole_view():
            return self._record_event(
                {
                    "selected_site_count": 0,
                    "upserted_row_count": 0,
                    "changed_row_count": 0,
                    "evidence_row_count": 0,
                    "signal_row_count": 0,
                    "source_blocker": "landintel_store.v_bgs_borehole_master_clean_missing",
                },
                status="blocked",
            )

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select
                    site.id as canonical_site_id,
                    site.geometry,
                    priority.site_priority_band,
                    priority.site_priority_rank,
                    priority.priority_source,
                    existing.source_record_signature as previous_signature,
                    existing.updated_at as previous_updated_at
                from landintel_reporting.v_constraint_priority_sites as priority
                join landintel.canonical_sites as site
                  on site.id = priority.canonical_site_id
                left join landintel_store.site_bgs_borehole_context as existing
                  on existing.canonical_site_id = site.id
                where site.geometry is not null
                  and (:authority_name = '' or site.authority_name ilike :authority_name_like)
                  and (:priority_band = '' or priority.site_priority_band = :priority_band)
                  and (
                        :force_refresh = true
                        or existing.canonical_site_id is null
                        or existing.updated_at < now() - make_interval(days => :max_age_days)
                  )
                order by
                    existing.updated_at nulls first,
                    priority.site_priority_rank,
                    site.updated_at desc nulls last,
                    site.id
                limit :batch_size
            ),
            measured as (
                select
                    selected_sites.canonical_site_id,
                    selected_sites.site_priority_band,
                    selected_sites.site_priority_rank,
                    selected_sites.priority_source,
                    selected_sites.previous_signature,
                    nearest.bgs_id as nearest_borehole_id,
                    nearest.registration_number as nearest_borehole_reference,
                    nearest.borehole_name as nearest_borehole_name,
                    round(nearest.nearest_distance_m::numeric, 2) as nearest_borehole_distance_m,
                    nearest.depth_m as nearest_borehole_depth_m,
                    nearest.has_log_available as nearest_borehole_has_log,
                    nearest.operator_use_status as nearest_borehole_operator_use_status,
                    coalesce(counts.boreholes_inside_site, 0)::integer as boreholes_inside_site,
                    coalesce(counts.boreholes_within_100m, 0)::integer as boreholes_within_100m,
                    coalesce(counts.boreholes_within_250m, 0)::integer as boreholes_within_250m,
                    coalesce(counts.boreholes_within_500m, 0)::integer as boreholes_within_500m,
                    coalesce(counts.boreholes_within_1km, 0)::integer as boreholes_within_1km,
                    coalesce(counts.deep_boreholes_within_500m, 0)::integer as deep_boreholes_within_500m,
                    coalesce(counts.deep_boreholes_within_1km, 0)::integer as deep_boreholes_within_1km,
                    coalesce(counts.log_available_within_500m, 0)::integer as log_available_within_500m,
                    coalesce(counts.log_available_within_1km, 0)::integer as log_available_within_1km,
                    coalesce(counts.confidential_boreholes_within_1km, 0)::integer as confidential_boreholes_within_1km,
                    counts.deepest_borehole_depth_m,
                    case
                        when coalesce(counts.boreholes_within_1km, 0) >= 10
                          or coalesce(counts.log_available_within_1km, 0) >= 3 then 'strong_borehole_index_coverage'
                        when coalesce(counts.boreholes_within_1km, 0) >= 3
                          or coalesce(counts.log_available_within_1km, 0) >= 1 then 'moderate_borehole_index_coverage'
                        when coalesce(counts.boreholes_within_1km, 0) > 0 then 'limited_borehole_index_coverage'
                        else 'no_borehole_index_record_within_1km'
                    end as evidence_density_signal,
                    case
                        when coalesce(counts.boreholes_within_1km, 0) = 0 then 'high_uncertainty_no_nearby_index_evidence'
                        when coalesce(counts.log_available_within_1km, 0) > 0 then 'logs_available_for_manual_pre_si_review'
                        when coalesce(counts.boreholes_within_1km, 0) >= 3 then 'index_records_available_but_logs_not_confirmed'
                        else 'limited_index_evidence_ground_uncertainty_remains_high'
                    end as ground_uncertainty_signal
                from selected_sites
                left join lateral (
                    select
                        borehole.bgs_id,
                        borehole.registration_number,
                        borehole.borehole_name,
                        borehole.depth_m,
                        borehole.has_log_available,
                        borehole.operator_use_status,
                        st_distance(selected_sites.geometry, borehole.geom_27700) as nearest_distance_m
                    from landintel_store.v_bgs_borehole_master_clean as borehole
                    where borehole.has_valid_geometry = true
                    order by selected_sites.geometry operator(extensions.<->) borehole.geom_27700, borehole.bgs_id
                    limit 1
                ) as nearest on true
                cross join lateral (
                    select
                        count(*) filter (where st_intersects(selected_sites.geometry, borehole.geom_27700))::integer as boreholes_inside_site,
                        count(*) filter (where st_dwithin(selected_sites.geometry, borehole.geom_27700, 100))::integer as boreholes_within_100m,
                        count(*) filter (where st_dwithin(selected_sites.geometry, borehole.geom_27700, 250))::integer as boreholes_within_250m,
                        count(*) filter (where st_dwithin(selected_sites.geometry, borehole.geom_27700, 500))::integer as boreholes_within_500m,
                        count(*)::integer as boreholes_within_1km,
                        count(*) filter (where borehole.depth_m >= 10 and st_dwithin(selected_sites.geometry, borehole.geom_27700, 500))::integer as deep_boreholes_within_500m,
                        count(*) filter (where borehole.depth_m >= 10)::integer as deep_boreholes_within_1km,
                        count(*) filter (where borehole.has_log_available = true and st_dwithin(selected_sites.geometry, borehole.geom_27700, 500))::integer as log_available_within_500m,
                        count(*) filter (where borehole.has_log_available = true)::integer as log_available_within_1km,
                        count(*) filter (where borehole.is_confidential = true)::integer as confidential_boreholes_within_1km,
                        max(borehole.depth_m) as deepest_borehole_depth_m
                    from landintel_store.v_bgs_borehole_master_clean as borehole
                    where borehole.has_valid_geometry = true
                      and borehole.geom_27700 operator(extensions.&&) st_expand(selected_sites.geometry, 1000)
                      and st_dwithin(selected_sites.geometry, borehole.geom_27700, 1000)
                ) as counts
            ),
            prepared as (
                select
                    measured.*,
                    md5(concat_ws(
                        '|',
                        measured.canonical_site_id::text,
                        coalesce(measured.nearest_borehole_id::text, ''),
                        coalesce(measured.nearest_borehole_distance_m::text, ''),
                        measured.boreholes_inside_site::text,
                        measured.boreholes_within_100m::text,
                        measured.boreholes_within_250m::text,
                        measured.boreholes_within_500m::text,
                        measured.boreholes_within_1km::text,
                        measured.deep_boreholes_within_500m::text,
                        measured.deep_boreholes_within_1km::text,
                        measured.log_available_within_500m::text,
                        measured.log_available_within_1km::text,
                        measured.confidential_boreholes_within_1km::text,
                        coalesce(measured.deepest_borehole_depth_m::text, ''),
                        measured.evidence_density_signal,
                        measured.ground_uncertainty_signal
                    )) as current_signature
                from measured
            ),
            upserted as (
                insert into landintel_store.site_bgs_borehole_context (
                    canonical_site_id,
                    source_key,
                    source_family,
                    site_priority_band,
                    site_priority_rank,
                    priority_source,
                    nearest_borehole_id,
                    nearest_borehole_reference,
                    nearest_borehole_name,
                    nearest_borehole_distance_m,
                    nearest_borehole_depth_m,
                    nearest_borehole_has_log,
                    nearest_borehole_operator_use_status,
                    boreholes_inside_site,
                    boreholes_within_100m,
                    boreholes_within_250m,
                    boreholes_within_500m,
                    boreholes_within_1km,
                    deep_boreholes_within_500m,
                    deep_boreholes_within_1km,
                    log_available_within_500m,
                    log_available_within_1km,
                    confidential_boreholes_within_1km,
                    deepest_borehole_depth_m,
                    evidence_density_signal,
                    ground_uncertainty_signal,
                    source_record_signature,
                    safe_use_caveat,
                    metadata,
                    measured_at,
                    updated_at
                )
                select
                    prepared.canonical_site_id,
                    :source_key,
                    :source_family,
                    prepared.site_priority_band,
                    prepared.site_priority_rank,
                    prepared.priority_source,
                    prepared.nearest_borehole_id,
                    prepared.nearest_borehole_reference,
                    prepared.nearest_borehole_name,
                    prepared.nearest_borehole_distance_m,
                    prepared.nearest_borehole_depth_m,
                    prepared.nearest_borehole_has_log,
                    prepared.nearest_borehole_operator_use_status,
                    prepared.boreholes_inside_site,
                    prepared.boreholes_within_100m,
                    prepared.boreholes_within_250m,
                    prepared.boreholes_within_500m,
                    prepared.boreholes_within_1km,
                    prepared.deep_boreholes_within_500m,
                    prepared.deep_boreholes_within_1km,
                    prepared.log_available_within_500m,
                    prepared.log_available_within_1km,
                    prepared.confidential_boreholes_within_1km,
                    prepared.deepest_borehole_depth_m,
                    prepared.evidence_density_signal,
                    prepared.ground_uncertainty_signal,
                    prepared.current_signature,
                    cast(:safe_use_caveat as text),
                    jsonb_build_object(
                        'source_key', cast(:source_key as text),
                        'source_family', cast(:source_family as text),
                        'phase', 'G1',
                        'basis', 'BGS Single Onshore Borehole Index proximity and density context',
                        'deep_borehole_threshold_m', 10
                    ),
                    now(),
                    now()
                from prepared
                on conflict (canonical_site_id) do update set
                    site_priority_band = excluded.site_priority_band,
                    site_priority_rank = excluded.site_priority_rank,
                    priority_source = excluded.priority_source,
                    nearest_borehole_id = excluded.nearest_borehole_id,
                    nearest_borehole_reference = excluded.nearest_borehole_reference,
                    nearest_borehole_name = excluded.nearest_borehole_name,
                    nearest_borehole_distance_m = excluded.nearest_borehole_distance_m,
                    nearest_borehole_depth_m = excluded.nearest_borehole_depth_m,
                    nearest_borehole_has_log = excluded.nearest_borehole_has_log,
                    nearest_borehole_operator_use_status = excluded.nearest_borehole_operator_use_status,
                    boreholes_inside_site = excluded.boreholes_inside_site,
                    boreholes_within_100m = excluded.boreholes_within_100m,
                    boreholes_within_250m = excluded.boreholes_within_250m,
                    boreholes_within_500m = excluded.boreholes_within_500m,
                    boreholes_within_1km = excluded.boreholes_within_1km,
                    deep_boreholes_within_500m = excluded.deep_boreholes_within_500m,
                    deep_boreholes_within_1km = excluded.deep_boreholes_within_1km,
                    log_available_within_500m = excluded.log_available_within_500m,
                    log_available_within_1km = excluded.log_available_within_1km,
                    confidential_boreholes_within_1km = excluded.confidential_boreholes_within_1km,
                    deepest_borehole_depth_m = excluded.deepest_borehole_depth_m,
                    evidence_density_signal = excluded.evidence_density_signal,
                    ground_uncertainty_signal = excluded.ground_uncertainty_signal,
                    source_record_signature = excluded.source_record_signature,
                    safe_use_caveat = excluded.safe_use_caveat,
                    metadata = excluded.metadata,
                    measured_at = now(),
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join prepared
                  on prepared.canonical_site_id = upserted.canonical_site_id
                where prepared.previous_signature is distinct from prepared.current_signature
            ),
            deleted_evidence as (
                delete from landintel.evidence_references as evidence
                using changed
                where evidence.canonical_site_id = changed.canonical_site_id
                  and evidence.source_family = :source_family
                  and evidence.metadata ->> 'source_key' = :source_key
                returning evidence.id
            ),
            inserted_evidence as (
                insert into landintel.evidence_references (
                    canonical_site_id,
                    source_family,
                    source_dataset,
                    source_record_id,
                    source_reference,
                    confidence,
                    metadata
                )
                select
                    changed.canonical_site_id,
                    :source_family,
                    'BGS Single Onshore Borehole Index site context',
                    changed.id::text,
                    coalesce(changed.nearest_borehole_reference, changed.evidence_density_signal),
                    'medium',
                    jsonb_build_object(
                        'source_key', cast(:source_key as text),
                        'nearest_borehole_id', changed.nearest_borehole_id,
                        'nearest_borehole_distance_m', changed.nearest_borehole_distance_m,
                        'boreholes_within_1km', changed.boreholes_within_1km,
                        'log_available_within_1km', changed.log_available_within_1km,
                        'safe_use_caveat', changed.safe_use_caveat
                    )
                from changed
                returning id
            ),
            deleted_signals as (
                delete from landintel.site_signals as signal
                using changed
                where signal.canonical_site_id = changed.canonical_site_id
                  and signal.source_family = :source_family
                  and signal.metadata ->> 'source_key' = :source_key
                returning signal.id
            ),
            signal_rows as (
                select
                    changed.canonical_site_id,
                    'bgs_borehole:nearest_distance:' || changed.canonical_site_id::text as signal_key,
                    jsonb_build_object('nearest_borehole_distance_m', changed.nearest_borehole_distance_m) as signal_value,
                    'derived'::text as signal_status,
                    'ground_context'::text as signal_family,
                    'bgs_nearest_borehole_distance'::text as signal_name,
                    coalesce(changed.nearest_borehole_distance_m::text, 'not_available') as signal_value_text,
                    changed.nearest_borehole_distance_m as signal_value_numeric,
                    0.65::numeric as confidence,
                    changed.id::text as source_record_id,
                    'bgs_borehole_nearest_distance'::text as fact_label,
                    changed.safe_use_caveat,
                    changed.evidence_density_signal,
                    changed.ground_uncertainty_signal
                from changed

                union all

                select
                    changed.canonical_site_id,
                    'bgs_borehole:evidence_density:' || changed.canonical_site_id::text,
                    jsonb_build_object('boreholes_within_1km', changed.boreholes_within_1km, 'log_available_within_1km', changed.log_available_within_1km),
                    'derived',
                    'ground_context',
                    'bgs_borehole_evidence_density',
                    changed.evidence_density_signal,
                    changed.boreholes_within_1km,
                    0.65,
                    changed.id::text,
                    'bgs_borehole_evidence_density',
                    changed.safe_use_caveat,
                    changed.evidence_density_signal,
                    changed.ground_uncertainty_signal
                from changed

                union all

                select
                    changed.canonical_site_id,
                    'bgs_borehole:ground_uncertainty:' || changed.canonical_site_id::text,
                    jsonb_build_object('ground_uncertainty_signal', changed.ground_uncertainty_signal),
                    'derived',
                    'ground_context',
                    'bgs_borehole_ground_uncertainty',
                    changed.ground_uncertainty_signal,
                    null::numeric,
                    0.6,
                    changed.id::text,
                    'bgs_borehole_ground_uncertainty',
                    changed.safe_use_caveat,
                    changed.evidence_density_signal,
                    changed.ground_uncertainty_signal
                from changed
            ),
            inserted_signals as (
                insert into landintel.site_signals (
                    canonical_site_id,
                    signal_key,
                    signal_value,
                    signal_status,
                    signal_family,
                    signal_name,
                    signal_value_text,
                    signal_value_numeric,
                    confidence,
                    source_family,
                    source_record_id,
                    fact_label,
                    evidence_metadata,
                    metadata,
                    current_flag
                )
                select
                    signal_rows.canonical_site_id,
                    signal_rows.signal_key,
                    signal_rows.signal_value,
                    signal_rows.signal_status,
                    signal_rows.signal_family,
                    signal_rows.signal_name,
                    signal_rows.signal_value_text,
                    signal_rows.signal_value_numeric,
                    signal_rows.confidence,
                    :source_family,
                    signal_rows.source_record_id,
                    signal_rows.fact_label,
                    jsonb_build_object('source_key', cast(:source_key as text), 'safe_use_caveat', signal_rows.safe_use_caveat),
                    jsonb_build_object(
                        'source_key', cast(:source_key as text),
                        'evidence_density_signal', signal_rows.evidence_density_signal,
                        'ground_uncertainty_signal', signal_rows.ground_uncertainty_signal,
                        'safe_use_caveat', signal_rows.safe_use_caveat
                    ),
                    true
                from signal_rows
                returning id
            )
            select
                (select count(*)::integer from selected_sites) as selected_site_count,
                (select count(*)::integer from upserted) as upserted_row_count,
                (select count(*)::integer from changed) as changed_row_count,
                (select count(*)::integer from inserted_evidence) as evidence_row_count,
                (select count(*)::integer from inserted_signals) as signal_row_count
            """,
            {
                "source_key": SOURCE_KEY,
                "source_family": SOURCE_FAMILY,
                "safe_use_caveat": SAFE_USE_CAVEAT,
                "batch_size": self.batch_size,
                "max_age_days": self.max_age_days,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
                "priority_band": self.priority_band_filter,
                "force_refresh": self.force_refresh,
            },
        ) or {}

        return self._record_event(proof, status="success")

    def audit(self) -> dict[str, Any]:
        return {
            "message": "site_bgs_borehole_context_proof",
            "context_counts": self.database.fetch_one(
                """
                select
                    count(*)::integer as context_row_count,
                    count(distinct canonical_site_id)::integer as measured_site_count,
                    count(*) filter (where boreholes_within_1km > 0)::integer as sites_with_borehole_index_evidence,
                    count(*) filter (where log_available_within_1km > 0)::integer as sites_with_log_available_nearby,
                    max(measured_at) as latest_measured_at
                from landintel_store.site_bgs_borehole_context
                """
            ),
            "priority_counts": self.database.fetch_all(
                """
                select
                    site_priority_band,
                    evidence_density_signal,
                    ground_uncertainty_signal,
                    count(*)::integer as site_count
                from landintel_store.site_bgs_borehole_context
                group by site_priority_band, evidence_density_signal, ground_uncertainty_signal
                order by site_priority_band, evidence_density_signal, ground_uncertainty_signal
                limit 40
                """
            ),
            "operator_sample": self.database.fetch_all(
                """
                select
                    canonical_site_id,
                    site_label,
                    authority_name,
                    site_priority_band,
                    nearest_borehole_distance_m,
                    boreholes_within_1km,
                    log_available_within_1km,
                    evidence_density_signal,
                    ground_uncertainty_signal,
                    operator_summary,
                    safe_use_caveat
                from landintel_reporting.v_site_bgs_borehole_context
                order by site_priority_rank, boreholes_within_1km desc, nearest_borehole_distance_m nulls last
                limit 20
                """
            ),
        }

    def _has_clean_borehole_view(self) -> bool:
        return bool(
            self.database.scalar(
                "select to_regclass('landintel_store.v_bgs_borehole_master_clean') is not null"
            )
        )

    def _record_event(self, proof: dict[str, Any], *, status: str) -> dict[str, Any]:
        raw_rows = int(proof.get("upserted_row_count") or 0)
        evidence_rows = int(proof.get("evidence_row_count") or 0)
        signal_rows = int(proof.get("signal_row_count") or 0)
        self.database.execute(
            """
            insert into landintel.source_expansion_events (
                command_name,
                source_key,
                source_family,
                status,
                raw_rows,
                linked_rows,
                measured_rows,
                evidence_rows,
                signal_rows,
                change_event_rows,
                summary,
                metadata
            ) values (
                'refresh-site-bgs-borehole-context',
                :source_key,
                :source_family,
                :status,
                :raw_rows,
                :linked_rows,
                :measured_rows,
                :evidence_rows,
                :signal_rows,
                0,
                :summary,
                cast(:metadata as jsonb)
            )
            """,
            {
                "source_key": SOURCE_KEY,
                "source_family": SOURCE_FAMILY,
                "status": status,
                "raw_rows": raw_rows,
                "linked_rows": raw_rows,
                "measured_rows": raw_rows,
                "evidence_rows": evidence_rows,
                "signal_rows": signal_rows,
                "summary": "Bounded site-to-BGS-borehole context refreshed for candidate sites.",
                "metadata": _json_dumps(
                    {
                        "batch_size": self.batch_size,
                        "authority_filter": self.authority_filter,
                        "priority_band_filter": self.priority_band_filter,
                        "force_refresh": self.force_refresh,
                        "safe_use_caveat": SAFE_USE_CAVEAT,
                    }
                ),
            },
        )
        self.database.execute(
            """
            insert into landintel.source_freshness_states (
                source_scope_key,
                source_family,
                source_dataset,
                source_name,
                source_access_mode,
                refresh_cadence,
                max_staleness_days,
                source_observed_at,
                last_checked_at,
                last_success_at,
                next_refresh_due_at,
                freshness_status,
                live_access_status,
                ranking_eligible,
                review_output_eligible,
                stale_reason_code,
                check_summary,
                records_observed,
                metadata,
                updated_at
            ) values (
                'phase_g1:bgs_borehole_context',
                :source_family,
                'BGS Single Onshore Borehole Index site context',
                'BGS borehole site context',
                'known_origin_manual_bulk_upload',
                'monthly',
                30,
                now(),
                now(),
                :last_success_at,
                now() + interval '30 days',
                :freshness_status,
                :live_access_status,
                false,
                true,
                :stale_reason_code,
                :check_summary,
                :records_observed,
                cast(:metadata as jsonb),
                now()
            )
            on conflict (source_scope_key) do update set
                source_family = excluded.source_family,
                source_dataset = excluded.source_dataset,
                source_name = excluded.source_name,
                source_access_mode = excluded.source_access_mode,
                source_observed_at = excluded.source_observed_at,
                last_checked_at = excluded.last_checked_at,
                last_success_at = excluded.last_success_at,
                next_refresh_due_at = excluded.next_refresh_due_at,
                freshness_status = excluded.freshness_status,
                live_access_status = excluded.live_access_status,
                stale_reason_code = excluded.stale_reason_code,
                check_summary = excluded.check_summary,
                records_observed = excluded.records_observed,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            {
                "source_family": SOURCE_FAMILY,
                "last_success_at": datetime.now(timezone.utc) if status == "success" else None,
                "freshness_status": "current" if status == "success" else "blocked",
                "live_access_status": "manual_bulk_upload_available" if status == "success" else "missing_clean_borehole_view",
                "stale_reason_code": None if status == "success" else proof.get("source_blocker"),
                "check_summary": "Bounded BGS borehole site-context refresh completed." if status == "success" else str(proof.get("source_blocker")),
                "records_observed": raw_rows,
                "metadata": _json_dumps({"safe_use_caveat": SAFE_USE_CAVEAT, **proof}),
            },
        )
        return {
            "message": "site_bgs_borehole_context_refresh",
            "source_key": SOURCE_KEY,
            "source_family": SOURCE_FAMILY,
            "batch_size": self.batch_size,
            "authority_filter": self.authority_filter,
            "priority_band_filter": self.priority_band_filter,
            "force_refresh": self.force_refresh,
            **proof,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh or audit bounded site-to-BGS-borehole context.")
    parser.add_argument("command", choices=("refresh-site-bgs-borehole-context", "audit-site-bgs-borehole-context"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    runner = BgsBoreholeContextRunner(database)
    try:
        if args.command == "refresh-site-bgs-borehole-context":
            result = runner.refresh()
            print(json.dumps(result, default=str, ensure_ascii=False), flush=True)
            logger.info(
                "site_bgs_borehole_context_refresh_completed",
                extra={key: value for key, value in result.items() if key != "message"},
            )
        else:
            result = runner.audit()
            print(json.dumps(result, default=str, ensure_ascii=False), flush=True)
            logger.info("site_bgs_borehole_context_audit_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception(
            "site_bgs_borehole_context_command_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

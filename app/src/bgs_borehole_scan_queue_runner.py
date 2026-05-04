"""Bounded BGS borehole scan/log registry and candidate-site queue."""

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


REGISTRY_SOURCE_KEY = "bgs_borehole_scan_registry"
QUEUE_SOURCE_KEY = "bgs_borehole_scan_queue"
SOURCE_FAMILY = "bgs"
REGISTRY_SAFE_CAVEAT = (
    "BGS scan/log registry stores source links only. It does not download scans, run OCR, "
    "store PDF blobs in Postgres or provide final ground-condition interpretation."
)
QUEUE_SAFE_CAVEAT = (
    "BGS scan/log queue is for bounded candidate-site review only. Linked source records are not "
    "downloaded, OCRed or treated as engineering or abnormal-cost evidence."
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


class BgsBoreholeScanQueueRunner:
    """Refresh source links and queue candidate-site BGS logs without fetching assets."""

    def __init__(self, database: Database) -> None:
        self.database = database
        self.registry_batch_size = _env_int("BGS_SCAN_REGISTRY_BATCH_SIZE", 250, 1000)
        self.queue_site_batch_size = _env_int("BGS_SCAN_QUEUE_SITE_BATCH_SIZE", 10, 50)
        self.queue_max_per_site = _env_int("BGS_SCAN_QUEUE_MAX_PER_SITE", 3, 10)
        self.queue_max_rows = _env_int("BGS_SCAN_QUEUE_MAX_ROWS", 25, 250)
        self.max_age_days = _env_int("BGS_SCAN_QUEUE_MAX_AGE_DAYS", 30, 365)
        self.authority_filter = os.getenv("BGS_SCAN_QUEUE_AUTHORITY") or os.getenv("PHASE2_AUTHORITY") or ""
        self.priority_band_filter = os.getenv("BGS_SCAN_QUEUE_PRIORITY_BAND", "")
        self.force_refresh = _env_bool("BGS_SCAN_QUEUE_FORCE_REFRESH", False)

    def refresh_registry(self) -> dict[str, Any]:
        if not self._has_clean_borehole_view():
            return self._record_event(
                command_name="refresh-bgs-borehole-scan-registry",
                source_key=REGISTRY_SOURCE_KEY,
                status="blocked",
                proof={
                    "selected_record_count": 0,
                    "upserted_record_count": 0,
                    "changed_record_count": 0,
                    "source_blocker": "landintel_store.v_bgs_borehole_master_clean_missing",
                },
            )

        proof = self.database.fetch_one(
            """
            with selected_records as (
                select
                    borehole.bgs_id,
                    borehole.registration_number,
                    borehole.borehole_name,
                    borehole.grid_reference,
                    borehole.easting,
                    borehole.northing,
                    borehole.geom_27700,
                    borehole.depth_m,
                    borehole.has_log_available,
                    borehole.ags_log_url,
                    borehole.operator_use_status,
                    existing.source_record_signature as previous_signature
                from landintel_store.v_bgs_borehole_master_clean as borehole
                left join landintel_store.bgs_borehole_scan_registry as existing
                  on existing.bgs_id = borehole.bgs_id
                where borehole.has_log_available = true
                  and nullif(borehole.ags_log_url, '') is not null
                  and (
                        :force_refresh = true
                        or existing.bgs_id is null
                        or existing.updated_at < now() - make_interval(days => :max_age_days)
                  )
                order by existing.updated_at nulls first, borehole.bgs_id
                limit :batch_size
            ),
            prepared as (
                select
                    selected_records.*,
                    md5(concat_ws(
                        '|',
                        selected_records.bgs_id::text,
                        coalesce(selected_records.registration_number, ''),
                        coalesce(selected_records.borehole_name, ''),
                        coalesce(selected_records.grid_reference, ''),
                        coalesce(selected_records.easting::text, ''),
                        coalesce(selected_records.northing::text, ''),
                        coalesce(selected_records.depth_m::text, ''),
                        coalesce(selected_records.ags_log_url, ''),
                        coalesce(selected_records.operator_use_status, '')
                    )) as current_signature
                from selected_records
            ),
            upserted as (
                insert into landintel_store.bgs_borehole_scan_registry (
                    source_key,
                    source_family,
                    bgs_id,
                    registration_number,
                    borehole_name,
                    grid_reference,
                    easting,
                    northing,
                    geom_27700,
                    depth_m,
                    has_log_available,
                    ags_log_url,
                    operator_use_status,
                    registry_status,
                    fetch_status,
                    source_record_signature,
                    safe_use_caveat,
                    metadata,
                    last_seen_at,
                    updated_at
                )
                select
                    :source_key,
                    :source_family,
                    prepared.bgs_id,
                    prepared.registration_number,
                    prepared.borehole_name,
                    prepared.grid_reference,
                    prepared.easting,
                    prepared.northing,
                    prepared.geom_27700,
                    prepared.depth_m,
                    prepared.has_log_available,
                    prepared.ags_log_url,
                    prepared.operator_use_status,
                    'linked_not_downloaded',
                    'not_queued',
                    prepared.current_signature,
                    cast(:safe_use_caveat as text),
                    jsonb_build_object(
                        'source_key', cast(:source_key as text),
                        'source_family', cast(:source_family as text),
                        'phase', 'G2',
                        'basis', 'BGS clean borehole master has AGS/log URL',
                        'download_assets', false,
                        'ocr', false
                    ),
                    now(),
                    now()
                from prepared
                on conflict (bgs_id) do update set
                    source_key = excluded.source_key,
                    source_family = excluded.source_family,
                    registration_number = excluded.registration_number,
                    borehole_name = excluded.borehole_name,
                    grid_reference = excluded.grid_reference,
                    easting = excluded.easting,
                    northing = excluded.northing,
                    geom_27700 = excluded.geom_27700,
                    depth_m = excluded.depth_m,
                    has_log_available = excluded.has_log_available,
                    ags_log_url = excluded.ags_log_url,
                    operator_use_status = excluded.operator_use_status,
                    registry_status = excluded.registry_status,
                    source_record_signature = excluded.source_record_signature,
                    safe_use_caveat = excluded.safe_use_caveat,
                    metadata = excluded.metadata,
                    last_seen_at = now(),
                    updated_at = now()
                returning bgs_id
            ),
            changed as (
                select prepared.bgs_id
                from prepared
                where prepared.previous_signature is distinct from prepared.current_signature
            )
            select
                (select count(*)::integer from selected_records) as selected_record_count,
                (select count(*)::integer from upserted) as upserted_record_count,
                (select count(*)::integer from changed) as changed_record_count
            """,
            {
                "source_key": REGISTRY_SOURCE_KEY,
                "source_family": SOURCE_FAMILY,
                "safe_use_caveat": REGISTRY_SAFE_CAVEAT,
                "batch_size": self.registry_batch_size,
                "max_age_days": self.max_age_days,
                "force_refresh": self.force_refresh,
            },
        ) or {}

        return self._record_event(
            command_name="refresh-bgs-borehole-scan-registry",
            source_key=REGISTRY_SOURCE_KEY,
            status="success",
            proof=proof,
        )

    def queue_scans(self) -> dict[str, Any]:
        if not self._has_queue_prerequisites():
            return self._record_event(
                command_name="queue-bgs-borehole-scans",
                source_key=QUEUE_SOURCE_KEY,
                status="blocked",
                proof={
                    "selected_site_count": 0,
                    "queued_row_count": 0,
                    "changed_queue_row_count": 0,
                    "source_blocker": "bgs_scan_queue_prerequisites_missing",
                },
            )

        proof = self.database.fetch_one(
            """
            with selected_sites as (
                select
                    context.id as context_id,
                    context.canonical_site_id,
                    site.geometry,
                    context.site_priority_band,
                    context.site_priority_rank,
                    context.priority_source,
                    existing_queue.latest_queue_updated_at
                from landintel_store.site_bgs_borehole_context as context
                join landintel.canonical_sites as site
                  on site.id = context.canonical_site_id
                left join lateral (
                    select max(queue.updated_at) as latest_queue_updated_at
                    from landintel_store.bgs_borehole_scan_fetch_queue as queue
                    where queue.canonical_site_id = context.canonical_site_id
                ) as existing_queue on true
                where site.geometry is not null
                  and context.log_available_within_1km > 0
                  and (:authority_name = '' or site.authority_name ilike :authority_name_like)
                  and (:priority_band = '' or context.site_priority_band = :priority_band)
                  and (
                        :force_refresh = true
                        or existing_queue.latest_queue_updated_at is null
                        or existing_queue.latest_queue_updated_at < now() - make_interval(days => :max_age_days)
                  )
                order by
                    existing_queue.latest_queue_updated_at nulls first,
                    context.site_priority_rank,
                    context.log_available_within_1km desc,
                    context.updated_at desc nulls last,
                    context.canonical_site_id
                limit :site_batch_size
            ),
            candidate_assets as (
                select
                    selected_sites.context_id,
                    selected_sites.canonical_site_id,
                    selected_sites.site_priority_band,
                    selected_sites.site_priority_rank,
                    selected_sites.priority_source,
                    borehole.bgs_id,
                    borehole.registration_number,
                    borehole.borehole_name,
                    borehole.grid_reference,
                    borehole.easting,
                    borehole.northing,
                    borehole.geom_27700,
                    borehole.depth_m as borehole_depth_m,
                    borehole.has_log_available,
                    borehole.ags_log_url,
                    borehole.operator_use_status,
                    round(st_distance(selected_sites.geometry, borehole.geom_27700)::numeric, 2) as borehole_distance_m
                from selected_sites
                join lateral (
                    select borehole.*
                    from landintel_store.v_bgs_borehole_master_clean as borehole
                    where borehole.has_valid_geometry = true
                      and borehole.has_log_available = true
                      and nullif(borehole.ags_log_url, '') is not null
                      and st_dwithin(selected_sites.geometry, borehole.geom_27700, 1000)
                    order by selected_sites.geometry operator(extensions.<->) borehole.geom_27700, borehole.bgs_id
                    limit :max_per_site
                ) as borehole on true
            ),
            registry_source_records as (
                select distinct on (candidate_assets.bgs_id)
                    candidate_assets.bgs_id,
                    candidate_assets.registration_number,
                    candidate_assets.borehole_name,
                    candidate_assets.grid_reference,
                    candidate_assets.easting,
                    candidate_assets.northing,
                    candidate_assets.geom_27700,
                    candidate_assets.borehole_depth_m,
                    candidate_assets.has_log_available,
                    candidate_assets.ags_log_url,
                    candidate_assets.operator_use_status,
                    md5(concat_ws(
                        '|',
                        candidate_assets.bgs_id::text,
                        coalesce(candidate_assets.registration_number, ''),
                        coalesce(candidate_assets.borehole_name, ''),
                        coalesce(candidate_assets.grid_reference, ''),
                        coalesce(candidate_assets.easting::text, ''),
                        coalesce(candidate_assets.northing::text, ''),
                        coalesce(candidate_assets.borehole_depth_m::text, ''),
                        coalesce(candidate_assets.ags_log_url, ''),
                        coalesce(candidate_assets.operator_use_status, '')
                    )) as current_registry_signature
                from candidate_assets
                order by candidate_assets.bgs_id, candidate_assets.borehole_distance_m nulls last
            ),
            upserted_registry as (
                insert into landintel_store.bgs_borehole_scan_registry as existing_registry (
                    source_key,
                    source_family,
                    bgs_id,
                    registration_number,
                    borehole_name,
                    grid_reference,
                    easting,
                    northing,
                    geom_27700,
                    depth_m,
                    has_log_available,
                    ags_log_url,
                    operator_use_status,
                    registry_status,
                    fetch_status,
                    source_record_signature,
                    safe_use_caveat,
                    metadata,
                    last_seen_at,
                    updated_at
                )
                select
                    :registry_source_key,
                    :source_family,
                    registry_source_records.bgs_id,
                    registry_source_records.registration_number,
                    registry_source_records.borehole_name,
                    registry_source_records.grid_reference,
                    registry_source_records.easting,
                    registry_source_records.northing,
                    registry_source_records.geom_27700,
                    registry_source_records.borehole_depth_m,
                    registry_source_records.has_log_available,
                    registry_source_records.ags_log_url,
                    registry_source_records.operator_use_status,
                    'linked_not_downloaded',
                    'not_queued',
                    registry_source_records.current_registry_signature,
                    cast(:registry_safe_caveat as text),
                    jsonb_build_object(
                        'source_key', cast(:registry_source_key as text),
                        'source_family', cast(:source_family as text),
                        'phase', 'G2',
                        'basis', 'candidate-site nearby BGS clean borehole master record has AGS/log URL',
                        'download_assets', false,
                        'ocr', false
                    ),
                    now(),
                    now()
                from registry_source_records
                on conflict (bgs_id) do update set
                    source_key = excluded.source_key,
                    source_family = excluded.source_family,
                    registration_number = excluded.registration_number,
                    borehole_name = excluded.borehole_name,
                    grid_reference = excluded.grid_reference,
                    easting = excluded.easting,
                    northing = excluded.northing,
                    geom_27700 = excluded.geom_27700,
                    depth_m = excluded.depth_m,
                    has_log_available = excluded.has_log_available,
                    ags_log_url = excluded.ags_log_url,
                    operator_use_status = excluded.operator_use_status,
                    registry_status = excluded.registry_status,
                    fetch_status = case
                        when existing_registry.fetch_status in ('not_queued', 'linked_not_downloaded')
                            then existing_registry.fetch_status
                        else existing_registry.fetch_status
                    end,
                    source_record_signature = excluded.source_record_signature,
                    safe_use_caveat = excluded.safe_use_caveat,
                    metadata = excluded.metadata,
                    last_seen_at = now(),
                    updated_at = now()
                returning id, bgs_id
            ),
            prepared as (
                select
                    candidate_assets.*,
                    upserted_registry.id as registry_id,
                    md5(concat_ws(
                        '|',
                        candidate_assets.canonical_site_id::text,
                        candidate_assets.bgs_id::text,
                        coalesce(candidate_assets.borehole_distance_m::text, ''),
                        coalesce(candidate_assets.site_priority_band, ''),
                        coalesce(candidate_assets.site_priority_rank::text, ''),
                        coalesce(candidate_assets.priority_source, '')
                    )) as current_signature
                from candidate_assets
                join upserted_registry
                  on upserted_registry.bgs_id = candidate_assets.bgs_id
                order by
                    candidate_assets.site_priority_rank,
                    candidate_assets.borehole_distance_m nulls last,
                    candidate_assets.bgs_id
                limit :max_queue_rows
            ),
            upserted as (
                insert into landintel_store.bgs_borehole_scan_fetch_queue as existing_queue (
                    canonical_site_id,
                    registry_id,
                    bgs_id,
                    source_key,
                    source_family,
                    site_priority_band,
                    site_priority_rank,
                    priority_source,
                    borehole_distance_m,
                    borehole_depth_m,
                    queue_status,
                    fetch_status,
                    requested_action,
                    source_record_signature,
                    safe_use_caveat,
                    metadata,
                    queued_at,
                    updated_at
                )
                select
                    prepared.canonical_site_id,
                    prepared.registry_id,
                    prepared.bgs_id,
                    :source_key,
                    :source_family,
                    prepared.site_priority_band,
                    prepared.site_priority_rank,
                    prepared.priority_source,
                    prepared.borehole_distance_m,
                    prepared.borehole_depth_m,
                    'queued',
                    'linked_not_downloaded',
                    'manual_pre_si_log_review',
                    prepared.current_signature,
                    cast(:safe_use_caveat as text),
                    jsonb_build_object(
                        'source_key', cast(:source_key as text),
                        'source_family', cast(:source_family as text),
                        'phase', 'G2',
                        'site_bgs_context_id', prepared.context_id,
                        'download_assets', false,
                        'ocr', false
                    ),
                    now(),
                    now()
                from prepared
                on conflict (canonical_site_id, bgs_id) do update set
                    registry_id = excluded.registry_id,
                    source_key = excluded.source_key,
                    source_family = excluded.source_family,
                    site_priority_band = excluded.site_priority_band,
                    site_priority_rank = excluded.site_priority_rank,
                    priority_source = excluded.priority_source,
                    borehole_distance_m = excluded.borehole_distance_m,
                    borehole_depth_m = excluded.borehole_depth_m,
                    queue_status = case
                        when existing_queue.queue_status in ('queued', 'linked_not_downloaded')
                            then excluded.queue_status
                        else existing_queue.queue_status
                    end,
                    fetch_status = case
                        when existing_queue.fetch_status in ('not_queued', 'linked_not_downloaded')
                            then excluded.fetch_status
                        else existing_queue.fetch_status
                    end,
                    requested_action = excluded.requested_action,
                    source_record_signature = excluded.source_record_signature,
                    safe_use_caveat = excluded.safe_use_caveat,
                    metadata = excluded.metadata,
                    updated_at = now()
                returning *
            ),
            changed as (
                select upserted.*
                from upserted
                join prepared
                  on prepared.canonical_site_id = upserted.canonical_site_id
                 and prepared.bgs_id = upserted.bgs_id
                where upserted.source_record_signature = prepared.current_signature
            )
            select
                (select count(*)::integer from selected_sites) as selected_site_count,
                (select count(*)::integer from prepared) as candidate_asset_count,
                (select count(*)::integer from upserted_registry) as registry_upserted_for_queue_count,
                (select count(*)::integer from upserted) as queued_row_count,
                (select count(*)::integer from changed) as changed_queue_row_count
            """,
            {
                "source_key": QUEUE_SOURCE_KEY,
                "registry_source_key": REGISTRY_SOURCE_KEY,
                "source_family": SOURCE_FAMILY,
                "safe_use_caveat": QUEUE_SAFE_CAVEAT,
                "registry_safe_caveat": REGISTRY_SAFE_CAVEAT,
                "site_batch_size": self.queue_site_batch_size,
                "max_per_site": self.queue_max_per_site,
                "max_queue_rows": self.queue_max_rows,
                "max_age_days": self.max_age_days,
                "authority_name": self.authority_filter,
                "authority_name_like": f"%{self.authority_filter}%",
                "priority_band": self.priority_band_filter,
                "force_refresh": self.force_refresh,
            },
        ) or {}

        self.database.execute(
            """
            update landintel_store.bgs_borehole_scan_registry as registry
            set fetch_status = 'queued',
                updated_at = now()
            where exists (
                select 1
                from landintel_store.bgs_borehole_scan_fetch_queue as queue
                where queue.registry_id = registry.id
                  and queue.fetch_status = 'linked_not_downloaded'
            )
              and registry.fetch_status in ('not_queued', 'linked_not_downloaded')
            """,
        )

        return self._record_event(
            command_name="queue-bgs-borehole-scans",
            source_key=QUEUE_SOURCE_KEY,
            status="success",
            proof=proof,
        )

    def audit(self) -> dict[str, Any]:
        return {
            "message": "bgs_borehole_scan_queue_proof",
            "registry_counts": self.database.fetch_one(
                """
                select
                    count(*)::integer as registry_row_count,
                    count(*) filter (where has_log_available = true)::integer as rows_with_log_available,
                    count(*) filter (where ags_log_url is not null)::integer as rows_with_source_link,
                    count(*) filter (where geom_27700 is not null)::integer as rows_with_geometry,
                    max(updated_at) as latest_registry_updated_at
                from landintel_store.bgs_borehole_scan_registry
                """
            ),
            "queue_counts": self.database.fetch_one(
                """
                select
                    count(*)::integer as queue_row_count,
                    count(distinct canonical_site_id)::integer as queued_site_count,
                    count(distinct bgs_id)::integer as queued_bgs_record_count,
                    max(updated_at) as latest_queue_updated_at
                from landintel_store.bgs_borehole_scan_fetch_queue
                """
            ),
            "queue_status_counts": self.database.fetch_all(
                """
                select
                    queue_status,
                    fetch_status,
                    count(*)::integer as row_count
                from landintel_store.bgs_borehole_scan_fetch_queue
                group by queue_status, fetch_status
                order by queue_status, fetch_status
                """
            ),
            "priority_counts": self.database.fetch_all(
                """
                select
                    site_priority_band,
                    count(distinct canonical_site_id)::integer as queued_site_count,
                    count(*)::integer as queued_row_count,
                    min(borehole_distance_m) as nearest_queued_borehole_m
                from landintel_store.bgs_borehole_scan_fetch_queue
                group by site_priority_band
                order by min(site_priority_rank) nulls last, site_priority_band
                limit 20
                """
            ),
            "operator_sample": self.database.fetch_all(
                """
                select
                    canonical_site_id,
                    site_label,
                    authority_name,
                    site_priority_band,
                    bgs_id,
                    registration_number,
                    borehole_name,
                    borehole_distance_m,
                    queue_status,
                    fetch_status,
                    requested_action,
                    safe_use_caveat
                from landintel_reporting.v_bgs_scan_queue
                order by site_priority_rank, borehole_distance_m nulls last, bgs_id
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

    def _has_queue_prerequisites(self) -> bool:
        return bool(
            self.database.scalar(
                """
                select
                    to_regclass('landintel_store.site_bgs_borehole_context') is not null
                    and to_regclass('landintel_store.v_bgs_borehole_master_clean') is not null
                    and to_regclass('landintel_store.bgs_borehole_scan_registry') is not null
                    and to_regclass('landintel_store.bgs_borehole_scan_fetch_queue') is not null
                """
            )
        )

    def _record_event(
        self,
        *,
        command_name: str,
        source_key: str,
        status: str,
        proof: dict[str, Any],
    ) -> dict[str, Any]:
        observed_rows = int(
            proof.get("upserted_record_count")
            or proof.get("queued_row_count")
            or proof.get("selected_record_count")
            or proof.get("candidate_asset_count")
            or 0
        )
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
                :command_name,
                :source_key,
                :source_family,
                :status,
                :raw_rows,
                :linked_rows,
                0,
                0,
                0,
                0,
                :summary,
                cast(:metadata as jsonb)
            )
            """,
            {
                "command_name": command_name,
                "source_key": source_key,
                "source_family": SOURCE_FAMILY,
                "status": status,
                "raw_rows": observed_rows,
                "linked_rows": observed_rows,
                "summary": "BGS scan/log source links refreshed or queued without fetching assets.",
                "metadata": _json_dumps(
                    {
                        "registry_batch_size": self.registry_batch_size,
                        "queue_site_batch_size": self.queue_site_batch_size,
                        "queue_max_per_site": self.queue_max_per_site,
                        "queue_max_rows": self.queue_max_rows,
                        "authority_filter": self.authority_filter,
                        "priority_band_filter": self.priority_band_filter,
                        "force_refresh": self.force_refresh,
                        **proof,
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
                :source_scope_key,
                :source_family,
                'BGS borehole scan/log source links',
                :source_name,
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
                "source_scope_key": f"phase_g2:{source_key}",
                "source_family": SOURCE_FAMILY,
                "source_name": "BGS scan/log registry" if source_key == REGISTRY_SOURCE_KEY else "BGS scan/log candidate queue",
                "last_success_at": datetime.now(timezone.utc) if status == "success" else None,
                "freshness_status": "current" if status == "success" else "blocked",
                "live_access_status": "linked_not_downloaded" if status == "success" else str(proof.get("source_blocker")),
                "stale_reason_code": None if status == "success" else proof.get("source_blocker"),
                "check_summary": "BGS scan/log source-link workflow completed without fetching assets."
                if status == "success"
                else str(proof.get("source_blocker")),
                "records_observed": observed_rows,
                "metadata": _json_dumps({"download_assets": False, "ocr": False, **proof}),
            },
        )
        return {
            "message": f"{source_key}_proof",
            "source_key": source_key,
            "source_family": SOURCE_FAMILY,
            "command_name": command_name,
            "status": status,
            "registry_batch_size": self.registry_batch_size,
            "queue_site_batch_size": self.queue_site_batch_size,
            "queue_max_per_site": self.queue_max_per_site,
            "queue_max_rows": self.queue_max_rows,
            "authority_filter": self.authority_filter,
            "priority_band_filter": self.priority_band_filter,
            "force_refresh": self.force_refresh,
            **proof,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh or audit BGS borehole scan/log source-link queue.")
    parser.add_argument(
        "command",
        choices=(
            "refresh-bgs-borehole-scan-registry",
            "queue-bgs-borehole-scans",
            "audit-bgs-borehole-scan-queue",
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    runner = BgsBoreholeScanQueueRunner(database)
    try:
        if args.command == "refresh-bgs-borehole-scan-registry":
            result = runner.refresh_registry()
        elif args.command == "queue-bgs-borehole-scans":
            result = runner.queue_scans()
        else:
            result = runner.audit()

        print(json.dumps(result, default=str, ensure_ascii=False), flush=True)
        logger.info(
            "bgs_borehole_scan_queue_command_completed",
            extra={key: value for key, value in result.items() if key != "message"},
        )
        return 0
    except Exception:
        logger.exception(
            "bgs_borehole_scan_queue_command_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

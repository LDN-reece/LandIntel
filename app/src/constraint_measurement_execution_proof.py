"""Bounded execution proof for priority constraint measurement batches."""

from __future__ import annotations

import argparse
import json
import os
import time
import traceback
from collections import defaultdict
from typing import Any

from config.settings import get_settings
from src.db import Database
from src.logging_config import configure_logging


DEFAULT_MAX_PROOF_PAIR_BATCH_SIZE = 25
ABSOLUTE_MAX_PROOF_PAIR_BATCH_SIZE = 250
DEFAULT_PROOF_PAIR_BATCH_SIZE = 10
DEFAULT_DRAIN_MAX_BATCHES = 5
ABSOLUTE_DRAIN_MAX_BATCHES = 25
DEFAULT_DRAIN_RUNTIME_MINUTES = 35
DEFAULT_LAYER_SITE_BATCH_SIZE = 25
DEFAULT_HEAVY_LAYER_SITE_BATCH_SIZE = 1
DEFAULT_HEAVY_LAYER_KEYS = (
    "naturescot:protectedareas_sac",
    "naturescot:protectedareas_spa",
)
DEFAULT_SOURCE_FAMILY_SITE_PRIORITY_BAND = "title_spend_candidates"
ALLOWED_SOURCE_FAMILY_SITE_PRIORITY_BANDS = {
    "title_spend_candidates",
    "review_queue",
    "ldn_candidate_screen",
}
LOG_RECORD_RESERVED_KEYS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


def _env_int(name: str, default: int) -> int:
    raw_value = str(os.getenv(name) or "").strip()
    if not raw_value:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw_value = str(os.getenv(name) or "").strip()
    if not raw_value:
        return default
    try:
        return float(raw_value)
    except ValueError:
        return default


def _env_text(name: str, default: str = "") -> str:
    return str(os.getenv(name) or default).strip()


def _env_csv_set(name: str, default: tuple[str, ...]) -> set[str]:
    raw_value = os.getenv(name)
    if not raw_value:
        return set(default)
    return {value.strip() for value in raw_value.split(",") if value.strip()}


def _env_csv_list(name: str) -> list[str]:
    raw_value = os.getenv(name)
    if not raw_value:
        return []
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def _env_bool(name: str, default: bool = False) -> bool:
    raw_value = str(os.getenv(name) or "").strip().lower()
    if not raw_value:
        return default
    return raw_value in {"1", "true", "yes", "y", "on"}


def _safe_log_extra(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in LOG_RECORD_RESERVED_KEYS}


def _max_pair_batch_size() -> int:
    """Return the configured hard cap, bounded to a safe absolute ceiling."""

    configured_cap = _env_int("CONSTRAINT_PROOF_MAX_PAIR_BATCH_SIZE", DEFAULT_MAX_PROOF_PAIR_BATCH_SIZE)
    return min(max(configured_cap, 1), ABSOLUTE_MAX_PROOF_PAIR_BATCH_SIZE)


def _bounded_batch_size() -> tuple[int, int, int]:
    requested_batch_size = _env_int("CONSTRAINT_PROOF_PAIR_BATCH_SIZE", DEFAULT_PROOF_PAIR_BATCH_SIZE)
    max_batch_size = _max_pair_batch_size()
    batch_size = min(max(requested_batch_size, 1), max_batch_size)
    return requested_batch_size, batch_size, max_batch_size


def _layer_site_chunks(layer_key: str, site_location_ids: list[str]) -> list[list[str]]:
    normal_batch_size = max(_env_int("CONSTRAINT_PROOF_LAYER_SITE_BATCH_SIZE", DEFAULT_LAYER_SITE_BATCH_SIZE), 1)
    heavy_batch_size = max(
        _env_int("CONSTRAINT_PROOF_HEAVY_LAYER_SITE_BATCH_SIZE", DEFAULT_HEAVY_LAYER_SITE_BATCH_SIZE),
        1,
    )
    heavy_layer_keys = _env_csv_set("CONSTRAINT_PROOF_HEAVY_LAYER_KEYS", DEFAULT_HEAVY_LAYER_KEYS)
    batch_size = min(normal_batch_size, heavy_batch_size) if layer_key in heavy_layer_keys else normal_batch_size
    return [site_location_ids[offset : offset + batch_size] for offset in range(0, len(site_location_ids), batch_size)]


def _classify_layer_site_relationship_candidates(
    database: Database,
    *,
    layer_key: str,
    site_location_ids: list[str],
) -> list[dict[str, Any]]:
    """Split requested sites into exact spatial candidates and safe no-hit rows.

    The full finalizer is intentionally rich, but it is too expensive for large
    no-hit layers such as NatureScot SAC/SPA. A site can only skip the finalizer
    when the exact spatial prefilter finds no intersecting/within-buffer source
    feature and there is no existing measurement or summary to clean up.
    """

    if not site_location_ids:
        return []
    return database.fetch_all(
        """
        with layer_row as (
            select id, layer_key, constraint_group, buffer_distance_m
            from public.constraint_layer_registry
            where layer_key = :layer_key
              and is_active = true
        ),
        requested_sites as (
            select distinct input.site_location_id
            from unnest(cast(:site_location_ids as text[])) as input(site_location_id)
        ),
        site_batch as (
            select
                site.id::text as site_id,
                site.id::text as site_location_id,
                site.geometry
            from landintel.canonical_sites as site
            join requested_sites as requested
              on requested.site_location_id = site.id::text
            where site.geometry is not null
        ),
        classified as (
            select
                site_batch.site_location_id,
                exists (
                    select 1
                    from public.constraint_source_features as feature
                    where feature.constraint_layer_id = layer_row.id
                      and feature.geometry is not null
                      and feature.geometry OPERATOR(extensions.&&)
                            st_expand(site_batch.geometry, greatest(layer_row.buffer_distance_m, 0)::double precision)
                      and (
                            (
                                layer_row.buffer_distance_m > 0
                                and st_dwithin(site_batch.geometry, feature.geometry, layer_row.buffer_distance_m)
                            )
                            or (
                                layer_row.buffer_distance_m = 0
                                and st_intersects(site_batch.geometry, feature.geometry)
                            )
                          )
                ) as has_exact_spatial_candidate,
                exists (
                    select 1
                    from public.site_constraint_measurements as measurement
                    where measurement.constraint_layer_id = layer_row.id
                      and measurement.site_location_id = site_batch.site_location_id
                ) as has_existing_measurement,
                exists (
                    select 1
                    from public.site_constraint_group_summaries as summary
                    where summary.constraint_layer_id = layer_row.id
                      and summary.site_location_id = site_batch.site_location_id
                ) as has_existing_summary
            from site_batch
            cross join layer_row
        )
        select
            site_location_id,
            has_exact_spatial_candidate,
            has_existing_measurement,
            has_existing_summary,
            (
                has_exact_spatial_candidate
                or has_existing_measurement
                or has_existing_summary
            ) as requires_full_finalizer
        from classified
        order by site_location_id
        """,
        {
            "layer_key": layer_key,
            "site_location_ids": site_location_ids,
        },
    )


def _mark_no_hit_scan_state(
    database: Database,
    *,
    layer_key: str,
    site_location_ids: list[str],
) -> dict[str, Any]:
    if not site_location_ids:
        return {"no_hit_scan_state_count": 0}
    return database.fetch_one(
        """
        with layer_row as (
            select id, layer_key, constraint_group
            from public.constraint_layer_registry
            where layer_key = :layer_key
              and is_active = true
        ),
        requested_sites as (
            select distinct input.site_location_id
            from unnest(cast(:site_location_ids as text[])) as input(site_location_id)
        ),
        anchor as (
            select
                site.id::text as site_id,
                site.id::text as site_location_id
            from landintel.canonical_sites as site
            join requested_sites as requested
              on requested.site_location_id = site.id::text
            where site.geometry is not null
        ),
        upserted as (
            insert into public.site_constraint_measurement_scan_state (
                site_id,
                site_location_id,
                constraint_layer_id,
                scan_scope,
                latest_measurement_count,
                latest_summary_signature,
                has_constraint_relationship,
                scanned_at,
                updated_at,
                metadata
            )
            select
                anchor.site_id,
                anchor.site_location_id,
                layer_row.id,
                'canonical_site_geometry',
                0,
                null,
                false,
                now(),
                now(),
                jsonb_build_object(
                    'constraint_layer_key', layer_row.layer_key,
                    'constraint_group', layer_row.constraint_group,
                    'source_expansion_constraint', true,
                    'has_constraint_relationship', false,
                    'measurement_method', 'exact_spatial_no_hit_prefilter',
                    'source_limitation', 'no_exact_intersect_or_buffer_candidate_found'
                )
            from anchor
            cross join layer_row
            on conflict (constraint_layer_id, site_location_id, scan_scope)
            do update set
                site_id = excluded.site_id,
                latest_measurement_count = excluded.latest_measurement_count,
                latest_summary_signature = excluded.latest_summary_signature,
                has_constraint_relationship = excluded.has_constraint_relationship,
                scanned_at = excluded.scanned_at,
                updated_at = now(),
                metadata = excluded.metadata
            returning site_location_id
        )
        select count(*)::integer as no_hit_scan_state_count
        from upserted
        """,
        {
            "layer_key": layer_key,
            "site_location_ids": site_location_ids,
        },
    ) or {"no_hit_scan_state_count": 0}


def _measure_layer_site_chunks(
    database: Database,
    *,
    layer_key: str,
    site_location_ids: list[str],
    overlap_delta_pct: float,
    distance_delta_m: float,
    errors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    measurement_results: list[dict[str, Any]] = []
    classified_rows = _classify_layer_site_relationship_candidates(
        database,
        layer_key=layer_key,
        site_location_ids=site_location_ids,
    )
    classified_by_site = {str(row["site_location_id"]): row for row in classified_rows}
    no_hit_site_ids = [
        site_location_id
        for site_location_id, row in classified_by_site.items()
        if not row.get("requires_full_finalizer")
    ]
    finalizer_site_ids = [
        site_location_id
        for site_location_id in site_location_ids
        if site_location_id not in classified_by_site
        or classified_by_site[site_location_id].get("requires_full_finalizer")
    ]
    if no_hit_site_ids:
        no_hit_result = _mark_no_hit_scan_state(
            database,
            layer_key=layer_key,
            site_location_ids=no_hit_site_ids,
        )
        measurement_results.append(
            {
                "layer_key": layer_key,
                "site_location_count": len(no_hit_site_ids),
                "measurement_count": 0,
                "summary_count": 0,
                "friction_fact_count": 0,
                "evidence_count": 0,
                "signal_count": 0,
                "change_event_count": 0,
                "affected_site_count": 0,
                "material_change_count": 0,
                "no_hit_scan_state_count": no_hit_result.get("no_hit_scan_state_count", 0),
                "measurement_method": "exact_spatial_no_hit_prefilter",
            }
        )
    def run_finalizer_chunk(
        site_location_chunk: list[str],
        *,
        chunk_index: int,
        chunk_count: int,
        measurement_method: str | None = None,
    ) -> dict[str, Any]:
        result = database.fetch_one(
            """
            select *
            from public.refresh_constraint_measurements_for_layer_sites(
                :layer_key,
                cast(:site_location_ids as text[]),
                cast(:overlap_delta_pct as numeric),
                cast(:distance_delta_m as numeric)
            )
            """,
            {
                "layer_key": layer_key,
                "site_location_ids": site_location_chunk,
                "overlap_delta_pct": overlap_delta_pct,
                "distance_delta_m": distance_delta_m,
            },
        ) or {}
        result = dict(result)
        result.setdefault("layer_key", layer_key)
        result.setdefault("site_location_count", len(site_location_chunk))
        result.setdefault("chunk_index", chunk_index)
        result.setdefault("chunk_count", chunk_count)
        if measurement_method:
            result.setdefault("measurement_method", measurement_method)
        return result

    chunks = _layer_site_chunks(layer_key, finalizer_site_ids)
    for chunk_index, site_location_chunk in enumerate(chunks, start=1):
        try:
            measurement_results.append(
                run_finalizer_chunk(
                    site_location_chunk,
                    chunk_index=chunk_index,
                    chunk_count=len(chunks),
                )
            )
        except Exception as exc:
            if len(site_location_chunk) <= 1:
                errors.append(
                    {
                        "layer_key": layer_key,
                        "site_location_count": len(site_location_chunk),
                        "chunk_index": chunk_index,
                        "chunk_count": len(chunks),
                        "error": str(exc)[:1000],
                    }
                )
                continue

            retry_errors: list[dict[str, Any]] = []
            for retry_index, site_location_id in enumerate(site_location_chunk, start=1):
                try:
                    retry_result = run_finalizer_chunk(
                        [site_location_id],
                        chunk_index=chunk_index,
                        chunk_count=len(chunks),
                        measurement_method="single_site_retry_after_chunk_timeout",
                    )
                    retry_result.setdefault("parent_chunk_site_count", len(site_location_chunk))
                    retry_result.setdefault("retry_index", retry_index)
                    measurement_results.append(retry_result)
                except Exception as retry_exc:
                    retry_errors.append(
                        {
                            "layer_key": layer_key,
                            "site_location_count": 1,
                            "site_location_id": site_location_id,
                            "chunk_index": chunk_index,
                            "chunk_count": len(chunks),
                            "retry_index": retry_index,
                            "parent_chunk_site_count": len(site_location_chunk),
                            "parent_chunk_error": str(exc)[:500],
                            "error": str(retry_exc)[:1000],
                        }
                    )
            errors.extend(retry_errors)
    return measurement_results


def _collect_flood_title_spend_counts(database: Database) -> dict[str, Any]:
    return database.fetch_one(
        """
        with title_sites as (
            select distinct priority_sites.site_location_id
            from landintel_reporting.v_constraint_priority_sites as priority_sites
            where priority_sites.site_priority_band = 'title_spend_candidates'
        ), flood_layers as (
            select priority_layers.constraint_layer_id::uuid as constraint_layer_id
            from landintel_reporting.v_constraint_priority_layers as priority_layers
            where priority_layers.constraint_priority_family = 'flood'
              and priority_layers.is_active = true
              and exists (
                  select 1
                  from public.constraint_source_features as feature
                  where feature.constraint_layer_id = priority_layers.constraint_layer_id::uuid
              )
        )
        select
            (select count(*)::integer from title_sites) as candidate_sites_in_cohort,
            (select count(*)::integer from flood_layers) as flood_layer_count,
            (
                select count(distinct measurement.site_location_id)::integer
                from public.site_constraint_measurements as measurement
                join title_sites
                  on title_sites.site_location_id = measurement.site_location_id
                join flood_layers
                  on flood_layers.constraint_layer_id = measurement.constraint_layer_id
            ) as sites_with_measurements,
            (
                select count(*)::integer
                from public.site_constraint_measurements as measurement
                join title_sites
                  on title_sites.site_location_id = measurement.site_location_id
                join flood_layers
                  on flood_layers.constraint_layer_id = measurement.constraint_layer_id
            ) as measurement_rows,
            (
                select count(distinct scan_state.site_location_id)::integer
                from public.site_constraint_measurement_scan_state as scan_state
                join title_sites
                  on title_sites.site_location_id = scan_state.site_location_id
                join flood_layers
                  on flood_layers.constraint_layer_id = scan_state.constraint_layer_id
                where scan_state.scan_scope = 'canonical_site_geometry'
            ) as sites_with_scan_state,
            (
                select count(*)::integer
                from public.site_constraint_measurement_scan_state as scan_state
                join title_sites
                  on title_sites.site_location_id = scan_state.site_location_id
                join flood_layers
                  on flood_layers.constraint_layer_id = scan_state.constraint_layer_id
                where scan_state.scan_scope = 'canonical_site_geometry'
            ) as scan_state_rows
        """
    ) or {}


def _collect_source_family_counts(
    database: Database,
    *,
    site_priority_band: str,
    source_family: str | None,
    layer_key: str | None,
    excluded_layer_keys: list[str] | None = None,
) -> dict[str, Any]:
    excluded_layer_keys_csv = ",".join(excluded_layer_keys or [])
    return database.fetch_one(
        """
        with priority_sites as (
            select distinct priority_sites.site_location_id
            from landintel_reporting.v_constraint_priority_sites as priority_sites
            where priority_sites.site_priority_band = :site_priority_band
        ), filtered_layers as (
            select priority_layers.constraint_layer_id::uuid as constraint_layer_id
            from landintel_reporting.v_constraint_priority_layers as priority_layers
            where priority_layers.is_active = true
              and (:source_family = '' or priority_layers.source_family = :source_family)
              and (:layer_key = '' or priority_layers.layer_key = :layer_key)
              and (
                  :excluded_layer_keys_csv = ''
                  or priority_layers.layer_key <> all(string_to_array(:excluded_layer_keys_csv, ','))
              )
              and exists (
                  select 1
                  from public.constraint_source_features as feature
                  where feature.constraint_layer_id = priority_layers.constraint_layer_id::uuid
              )
        )
        select
            (select count(*)::integer from priority_sites) as candidate_sites_in_cohort,
            (select count(*)::integer from filtered_layers) as filtered_layer_count,
            (
                select count(distinct measurement.site_location_id)::integer
                from public.site_constraint_measurements as measurement
                join priority_sites
                  on priority_sites.site_location_id = measurement.site_location_id
                join filtered_layers
                  on filtered_layers.constraint_layer_id = measurement.constraint_layer_id
            ) as sites_with_measurements,
            (
                select count(*)::integer
                from public.site_constraint_measurements as measurement
                join priority_sites
                  on priority_sites.site_location_id = measurement.site_location_id
                join filtered_layers
                  on filtered_layers.constraint_layer_id = measurement.constraint_layer_id
            ) as measurement_rows,
            (
                select count(distinct scan_state.site_location_id)::integer
                from public.site_constraint_measurement_scan_state as scan_state
                join priority_sites
                  on priority_sites.site_location_id = scan_state.site_location_id
                join filtered_layers
                  on filtered_layers.constraint_layer_id = scan_state.constraint_layer_id
                where scan_state.scan_scope = 'canonical_site_geometry'
            ) as sites_with_scan_state,
            (
                select count(*)::integer
                from public.site_constraint_measurement_scan_state as scan_state
                join priority_sites
                  on priority_sites.site_location_id = scan_state.site_location_id
                join filtered_layers
                  on filtered_layers.constraint_layer_id = scan_state.constraint_layer_id
                where scan_state.scan_scope = 'canonical_site_geometry'
            ) as scan_state_rows
        """,
        {
            "site_priority_band": site_priority_band,
            "source_family": source_family or "",
            "layer_key": layer_key or "",
            "excluded_layer_keys_csv": excluded_layer_keys_csv,
        },
    ) or {}


def _candidate_pairs(database: Database, batch_size: int) -> list[dict[str, Any]]:
    return database.fetch_all(
        """
        with priority_sites as (
            select
                priority_sites.canonical_site_id,
                priority_sites.site_location_id,
                priority_sites.site_label,
                priority_sites.authority_name,
                priority_sites.area_acres,
                priority_sites.site_priority_rank,
                priority_sites.site_priority_band
            from landintel_reporting.v_constraint_priority_sites as priority_sites
            where priority_sites.site_priority_band = 'title_spend_candidates'
        ),
        active_layers as (
            select
                priority_layers.constraint_layer_id::uuid as constraint_layer_id,
                priority_layers.layer_key,
                priority_layers.layer_name,
                priority_layers.source_family,
                priority_layers.constraint_group,
                priority_layers.constraint_priority_rank,
                priority_layers.constraint_priority_family
            from landintel_reporting.v_constraint_priority_layers as priority_layers
            where priority_layers.is_active = true
              and priority_layers.constraint_priority_family = 'flood'
              and exists (
                  select 1
                  from public.constraint_source_features as feature
                  where feature.constraint_layer_id = priority_layers.constraint_layer_id::uuid
              )
        ),
        candidate_pairs as (
            select
                priority_sites.canonical_site_id,
                priority_sites.site_location_id,
                priority_sites.site_label,
                priority_sites.authority_name,
                priority_sites.area_acres,
                priority_sites.site_priority_rank,
                priority_sites.site_priority_band,
                active_layers.constraint_layer_id,
                active_layers.layer_key,
                active_layers.layer_name,
                active_layers.source_family,
                active_layers.constraint_group,
                active_layers.constraint_priority_rank,
                active_layers.constraint_priority_family
            from priority_sites
            cross join active_layers
            left join public.site_constraint_measurement_scan_state as scan_state
              on scan_state.site_location_id = priority_sites.site_location_id
             and scan_state.constraint_layer_id = active_layers.constraint_layer_id
             and scan_state.scan_scope = 'canonical_site_geometry'
            where scan_state.id is null
        )
        select
            row_number() over (
                order by
                    candidate_pairs.site_priority_rank,
                    candidate_pairs.constraint_priority_rank,
                    candidate_pairs.authority_name nulls last,
                    candidate_pairs.area_acres desc nulls last,
                    candidate_pairs.site_location_id,
                    candidate_pairs.layer_key
            ) as queue_rank,
            canonical_site_id::text as canonical_site_id,
            site_location_id,
            site_label,
            authority_name,
            area_acres,
            layer_key,
            constraint_priority_family,
            site_priority_band
        from candidate_pairs
        order by queue_rank
        limit :batch_size
        """,
        {"batch_size": batch_size},
    )


def _source_family_candidate_pairs(
    database: Database,
    *,
    site_priority_band: str,
    source_family: str | None,
    layer_key: str | None,
    batch_size: int,
    excluded_layer_keys: list[str] | None = None,
) -> list[dict[str, Any]]:
    excluded_layer_keys_csv = ",".join(excluded_layer_keys or [])
    return database.fetch_all(
        """
        with priority_sites as (
            select
                priority_sites.canonical_site_id,
                priority_sites.site_location_id,
                priority_sites.site_label,
                priority_sites.authority_name,
                priority_sites.area_acres,
                priority_sites.site_priority_rank,
                priority_sites.site_priority_band
            from landintel_reporting.v_constraint_priority_sites as priority_sites
            where priority_sites.site_priority_band = :site_priority_band
        ),
        active_layers as (
            select
                priority_layers.constraint_layer_id::uuid as constraint_layer_id,
                priority_layers.layer_key,
                priority_layers.layer_name,
                priority_layers.source_family,
                priority_layers.constraint_group,
                priority_layers.constraint_priority_rank,
                priority_layers.constraint_priority_family
            from landintel_reporting.v_constraint_priority_layers as priority_layers
            where priority_layers.is_active = true
              and priority_layers.constraint_priority_rank <= 8
              and (:source_family = '' or priority_layers.source_family = :source_family)
              and (:layer_key = '' or priority_layers.layer_key = :layer_key)
              and (
                  :excluded_layer_keys_csv = ''
                  or priority_layers.layer_key <> all(string_to_array(:excluded_layer_keys_csv, ','))
              )
              and exists (
                  select 1
                  from public.constraint_source_features as feature
                  where feature.constraint_layer_id = priority_layers.constraint_layer_id::uuid
              )
        ),
        candidate_pairs as (
            select
                priority_sites.canonical_site_id,
                priority_sites.site_location_id,
                priority_sites.site_label,
                priority_sites.authority_name,
                priority_sites.area_acres,
                priority_sites.site_priority_rank,
                priority_sites.site_priority_band,
                active_layers.constraint_layer_id,
                active_layers.layer_key,
                active_layers.layer_name,
                active_layers.source_family,
                active_layers.constraint_group,
                active_layers.constraint_priority_rank,
                active_layers.constraint_priority_family
            from priority_sites
            cross join active_layers
            left join public.site_constraint_measurement_scan_state as scan_state
              on scan_state.site_location_id = priority_sites.site_location_id
             and scan_state.constraint_layer_id = active_layers.constraint_layer_id
             and scan_state.scan_scope = 'canonical_site_geometry'
            where scan_state.id is null
        )
        select
            row_number() over (
                order by
                    candidate_pairs.site_priority_rank,
                    candidate_pairs.constraint_priority_rank,
                    candidate_pairs.authority_name nulls last,
                    candidate_pairs.area_acres desc nulls last,
                    candidate_pairs.site_location_id,
                    candidate_pairs.layer_key
            ) as queue_rank,
            canonical_site_id::text as canonical_site_id,
            site_location_id,
            site_label,
            authority_name,
            area_acres,
            layer_key,
            layer_name,
            source_family,
            constraint_group,
            constraint_priority_family,
            site_priority_band
        from candidate_pairs
        order by queue_rank
        limit :batch_size
        """,
        {
            "site_priority_band": site_priority_band,
            "source_family": source_family or "",
            "layer_key": layer_key or "",
            "batch_size": batch_size,
            "excluded_layer_keys_csv": excluded_layer_keys_csv,
        },
    )


def run_flood_title_spend_measurement_proof(database: Database) -> dict[str, Any]:
    """Run one tiny flood-only title-spend measurement batch through the existing finalizer."""

    started_at = time.monotonic()
    requested_batch_size, batch_size, max_batch_size = _bounded_batch_size()
    overlap_delta_pct = _env_float("CONSTRAINT_MATERIAL_OVERLAP_DELTA_PCT", 1.0)
    distance_delta_m = _env_float("CONSTRAINT_MATERIAL_DISTANCE_DELTA_M", 25.0)

    before_counts = _collect_flood_title_spend_counts(database)
    pairs = _candidate_pairs(database, batch_size)
    pairs_by_layer: dict[str, list[str]] = defaultdict(list)
    for pair in pairs:
        site_location_id = str(pair["site_location_id"])
        layer_key = str(pair["layer_key"])
        if site_location_id not in pairs_by_layer[layer_key]:
            pairs_by_layer[layer_key].append(site_location_id)

    measurement_results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for layer_key, site_location_ids in sorted(pairs_by_layer.items()):
        measurement_results.extend(
            _measure_layer_site_chunks(
                database,
                layer_key=layer_key,
                site_location_ids=site_location_ids,
                overlap_delta_pct=overlap_delta_pct,
                distance_delta_m=distance_delta_m,
                errors=errors,
            )
        )

    after_counts = _collect_flood_title_spend_counts(database)
    unique_sites = {str(pair["site_location_id"]) for pair in pairs}
    runtime_seconds = round(time.monotonic() - started_at, 3)
    return {
        "message": "constraint_measurement_proof_flood_title_spend",
        "command": "constraint-measurement-proof-flood-title-spend",
        "cohort": "title_spend_candidates",
        "constraint_priority_family": "flood",
        "requested_pair_batch_size": requested_batch_size,
        "effective_pair_batch_size": batch_size,
        "hard_pair_batch_cap": max_batch_size,
        "absolute_pair_batch_cap": ABSOLUTE_MAX_PROOF_PAIR_BATCH_SIZE,
        "candidate_sites_in_cohort": before_counts.get("candidate_sites_in_cohort", 0),
        "flood_layer_count": before_counts.get("flood_layer_count", 0),
        "candidate_site_layer_pairs_selected": len(pairs),
        "candidate_sites_selected": len(unique_sites),
        "candidate_layers_selected": len(pairs_by_layer),
        "sites_with_measurements_before": before_counts.get("sites_with_measurements", 0),
        "measurement_rows_before": before_counts.get("measurement_rows", 0),
        "sites_with_scan_state_before": before_counts.get("sites_with_scan_state", 0),
        "scan_state_rows_before": before_counts.get("scan_state_rows", 0),
        "sites_measured_in_run": len(unique_sites),
        "site_layer_pairs_processed": len(pairs),
        "measurement_results": measurement_results,
        "sites_with_measurements_after": after_counts.get("sites_with_measurements", 0),
        "measurement_rows_after": after_counts.get("measurement_rows", 0),
        "sites_with_scan_state_after": after_counts.get("sites_with_scan_state", 0),
        "scan_state_rows_after": after_counts.get("scan_state_rows", 0),
        "errors": errors,
        "runtime_seconds": runtime_seconds,
        "safety_caveat": (
            "Bounded flood-only title_spend_candidates proof. Uses existing "
            "public.refresh_constraint_measurements_for_layer_sites finalizer and scan-state logic; "
            "does not run broad all-layer or wider canonical scans."
        ),
    }


def run_title_spend_source_family_measurement_proof(database: Database) -> dict[str, Any]:
    """Run a tiny title-spend batch for one explicitly filtered constraint source family."""

    started_at = time.monotonic()
    requested_batch_size, batch_size, max_batch_size = _bounded_batch_size()
    overlap_delta_pct = _env_float("CONSTRAINT_MATERIAL_OVERLAP_DELTA_PCT", 1.0)
    distance_delta_m = _env_float("CONSTRAINT_MATERIAL_DISTANCE_DELTA_M", 25.0)
    site_priority_band = _env_text(
        "CONSTRAINT_PROOF_SITE_PRIORITY_BAND",
        DEFAULT_SOURCE_FAMILY_SITE_PRIORITY_BAND,
    )
    source_family = _env_text("CONSTRAINT_MEASURE_SOURCE_FAMILY") or None
    layer_key = _env_text("CONSTRAINT_MEASURE_LAYER_KEY") or None
    excluded_layer_keys = _env_csv_list("CONSTRAINT_MEASURE_EXCLUDE_LAYER_KEYS")

    if site_priority_band not in ALLOWED_SOURCE_FAMILY_SITE_PRIORITY_BANDS:
        raise ValueError(
            "CONSTRAINT_PROOF_SITE_PRIORITY_BAND must be one of "
            f"{sorted(ALLOWED_SOURCE_FAMILY_SITE_PRIORITY_BANDS)}"
        )
    if not source_family and not layer_key:
        raise ValueError(
            "constraint-measurement-proof-title-spend-source-family requires source family or layer key via "
            "CONSTRAINT_MEASURE_SOURCE_FAMILY or CONSTRAINT_MEASURE_LAYER_KEY. "
            "This guard prevents broad all-layer runs."
        )

    before_counts = _collect_source_family_counts(
        database,
        site_priority_band=site_priority_band,
        source_family=source_family,
        layer_key=layer_key,
        excluded_layer_keys=excluded_layer_keys,
    )
    pairs = _source_family_candidate_pairs(
        database,
        site_priority_band=site_priority_band,
        source_family=source_family,
        layer_key=layer_key,
        batch_size=batch_size,
        excluded_layer_keys=excluded_layer_keys,
    )
    pairs_by_layer: dict[str, list[str]] = defaultdict(list)
    for pair in pairs:
        site_location_id = str(pair["site_location_id"])
        pair_layer_key = str(pair["layer_key"])
        if site_location_id not in pairs_by_layer[pair_layer_key]:
            pairs_by_layer[pair_layer_key].append(site_location_id)

    measurement_results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for pair_layer_key, site_location_ids in sorted(pairs_by_layer.items()):
        measurement_results.extend(
            _measure_layer_site_chunks(
                database,
                layer_key=pair_layer_key,
                site_location_ids=site_location_ids,
                overlap_delta_pct=overlap_delta_pct,
                distance_delta_m=distance_delta_m,
                errors=errors,
            )
        )

    after_counts = _collect_source_family_counts(
        database,
        site_priority_band=site_priority_band,
        source_family=source_family,
        layer_key=layer_key,
        excluded_layer_keys=excluded_layer_keys,
    )
    unique_sites = {str(pair["site_location_id"]) for pair in pairs}
    unique_source_families = sorted({str(pair["source_family"]) for pair in pairs if pair.get("source_family")})
    unique_priority_families = sorted(
        {str(pair["constraint_priority_family"]) for pair in pairs if pair.get("constraint_priority_family")}
    )
    runtime_seconds = round(time.monotonic() - started_at, 3)
    return {
        "message": "constraint_measurement_proof_title_spend_source_family",
        "command": "constraint-measurement-proof-title-spend-source-family",
        "cohort": site_priority_band,
        "constraint_source_family": source_family,
        "constraint_layer_key": layer_key,
        "excluded_layer_keys": excluded_layer_keys,
        "constraint_source_families_selected": unique_source_families,
        "constraint_priority_families_selected": unique_priority_families,
        "requested_pair_batch_size": requested_batch_size,
        "effective_pair_batch_size": batch_size,
        "hard_pair_batch_cap": max_batch_size,
        "absolute_pair_batch_cap": ABSOLUTE_MAX_PROOF_PAIR_BATCH_SIZE,
        "candidate_sites_in_cohort": before_counts.get("candidate_sites_in_cohort", 0),
        "filtered_layer_count": before_counts.get("filtered_layer_count", 0),
        "candidate_site_layer_pairs_selected": len(pairs),
        "candidate_sites_selected": len(unique_sites),
        "candidate_layers_selected": len(pairs_by_layer),
        "sites_with_measurements_before": before_counts.get("sites_with_measurements", 0),
        "measurement_rows_before": before_counts.get("measurement_rows", 0),
        "sites_with_scan_state_before": before_counts.get("sites_with_scan_state", 0),
        "scan_state_rows_before": before_counts.get("scan_state_rows", 0),
        "sites_measured_in_run": len(unique_sites),
        "site_layer_pairs_processed": len(pairs),
        "measurement_results": measurement_results,
        "sites_with_measurements_after": after_counts.get("sites_with_measurements", 0),
        "measurement_rows_after": after_counts.get("measurement_rows", 0),
        "sites_with_scan_state_after": after_counts.get("sites_with_scan_state", 0),
        "scan_state_rows_after": after_counts.get("scan_state_rows", 0),
        "errors": errors,
        "runtime_seconds": runtime_seconds,
        "safety_caveat": (
            "Bounded title_spend_candidates source-family proof. Requires an explicit "
            "CONSTRAINT_MEASURE_SOURCE_FAMILY or CONSTRAINT_MEASURE_LAYER_KEY filter, uses the existing "
            "public.refresh_constraint_measurements_for_layer_sites finalizer and scan-state logic, and "
            "does not run broad all-layer or wider canonical scans."
        ),
    }


def run_source_family_measurement_drain(database: Database) -> dict[str, Any]:
    """Run repeated bounded source-family batches inside one guarded workflow run."""

    started_at = time.monotonic()
    max_batches = min(
        max(_env_int("CONSTRAINT_PROOF_DRAIN_MAX_BATCHES", DEFAULT_DRAIN_MAX_BATCHES), 1),
        ABSOLUTE_DRAIN_MAX_BATCHES,
    )
    runtime_minutes = max(_env_int("CONSTRAINT_PROOF_DRAIN_RUNTIME_MINUTES", DEFAULT_DRAIN_RUNTIME_MINUTES), 1)
    max_runtime_seconds = runtime_minutes * 60
    site_priority_band = _env_text(
        "CONSTRAINT_PROOF_SITE_PRIORITY_BAND",
        DEFAULT_SOURCE_FAMILY_SITE_PRIORITY_BAND,
    )
    source_family = _env_text("CONSTRAINT_MEASURE_SOURCE_FAMILY") or None
    layer_key = _env_text("CONSTRAINT_MEASURE_LAYER_KEY") or None
    excluded_layer_keys = _env_csv_list("CONSTRAINT_MEASURE_EXCLUDE_LAYER_KEYS")

    if site_priority_band not in ALLOWED_SOURCE_FAMILY_SITE_PRIORITY_BANDS:
        raise ValueError(
            "CONSTRAINT_PROOF_SITE_PRIORITY_BAND must be one of "
            f"{sorted(ALLOWED_SOURCE_FAMILY_SITE_PRIORITY_BANDS)}"
        )
    if not source_family and not layer_key:
        raise ValueError(
            "constraint-measurement-drain-source-family requires source family or layer key via "
            "CONSTRAINT_MEASURE_SOURCE_FAMILY or CONSTRAINT_MEASURE_LAYER_KEY. "
            "This guard prevents broad all-layer runs."
        )

    before_counts = _collect_source_family_counts(
        database,
        site_priority_band=site_priority_band,
        source_family=source_family,
        layer_key=layer_key,
        excluded_layer_keys=excluded_layer_keys,
    )
    batch_results: list[dict[str, Any]] = []
    total_pairs_processed = 0
    total_candidate_sites_selected = 0
    total_errors: list[dict[str, Any]] = []
    exhausted = False
    stopped_for_runtime = False

    for batch_index in range(1, max_batches + 1):
        elapsed_seconds = time.monotonic() - started_at
        if elapsed_seconds >= max_runtime_seconds:
            stopped_for_runtime = True
            break
        proof = run_title_spend_source_family_measurement_proof(database)
        candidate_pairs = int(proof.get("candidate_site_layer_pairs_selected") or 0)
        processed_pairs = int(proof.get("site_layer_pairs_processed") or 0)
        errors = list(proof.get("errors") or [])
        total_pairs_processed += processed_pairs
        total_candidate_sites_selected += int(proof.get("candidate_sites_selected") or 0)
        total_errors.extend(errors)
        batch_results.append(
            {
                "batch_index": batch_index,
                "candidate_site_layer_pairs_selected": candidate_pairs,
                "site_layer_pairs_processed": processed_pairs,
                "candidate_sites_selected": proof.get("candidate_sites_selected", 0),
                "candidate_layers_selected": proof.get("candidate_layers_selected", 0),
                "constraint_source_families_selected": proof.get("constraint_source_families_selected", []),
                "constraint_priority_families_selected": proof.get("constraint_priority_families_selected", []),
                "measurement_rows_before": proof.get("measurement_rows_before", 0),
                "measurement_rows_after": proof.get("measurement_rows_after", 0),
                "scan_state_rows_before": proof.get("scan_state_rows_before", 0),
                "scan_state_rows_after": proof.get("scan_state_rows_after", 0),
                "runtime_seconds": proof.get("runtime_seconds", 0),
                "error_count": len(errors),
            }
        )
        if errors:
            break
        if candidate_pairs == 0 or processed_pairs == 0:
            exhausted = True
            break

    after_counts = _collect_source_family_counts(
        database,
        site_priority_band=site_priority_band,
        source_family=source_family,
        layer_key=layer_key,
        excluded_layer_keys=excluded_layer_keys,
    )
    runtime_seconds = round(time.monotonic() - started_at, 3)
    return {
        "message": "constraint_measurement_drain_source_family",
        "command": "constraint-measurement-drain-source-family",
        "cohort": site_priority_band,
        "constraint_source_family": source_family,
        "constraint_layer_key": layer_key,
        "excluded_layer_keys": excluded_layer_keys,
        "max_batches": max_batches,
        "max_runtime_minutes": runtime_minutes,
        "batches_attempted": len(batch_results),
        "site_layer_pairs_processed": total_pairs_processed,
        "candidate_sites_selected_total": total_candidate_sites_selected,
        "candidate_sites_in_cohort": before_counts.get("candidate_sites_in_cohort", 0),
        "filtered_layer_count": before_counts.get("filtered_layer_count", 0),
        "measurement_rows_before": before_counts.get("measurement_rows", 0),
        "scan_state_rows_before": before_counts.get("scan_state_rows", 0),
        "measurement_rows_after": after_counts.get("measurement_rows", 0),
        "scan_state_rows_after": after_counts.get("scan_state_rows", 0),
        "exhausted": exhausted,
        "stopped_for_runtime": stopped_for_runtime,
        "batch_results": batch_results,
        "errors": total_errors,
        "runtime_seconds": runtime_seconds,
        "safety_caveat": (
            "Bounded source-family drain. Requires explicit CONSTRAINT_MEASURE_SOURCE_FAMILY "
            "or CONSTRAINT_MEASURE_LAYER_KEY and an allowed site priority band. Reuses the existing "
            "public.refresh_constraint_measurements_for_layer_sites finalizer and scan-state logic; "
            "does not run broad all-layer or wider canonical scans."
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run bounded constraint measurement execution proof commands.")
    parser.add_argument(
        "command",
        choices=(
            "constraint-measurement-proof-flood-title-spend",
            "constraint-measurement-proof-title-spend-source-family",
            "constraint-measurement-drain-source-family",
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    database = Database(settings)
    try:
        if args.command == "constraint-measurement-proof-title-spend-source-family":
            proof = run_title_spend_source_family_measurement_proof(database)
        elif args.command == "constraint-measurement-drain-source-family":
            proof = run_source_family_measurement_drain(database)
        else:
            proof = run_flood_title_spend_measurement_proof(database)
        print(json.dumps(proof, default=str, ensure_ascii=False), flush=True)
        if proof["errors"]:
            logger.warning("constraint_measurement_proof_completed_with_errors", extra=_safe_log_extra(proof))
            return 0 if _env_bool("CONSTRAINT_PROOF_ALLOW_LAYER_ERRORS") else 1
        logger.info("constraint_measurement_proof_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception(
            "constraint_measurement_proof_failed",
            extra={"command": args.command, "traceback": traceback.format_exc()},
        )
        return 1
    finally:
        database.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

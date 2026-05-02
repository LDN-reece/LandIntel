"""Batched catch-up scan runner for incremental reconcile queue seeding."""

from __future__ import annotations

import argparse
import traceback
from time import monotonic
from typing import Any

from config.settings import get_settings
from src.logging_config import configure_logging
from src.models.ingest_runs import IngestRunRecord, IngestRunUpdate
from src.source_reconcile_incremental import HLA_DATASET, PLANNING_DATASET, IncrementalReconcileRunner


class IncrementalReconcileCatchupRunner(IncrementalReconcileRunner):
    """Seed reconcile work in bounded batches across the full live source tables."""

    def reconcile_catchup_scan(
        self,
        *,
        source_family: str | None = None,
        runtime_minutes: int | None = None,
        batch_limit: int | None = None,
    ) -> dict[str, int]:
        effective_runtime_minutes = runtime_minutes or self.settings.reconcile_runtime_minutes
        effective_batch_limit = max(1, min(batch_limit or self.settings.reconcile_queue_batch_limit, 200))
        run_id = self.loader.create_ingest_run(
            IngestRunRecord(
                run_type="reconcile_catchup_scan",
                source_name="landintel.source_reconcile_state",
                status="running",
                metadata={
                    "source_family": source_family,
                    "runtime_minutes": effective_runtime_minutes,
                    "batch_limit": effective_batch_limit,
                },
            )
        )
        started = monotonic()
        try:
            result = {
                "planning_seeded": 0,
                "planning_retired": 0,
                "hla_seeded": 0,
                "hla_retired": 0,
            }
            if source_family in (None, "planning") and not self._runtime_limit_reached(started, effective_runtime_minutes):
                planning_counts = self._seed_family_scan(
                    source_family="planning",
                    source_dataset=PLANNING_DATASET,
                    batch_limit=effective_batch_limit,
                    started=started,
                    runtime_minutes=effective_runtime_minutes,
                )
                result["planning_seeded"] = planning_counts["seeded"]
                result["planning_retired"] = planning_counts["retired"]
            if source_family in (None, "hla") and not self._runtime_limit_reached(started, effective_runtime_minutes):
                hla_counts = self._seed_family_scan(
                    source_family="hla",
                    source_dataset=HLA_DATASET,
                    batch_limit=effective_batch_limit,
                    started=started,
                    runtime_minutes=effective_runtime_minutes,
                )
                result["hla_seeded"] = hla_counts["seeded"]
                result["hla_retired"] = hla_counts["retired"]
            total = sum(result.values())
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(
                    status="success",
                    records_fetched=total,
                    records_loaded=total,
                    records_retained=total,
                    metadata=result,
                    finished=True,
                ),
            )
            self.logger.info("incremental_reconcile_catchup_completed", extra=result)
            return result
        except Exception as exc:
            self.loader.update_ingest_run(
                run_id,
                IngestRunUpdate(status="failed", error_message=str(exc), metadata={"traceback": traceback.format_exc()}, finished=True),
            )
            raise

    def _seed_family_scan(
        self,
        *,
        source_family: str,
        source_dataset: str,
        batch_limit: int,
        started: float,
        runtime_minutes: int,
    ) -> dict[str, int]:
        counts = {"seeded": 0, "retired": 0}
        for authority_name in self._authority_scope_for_family(source_family):
            if self._runtime_limit_reached(started, runtime_minutes):
                break
            while not self._runtime_limit_reached(started, runtime_minutes):
                batch = self._fetch_source_batch(
                    source_family=source_family,
                    authority_name=authority_name,
                    batch_limit=batch_limit,
                )
                if not batch:
                    break
                self._upsert_state_batch(
                    source_family=source_family,
                    source_dataset=source_dataset,
                    batch=batch,
                )
                self._queue_upsert_batch(
                    source_family=source_family,
                    source_dataset=source_dataset,
                    batch=batch,
                )
                counts["seeded"] += len(batch)
                self.logger.info(
                    "incremental_reconcile_catchup_progress",
                    extra={
                        "source_family": source_family,
                        "authority_name": authority_name,
                        "seeded": counts["seeded"],
                        "retired": counts["retired"],
                    },
                )
            while not self._runtime_limit_reached(started, runtime_minutes):
                retirement_batch = self._fetch_retirement_batch(
                    source_family=source_family,
                    authority_name=authority_name,
                    batch_limit=batch_limit,
                )
                if not retirement_batch:
                    break
                self._apply_retirement_batch(
                    source_family=source_family,
                    source_dataset=source_dataset,
                    batch=retirement_batch,
                )
                counts["retired"] += len(retirement_batch)
                self.logger.info(
                    "incremental_reconcile_catchup_retirement_progress",
                    extra={
                        "source_family": source_family,
                        "authority_name": authority_name,
                        "seeded": counts["seeded"],
                        "retired": counts["retired"],
                    },
                )
        return counts

    def _fetch_source_batch(
        self,
        *,
        source_family: str,
        authority_name: str,
        batch_limit: int,
    ) -> list[dict[str, Any]]:
        if source_family == "planning":
            missing_batch = self.database.fetch_all(
                """
                with source_candidates as (
                    select
                        planning.authority_name,
                        planning.source_record_id,
                        planning.planning_reference,
                        planning.proposal_text,
                        planning.application_status,
                        planning.decision,
                        planning.appeal_status,
                        planning.raw_payload,
                        planning.geometry,
                        planning.canonical_site_id as source_canonical_site_id,
                        planning.ingest_run_id
                    from landintel.planning_application_records as planning
                    left join landintel.source_reconcile_state as state_row
                      on state_row.source_family = 'planning'
                     and state_row.authority_name = planning.authority_name
                     and state_row.source_record_id = planning.source_record_id
                    where planning.authority_name = :authority_name
                      and state_row.id is null
                    order by planning.source_record_id asc
                    limit :batch_limit
                ),
                prepared as (
                    select
                        source_candidates.authority_name,
                        source_candidates.source_record_id,
                        landintel.planning_reconcile_signature(
                            source_candidates.source_record_id,
                            source_candidates.authority_name,
                            source_candidates.planning_reference,
                            source_candidates.proposal_text,
                            source_candidates.application_status,
                            source_candidates.decision,
                            source_candidates.appeal_status,
                            source_candidates.raw_payload,
                            source_candidates.geometry
                        ) as source_signature,
                        landintel.normalized_geometry_hash(source_candidates.geometry) as geometry_hash,
                        source_candidates.source_canonical_site_id,
                        source_candidates.ingest_run_id
                    from source_candidates
                )
                select prepared.*
                from prepared
                """,
                {
                    "authority_name": authority_name,
                    "batch_limit": batch_limit,
                },
            )
            if missing_batch:
                return missing_batch
            return self.database.fetch_all(
                """
                with source_candidates as (
                    select
                        planning.authority_name,
                        planning.source_record_id,
                        planning.planning_reference,
                        planning.proposal_text,
                        planning.application_status,
                        planning.decision,
                        planning.appeal_status,
                        planning.raw_payload,
                        planning.geometry,
                        planning.canonical_site_id as source_canonical_site_id,
                        planning.ingest_run_id,
                        state_row.current_source_signature,
                        state_row.current_geometry_hash,
                        state_row.current_canonical_site_id,
                        state_row.active_flag,
                        state_row.last_processed_at
                    from landintel.planning_application_records as planning
                    join landintel.source_reconcile_state as state_row
                      on state_row.source_family = 'planning'
                     and state_row.authority_name = planning.authority_name
                     and state_row.source_record_id = planning.source_record_id
                    where planning.authority_name = :authority_name
                      and (
                            state_row.active_flag = false
                         or (
                                planning.canonical_site_id is not null
                                and state_row.current_canonical_site_id is null
                                and state_row.last_processed_at is null
                            )
                         or planning.updated_at > coalesce(state_row.updated_at, timestamp with time zone 'epoch')
                      )
                    order by planning.updated_at desc nulls last, planning.source_record_id asc
                    limit :batch_limit
                ),
                prepared as (
                    select
                        source_candidates.authority_name,
                        source_candidates.source_record_id,
                        landintel.planning_reconcile_signature(
                            source_candidates.source_record_id,
                            source_candidates.authority_name,
                            source_candidates.planning_reference,
                            source_candidates.proposal_text,
                            source_candidates.application_status,
                            source_candidates.decision,
                            source_candidates.appeal_status,
                            source_candidates.raw_payload,
                            source_candidates.geometry
                        ) as source_signature,
                        landintel.normalized_geometry_hash(source_candidates.geometry) as geometry_hash,
                        source_candidates.source_canonical_site_id,
                        source_candidates.ingest_run_id,
                        source_candidates.current_source_signature,
                        source_candidates.current_geometry_hash,
                        source_candidates.current_canonical_site_id,
                        source_candidates.active_flag,
                        source_candidates.last_processed_at
                    from source_candidates
                )
                select
                    authority_name,
                    source_record_id,
                    source_signature,
                    geometry_hash,
                    source_canonical_site_id,
                    ingest_run_id
                from prepared
                where current_source_signature is distinct from source_signature
                   or current_geometry_hash is distinct from geometry_hash
                   or active_flag = false
                   or (
                        source_canonical_site_id is not null
                        and current_canonical_site_id is null
                        and last_processed_at is null
                   )
                """,
                {
                    "authority_name": authority_name,
                    "batch_limit": batch_limit,
                },
            )
        missing_batch = self.database.fetch_all(
            """
            with source_candidates as (
                select
                    hla.authority_name,
                    hla.source_record_id,
                    hla.site_reference,
                    hla.site_name,
                    hla.effectiveness_status,
                    hla.programming_horizon,
                    hla.constraint_reasons,
                    hla.remaining_capacity,
                    hla.raw_payload,
                    hla.geometry,
                    hla.canonical_site_id as source_canonical_site_id,
                    hla.ingest_run_id
                from landintel.hla_site_records as hla
                left join landintel.source_reconcile_state as state_row
                  on state_row.source_family = 'hla'
                 and state_row.authority_name = hla.authority_name
                 and state_row.source_record_id = hla.source_record_id
                where hla.authority_name = :authority_name
                  and state_row.id is null
                order by hla.source_record_id asc
                limit :batch_limit
            ),
            prepared as (
                select
                    source_candidates.authority_name,
                    source_candidates.source_record_id,
                    landintel.hla_reconcile_signature(
                        source_candidates.source_record_id,
                        source_candidates.authority_name,
                        source_candidates.site_reference,
                        source_candidates.site_name,
                        source_candidates.effectiveness_status,
                        source_candidates.programming_horizon,
                        source_candidates.constraint_reasons,
                        source_candidates.remaining_capacity,
                        source_candidates.raw_payload,
                        source_candidates.geometry
                    ) as source_signature,
                    landintel.normalized_geometry_hash(source_candidates.geometry) as geometry_hash,
                    source_candidates.source_canonical_site_id,
                    source_candidates.ingest_run_id
                from source_candidates
            )
            select prepared.*
            from prepared
            """,
            {
                "authority_name": authority_name,
                "batch_limit": batch_limit,
            },
        )
        if missing_batch:
            return missing_batch
        return self.database.fetch_all(
            """
            with source_candidates as (
                select
                    hla.authority_name,
                    hla.source_record_id,
                    hla.site_reference,
                    hla.site_name,
                    hla.effectiveness_status,
                    hla.programming_horizon,
                    hla.constraint_reasons,
                    hla.remaining_capacity,
                    hla.raw_payload,
                    hla.geometry,
                    hla.canonical_site_id as source_canonical_site_id,
                    hla.ingest_run_id,
                    state_row.current_source_signature,
                    state_row.current_geometry_hash,
                    state_row.current_canonical_site_id,
                    state_row.active_flag,
                    state_row.last_processed_at
                from landintel.hla_site_records as hla
                join landintel.source_reconcile_state as state_row
                  on state_row.source_family = 'hla'
                 and state_row.authority_name = hla.authority_name
                 and state_row.source_record_id = hla.source_record_id
                where hla.authority_name = :authority_name
                  and (
                        state_row.active_flag = false
                     or (
                            hla.canonical_site_id is not null
                            and state_row.current_canonical_site_id is null
                            and state_row.last_processed_at is null
                        )
                     or hla.updated_at > coalesce(state_row.updated_at, timestamp with time zone 'epoch')
                  )
                order by hla.updated_at desc nulls last, hla.source_record_id asc
                limit :batch_limit
            ),
            prepared as (
                select
                    source_candidates.authority_name,
                    source_candidates.source_record_id,
                    landintel.hla_reconcile_signature(
                        source_candidates.source_record_id,
                        source_candidates.authority_name,
                        source_candidates.site_reference,
                        source_candidates.site_name,
                        source_candidates.effectiveness_status,
                        source_candidates.programming_horizon,
                        source_candidates.constraint_reasons,
                        source_candidates.remaining_capacity,
                        source_candidates.raw_payload,
                        source_candidates.geometry
                    ) as source_signature,
                    landintel.normalized_geometry_hash(source_candidates.geometry) as geometry_hash,
                    source_candidates.source_canonical_site_id,
                    source_candidates.ingest_run_id,
                    source_candidates.current_source_signature,
                    source_candidates.current_geometry_hash,
                    source_candidates.current_canonical_site_id,
                    source_candidates.active_flag,
                    source_candidates.last_processed_at
                from source_candidates
            )
            select
                authority_name,
                source_record_id,
                source_signature,
                geometry_hash,
                source_canonical_site_id,
                ingest_run_id
            from prepared
            where current_source_signature is distinct from source_signature
               or current_geometry_hash is distinct from geometry_hash
               or active_flag = false
               or (
                    source_canonical_site_id is not null
                    and current_canonical_site_id is null
                    and last_processed_at is null
               )
            """,
            {
                "authority_name": authority_name,
                "batch_limit": batch_limit,
            },
        )

    def _fetch_retirement_batch(
        self,
        *,
        source_family: str,
        authority_name: str,
        batch_limit: int,
    ) -> list[dict[str, Any]]:
        source_table = self._source_table_name(source_family)
        return self.database.fetch_all(
            f"""
            select
                state_row.id as state_id,
                state_row.authority_name,
                state_row.source_record_id,
                state_row.current_canonical_site_id,
                state_row.last_seen_ingest_run_id as ingest_run_id
            from landintel.source_reconcile_state as state_row
            where state_row.source_family = :source_family
              and state_row.authority_name = :authority_name
              and state_row.active_flag = true
              and not exists (
                  select 1
                  from {source_table} as source_row
                  where source_row.authority_name = state_row.authority_name
                    and source_row.source_record_id = state_row.source_record_id
              )
            order by state_row.source_record_id asc
            limit :batch_limit
            """,
            {
                "source_family": source_family,
                "authority_name": authority_name,
                "batch_limit": batch_limit,
            },
        )

    def _upsert_state_batch(
        self,
        *,
        source_family: str,
        source_dataset: str,
        batch: list[dict[str, Any]],
    ) -> None:
        params_list = []
        for row in batch:
            has_legacy_site = row.get("source_canonical_site_id") is not None
            params_list.append(
                {
                    "source_family": source_family,
                    "source_dataset": source_dataset,
                    "authority_name": row["authority_name"],
                    "source_record_id": row["source_record_id"],
                    "source_signature": row.get("source_signature"),
                    "geometry_hash": row.get("geometry_hash"),
                    "ingest_run_id": row.get("ingest_run_id"),
                    "source_canonical_site_id": row.get("source_canonical_site_id"),
                    "initial_match_method": "legacy_link" if has_legacy_site else None,
                    "initial_match_confidence": 1.0 if source_family == "hla" and has_legacy_site else 0.7 if has_legacy_site else None,
                    "initial_publish_state": "published" if has_legacy_site else "blocked",
                    "metadata": self._json_dumps({"source_table": self._source_table_name(source_family)}),
                }
            )
        self.database.execute_many(
            """
            insert into landintel.source_reconcile_state (
                source_family,
                source_dataset,
                authority_name,
                source_record_id,
                active_flag,
                lifecycle_status,
                current_source_signature,
                current_geometry_hash,
                last_seen_ingest_run_id,
                last_seen_at,
                current_canonical_site_id,
                previous_canonical_site_id,
                match_method,
                match_confidence,
                publish_state,
                review_required,
                review_reason_code,
                candidate_site_ids,
                metadata,
                updated_at
            )
            values (
                :source_family,
                :source_dataset,
                :authority_name,
                :source_record_id,
                true,
                'active',
                :source_signature,
                :geometry_hash,
                cast(:ingest_run_id as uuid),
                now(),
                cast(:source_canonical_site_id as uuid),
                cast(:source_canonical_site_id as uuid),
                :initial_match_method,
                :initial_match_confidence,
                :initial_publish_state,
                false,
                null,
                '{}'::uuid[],
                cast(:metadata as jsonb),
                now()
            )
            on conflict (source_family, authority_name, source_record_id) do update
            set source_dataset = excluded.source_dataset,
                active_flag = true,
                lifecycle_status = case
                    when landintel.source_reconcile_state.lifecycle_status = 'retired' then 'active'
                    else landintel.source_reconcile_state.lifecycle_status
                end,
                current_source_signature = excluded.current_source_signature,
                current_geometry_hash = excluded.current_geometry_hash,
                last_seen_ingest_run_id = excluded.last_seen_ingest_run_id,
                last_seen_at = excluded.last_seen_at,
                current_canonical_site_id = case
                    when landintel.source_reconcile_state.current_canonical_site_id is null
                     and excluded.current_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then excluded.current_canonical_site_id
                    else landintel.source_reconcile_state.current_canonical_site_id
                end,
                previous_canonical_site_id = case
                    when landintel.source_reconcile_state.previous_canonical_site_id is null
                     and excluded.previous_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then excluded.previous_canonical_site_id
                    else landintel.source_reconcile_state.previous_canonical_site_id
                end,
                match_method = case
                    when landintel.source_reconcile_state.match_method is null
                     and excluded.current_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then excluded.match_method
                    else landintel.source_reconcile_state.match_method
                end,
                match_confidence = case
                    when landintel.source_reconcile_state.match_confidence is null
                     and excluded.current_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then excluded.match_confidence
                    else landintel.source_reconcile_state.match_confidence
                end,
                publish_state = case
                    when landintel.source_reconcile_state.current_canonical_site_id is null
                     and excluded.current_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then 'published'
                    else landintel.source_reconcile_state.publish_state
                end,
                review_required = case
                    when landintel.source_reconcile_state.current_canonical_site_id is null
                     and excluded.current_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then false
                    else landintel.source_reconcile_state.review_required
                end,
                review_reason_code = case
                    when landintel.source_reconcile_state.current_canonical_site_id is null
                     and excluded.current_canonical_site_id is not null
                     and landintel.source_reconcile_state.last_processed_at is null
                        then null
                    else landintel.source_reconcile_state.review_reason_code
                end,
                metadata = coalesce(landintel.source_reconcile_state.metadata, '{}'::jsonb) || excluded.metadata,
                updated_at = now()
            """,
            params_list,
        )

    def _queue_upsert_batch(
        self,
        *,
        source_family: str,
        source_dataset: str,
        batch: list[dict[str, Any]],
    ) -> None:
        params_list = [
            {
                "source_family": source_family,
                "source_dataset": source_dataset,
                "authority_name": row["authority_name"],
                "source_record_id": row["source_record_id"],
                "source_signature": row.get("source_signature"),
                "geometry_hash": row.get("geometry_hash"),
                "ingest_run_id": row.get("ingest_run_id"),
            }
            for row in batch
        ]
        self.database.execute_many(
            """
            insert into landintel.source_reconcile_queue (
                state_id,
                source_family,
                source_dataset,
                authority_name,
                source_record_id,
                work_type,
                priority,
                status,
                source_signature,
                geometry_hash,
                previous_canonical_site_id,
                candidate_site_ids,
                claimed_by,
                claimed_at,
                lease_expires_at,
                attempt_count,
                next_attempt_at,
                processed_at,
                error_code,
                error_message,
                review_reason_code,
                ingest_run_id,
                metadata,
                updated_at
            )
            select
                state_row.id,
                :source_family,
                :source_dataset,
                :authority_name,
                :source_record_id,
                'upsert',
                100,
                'pending',
                :source_signature,
                :geometry_hash,
                state_row.current_canonical_site_id,
                '{}'::uuid[],
                null,
                null,
                null,
                0,
                null,
                null,
                null,
                null,
                null,
                cast(:ingest_run_id as uuid),
                '{}'::jsonb,
                now()
            from landintel.source_reconcile_state as state_row
            where state_row.source_family = :source_family
              and state_row.authority_name = :authority_name
              and state_row.source_record_id = :source_record_id
            on conflict (state_id) do update
            set source_family = excluded.source_family,
                source_dataset = excluded.source_dataset,
                authority_name = excluded.authority_name,
                source_record_id = excluded.source_record_id,
                work_type = 'upsert',
                priority = 100,
                status = 'pending',
                source_signature = excluded.source_signature,
                geometry_hash = excluded.geometry_hash,
                previous_canonical_site_id = excluded.previous_canonical_site_id,
                candidate_site_ids = '{}'::uuid[],
                claimed_by = null,
                claimed_at = null,
                lease_expires_at = null,
                attempt_count = 0,
                next_attempt_at = null,
                processed_at = null,
                error_code = null,
                error_message = null,
                review_reason_code = null,
                ingest_run_id = excluded.ingest_run_id,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            params_list,
        )
        self._link_state_to_queue(source_family=source_family, batch=batch)

    def _apply_retirement_batch(
        self,
        *,
        source_family: str,
        source_dataset: str,
        batch: list[dict[str, Any]],
    ) -> None:
        update_params = [{"state_id": row["state_id"]} for row in batch]
        self.database.execute_many(
            """
            update landintel.source_reconcile_state
            set active_flag = false,
                lifecycle_status = 'retired',
                publish_state = 'blocked',
                review_required = false,
                review_reason_code = null,
                candidate_site_ids = '{}'::uuid[],
                updated_at = now()
            where id = cast(:state_id as uuid)
            """,
            update_params,
        )
        queue_params = [
            {
                "state_id": row["state_id"],
                "source_family": source_family,
                "source_dataset": source_dataset,
                "authority_name": row["authority_name"],
                "source_record_id": row["source_record_id"],
                "previous_canonical_site_id": row.get("current_canonical_site_id"),
                "ingest_run_id": row.get("ingest_run_id"),
            }
            for row in batch
        ]
        self.database.execute_many(
            """
            insert into landintel.source_reconcile_queue (
                state_id,
                source_family,
                source_dataset,
                authority_name,
                source_record_id,
                work_type,
                priority,
                status,
                source_signature,
                geometry_hash,
                previous_canonical_site_id,
                candidate_site_ids,
                claimed_by,
                claimed_at,
                lease_expires_at,
                attempt_count,
                next_attempt_at,
                processed_at,
                error_code,
                error_message,
                review_reason_code,
                ingest_run_id,
                metadata,
                updated_at
            )
            values (
                cast(:state_id as uuid),
                :source_family,
                :source_dataset,
                :authority_name,
                :source_record_id,
                'retire',
                110,
                'pending',
                null,
                null,
                cast(:previous_canonical_site_id as uuid),
                '{}'::uuid[],
                null,
                null,
                null,
                0,
                null,
                null,
                null,
                null,
                null,
                cast(:ingest_run_id as uuid),
                '{}'::jsonb,
                now()
            )
            on conflict (state_id) do update
            set work_type = 'retire',
                priority = 110,
                status = 'pending',
                source_signature = null,
                geometry_hash = null,
                previous_canonical_site_id = excluded.previous_canonical_site_id,
                candidate_site_ids = '{}'::uuid[],
                claimed_by = null,
                claimed_at = null,
                lease_expires_at = null,
                attempt_count = 0,
                next_attempt_at = null,
                processed_at = null,
                error_code = null,
                error_message = null,
                review_reason_code = null,
                ingest_run_id = excluded.ingest_run_id,
                metadata = excluded.metadata,
                updated_at = now()
            """,
            queue_params,
        )
        self._link_state_to_queue(
            source_family=source_family,
            batch=[
                {"authority_name": row["authority_name"], "source_record_id": row["source_record_id"]}
                for row in batch
            ],
        )

    def _link_state_to_queue(self, *, source_family: str, batch: list[dict[str, Any]]) -> None:
        params_list = [
            {
                "source_family": source_family,
                "authority_name": row["authority_name"],
                "source_record_id": row["source_record_id"],
            }
            for row in batch
        ]
        self.database.execute_many(
            """
            update landintel.source_reconcile_state as state_row
            set last_queue_item_id = queue_row.id,
                updated_at = now()
            from landintel.source_reconcile_queue as queue_row
            where queue_row.state_id = state_row.id
              and state_row.source_family = :source_family
              and state_row.authority_name = :authority_name
              and state_row.source_record_id = :source_record_id
            """,
            params_list,
        )

    def _authority_scope_for_family(self, source_family: str) -> list[str]:
        source_table = self._source_table_name(source_family)
        rows = self.database.fetch_all(
            f"""
            select authority_name
            from (
                select distinct authority_name
                from {source_table}
                where authority_name is not null
                union
                select distinct authority_name
                from landintel.source_reconcile_state
                where source_family = :source_family
                  and authority_name is not null
            ) as authority_scope
            order by authority_name asc
            """,
            {"source_family": source_family},
        )
        return [str(row["authority_name"]) for row in rows if row.get("authority_name")]

    def _source_table_name(self, source_family: str) -> str:
        return "landintel.planning_application_records" if source_family == "planning" else "landintel.hla_site_records"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the LandIntel incremental reconcile catch-up scan.")
    parser.add_argument("command", choices=("reconcile-catchup-scan",))
    parser.add_argument("--source-family", choices=("planning", "hla"), default=None)
    parser.add_argument("--runtime-minutes", type=int, default=None)
    parser.add_argument("--batch-limit", type=int, default=None)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = IncrementalReconcileCatchupRunner(settings, logger)
    try:
        runner.reconcile_catchup_scan(
            source_family=args.source_family,
            runtime_minutes=args.runtime_minutes,
            batch_limit=args.batch_limit,
        )
        runner.logger.info("incremental_reconcile_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        runner.logger.exception("incremental_reconcile_command_failed", extra={"command": args.command})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

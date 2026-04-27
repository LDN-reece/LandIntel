create table if not exists landintel.source_freshness_states (
    id uuid primary key default gen_random_uuid(),
    source_scope_key text not null,
    source_family text not null,
    source_dataset text not null default 'unknown',
    source_name text not null,
    authority_name text,
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    source_access_mode text not null default 'unknown',
    source_url text,
    refresh_cadence text not null default 'weekly',
    max_staleness_days integer not null default 7,
    source_published_at timestamptz,
    source_observed_at timestamptz,
    last_checked_at timestamptz,
    last_success_at timestamptz,
    last_failure_at timestamptz,
    next_refresh_due_at timestamptz,
    freshness_status text not null default 'unknown',
    live_access_status text not null default 'unknown',
    ranking_eligible boolean not null default false,
    review_output_eligible boolean not null default false,
    stale_reason_code text,
    check_summary text,
    records_observed integer not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.source_registry
    add column if not exists source_published_at timestamptz;

alter table public.source_registry
    add column if not exists last_checked_at timestamptz;

alter table public.source_registry
    add column if not exists last_success_at timestamptz;

alter table public.source_registry
    add column if not exists next_refresh_due_at timestamptz;

alter table public.source_registry
    add column if not exists freshness_status text not null default 'unknown';

create unique index if not exists landintel_source_freshness_scope_uidx
    on landintel.source_freshness_states (source_scope_key);

create index if not exists landintel_source_freshness_family_status_idx
    on landintel.source_freshness_states (source_family, freshness_status, last_checked_at desc);

create index if not exists landintel_source_freshness_due_idx
    on landintel.source_freshness_states (next_refresh_due_at)
    where next_refresh_due_at is not null;

create index if not exists landintel_source_freshness_registry_idx
    on landintel.source_freshness_states (source_registry_id)
    where source_registry_id is not null;

create index if not exists source_registry_freshness_idx
    on public.source_registry (freshness_status, last_checked_at desc);

drop view if exists analytics.v_live_source_coverage_freshness;
drop view if exists analytics.v_source_freshness_matrix;
drop view if exists analytics.v_source_freshness_current;

create or replace view analytics.v_source_freshness_current
with (security_invoker = true) as
with ingest_run_mapping(
    run_type,
    source_family,
    source_dataset,
    source_name,
    source_access_mode,
    refresh_cadence,
    max_staleness_days,
    ranking_eligible,
    review_output_eligible
) as (
    values
        ('ingest_planning_history', 'planning', 'Planning Applications: Official - Scotland', 'Planning Applications: Official - Scotland', 'spatialhub_wfs_authenticated', 'weekly', 7, true, true),
        ('ingest_hla', 'hla', 'Housing Land Audit / Housing Land Supply', 'Housing Land Audit / Housing Land Supply', 'spatialhub_wfs_authenticated', 'monthly', 30, true, true),
        ('reconcile_canonical_sites', 'canonical', 'Canonical site reconciliation', 'Canonical site reconciliation', 'supabase_reconcile_publish', 'weekly', 7, true, true),
        ('ingest_bgs', 'bgs', 'BGS OpenGeoscience API', 'BGS OpenGeoscience API', 'ogc_api_features', 'monthly', 30, false, true),
        ('ingest_ros_cadastral', 'ros_cadastral', 'Registers of Scotland cadastral parcels', 'Registers of Scotland cadastral parcels', 'manual_or_blob_load', 'quarterly', 90, true, true),
        ('load_boundaries', 'local_authority_boundaries', 'Local authority boundaries', 'Local authority boundaries', 'manual_or_blob_load', 'quarterly', 90, false, true)
),
latest_ingest_runs as (
    select
        mapping.*,
        runs.id as ingest_run_id,
        runs.status,
        runs.started_at,
        runs.finished_at,
        runs.records_fetched,
        runs.records_loaded,
        runs.records_retained,
        runs.error_message,
        row_number() over (
            partition by mapping.source_family, mapping.source_dataset
            order by coalesce(runs.finished_at, runs.started_at) desc nulls last, runs.id desc
        ) as row_number
    from ingest_run_mapping as mapping
    join public.ingest_runs as runs
        on runs.run_type = mapping.run_type
),
explicit_freshness as (
    select
        freshness.id,
        freshness.source_scope_key,
        freshness.source_family,
        freshness.source_dataset,
        freshness.source_name,
        freshness.authority_name,
        freshness.source_registry_id,
        freshness.ingest_run_id,
        freshness.source_access_mode,
        freshness.source_url,
        freshness.refresh_cadence,
        freshness.max_staleness_days,
        freshness.source_published_at,
        freshness.source_observed_at,
        freshness.last_checked_at,
        freshness.last_success_at,
        freshness.last_failure_at,
        freshness.next_refresh_due_at,
        freshness.freshness_status,
        freshness.live_access_status,
        freshness.ranking_eligible,
        freshness.review_output_eligible,
        freshness.stale_reason_code,
        freshness.check_summary,
        freshness.records_observed,
        freshness.metadata,
        freshness.created_at,
        freshness.updated_at
    from landintel.source_freshness_states as freshness
),
derived_freshness as (
    select
        null::uuid as id,
        'ingest_run:' || latest.source_family || ':' || md5(latest.source_dataset) as source_scope_key,
        latest.source_family,
        latest.source_dataset,
        latest.source_name,
        null::text as authority_name,
        null::uuid as source_registry_id,
        latest.ingest_run_id,
        latest.source_access_mode,
        null::text as source_url,
        latest.refresh_cadence,
        latest.max_staleness_days,
        null::timestamptz as source_published_at,
        coalesce(latest.finished_at, latest.started_at) as source_observed_at,
        coalesce(latest.finished_at, latest.started_at) as last_checked_at,
        case
            when latest.status in ('success', 'partial_success') then coalesce(latest.finished_at, latest.started_at)
        end as last_success_at,
        case
            when latest.status not in ('success', 'partial_success') then coalesce(latest.finished_at, latest.started_at)
        end as last_failure_at,
        case
            when latest.status in ('success', 'partial_success') then coalesce(latest.finished_at, latest.started_at) + make_interval(days => greatest(latest.max_staleness_days, 1))
        end as next_refresh_due_at,
        case
            when latest.status in ('success', 'partial_success') then 'current'
            when latest.status in ('running', 'started') then 'unknown'
            else 'failed'
        end as freshness_status,
        case
            when latest.status in ('success', 'partial_success') then 'reachable'
            when latest.status in ('running', 'started') then 'not_checked'
            else 'failing'
        end as live_access_status,
        latest.ranking_eligible,
        latest.review_output_eligible,
        case
            when latest.status not in ('success', 'partial_success', 'running', 'started') then 'latest_ingest_failed'
        end as stale_reason_code,
        case
            when latest.status in ('success', 'partial_success') then 'Latest successful ingest run proves this source has been refreshed.'
            when latest.status in ('running', 'started') then 'Latest ingest run is still in progress or did not finish cleanly.'
            else coalesce(latest.error_message, 'Latest ingest run failed.')
        end as check_summary,
        greatest(
            coalesce(latest.records_fetched, 0),
            coalesce(latest.records_loaded, 0),
            coalesce(latest.records_retained, 0)
        )::bigint as records_observed,
        jsonb_build_object(
            'derived_from', 'public.ingest_runs',
            'run_type', latest.run_type,
            'run_status', latest.status,
            'records_fetched', latest.records_fetched,
            'records_loaded', latest.records_loaded,
            'records_retained', latest.records_retained
        ) as metadata,
        coalesce(latest.started_at, now()) as created_at,
        coalesce(latest.finished_at, latest.started_at, now()) as updated_at
    from latest_ingest_runs as latest
    where latest.row_number = 1
),
freshness_rows as (
    select * from explicit_freshness
    union all
    select * from derived_freshness
)
select
    freshness.*,
    case
        when freshness.freshness_status in ('core_pending_adapter', 'explicitly_deferred', 'discovery_only', 'manual_snapshot', 'failed') then freshness.freshness_status
        when freshness.last_checked_at is null then 'unknown'
        when freshness.next_refresh_due_at is not null and freshness.next_refresh_due_at < now() then 'stale'
        when freshness.last_checked_at < now() - make_interval(days => greatest(freshness.max_staleness_days, 1)) then 'stale'
        else freshness.freshness_status
    end as effective_freshness_status,
    case
        when freshness.freshness_status = 'failed' then coalesce(freshness.stale_reason_code, 'latest_check_failed')
        when freshness.last_checked_at is null then 'source_not_checked'
        when freshness.next_refresh_due_at is not null and freshness.next_refresh_due_at < now() then 'refresh_due'
        when freshness.last_checked_at < now() - make_interval(days => greatest(freshness.max_staleness_days, 1)) then 'max_staleness_exceeded'
        else freshness.stale_reason_code
    end as effective_reason_code
from freshness_rows as freshness;

create or replace view analytics.v_source_freshness_matrix
with (security_invoker = true) as
with source_families(source_family, phase_one_role, ranking_policy) as (
    values
        ('planning', 'critical', 'live_ranking'),
        ('hla', 'critical', 'live_ranking'),
        ('title_number', 'critical', 'control_spine'),
        ('canonical', 'critical', 'canonical_spine'),
        ('ros_cadastral', 'critical', 'canonical_spine'),
        ('local_authority_boundaries', 'critical', 'review_context'),
        ('ldp', 'critical', 'core_policy_storage_licence_gated'),
        ('settlement', 'critical', 'core_policy_pending_adapter'),
        ('flood', 'target_live', 'constraints_drag'),
        ('bgs', 'context', 'ground_context'),
        ('ela', 'target_live', 'future_context'),
        ('vdl', 'target_live', 'future_context'),
        ('sepa_flood', 'target_live', 'constraints_drag'),
        ('coal_authority', 'target_live', 'constraints_drag'),
        ('hes', 'target_live', 'constraints_drag'),
        ('naturescot', 'target_live', 'constraints_drag'),
        ('contaminated_land', 'target_live', 'constraints_drag'),
        ('tpo', 'target_live', 'constraints_drag'),
        ('culverts', 'target_live', 'utilities_and_constraints_drag'),
        ('conservation_areas', 'target_live', 'constraints_drag'),
        ('sgn_assets', 'context', 'utilities_context')
),
rollup as (
    select
        current.source_family,
        count(*)::bigint as freshness_record_count,
        count(*) filter (where current.effective_freshness_status = 'current')::bigint as current_count,
        count(*) filter (where current.effective_freshness_status = 'stale')::bigint as stale_count,
        count(*) filter (where current.effective_freshness_status = 'unknown')::bigint as unknown_count,
        count(*) filter (where current.effective_freshness_status = 'failed')::bigint as failed_count,
        count(*) filter (where current.effective_freshness_status = 'manual_snapshot')::bigint as manual_snapshot_count,
        count(*) filter (where current.effective_freshness_status in ('core_pending_adapter', 'explicitly_deferred', 'discovery_only'))::bigint as deferred_count,
        max(current.last_checked_at) as latest_checked_at,
        max(current.last_success_at) as latest_success_at,
        min(current.next_refresh_due_at) filter (where current.next_refresh_due_at is not null) as next_refresh_due_at,
        coalesce(sum(current.records_observed), 0)::bigint as records_observed,
        bool_or(current.ranking_eligible and current.effective_freshness_status = 'current') as has_current_ranking_source,
        bool_or(current.review_output_eligible and current.effective_freshness_status = 'current') as has_current_review_source,
        array_remove(array_agg(distinct current.effective_freshness_status order by current.effective_freshness_status), null::text) as freshness_statuses,
        array_remove(array_agg(distinct current.effective_reason_code order by current.effective_reason_code), null::text) as reason_codes
    from analytics.v_source_freshness_current as current
    group by current.source_family
)
select
    family.source_family,
    family.phase_one_role,
    family.ranking_policy,
    coalesce(rollup.freshness_record_count, 0) as freshness_record_count,
    coalesce(rollup.current_count, 0) as current_count,
    coalesce(rollup.stale_count, 0) as stale_count,
    coalesce(rollup.unknown_count, 0) as unknown_count,
    coalesce(rollup.failed_count, 0) as failed_count,
    coalesce(rollup.manual_snapshot_count, 0) as manual_snapshot_count,
    coalesce(rollup.deferred_count, 0) as deferred_count,
    rollup.latest_checked_at,
    rollup.latest_success_at,
    rollup.next_refresh_due_at,
    coalesce(rollup.records_observed, 0) as records_observed,
    coalesce(rollup.has_current_ranking_source, false) as has_current_ranking_source,
    coalesce(rollup.has_current_review_source, false) as has_current_review_source,
    coalesce(rollup.freshness_statuses, '{}'::text[]) as freshness_statuses,
    coalesce(rollup.reason_codes, '{}'::text[]) as reason_codes,
    case
        when coalesce(rollup.freshness_record_count, 0) = 0 then 'unknown'
        when coalesce(rollup.failed_count, 0) > 0 and coalesce(rollup.current_count, 0) = 0 then 'failed'
        when coalesce(rollup.stale_count, 0) > 0 and coalesce(rollup.current_count, 0) = 0 then 'stale'
        when coalesce(rollup.current_count, 0) > 0 then 'current'
        when 'core_pending_adapter' = any(coalesce(rollup.freshness_statuses, '{}'::text[])) then 'core_pending_adapter'
        when coalesce(rollup.deferred_count, 0) > 0 then 'explicitly_deferred'
        when coalesce(rollup.manual_snapshot_count, 0) > 0 then 'manual_snapshot'
        else 'unknown'
    end as source_freshness_status,
    case
        when family.ranking_policy = 'core_policy_pending_adapter'
         and coalesce(rollup.deferred_count, 0) > 0 then 'pass_core_policy_pending_adapter'
        when family.ranking_policy = 'core_policy_storage_licence_gated'
         and coalesce(rollup.current_count, 0) > 0 then 'pass_core_policy_storage_licence_gated'
        when family.ranking_policy = 'control_spine'
         and coalesce(rollup.current_count, 0) > 0 then 'pass_control_current'
        when family.ranking_policy like 'deferred%%' and coalesce(rollup.deferred_count, 0) > 0 then 'pass_deferred_monitored'
        when coalesce(rollup.has_current_ranking_source, false) then 'pass_current'
        when family.phase_one_role = 'context' and coalesce(rollup.current_count, 0) > 0 then 'pass_context_current'
        when family.phase_one_role = 'context' and coalesce(rollup.manual_snapshot_count, 0) > 0 then 'pass_context_manual_snapshot'
        when coalesce(rollup.freshness_record_count, 0) = 0 then 'block_no_freshness_record'
        when coalesce(rollup.failed_count, 0) > 0 then 'block_latest_check_failed'
        when coalesce(rollup.stale_count, 0) > 0 then 'block_stale_source'
        else 'block_unknown_freshness'
    end as ranking_freshness_gate
from source_families as family
left join rollup
  on rollup.source_family = family.source_family
order by family.source_family;

create or replace view analytics.v_live_source_coverage_freshness
with (security_invoker = true) as
select
    coverage.*,
    coalesce(freshness.source_freshness_status, 'unknown') as source_freshness_status,
    coalesce(freshness.ranking_freshness_gate, 'block_no_freshness_record') as ranking_freshness_gate,
    freshness.latest_checked_at as freshness_latest_checked_at,
    freshness.latest_success_at as freshness_latest_success_at,
    freshness.next_refresh_due_at as freshness_next_refresh_due_at,
    coalesce(freshness.reason_codes, '{}'::text[]) as freshness_reason_codes
from analytics.v_live_source_coverage as coverage
left join analytics.v_source_freshness_matrix as freshness
  on freshness.source_family = coverage.source_family;

alter table if exists landintel.source_freshness_states enable row level security;

revoke all on table landintel.source_freshness_states from anon, authenticated;
revoke all on table analytics.v_source_freshness_current from anon, authenticated;
revoke all on table analytics.v_source_freshness_matrix from anon, authenticated;
revoke all on table analytics.v_live_source_coverage_freshness from anon, authenticated;

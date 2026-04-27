create table if not exists landintel.source_estate_registry (
    id uuid primary key default gen_random_uuid(),
    source_key text not null unique,
    source_family text not null,
    source_name text not null,
    source_group text not null default 'unknown',
    phase_one_role text not null default 'context',
    source_status text not null default 'unknown',
    orchestration_mode text not null default 'unknown',
    endpoint_url text,
    auth_env_vars text[] not null default '{}'::text[],
    target_table text,
    reconciliation_path text,
    evidence_path text,
    signal_output text,
    ranking_impact text,
    resurfacing_trigger text,
    data_age_basis text,
    drive_folder_url text,
    notes text,
    ranking_eligible boolean not null default false,
    review_output_eligible boolean not null default true,
    last_registered_at timestamptz not null default now(),
    last_probe_at timestamptz,
    last_probe_status text not null default 'not_checked',
    last_probe_summary text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.source_corpus_assets (
    id uuid primary key default gen_random_uuid(),
    source_key text not null references landintel.source_estate_registry(source_key) on delete cascade,
    asset_key text not null,
    file_name text not null,
    drive_url text,
    local_reference text,
    layer_names text[] not null default '{}'::text[],
    feature_count bigint,
    modified_at timestamptz,
    asset_role text not null default 'static_snapshot',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, asset_key)
);

create index if not exists landintel_source_estate_family_status_idx
    on landintel.source_estate_registry (source_family, source_status, phase_one_role);

create index if not exists landintel_source_estate_probe_idx
    on landintel.source_estate_registry (last_probe_status, last_probe_at desc);

create index if not exists landintel_source_corpus_assets_source_idx
    on landintel.source_corpus_assets (source_key, modified_at desc);

drop view if exists analytics.v_phase_one_source_estate_matrix;

create or replace view analytics.v_phase_one_source_estate_matrix
with (security_invoker = true) as
with asset_rollup as (
    select
        asset.source_key,
        count(*)::bigint as asset_count,
        coalesce(sum(asset.feature_count), 0)::bigint as static_feature_count,
        max(asset.modified_at) as latest_static_asset_modified_at,
        array_remove(array_agg(distinct layer_name.layer_name order by layer_name.layer_name), null::text) as layer_names
    from landintel.source_corpus_assets as asset
    left join lateral unnest(asset.layer_names) as layer_name(layer_name) on true
    group by asset.source_key
),
freshness as (
    select
        matrix.source_family,
        matrix.source_freshness_status,
        matrix.ranking_freshness_gate,
        matrix.latest_checked_at,
        matrix.latest_success_at,
        matrix.next_refresh_due_at,
        matrix.records_observed,
        matrix.reason_codes
    from analytics.v_source_freshness_matrix as matrix
)
select
    registry.source_key,
    registry.source_family,
    registry.source_name,
    registry.source_group,
    registry.phase_one_role,
    registry.source_status,
    registry.orchestration_mode,
    registry.endpoint_url,
    registry.auth_env_vars,
    registry.target_table,
    registry.reconciliation_path,
    registry.evidence_path,
    registry.signal_output,
    registry.ranking_impact,
    registry.resurfacing_trigger,
    registry.data_age_basis,
    registry.drive_folder_url,
    registry.ranking_eligible,
    registry.review_output_eligible,
    registry.last_registered_at,
    registry.last_probe_at,
    registry.last_probe_status,
    registry.last_probe_summary,
    coalesce(asset_rollup.asset_count, 0) as asset_count,
    coalesce(asset_rollup.static_feature_count, 0) as static_feature_count,
    asset_rollup.latest_static_asset_modified_at,
    coalesce(asset_rollup.layer_names, '{}'::text[]) as layer_names,
    coalesce(freshness.source_freshness_status, 'unknown') as source_freshness_status,
    coalesce(freshness.ranking_freshness_gate, 'not_in_freshness_matrix') as ranking_freshness_gate,
    freshness.latest_checked_at as freshness_latest_checked_at,
    freshness.latest_success_at as freshness_latest_success_at,
    freshness.next_refresh_due_at as freshness_next_refresh_due_at,
    coalesce(freshness.records_observed, 0) as freshness_records_observed,
    coalesce(freshness.reason_codes, '{}'::text[]) as freshness_reason_codes,
    case
        when registry.source_status = 'live_internal_validation' then 'internal_validation_registered'
        when registry.source_status = 'core_pending_adapter' then 'core_pending_adapter'
        when registry.source_status in ('explicitly_deferred', 'discovery_only') then registry.source_status
        when registry.source_status in ('live_api', 'live_target') and registry.last_probe_status in ('reachable', 'current') then 'live_wired'
        when registry.source_status = 'static_snapshot' and coalesce(asset_rollup.asset_count, 0) > 0 then 'static_registered'
        when registry.last_probe_status = 'missing_required_secret' then 'blocked_missing_secret'
        when registry.last_probe_status = 'failed' then 'blocked_probe_failed'
        else 'registered_unproven'
    end as operational_status
from landintel.source_estate_registry as registry
left join asset_rollup
  on asset_rollup.source_key = registry.source_key
left join freshness
  on freshness.source_family = registry.source_family;

alter table if exists landintel.source_estate_registry enable row level security;
alter table if exists landintel.source_corpus_assets enable row level security;

revoke all on table landintel.source_estate_registry from anon, authenticated;
revoke all on table landintel.source_corpus_assets from anon, authenticated;
revoke all on table analytics.v_phase_one_source_estate_matrix from anon, authenticated;

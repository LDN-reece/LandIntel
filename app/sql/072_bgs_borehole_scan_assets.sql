create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

create table if not exists landintel_store.bgs_borehole_scan_assets (
    id uuid primary key default gen_random_uuid(),
    queue_id uuid not null references landintel_store.bgs_borehole_scan_fetch_queue(id) on delete cascade,
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    registry_id uuid not null references landintel_store.bgs_borehole_scan_registry(id) on delete cascade,
    bgs_id bigint not null,
    source_key text not null default 'bgs_borehole_scan_assets',
    source_family text not null default 'bgs',
    source_url text not null,
    asset_status text not null default 'linked_not_downloaded',
    fetch_status text not null default 'storage_not_configured',
    storage_bucket text,
    storage_path text,
    source_content_type text,
    source_content_length bigint,
    source_http_status integer,
    source_sha256 text,
    fetch_attempted_at timestamptz,
    fetched_at timestamptz,
    last_error text,
    safe_use_caveat text not null default 'BGS scan/log asset rows are manifest records only. Files are not stored in Postgres and asset availability is not ground-condition interpretation.',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists bgs_borehole_scan_assets_queue_uidx
    on landintel_store.bgs_borehole_scan_assets (queue_id);

create index if not exists bgs_borehole_scan_assets_status_idx
    on landintel_store.bgs_borehole_scan_assets (asset_status, fetch_status, updated_at);

create index if not exists bgs_borehole_scan_assets_site_idx
    on landintel_store.bgs_borehole_scan_assets (canonical_site_id, bgs_id);

create or replace view landintel_reporting.v_bgs_scan_assets
with (security_invoker = true) as
select
    asset.id as asset_id,
    asset.queue_id,
    asset.canonical_site_id,
    site.site_name_primary as site_label,
    site.authority_name,
    queue.site_priority_band,
    queue.site_priority_rank,
    queue.priority_source,
    registry.bgs_id,
    registry.registration_number,
    registry.borehole_name,
    registry.grid_reference,
    queue.borehole_distance_m,
    asset.source_url,
    asset.asset_status,
    asset.fetch_status,
    asset.storage_bucket,
    asset.storage_path,
    asset.source_content_type,
    asset.source_content_length,
    asset.source_http_status,
    asset.fetch_attempted_at,
    asset.fetched_at,
    asset.last_error,
    asset.safe_use_caveat,
    asset.updated_at
from landintel_store.bgs_borehole_scan_assets as asset
join landintel_store.bgs_borehole_scan_fetch_queue as queue
  on queue.id = asset.queue_id
join landintel_store.bgs_borehole_scan_registry as registry
  on registry.id = asset.registry_id
join landintel.canonical_sites as site
  on site.id = asset.canonical_site_id;

comment on table landintel_store.bgs_borehole_scan_assets
    is 'Bounded BGS scan/log asset manifest table. Stores source URI and optional storage manifest only; no PDF/blob payloads in Postgres.';

comment on view landintel_reporting.v_bgs_scan_assets
    is 'Operator-safe BGS scan/log asset manifest surface. Asset rows are source availability context, not interpreted ground-condition evidence.';

do $$
begin
    if to_regclass('landintel_store.object_ownership_registry') is not null then
        insert into landintel_store.object_ownership_registry (
            schema_name,
            object_name,
            object_type,
            current_status,
            owner_layer,
            canonical_role,
            source_family_or_module,
            exists_in_github,
            exists_in_supabase,
            represented_in_repo,
            safe_to_read,
            safe_to_write,
            safe_for_operator,
            safe_to_retire,
            replacement_object,
            risk_summary,
            recommended_action,
            metadata
        )
        values
            (
                'landintel_store',
                'bgs_borehole_scan_assets',
                'table',
                'current_keep',
                'landintel_store',
                'bounded BGS scan/log asset manifest',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                true,
                false,
                false,
                'landintel_reporting.v_bgs_scan_assets',
                'Stores URI/storage manifest only. It must not store PDF blobs in Postgres or imply reviewed ground conditions.',
                'Run candidate-site-first with low max-assets caps. Keep linked_not_downloaded when storage is not configured.',
                '{"phase":"G3","candidate_site_first":true,"pdf_blob_in_postgres":false,"final_ground_condition_interpretation":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_bgs_scan_assets',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'operator-safe BGS scan/log asset manifest',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Operator surface for BGS source asset manifests. It remains caveated as source availability context only.',
                'Use for manual Pre-SI source review planning. Do not treat asset presence as ground proof.',
                '{"phase":"G3","operator_safe":true,"source_availability_only":true}'::jsonb
            )
        on conflict (schema_name, object_name, object_type) do update set
            current_status = excluded.current_status,
            owner_layer = excluded.owner_layer,
            canonical_role = excluded.canonical_role,
            source_family_or_module = excluded.source_family_or_module,
            exists_in_github = excluded.exists_in_github,
            exists_in_supabase = excluded.exists_in_supabase,
            represented_in_repo = excluded.represented_in_repo,
            safe_to_read = excluded.safe_to_read,
            safe_to_write = excluded.safe_to_write,
            safe_for_operator = excluded.safe_for_operator,
            safe_to_retire = excluded.safe_to_retire,
            replacement_object = excluded.replacement_object,
            risk_summary = excluded.risk_summary,
            recommended_action = excluded.recommended_action,
            metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
            reviewed_at = now(),
            updated_at = now();
    end if;
end $$;

grant usage on schema landintel_store to authenticated;
grant usage on schema landintel_reporting to authenticated;
grant select on landintel_store.bgs_borehole_scan_assets to authenticated;
grant select on landintel_reporting.v_bgs_scan_assets to authenticated;

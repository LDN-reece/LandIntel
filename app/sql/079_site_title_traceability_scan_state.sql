create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

create table if not exists landintel_store.site_title_traceability_scan_state (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    scan_scope text not null default 'all_sources',
    site_priority_band text,
    site_priority_rank integer,
    priority_source text,
    authority_name text,
    area_acres numeric,
    scan_status text not null,
    parcel_candidate_rows integer not null default 0,
    primary_link_rows integer not null default 0,
    title_candidate_rows integer not null default 0,
    licensed_bridge_required_rows integer not null default 0,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    scanned_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint site_title_traceability_scan_state_status_chk check (
        scan_status in (
            'no_candidate',
            'parcel_candidate_available',
            'title_candidate_available',
            'error'
        )
    )
);

create unique index if not exists site_title_traceability_scan_state_site_scope_uidx
    on landintel_store.site_title_traceability_scan_state (canonical_site_id, scan_scope);

create index if not exists site_title_traceability_scan_state_scope_status_idx
    on landintel_store.site_title_traceability_scan_state (scan_scope, scan_status, scanned_at desc);

create index if not exists site_title_traceability_scan_state_priority_idx
    on landintel_store.site_title_traceability_scan_state (site_priority_band, site_priority_rank, scanned_at desc);

create or replace view landintel_reporting.v_site_title_traceability_scan_state as
select
    state.canonical_site_id,
    site.site_name_primary as site_label,
    state.scan_scope,
    state.site_priority_band,
    state.site_priority_rank,
    state.priority_source,
    state.authority_name,
    state.area_acres,
    state.scan_status,
    state.parcel_candidate_rows,
    state.primary_link_rows,
    state.title_candidate_rows,
    state.licensed_bridge_required_rows,
    state.last_error,
    state.scanned_at,
    state.updated_at,
    case
        when state.scan_status = 'no_candidate'
            then 'No RoS parcel candidate was found in the bounded traceability run. This is scan-state only, not a legal ownership conclusion.'
        when state.scan_status in ('parcel_candidate_available', 'title_candidate_available')
            then 'Traceability candidate exists, but ownership remains unconfirmed unless title review records support it.'
        else 'Traceability scan needs operator review.'
    end as caveat
from landintel_store.site_title_traceability_scan_state as state
join landintel.canonical_sites as site
  on site.id = state.canonical_site_id;

comment on table landintel_store.site_title_traceability_scan_state is
    'Bounded scan-state memory for site-to-RoS parcel/title traceability. Prevents repeated no-hit title indexing batches without creating ownership truth.';

comment on view landintel_reporting.v_site_title_traceability_scan_state is
    'Operator-readable title traceability scan-state. No-hit rows are indexing memory, not legal ownership evidence.';

grant select on landintel_reporting.v_site_title_traceability_scan_state to authenticated;

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
    metadata,
    updated_at
)
values
    (
        'landintel_store',
        'site_title_traceability_scan_state',
        'table',
        'current_keep',
        'landintel_store',
        'bounded title traceability scan-state',
        'title_number',
        true,
        true,
        true,
        true,
        true,
        false,
        false,
        null,
        'Scan-state only. It prevents repeated no-hit title indexing and does not prove ownership.',
        'Keep as operational memory for bounded title traceability proof runs.',
        '{"not_ownership_truth":true,"prevents_repeated_no_hit_indexing":true}'::jsonb,
        now()
    ),
    (
        'landintel_reporting',
        'v_site_title_traceability_scan_state',
        'view',
        'reporting_surface',
        'landintel_reporting',
        'operator title traceability scan-state surface',
        'title_number',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'landintel_store.site_title_traceability_scan_state',
        'Readable view over traceability scan-state. It must not be treated as legal ownership proof.',
        'Use to see which sites have been checked and where no RoS candidate was found.',
        '{"not_ownership_truth":true}'::jsonb,
        now()
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
    metadata = excluded.metadata,
    updated_at = now();

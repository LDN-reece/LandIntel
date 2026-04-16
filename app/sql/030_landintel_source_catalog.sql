create table if not exists landintel.source_catalog (
    id uuid primary key default gen_random_uuid(),
    source_key text not null unique,
    domain text not null,
    source_name text not null,
    source_role text,
    scope text,
    actionable_endpoint text,
    developer_page text,
    access_pattern text,
    auth_type text,
    primary_landintel_use text,
    why_it_matters text,
    primary_output_object text,
    primary_join_method text,
    secondary_join_method text,
    interacts_with text[] not null default '{}'::text[],
    suggested_raw_table text,
    suggested_normalized_table text,
    refresh_cadence text,
    existing_drive_asset boolean,
    existing_asset_note text,
    schema_minimum_fields text[] not null default '{}'::text[],
    critical_notes text,
    workflow_stage text not null default 'catalogued',
    workflow_ready boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.source_endpoint_catalog (
    id uuid primary key default gen_random_uuid(),
    endpoint_key text not null unique,
    source_key text references landintel.source_catalog(source_key) on delete set null,
    endpoint_name text not null,
    endpoint_url text not null,
    endpoint_type text,
    auth_required boolean,
    purpose text,
    notes text,
    endpoint_group text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.entity_blueprint_catalog (
    id uuid primary key default gen_random_uuid(),
    entity_name text not null unique,
    purpose text not null,
    minimum_required_fields text[] not null default '{}'::text[],
    primary_source text,
    primary_join_key text,
    secondary_join_key text,
    feeds_decision text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists landintel_source_catalog_domain_idx on landintel.source_catalog (domain);
create index if not exists landintel_source_catalog_stage_idx on landintel.source_catalog (workflow_stage);
create index if not exists landintel_source_endpoint_catalog_source_key_idx on landintel.source_endpoint_catalog (source_key);
create index if not exists landintel_source_endpoint_catalog_group_idx on landintel.source_endpoint_catalog (endpoint_group);

create or replace view landintel.v_source_catalog_status as
select
    sc.workflow_stage,
    count(*)::bigint as source_count,
    count(*) filter (where sc.workflow_ready)::bigint as ready_count
from landintel.source_catalog as sc
group by sc.workflow_stage
order by sc.workflow_stage;

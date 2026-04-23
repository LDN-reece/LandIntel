create table if not exists public.authority_aoi (
    id uuid primary key default gen_random_uuid(),
    authority_name text not null unique,
    active boolean not null default true,
    geometry geometry(multipolygon, 27700) not null,
    geometry_simplified geometry(multipolygon, 27700),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.source_registry (
    id uuid primary key default gen_random_uuid(),
    source_name text not null,
    source_type text not null,
    publisher text,
    metadata_uuid text,
    endpoint_url text,
    download_url text,
    record_json jsonb not null default '{}'::jsonb,
    geographic_extent geometry(multipolygon, 4326),
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.ingest_runs (
    id uuid primary key default gen_random_uuid(),
    run_type text not null,
    source_name text not null,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    status text not null,
    records_fetched integer not null default 0,
    records_loaded integer not null default 0,
    records_retained integer not null default 0,
    error_message text,
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists staging.ros_cadastral_parcels_raw (
    id bigserial primary key,
    run_id uuid not null references public.ingest_runs(id) on delete cascade,
    source_name text not null,
    source_file text,
    source_county text,
    ros_inspire_id text,
    raw_attributes jsonb not null default '{}'::jsonb,
    geometry geometry(multipolygon, 27700),
    loaded_at timestamptz not null default now()
);

create table if not exists staging.ros_cadastral_parcels_clean (
    id bigserial primary key,
    run_id uuid not null references public.ingest_runs(id) on delete cascade,
    source_name text not null,
    source_file text,
    source_county text,
    ros_inspire_id text,
    raw_attributes jsonb not null default '{}'::jsonb,
    geometry geometry(multipolygon, 27700),
    cleaned_at timestamptz not null default now()
);

create table if not exists public.ros_cadastral_parcels (
    id uuid primary key default gen_random_uuid(),
    ros_inspire_id text,
    authority_name text not null,
    source_county text,
    geometry geometry(multipolygon, 27700) not null,
    centroid geometry(point, 27700),
    area_sqm numeric not null,
    area_ha numeric not null,
    area_acres numeric not null,
    size_bucket text not null,
    size_bucket_label text not null,
    source_name text not null,
    source_file text,
    raw_attributes jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.land_objects (
    id uuid primary key default gen_random_uuid(),
    object_type text not null,
    source_system text not null,
    source_key text not null,
    authority_name text not null,
    geometry geometry(geometry, 27700) not null,
    area_sqm numeric not null,
    area_ha numeric not null,
    area_acres numeric not null,
    size_bucket text not null,
    size_bucket_label text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.land_object_toid_enrichment (
    id uuid primary key default gen_random_uuid(),
    land_object_id uuid not null references public.land_objects(id) on delete cascade,
    toid text not null,
    enrichment_source text not null default 'future_os_enrichment',
    match_method text,
    confidence numeric,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.land_object_title_matches (
    id uuid primary key default gen_random_uuid(),
    land_object_id uuid not null references public.land_objects(id) on delete cascade,
    ros_inspire_id text,
    title_number text,
    match_source text not null default 'future_scotlis_match',
    match_confidence numeric,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.land_object_address_links (
    id uuid primary key default gen_random_uuid(),
    land_object_id uuid not null references public.land_objects(id) on delete cascade,
    uprn text,
    address_text text,
    source_name text not null default 'future_address_linkage',
    linkage_confidence numeric,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);


create table if not exists public.site_reference_aliases (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    reference_family text not null
        check (reference_family in (
            'site_name_alias',
            'source_ref',
            'planning_ref',
            'ldp_ref',
            'hla_ref',
            'ela_ref',
            'vdl_ref',
            'council_ref',
            'title_number',
            'uprn',
            'usrn',
            'toid',
            'authority_ref'
        )),
    raw_reference_value text not null,
    normalised_reference_value text not null,
    source_dataset text not null,
    authority_name text,
    plan_period text,
    site_name_hint text,
    geometry_hash text,
    source_record_id text,
    source_identifier text,
    source_url text,
    relation_type text not null
        check (relation_type in (
            'direct_reference',
            'alias_table',
            'planning_reference',
            'title_linkage',
            'address_linkage',
            'uprn_linkage',
            'usrn_linkage',
            'toid_linkage',
            'geometry_overlap',
            'fuzzy_documentary',
            'manual'
        )),
    status text not null default 'matched'
        check (status in ('matched', 'probable', 'unresolved')),
    linked_confidence numeric,
    match_notes text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_geometry_versions (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    version_label text not null,
    version_status text not null default 'current'
        check (version_status in ('candidate', 'current', 'superseded')),
    source_dataset text not null,
    source_table text not null,
    source_record_id text,
    relation_type text not null
        check (relation_type in ('explicit_identifier', 'spatial_overlap', 'spatial_proximity', 'manual_link', 'canonical_union')),
    geometry_hash text not null,
    match_confidence numeric,
    centroid geometry(point, 27700),
    geometry geometry(geometry, 27700),
    area_sqm numeric,
    source_url text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_reconciliation_matches (
    id uuid primary key default gen_random_uuid(),
    site_id uuid references public.sites(id) on delete cascade,
    source_dataset text not null,
    source_table text not null,
    source_record_id text,
    raw_site_name text,
    raw_reference_value text,
    normalised_reference_value text,
    planning_reference text,
    title_number text,
    uprn text,
    usrn text,
    toid text,
    authority_name text,
    settlement_name text,
    relation_type text not null
        check (relation_type in (
            'direct_reference',
            'alias_table',
            'planning_reference',
            'title_linkage',
            'geometry_overlap',
            'fuzzy_documentary',
            'manual_review',
            'manual'
        )),
    confidence_score numeric,
    status text not null
        check (status in ('matched', 'probable', 'unresolved', 'rejected')),
    geometry_overlap_ratio numeric,
    geometry_distance_m numeric,
    match_notes text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_reconciliation_review_queue (
    id uuid primary key default gen_random_uuid(),
    candidate_site_id uuid references public.sites(id) on delete set null,
    source_dataset text not null,
    source_table text not null,
    source_record_id text,
    raw_site_name text,
    raw_reference_value text,
    normalised_reference_value text,
    planning_reference text,
    authority_name text,
    settlement_name text,
    confidence_score numeric,
    failure_reasons jsonb not null default '[]'::jsonb,
    candidate_matches jsonb not null default '[]'::jsonb,
    status text not null default 'pending'
        check (status in ('pending', 'reviewed', 'resolved', 'dismissed')),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

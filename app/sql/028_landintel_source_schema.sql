create schema if not exists landintel;

create table if not exists landintel.canonical_sites (
    id uuid primary key default gen_random_uuid(),
    site_code text unique,
    site_name_primary text not null,
    authority_name text,
    jurisdiction text not null default 'scotland',
    workflow_status text not null default 'new',
    primary_ros_parcel_id uuid references public.ros_cadastral_parcels(id),
    geometry geometry(MultiPolygon, 27700),
    centroid geometry(Point, 27700),
    area_acres numeric,
    surfaced_reason text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_reference_aliases (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id),
    source_family text not null,
    source_dataset text not null,
    authority_name text,
    plan_period text,
    site_name text,
    raw_reference_value text not null,
    normalized_reference_value text not null,
    planning_reference text,
    geometry_hash text,
    status text not null default 'unresolved' check (status in ('matched', 'probable', 'unresolved', 'rejected')),
    confidence numeric,
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_geometry_versions (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id),
    geometry_source text not null,
    version_label text,
    geometry geometry(MultiPolygon, 27700) not null,
    effective_from timestamptz not null default now(),
    effective_to timestamptz,
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists landintel.site_source_links (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id),
    source_family text not null,
    source_dataset text not null,
    source_record_id text not null,
    link_method text not null check (link_method in ('direct_reference', 'alias_match', 'planning_reference', 'spatial_overlap', 'fuzzy_documentary', 'manual')),
    confidence numeric,
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.planning_application_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id),
    authority_name text not null,
    planning_reference text,
    application_type text,
    proposal_text text,
    application_status text,
    decision text,
    lodged_date date,
    decision_date date,
    appeal_status text,
    refusal_themes text[] not null default '{}'::text[],
    geometry geometry(Geometry, 27700),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.ldp_site_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id),
    authority_name text not null,
    plan_name text,
    plan_period text,
    policy_reference text,
    site_reference text,
    site_name text,
    allocation_status text,
    proposed_use text,
    support_level text,
    policy_constraints text[] not null default '{}'::text[],
    geometry geometry(Geometry, 27700),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.settlement_boundary_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    authority_name text not null,
    settlement_name text not null,
    boundary_role text not null check (boundary_role in ('settlement', 'green_belt')),
    boundary_status text,
    geometry geometry(MultiPolygon, 27700) not null,
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.hla_site_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id),
    authority_name text not null,
    site_reference text,
    site_name text,
    effectiveness_status text,
    programming_horizon text,
    constraint_reasons text[] not null default '{}'::text[],
    developer_name text,
    remaining_capacity integer,
    completions integer,
    tenure text,
    brownfield_indicator boolean,
    geometry geometry(Geometry, 27700),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.bgs_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id),
    authority_name text,
    record_type text not null,
    title text,
    observed_date date,
    severity text,
    geometry geometry(Geometry, 27700),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.flood_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id),
    authority_name text,
    flood_source text not null,
    severity_band text,
    overlap_pct numeric,
    geometry geometry(Geometry, 27700),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.evidence_references (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id),
    source_family text not null,
    source_dataset text not null,
    source_record_id text not null,
    source_reference text,
    source_url text,
    confidence text check (confidence in ('high', 'medium', 'low')),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists landintel.site_signals (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id),
    signal_key text not null,
    signal_value jsonb not null default '{}'::jsonb,
    signal_status text not null default 'derived',
    source_family text not null,
    confidence text check (confidence in ('high', 'medium', 'low')),
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_assessments (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id),
    assessment_version integer not null default 1,
    bucket text,
    monetisation_horizon text,
    dominant_blocker text,
    scores jsonb not null default '{}'::jsonb,
    score_confidence jsonb not null default '{}'::jsonb,
    human_review_required boolean not null default false,
    explanation_text text,
    source_registry_id uuid references public.source_registry(id),
    ingest_run_id uuid references public.ingest_runs(id),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists landintel_canonical_sites_authority_idx on landintel.canonical_sites (authority_name);
create index if not exists landintel_canonical_sites_workflow_idx on landintel.canonical_sites (workflow_status);
create index if not exists landintel_canonical_sites_geometry_idx on landintel.canonical_sites using gist (geometry);
create index if not exists landintel_aliases_canonical_site_idx on landintel.site_reference_aliases (canonical_site_id);
create index if not exists landintel_aliases_source_registry_idx on landintel.site_reference_aliases (source_registry_id);
create index if not exists landintel_aliases_ingest_run_idx on landintel.site_reference_aliases (ingest_run_id);
create index if not exists landintel_aliases_normalized_ref_idx on landintel.site_reference_aliases (normalized_reference_value);
create index if not exists landintel_site_geometry_versions_site_idx on landintel.site_geometry_versions (canonical_site_id);
create index if not exists landintel_site_geometry_versions_geometry_idx on landintel.site_geometry_versions using gist (geometry);
create index if not exists landintel_site_source_links_site_idx on landintel.site_source_links (canonical_site_id);
create index if not exists landintel_site_source_links_source_registry_idx on landintel.site_source_links (source_registry_id);
create index if not exists landintel_site_source_links_ingest_run_idx on landintel.site_source_links (ingest_run_id);
create index if not exists landintel_planning_site_idx on landintel.planning_application_records (canonical_site_id);
create index if not exists landintel_planning_authority_idx on landintel.planning_application_records (authority_name);
create index if not exists landintel_planning_reference_idx on landintel.planning_application_records (planning_reference);
create index if not exists landintel_planning_source_registry_idx on landintel.planning_application_records (source_registry_id);
create index if not exists landintel_planning_ingest_run_idx on landintel.planning_application_records (ingest_run_id);
create index if not exists landintel_planning_geometry_idx on landintel.planning_application_records using gist (geometry);
create index if not exists landintel_hla_site_idx on landintel.hla_site_records (canonical_site_id);
create index if not exists landintel_hla_authority_idx on landintel.hla_site_records (authority_name);
create index if not exists landintel_hla_site_reference_idx on landintel.hla_site_records (site_reference);
create index if not exists landintel_hla_source_registry_idx on landintel.hla_site_records (source_registry_id);
create index if not exists landintel_hla_ingest_run_idx on landintel.hla_site_records (ingest_run_id);
create index if not exists landintel_hla_geometry_idx on landintel.hla_site_records using gist (geometry);
create index if not exists landintel_ldp_site_idx on landintel.ldp_site_records (canonical_site_id);
create index if not exists landintel_ldp_source_registry_idx on landintel.ldp_site_records (source_registry_id);
create index if not exists landintel_ldp_ingest_run_idx on landintel.ldp_site_records (ingest_run_id);
create index if not exists landintel_ldp_geometry_idx on landintel.ldp_site_records using gist (geometry);
create index if not exists landintel_settlement_source_registry_idx on landintel.settlement_boundary_records (source_registry_id);
create index if not exists landintel_settlement_ingest_run_idx on landintel.settlement_boundary_records (ingest_run_id);
create index if not exists landintel_settlement_geometry_idx on landintel.settlement_boundary_records using gist (geometry);
create index if not exists landintel_bgs_site_idx on landintel.bgs_records (canonical_site_id);
create index if not exists landintel_bgs_source_registry_idx on landintel.bgs_records (source_registry_id);
create index if not exists landintel_bgs_ingest_run_idx on landintel.bgs_records (ingest_run_id);
create index if not exists landintel_bgs_record_type_idx on landintel.bgs_records (record_type);
create index if not exists landintel_bgs_geometry_idx on landintel.bgs_records using gist (geometry);
create index if not exists landintel_flood_site_idx on landintel.flood_records (canonical_site_id);
create index if not exists landintel_flood_source_registry_idx on landintel.flood_records (source_registry_id);
create index if not exists landintel_flood_ingest_run_idx on landintel.flood_records (ingest_run_id);
create index if not exists landintel_flood_geometry_idx on landintel.flood_records using gist (geometry);
create index if not exists landintel_evidence_site_idx on landintel.evidence_references (canonical_site_id);
create index if not exists landintel_evidence_source_registry_idx on landintel.evidence_references (source_registry_id);
create index if not exists landintel_evidence_ingest_run_idx on landintel.evidence_references (ingest_run_id);
create index if not exists landintel_site_signals_site_idx on landintel.site_signals (canonical_site_id);
create index if not exists landintel_site_signals_source_registry_idx on landintel.site_signals (source_registry_id);
create index if not exists landintel_site_signals_ingest_run_idx on landintel.site_signals (ingest_run_id);
create index if not exists landintel_site_assessments_site_idx on landintel.site_assessments (canonical_site_id);
create index if not exists landintel_site_assessments_source_registry_idx on landintel.site_assessments (source_registry_id);
create index if not exists landintel_site_assessments_ingest_run_idx on landintel.site_assessments (ingest_run_id);

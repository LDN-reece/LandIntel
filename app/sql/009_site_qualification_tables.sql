create table if not exists public.sites (
    id uuid primary key default gen_random_uuid(),
    site_code text not null unique,
    site_name text not null,
    workflow_status text not null default 'new'
        check (workflow_status in ('new', 'under_review', 'shortlisted', 'rejected', 'further_dd_required', 'progressed')),
    source_method text not null default 'manual',
    primary_land_object_id uuid references public.land_objects(id) on delete set null,
    primary_ros_parcel_id uuid references public.ros_cadastral_parcels(id) on delete set null,
    surfaced_for_review boolean not null default true,
    surfaced_reason text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_locations (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null unique references public.sites(id) on delete cascade,
    authority_name text not null,
    county text,
    postcode text,
    nearest_settlement text,
    settlement_relationship text,
    within_settlement_boundary boolean,
    distance_to_settlement_boundary_m numeric,
    source_dataset text not null default 'site_location',
    source_record_id text,
    source_url text,
    centroid geometry(point, 27700),
    geometry geometry(geometry, 27700),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_parcels (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    land_object_id uuid references public.land_objects(id) on delete set null,
    ros_parcel_id uuid references public.ros_cadastral_parcels(id) on delete set null,
    title_number text,
    parcel_reference text,
    source_dataset text not null default 'site_parcel',
    source_record_id text,
    source_url text,
    is_primary boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_geometry_components (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    component_role text not null default 'boundary_input',
    source_table text not null,
    source_record_id text not null,
    source_identifier text,
    source_dataset text not null,
    relation_type text not null
        check (relation_type in ('explicit_identifier', 'spatial_overlap', 'spatial_proximity', 'manual_link')),
    overlap_area_sqm numeric,
    overlap_ratio numeric,
    distance_m numeric,
    is_primary boolean not null default false,
    source_url text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.planning_records (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    record_type text not null,
    application_reference text,
    application_outcome text,
    application_status text,
    decision_date date,
    description text,
    source_dataset text not null,
    source_record_id text,
    source_url text,
    import_version text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.planning_context_records (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    context_type text not null,
    context_status text,
    context_label text,
    distance_m numeric,
    description text,
    source_dataset text not null,
    source_record_id text,
    source_url text,
    import_version text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_constraints (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    constraint_type text not null,
    severity text not null
        check (severity in ('none', 'low', 'medium', 'high', 'unknown')),
    status text,
    distance_m numeric,
    description text,
    source_dataset text not null,
    source_record_id text,
    source_url text,
    import_version text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.comparable_market_records (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    comparable_type text not null,
    address text,
    transaction_type text,
    price_gbp numeric,
    price_per_sqft_gbp numeric,
    sale_date date,
    distance_m numeric,
    record_strength text
        check (record_strength in ('low', 'medium', 'high', 'unknown')),
    description text,
    source_dataset text not null,
    source_record_id text,
    source_url text,
    import_version text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.buyer_profiles (
    id uuid primary key default gen_random_uuid(),
    profile_code text not null unique,
    buyer_name text not null,
    target_strategy text not null,
    min_acres numeric,
    max_acres numeric,
    preferred_authorities text[] not null default '{}'::text[],
    min_price_per_sqft_gbp numeric,
    notes text,
    is_active boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_buyer_matches (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    buyer_profile_id uuid not null references public.buyer_profiles(id) on delete cascade,
    fit_rating text not null
        check (fit_rating in ('strong', 'moderate', 'weak', 'excluded')),
    match_reason text,
    evidence_summary text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_analysis_runs (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    run_type text not null default 'rule_engine',
    ruleset_version text not null,
    status text not null
        check (status in ('running', 'completed', 'failed')),
    triggered_by text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    completed_at timestamptz
);

create table if not exists public.site_signals (
    id uuid primary key default gen_random_uuid(),
    analysis_run_id uuid not null references public.site_analysis_runs(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    signal_key text not null,
    signal_label text not null,
    signal_group text not null,
    value_type text not null
        check (value_type in ('boolean', 'numeric', 'text', 'json')),
    signal_state text not null default 'known'
        check (signal_state in ('known', 'unknown', 'inferred')),
    bool_value boolean,
    numeric_value numeric,
    text_value text,
    json_value jsonb,
    reasoning text,
    created_at timestamptz not null default now()
);

create table if not exists public.site_interpretations (
    id uuid primary key default gen_random_uuid(),
    analysis_run_id uuid not null references public.site_analysis_runs(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    interpretation_key text not null,
    category text not null
        check (category in ('positive', 'risk', 'possible_fatal', 'unknown')),
    title text not null,
    summary text not null,
    reasoning text not null,
    rule_code text not null,
    priority integer not null default 100,
    created_at timestamptz not null default now()
);

create table if not exists public.evidence_references (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    source_table text not null,
    source_record_id text,
    dataset_name text not null,
    source_identifier text,
    source_url text,
    observed_at timestamptz,
    import_version text,
    confidence_label text,
    confidence_score numeric,
    assertion text not null,
    excerpt text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.site_signal_evidence (
    signal_id uuid not null references public.site_signals(id) on delete cascade,
    evidence_reference_id uuid not null references public.evidence_references(id) on delete cascade,
    created_at timestamptz not null default now(),
    primary key (signal_id, evidence_reference_id)
);

create table if not exists public.site_interpretation_evidence (
    interpretation_id uuid not null references public.site_interpretations(id) on delete cascade,
    evidence_reference_id uuid not null references public.evidence_references(id) on delete cascade,
    created_at timestamptz not null default now(),
    primary key (interpretation_id, evidence_reference_id)
);

create table if not exists public.site_review_status_history (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    workflow_status text not null
        check (workflow_status in ('new', 'under_review', 'shortlisted', 'rejected', 'further_dd_required', 'progressed')),
    note text,
    changed_by text not null default 'system',
    created_at timestamptz not null default now()
);

create table if not exists public.site_refresh_queue (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    trigger_source text not null,
    source_table text,
    source_record_id text,
    refresh_scope text not null default 'signals_and_interpretations',
    status text not null default 'pending'
        check (status in ('pending', 'processing', 'completed', 'failed')),
    requested_at timestamptz not null default now(),
    processed_at timestamptz,
    error_message text,
    metadata jsonb not null default '{}'::jsonb
);

create table if not exists analytics.site_search_cache (
    site_id uuid primary key references public.sites(id) on delete cascade,
    site_code text not null,
    site_name text not null,
    workflow_status text not null,
    authority_name text,
    nearest_settlement text,
    settlement_relationship text,
    area_acres numeric,
    parcel_count bigint,
    component_count bigint,
    primary_title_number text,
    within_settlement_boundary boolean,
    distance_to_settlement_boundary_m numeric,
    previous_application_exists boolean,
    previous_application_outcome text,
    allocation_status text,
    supportive_nearby_growth_context boolean,
    flood_risk text,
    mining_risk text,
    access_status text,
    critical_constraint_count numeric,
    new_build_comparable_strength text,
    comparable_sale_count numeric,
    buyer_fit_count numeric,
    positive_count bigint not null default 0,
    risk_count bigint not null default 0,
    possible_fatal_count bigint not null default 0,
    unknown_count bigint not null default 0,
    surfaced_reason text,
    current_analysis_run_id uuid,
    current_ruleset_version text,
    updated_at timestamptz not null default now()
);

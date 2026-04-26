create schema if not exists landintel;

create table if not exists landintel.canonical_sites (
    id uuid primary key default gen_random_uuid(),
    site_code text not null,
    site_name_primary text,
    authority_name text,
    jurisdiction text not null default 'scotland',
    workflow_status text not null default 'new_candidate',
    primary_ros_parcel_id uuid references public.ros_cadastral_parcels(id) on delete set null,
    geometry geometry(geometry, 27700),
    centroid geometry(point, 27700),
    area_acres numeric,
    surfaced_reason text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.planning_application_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    planning_reference text,
    application_type text,
    proposal_text text,
    application_status text,
    decision text,
    lodged_date date,
    decision_date date,
    appeal_status text,
    refusal_themes text[] not null default '{}'::text[],
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.hla_site_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
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
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.ldp_site_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    plan_name text,
    plan_period text,
    policy_reference text,
    site_reference text,
    site_name text,
    allocation_status text,
    proposed_use text,
    support_level text,
    policy_constraints text[] not null default '{}'::text[],
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.settlement_boundary_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    authority_name text,
    settlement_name text,
    boundary_role text,
    boundary_status text,
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.bgs_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    record_type text,
    title text,
    observed_date date,
    severity text,
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.flood_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    flood_source text,
    severity_band text,
    overlap_pct numeric,
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.ela_site_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    site_reference text,
    site_name text,
    employment_status text,
    proposed_use text,
    owner_name text,
    brownfield_indicator boolean,
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.vdl_site_records (
    id uuid primary key default gen_random_uuid(),
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    site_reference text,
    site_name text,
    vacancy_status text,
    derelict_status text,
    previous_use text,
    brownfield_indicator boolean,
    geometry geometry(geometry, 27700),
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_source_links (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_family text not null,
    source_dataset text not null,
    source_record_id text not null,
    link_method text,
    confidence numeric,
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    reconcile_state_id uuid,
    active_flag boolean not null default true,
    retired_at timestamptz
);

create table if not exists landintel.site_reference_aliases (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_family text not null,
    source_dataset text not null,
    authority_name text,
    plan_period text,
    site_name text,
    raw_reference_value text,
    normalized_reference_value text,
    planning_reference text,
    geometry_hash text,
    status text not null default 'matched',
    confidence numeric,
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    reconcile_state_id uuid,
    active_flag boolean not null default true,
    retired_at timestamptz
);

create table if not exists landintel.evidence_references (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_family text not null,
    source_dataset text not null,
    source_record_id text,
    source_reference text,
    source_url text,
    confidence text,
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    reconcile_state_id uuid,
    active_flag boolean not null default true,
    retired_at timestamptz
);

create table if not exists landintel.source_reconcile_state (
    id uuid primary key default gen_random_uuid(),
    source_family text not null,
    source_dataset text not null,
    authority_name text not null,
    source_record_id text not null,
    active_flag boolean not null default true,
    lifecycle_status text not null default 'active',
    current_source_signature text,
    current_geometry_hash text,
    last_seen_ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    last_seen_at timestamptz,
    last_processed_at timestamptz,
    current_canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    previous_canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    match_method text,
    match_confidence numeric,
    publish_state text not null default 'pending',
    review_required boolean not null default false,
    review_reason_code text,
    candidate_site_ids uuid[] not null default '{}'::uuid[],
    last_queue_item_id uuid,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.source_reconcile_queue (
    id uuid primary key default gen_random_uuid(),
    state_id uuid not null references landintel.source_reconcile_state(id) on delete cascade,
    source_family text not null,
    source_dataset text not null,
    authority_name text not null,
    source_record_id text not null,
    work_type text not null,
    priority integer not null default 100,
    status text not null default 'pending',
    source_signature text,
    geometry_hash text,
    previous_canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    candidate_site_ids uuid[] not null default '{}'::uuid[],
    claimed_by text,
    claimed_at timestamptz,
    lease_expires_at timestamptz,
    attempt_count integer not null default 0,
    next_attempt_at timestamptz,
    processed_at timestamptz,
    error_code text,
    error_message text,
    review_reason_code text,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_signals (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    signal_key text not null,
    signal_value jsonb not null default '{}'::jsonb,
    signal_status text not null default 'unknown',
    source_family text not null default 'system',
    confidence text not null default 'low',
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table landintel.site_signals
    add column if not exists signal_label text,
    add column if not exists signal_group text,
    add column if not exists fact_label text,
    add column if not exists reasoning text;

create table if not exists landintel.site_assessments (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    assessment_version integer not null default 1,
    bucket text,
    monetisation_horizon text,
    dominant_blocker text,
    scores jsonb not null default '{}'::jsonb,
    score_confidence jsonb not null default '{}'::jsonb,
    human_review_required boolean not null default false,
    explanation_text text,
    source_registry_id uuid references public.source_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table landintel.site_assessments
    add column if not exists overall_tier text,
    add column if not exists overall_rank_score numeric,
    add column if not exists queue_recommendation text,
    add column if not exists why_it_surfaced text,
    add column if not exists why_it_survived text,
    add column if not exists good_items jsonb not null default '[]'::jsonb,
    add column if not exists bad_items jsonb not null default '[]'::jsonb,
    add column if not exists ugly_items jsonb not null default '[]'::jsonb,
    add column if not exists subrank_summary jsonb not null default '{}'::jsonb,
    add column if not exists title_state text,
    add column if not exists ownership_control_fact_label text,
    add column if not exists resurfaced_reason text,
    add column if not exists latest_assessment_at timestamptz;

create table if not exists landintel.canonical_site_refresh_queue (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    refresh_scope text not null default 'assessment',
    trigger_source text not null default 'manual',
    source_family text,
    source_record_id text,
    status text not null default 'pending',
    claimed_by text,
    claimed_at timestamptz,
    lease_expires_at timestamptz,
    attempt_count integer not null default 0,
    next_attempt_at timestamptz,
    processed_at timestamptz,
    error_message text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_review_events (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    event_type text not null,
    review_status text,
    actor_name text not null default 'system',
    note_text text,
    reason_text text,
    title_number text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint site_review_events_type_check check (
        event_type in (
            'status_change',
            'note',
            'manual_override',
            'buy_title_now',
            'title_ordered',
            'title_reviewed',
            'manual_revisit'
        )
    ),
    constraint site_review_events_status_check check (
        review_status is null or review_status in (
            'New candidate',
            'Queued for review',
            'Under review',
            'Need more evidence',
            'Rejected',
            'Watchlist',
            'Conditional',
            'Strong candidate',
            'Buy title now',
            'Title ordered',
            'Title reviewed',
            'Likely missed / controlled',
            'Not for us',
            'Agency angle only',
            'Parked'
        )
    )
);

create table if not exists landintel.site_manual_overrides (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    override_key text not null,
    override_value jsonb not null default '{}'::jsonb,
    actor_name text not null default 'system',
    reason_text text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists landintel.site_change_events (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_family text not null,
    change_category text not null,
    event_type text not null,
    event_summary text not null,
    source_record_id text,
    alert_priority text not null default 'normal',
    resurfaced_flag boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint site_change_events_priority_check check (
        alert_priority in ('normal', 'high', 'critical')
    )
);

create table if not exists landintel.site_geometry_diagnostics (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null unique references landintel.canonical_sites(id) on delete cascade,
    original_area_acres numeric,
    component_count integer,
    parcel_count integer,
    bbox_width_m numeric,
    bbox_height_m numeric,
    shape_compactness numeric,
    indicative_clean_area_acres numeric,
    indicative_usable_area_ratio numeric,
    sliver_flag boolean not null default false,
    fragmentation_flag boolean not null default false,
    width_depth_warning boolean not null default false,
    access_only_warning boolean not null default false,
    infrastructure_heavy_warning boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_spatial_links (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    linked_record_table text not null,
    linked_record_id text not null,
    linked_object_type text,
    link_role text,
    link_method text,
    link_distance_m numeric,
    overlap_area_sqm numeric,
    overlap_pct_of_site numeric,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_title_validation (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    title_number text,
    normalized_title_number text,
    matched_title_number text,
    validation_status text not null default 'commercial_inference',
    validation_method text,
    confidence numeric,
    title_source text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.constraint_layer_registry (
    id uuid primary key default gen_random_uuid(),
    layer_key text not null,
    layer_name text not null,
    source_name text,
    source_family text,
    constraint_group text,
    constraint_type text,
    geometry_type text,
    measurement_mode text,
    buffer_distance_m numeric,
    is_active boolean not null default true,
    source_registry_id uuid references public.source_registry(id) on delete set null,
    legacy_site_constraints_key text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.constraint_source_features (
    id uuid primary key default gen_random_uuid(),
    constraint_layer_id uuid not null references public.constraint_layer_registry(id) on delete cascade,
    source_feature_key text not null,
    feature_name text,
    source_reference text,
    authority_name text,
    severity_label text,
    source_url text,
    effective_from date,
    effective_to date,
    geometry geometry(geometry, 27700),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_constraint_measurements (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    constraint_layer_id uuid not null references public.constraint_layer_registry(id) on delete cascade,
    constraint_feature_id uuid references public.constraint_source_features(id) on delete cascade,
    measurement_source text,
    intersects boolean not null default false,
    within_buffer boolean not null default false,
    site_inside_feature boolean not null default false,
    feature_inside_site boolean not null default false,
    overlap_area_sqm numeric,
    overlap_pct_of_site numeric,
    overlap_pct_of_feature numeric,
    nearest_distance_m numeric,
    buffer_distance_m numeric,
    measured_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_constraint_group_summaries (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    constraint_layer_id uuid not null references public.constraint_layer_registry(id) on delete cascade,
    constraint_group text not null,
    summary_scope text not null default 'site',
    intersecting_feature_count integer not null default 0,
    buffered_feature_count integer not null default 0,
    total_overlap_area_sqm numeric,
    max_overlap_pct_of_site numeric,
    min_distance_m numeric,
    nearest_feature_id uuid references public.constraint_source_features(id) on delete set null,
    nearest_feature_name text,
    measured_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_commercial_friction_facts (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    constraint_group text not null,
    constraint_layer_id uuid references public.constraint_layer_registry(id) on delete set null,
    fact_key text not null,
    fact_label text not null,
    fact_value_text text,
    fact_value_numeric numeric,
    fact_unit text,
    fact_basis text,
    source_measurement_id uuid references public.site_constraint_measurements(id) on delete set null,
    source_summary_id uuid references public.site_constraint_group_summaries(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

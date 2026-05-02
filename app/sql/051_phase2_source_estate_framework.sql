alter table landintel.source_estate_registry
    add column if not exists programme_phase text not null default 'phase_one',
    add column if not exists module_key text,
    add column if not exists geography text,
    add column if not exists access_status text not null default 'source_registered',
    add column if not exists ingest_status text not null default 'source_registered',
    add column if not exists normalisation_status text not null default 'source_registered',
    add column if not exists site_link_status text not null default 'source_registered',
    add column if not exists measurement_status text not null default 'source_registered',
    add column if not exists evidence_status text not null default 'source_registered',
    add column if not exists signal_status text not null default 'source_registered',
    add column if not exists assessment_status text not null default 'source_registered',
    add column if not exists trusted_for_review boolean not null default false,
    add column if not exists limitation_notes text,
    add column if not exists next_action text,
    add column if not exists lifecycle_metadata jsonb not null default '{}'::jsonb;

alter table landintel.source_estate_registry
    drop constraint if exists source_estate_lifecycle_status_check;

alter table landintel.source_estate_registry
    add constraint source_estate_lifecycle_status_check
    check (
        access_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and ingest_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and normalisation_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and site_link_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and measurement_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and evidence_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and signal_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
        and assessment_status = any (array[
            'source_registered', 'access_required', 'access_confirmed', 'source_registered_no_rows',
            'raw_data_landed', 'normalised', 'linked_to_site', 'measured',
            'evidence_generated', 'signals_generated', 'assessment_ready',
            'trusted_for_review', 'gated', 'failed', 'stale'
        ]::text[])
    );

create index if not exists landintel_source_estate_phase2_module_idx
    on landintel.source_estate_registry (programme_phase, module_key, source_family);

create index if not exists landintel_source_estate_lifecycle_idx
    on landintel.source_estate_registry (
        access_status,
        ingest_status,
        normalisation_status,
        site_link_status,
        measurement_status,
        evidence_status,
        signal_status,
        assessment_status
    );

alter table landintel.canonical_site_refresh_queue
    drop constraint if exists canonical_site_refresh_queue_family_check;

alter table landintel.canonical_site_refresh_queue
    add constraint canonical_site_refresh_queue_family_check
    check (
        source_family is null
        or source_family = any (array[
            'planning',
            'hla',
            'ela',
            'vdl',
            'sepa_flood',
            'coal_authority',
            'hes',
            'naturescot',
            'contaminated_land',
            'tpo',
            'culverts',
            'conservation_areas',
            'greenbelt',
            'topography',
            'os_places',
            'os_features',
            'os_linked_identifiers',
            'os_openmap_local',
            'os_open_roads',
            'os_open_rivers',
            'os_boundary_line',
            'os_open_names',
            'os_open_greenspace',
            'os_open_zoomstack',
            'os_open_toid',
            'os_open_built_up_areas',
            'os_open_uprn',
            'os_open_usrn',
            'osm',
            'naptan',
            'statistics_gov_scot',
            'opentopography_srtm',
            'local_landscape_areas',
            'local_nature',
            'forestry_woodland',
            'sgn_assets',
            'planning_appeals',
            'title_control',
            'corporate_control',
            'power_infrastructure',
            'terrain_abnormal',
            'market_context',
            'amenities',
            'demographics',
            'planning_documents',
            'local_intelligence'
        ]::text[])
    );

create table if not exists landintel.planning_appeal_records (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'planning_appeals',
    source_record_id text not null,
    appeal_reference text,
    original_application_reference text,
    authority_name text,
    appellant text,
    site_address text,
    decision text,
    decision_date date,
    reporter_reasoning text,
    policy_references text[] not null default '{}'::text[],
    geometry geometry(Geometry, 27700),
    source_url text,
    source_estate_registry_id uuid references landintel.source_estate_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.planning_appeal_documents (
    id uuid primary key default gen_random_uuid(),
    appeal_record_id uuid references landintel.planning_appeal_records(id) on delete cascade,
    source_key text not null,
    source_family text not null default 'planning_appeals',
    source_record_id text not null,
    appeal_reference text,
    document_type text,
    document_url text,
    published_at timestamptz,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_planning_appeal_links (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    planning_appeal_record_id uuid references landintel.planning_appeal_records(id) on delete cascade,
    source_family text not null default 'planning_appeals',
    link_method text not null,
    link_confidence numeric,
    matched_reference text,
    distance_m numeric,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.appeal_issue_tags (
    id uuid primary key default gen_random_uuid(),
    planning_appeal_record_id uuid references landintel.planning_appeal_records(id) on delete cascade,
    source_family text not null default 'planning_appeals',
    issue_category text not null,
    issue_label text,
    source_excerpt text,
    confidence numeric,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.title_order_workflow (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    site_location_id text not null,
    source_key text not null default 'title_readiness_internal',
    source_family text not null default 'title_control',
    title_number text,
    normalized_title_number text,
    parcel_candidate_status text not null default 'title_required',
    possible_title_reference_status text not null default 'title_required',
    ownership_status_pre_title text not null default 'ownership_not_confirmed',
    title_required_flag boolean not null default true,
    title_order_status text not null default 'not_ordered',
    title_review_status text not null default 'not_reviewed',
    title_confidence_level numeric,
    planning_applicant_signal text,
    control_signal_summary text,
    next_action text not null default 'review_site_before_title_spend',
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.title_review_records (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    title_order_workflow_id uuid references landintel.title_order_workflow(id) on delete set null,
    source_key text not null default 'title_review_manual',
    source_family text not null default 'title_control',
    title_number text not null,
    normalized_title_number text,
    registered_proprietor text,
    proprietor_type text,
    company_number text,
    acquisition_date date,
    price_paid numeric,
    securities_summary text,
    burdens_summary text,
    reviewer text,
    review_date date,
    ownership_outcome text not null default 'ownership_unclear_after_review',
    next_action text,
    document_reference text,
    document_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, normalized_title_number)
);

create table if not exists landintel.ownership_control_signals (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_key text not null,
    source_family text not null default 'title_control',
    source_record_id text not null,
    signal_type text not null,
    signal_label text not null,
    signal_value_text text,
    confidence numeric,
    evidence_required boolean not null default true,
    ownership_confirmed boolean not null default false,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.corporate_owner_links (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    title_review_record_id uuid references landintel.title_review_records(id) on delete set null,
    source_key text not null default 'companies_house_control_context',
    source_family text not null default 'corporate_control',
    company_name text not null,
    company_number text,
    match_method text,
    match_confidence numeric,
    link_basis text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.known_controlled_sites (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'title_review_manual',
    source_family text not null default 'title_control',
    control_basis text not null,
    controller_name text,
    controller_type text,
    evidence_reference text,
    reviewer text,
    review_date date,
    next_action text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.power_assets (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'power_infrastructure',
    source_record_id text not null,
    authority_name text,
    asset_name text,
    asset_type text,
    dno_region text,
    capacity_status text,
    source_url text,
    updated_source_at timestamptz,
    geometry geometry(Geometry, 27700),
    source_estate_registry_id uuid references landintel.source_estate_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.power_capacity_zones (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'power_infrastructure',
    source_record_id text not null,
    zone_name text,
    dno_region text,
    capacity_status text,
    capacity_basis text,
    source_url text,
    geometry geometry(Geometry, 27700),
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_power_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'power_infrastructure_context',
    source_family text not null default 'power_infrastructure',
    nearest_asset_id uuid references landintel.power_assets(id) on delete set null,
    nearest_asset_type text,
    nearest_asset_distance_m numeric,
    overhead_line_crosses_site boolean,
    infrastructure_within_buffer boolean,
    dno_region text,
    capacity_context text not null default 'capacity_unknown',
    review_fact text not null default 'dno_review_required',
    measured_at timestamptz not null default now(),
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.infrastructure_friction_facts (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'power_infrastructure_context',
    source_family text not null default 'power_infrastructure',
    fact_key text not null,
    fact_label text not null,
    fact_value_text text,
    fact_value_numeric numeric,
    fact_unit text,
    evidence_basis text not null,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_terrain_metrics (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'os_terrain_context',
    source_family text not null default 'terrain_abnormal',
    mean_slope_degrees numeric,
    max_slope_degrees numeric,
    min_elevation_m numeric,
    max_elevation_m numeric,
    terrain_source text,
    measured_at timestamptz not null default now(),
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, source_key)
);

create table if not exists landintel.site_slope_profiles (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'os_terrain_context',
    source_family text not null default 'terrain_abnormal',
    slope_band text not null,
    area_sqm numeric,
    pct_of_site numeric,
    measured_at timestamptz not null default now(),
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, slope_band)
);

create table if not exists landintel.site_cut_fill_risk (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'terrain_abnormal_context',
    source_family text not null default 'terrain_abnormal',
    review_flag text not null default 'cut_fill_review_recommended',
    basis text,
    confidence numeric,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.site_ground_risk_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'terrain_abnormal_context',
    source_family text not null default 'terrain_abnormal',
    boreholes_within_250m integer,
    boreholes_within_500m integer,
    boreholes_within_1km integer,
    mining_constraint_present boolean not null default false,
    flood_constraint_present boolean not null default false,
    culvert_constraint_present boolean not null default false,
    measured_constraint_count integer not null default 0,
    abnormal_review_fact text not null default 'abnormal_cost_review_required',
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    measured_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.abnormal_cost_benchmarks (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'terrain_abnormal',
    benchmark_reference text not null,
    benchmark_type text,
    benchmark_value numeric,
    benchmark_unit text,
    evidence_basis text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, benchmark_reference)
);

create table if not exists landintel.site_abnormal_cost_flags (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'terrain_abnormal_context',
    source_family text not null default 'terrain_abnormal',
    flag_key text not null,
    flag_label text not null,
    flag_value_text text,
    confidence numeric,
    evidence_basis text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, flag_key)
);

create table if not exists landintel.market_transactions (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'market_context',
    source_record_id text not null,
    authority_name text,
    transaction_date date,
    price numeric,
    property_type text,
    address_text text,
    geometry geometry(Geometry, 27700),
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.epc_property_attributes (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'market_context',
    source_record_id text not null,
    authority_name text,
    address_text text,
    property_type text,
    floor_area_sqm numeric,
    epc_rating text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.market_area_metrics (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'market_context',
    area_code text not null,
    area_name text,
    authority_name text,
    metric_name text not null,
    metric_value numeric,
    metric_unit text,
    period_start date,
    period_end date,
    confidence text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, area_code, metric_name, period_end)
);

create table if not exists landintel.site_market_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'market_context_derived',
    source_family text not null default 'market_context',
    authority_name text,
    market_confidence_tier text,
    evidence_summary text,
    latest_metric_period date,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.internal_comparable_evidence (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'internal_comparable_evidence',
    source_family text not null default 'market_context',
    comparable_reference text not null,
    evidence_summary text,
    evidence_date date,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, comparable_reference)
);

create table if not exists landintel.buyer_bid_evidence (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'buyer_bid_evidence',
    source_family text not null default 'market_context',
    bid_reference text not null,
    buyer_name text,
    bid_date date,
    evidence_summary text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, bid_reference)
);

create table if not exists landintel.amenity_assets (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'amenities',
    source_record_id text not null,
    authority_name text,
    amenity_type text not null,
    amenity_name text,
    source_url text,
    geometry geometry(Geometry, 27700),
    source_estate_registry_id uuid references landintel.source_estate_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_amenity_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'amenity_context_derived',
    source_family text not null default 'amenities',
    amenity_type text not null,
    nearest_amenity_asset_id uuid references landintel.amenity_assets(id) on delete set null,
    nearest_amenity_name text,
    nearest_distance_m numeric,
    count_within_400m integer,
    count_within_800m integer,
    count_within_1600m integer,
    measured_at timestamptz not null default now(),
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, amenity_type)
);

create table if not exists landintel.location_strength_facts (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'amenity_context_derived',
    source_family text not null default 'amenities',
    fact_key text not null,
    fact_label text not null,
    fact_value_text text,
    fact_value_numeric numeric,
    fact_unit text,
    evidence_basis text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, fact_key)
);

create table if not exists landintel.open_location_spine_features (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null,
    source_record_id text not null,
    feature_type text not null,
    feature_name text,
    source_layer text,
    source_url text,
    geometry geometry(Geometry, 27700),
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_open_location_spine_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null,
    source_family text not null,
    feature_type text not null,
    nearest_feature_id uuid references landintel.open_location_spine_features(id) on delete set null,
    nearest_feature_name text,
    nearest_distance_m numeric,
    count_within_400m integer,
    count_within_800m integer,
    count_within_1600m integer,
    measured_at timestamptz not null default now(),
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id, source_key, feature_type)
);

create table if not exists landintel.demographic_area_metrics (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'demographics',
    area_code text not null,
    area_name text,
    area_type text,
    authority_name text,
    metric_name text not null,
    metric_value numeric,
    metric_unit text,
    period_start date,
    period_end date,
    confidence text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, area_code, metric_name, period_end)
);

create table if not exists landintel.site_demographic_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'demographic_context_derived',
    source_family text not null default 'demographics',
    area_code text,
    area_name text,
    area_type text,
    context_summary text,
    evidence_confidence text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.housing_demand_context (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'demographics',
    authority_name text not null,
    metric_name text not null,
    metric_value numeric,
    metric_unit text,
    period_start date,
    period_end date,
    evidence_summary text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, authority_name, metric_name, period_end)
);

create table if not exists landintel.planning_document_records (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'planning_documents',
    source_record_id text not null,
    application_reference text,
    authority_name text,
    document_type text,
    document_title text,
    document_url text,
    publication_date date,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.planning_document_extractions (
    id uuid primary key default gen_random_uuid(),
    planning_document_record_id uuid references landintel.planning_document_records(id) on delete cascade,
    source_family text not null default 'planning_documents',
    extraction_type text not null,
    extracted_text text,
    issue_category text,
    confidence numeric,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.section75_obligation_records (
    id uuid primary key default gen_random_uuid(),
    source_key text not null default 'section75_records',
    source_family text not null default 'planning_documents',
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    application_reference text,
    authority_name text,
    obligation_type text,
    obligation_summary text,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_planning_document_links (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    planning_document_record_id uuid references landintel.planning_document_records(id) on delete cascade,
    source_family text not null default 'planning_documents',
    link_method text not null,
    link_confidence numeric,
    matched_reference text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.intelligence_event_records (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null default 'local_intelligence',
    source_record_id text not null,
    authority_name text,
    settlement_name text,
    event_type text not null,
    event_title text,
    event_summary text,
    date_published date,
    source_url text,
    confidence numeric,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_intelligence_links (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    intelligence_event_record_id uuid references landintel.intelligence_event_records(id) on delete cascade,
    source_family text not null default 'local_intelligence',
    link_method text not null,
    link_confidence numeric,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.settlement_intelligence_links (
    id uuid primary key default gen_random_uuid(),
    intelligence_event_record_id uuid references landintel.intelligence_event_records(id) on delete cascade,
    settlement_name text not null,
    authority_name text,
    link_method text not null,
    link_confidence numeric,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists planning_appeal_records_source_uidx on landintel.planning_appeal_records (source_key, source_record_id);
create unique index if not exists planning_appeal_documents_source_uidx on landintel.planning_appeal_documents (source_key, source_record_id);
create unique index if not exists site_planning_appeal_links_uidx on landintel.site_planning_appeal_links (canonical_site_id, planning_appeal_record_id);
create unique index if not exists appeal_issue_tags_uidx on landintel.appeal_issue_tags (planning_appeal_record_id, issue_category, coalesce(issue_label, ''));
create unique index if not exists ownership_control_signals_uidx on landintel.ownership_control_signals (canonical_site_id, source_family, source_record_id, signal_type);
create unique index if not exists corporate_owner_links_uidx on landintel.corporate_owner_links (coalesce(canonical_site_id, '00000000-0000-0000-0000-000000000000'::uuid), source_key, coalesce(company_number, company_name));
create unique index if not exists known_controlled_sites_uidx on landintel.known_controlled_sites (canonical_site_id, control_basis);
create unique index if not exists power_assets_source_uidx on landintel.power_assets (source_key, source_record_id);
create unique index if not exists power_capacity_zones_source_uidx on landintel.power_capacity_zones (source_key, source_record_id);
create unique index if not exists infrastructure_friction_facts_uidx on landintel.infrastructure_friction_facts (canonical_site_id, source_family, fact_key);
create unique index if not exists market_transactions_source_uidx on landintel.market_transactions (source_key, source_record_id);
create unique index if not exists epc_property_attributes_source_uidx on landintel.epc_property_attributes (source_key, source_record_id);
create unique index if not exists amenity_assets_source_uidx on landintel.amenity_assets (source_key, source_record_id);
create unique index if not exists open_location_spine_features_source_uidx on landintel.open_location_spine_features (source_key, source_record_id);
create unique index if not exists planning_document_records_source_uidx on landintel.planning_document_records (source_key, source_record_id);
create unique index if not exists planning_document_extractions_uidx on landintel.planning_document_extractions (planning_document_record_id, extraction_type, coalesce(issue_category, ''));
create unique index if not exists section75_obligation_records_source_uidx on landintel.section75_obligation_records (source_key, source_record_id);
create unique index if not exists site_planning_document_links_uidx on landintel.site_planning_document_links (canonical_site_id, planning_document_record_id);
create unique index if not exists intelligence_event_records_source_uidx on landintel.intelligence_event_records (source_key, source_record_id);
create unique index if not exists site_intelligence_links_uidx on landintel.site_intelligence_links (canonical_site_id, intelligence_event_record_id);
create unique index if not exists settlement_intelligence_links_uidx on landintel.settlement_intelligence_links (intelligence_event_record_id, settlement_name, coalesce(authority_name, ''));

create index if not exists planning_appeal_records_geometry_gix on landintel.planning_appeal_records using gist (geometry);
create index if not exists power_assets_geometry_gix on landintel.power_assets using gist (geometry);
create index if not exists power_capacity_zones_geometry_gix on landintel.power_capacity_zones using gist (geometry);
create index if not exists market_transactions_geometry_gix on landintel.market_transactions using gist (geometry);
create index if not exists amenity_assets_geometry_gix on landintel.amenity_assets using gist (geometry);
create index if not exists open_location_spine_features_geometry_gix on landintel.open_location_spine_features using gist (geometry);
create index if not exists open_location_spine_features_family_type_idx on landintel.open_location_spine_features (source_family, source_key, feature_type);
create index if not exists open_location_spine_features_source_type_idx on landintel.open_location_spine_features (source_key, feature_type);

create index if not exists planning_appeal_links_site_idx on landintel.site_planning_appeal_links (canonical_site_id);
create index if not exists title_order_workflow_status_idx on landintel.title_order_workflow (title_order_status, title_review_status, updated_at desc);
create index if not exists title_review_records_site_idx on landintel.title_review_records (canonical_site_id, review_date desc);
create index if not exists ownership_control_signals_site_idx on landintel.ownership_control_signals (canonical_site_id, signal_type);
create index if not exists site_power_context_site_idx on landintel.site_power_context (canonical_site_id);
create index if not exists site_ground_risk_context_site_idx on landintel.site_ground_risk_context (canonical_site_id);
create index if not exists site_market_context_site_idx on landintel.site_market_context (canonical_site_id);
create index if not exists site_amenity_context_site_idx on landintel.site_amenity_context (canonical_site_id, amenity_type);
create index if not exists site_open_location_spine_context_site_idx on landintel.site_open_location_spine_context (canonical_site_id, source_key, feature_type);
create index if not exists site_demographic_context_site_idx on landintel.site_demographic_context (canonical_site_id);
create index if not exists site_planning_document_links_site_idx on landintel.site_planning_document_links (canonical_site_id);
create index if not exists site_intelligence_links_site_idx on landintel.site_intelligence_links (canonical_site_id);

drop view if exists analytics.v_landintel_source_lifecycle_stage_counts;
drop view if exists analytics.v_landintel_source_estate_matrix;
drop view if exists analytics.v_site_intelligence_events;
drop view if exists analytics.v_site_planning_document_context;
drop view if exists analytics.v_site_demographic_context;
drop view if exists analytics.v_open_location_spine_coverage;
drop view if exists analytics.v_site_open_location_spine_context;
drop view if exists analytics.v_site_amenity_context;
drop view if exists analytics.v_site_market_context;
drop view if exists analytics.v_site_abnormal_risk_context;
drop view if exists analytics.v_site_power_context;
drop view if exists analytics.v_site_control_signals;
drop view if exists analytics.v_title_readiness;
drop view if exists analytics.v_site_planning_appeal_context;
drop view if exists analytics.v_planning_appeal_coverage;

create or replace view analytics.v_planning_appeal_coverage
with (security_invoker = true) as
select
    count(distinct appeal.id)::bigint as appeal_record_count,
    count(distinct doc.id)::bigint as document_count,
    count(distinct link.canonical_site_id)::bigint as linked_site_count,
    count(distinct tag.id)::bigint as issue_tag_count,
    max(appeal.updated_at) as latest_record_updated_at
from landintel.planning_appeal_records as appeal
left join landintel.planning_appeal_documents as doc on doc.appeal_record_id = appeal.id
left join landintel.site_planning_appeal_links as link on link.planning_appeal_record_id = appeal.id
left join landintel.appeal_issue_tags as tag on tag.planning_appeal_record_id = appeal.id;

create or replace view analytics.v_site_planning_appeal_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    appeal.appeal_reference,
    appeal.original_application_reference,
    appeal.decision,
    appeal.decision_date,
    link.link_method,
    link.link_confidence,
    array_remove(array_agg(distinct tag.issue_category order by tag.issue_category), null::text) as issue_categories
from landintel.site_planning_appeal_links as link
join landintel.canonical_sites as site on site.id = link.canonical_site_id
join landintel.planning_appeal_records as appeal on appeal.id = link.planning_appeal_record_id
left join landintel.appeal_issue_tags as tag on tag.planning_appeal_record_id = appeal.id
group by site.id, site.site_name_primary, site.authority_name, appeal.appeal_reference, appeal.original_application_reference, appeal.decision, appeal.decision_date, link.link_method, link.link_confidence;

create or replace view analytics.v_title_readiness
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    workflow.site_location_id,
    workflow.ownership_status_pre_title,
    workflow.title_required_flag,
    workflow.parcel_candidate_status,
    workflow.possible_title_reference_status,
    workflow.title_confidence_level,
    workflow.title_order_status,
    workflow.title_review_status,
    review.title_number as reviewed_title_number,
    review.registered_proprietor,
    review.proprietor_type,
    review.company_number,
    review.acquisition_date,
    review.price_paid,
    review.ownership_outcome,
    workflow.next_action,
    workflow.updated_at as readiness_updated_at,
    review.review_date
from landintel.title_order_workflow as workflow
join landintel.canonical_sites as site on site.id = workflow.canonical_site_id
left join lateral (
    select *
    from landintel.title_review_records as review_row
    where review_row.canonical_site_id = workflow.canonical_site_id
    order by review_row.review_date desc nulls last, review_row.updated_at desc
    limit 1
) as review on true;

create or replace view analytics.v_site_control_signals
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    signal.signal_type,
    signal.signal_label,
    signal.signal_value_text,
    signal.confidence,
    signal.evidence_required,
    signal.ownership_confirmed,
    signal.updated_at
from landintel.ownership_control_signals as signal
join landintel.canonical_sites as site on site.id = signal.canonical_site_id;

create or replace view analytics.v_site_power_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.nearest_asset_type,
    context.nearest_asset_distance_m,
    context.overhead_line_crosses_site,
    context.infrastructure_within_buffer,
    context.dno_region,
    context.capacity_context,
    context.review_fact,
    context.measured_at
from landintel.site_power_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_site_abnormal_risk_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.boreholes_within_250m,
    context.boreholes_within_500m,
    context.boreholes_within_1km,
    context.mining_constraint_present,
    context.flood_constraint_present,
    context.culvert_constraint_present,
    context.measured_constraint_count,
    context.abnormal_review_fact,
    context.measured_at
from landintel.site_ground_risk_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_site_market_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.market_confidence_tier,
    context.evidence_summary,
    context.latest_metric_period,
    context.updated_at
from landintel.site_market_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_site_amenity_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.amenity_type,
    context.nearest_amenity_name,
    context.nearest_distance_m,
    context.count_within_400m,
    context.count_within_800m,
    context.count_within_1600m,
    context.measured_at
from landintel.site_amenity_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_open_location_spine_coverage
with (security_invoker = true) as
select
    feature.source_family,
    feature.source_key,
    feature.feature_type,
    count(*)::bigint as feature_count,
    count(*) filter (where feature.geometry is not null)::bigint as geometry_feature_count,
    count(distinct context.canonical_site_id)::bigint as linked_site_count,
    max(feature.updated_at) as latest_feature_updated_at,
    max(context.measured_at) as latest_context_measured_at
from landintel.open_location_spine_features as feature
left join landintel.site_open_location_spine_context as context
  on context.source_key = feature.source_key
 and context.feature_type = feature.feature_type
group by feature.source_family, feature.source_key, feature.feature_type;

create or replace view analytics.v_site_open_location_spine_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.source_key,
    context.source_family,
    context.feature_type,
    context.nearest_feature_name,
    context.nearest_distance_m,
    context.count_within_400m,
    context.count_within_800m,
    context.count_within_1600m,
    context.measured_at
from landintel.site_open_location_spine_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_site_demographic_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.area_code,
    context.area_name,
    context.area_type,
    context.context_summary,
    context.evidence_confidence,
    context.updated_at
from landintel.site_demographic_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_site_planning_document_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    document.application_reference,
    document.document_type,
    document.document_title,
    document.document_url,
    document.publication_date,
    link.link_method,
    link.link_confidence
from landintel.site_planning_document_links as link
join landintel.canonical_sites as site on site.id = link.canonical_site_id
join landintel.planning_document_records as document on document.id = link.planning_document_record_id;

create or replace view analytics.v_site_intelligence_events
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    event.event_type,
    event.event_title,
    event.event_summary,
    event.date_published,
    event.source_url,
    link.link_method,
    link.link_confidence
from landintel.site_intelligence_links as link
join landintel.canonical_sites as site on site.id = link.canonical_site_id
join landintel.intelligence_event_records as event on event.id = link.intelligence_event_record_id;

create or replace view analytics.v_landintel_source_estate_matrix
with (security_invoker = true) as
with source_rows as (
    select source_key, source_family, count(*)::bigint as row_count from landintel.planning_appeal_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.title_order_workflow group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.title_review_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.ownership_control_signals group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_owner_links group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.known_controlled_sites group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.power_assets group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.power_capacity_zones group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_power_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.infrastructure_friction_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_ground_risk_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_terrain_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_slope_profiles group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_cut_fill_risk group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_abnormal_cost_flags group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.market_transactions group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.epc_property_attributes group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.market_area_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_market_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.amenity_assets group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_amenity_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.location_strength_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.open_location_spine_features group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_open_location_spine_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.demographic_area_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_demographic_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.housing_demand_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.planning_document_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.section75_obligation_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.intelligence_event_records group by source_key, source_family
),
source_row_rollup as (
    select source_key, source_family, sum(row_count)::bigint as row_count
    from source_rows
    group by source_key, source_family
),
linked_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as linked_site_count
    from (
        select appeal.source_key, link.source_family, link.canonical_site_id
        from landintel.site_planning_appeal_links as link
        join landintel.planning_appeal_records as appeal on appeal.id = link.planning_appeal_record_id
        union all select source_key, source_family, canonical_site_id from landintel.title_order_workflow
        union all select source_key, source_family, canonical_site_id from landintel.ownership_control_signals where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.site_power_context
        union all select source_key, source_family, canonical_site_id from landintel.site_ground_risk_context
        union all select source_key, source_family, canonical_site_id from landintel.site_market_context
        union all select source_key, source_family, canonical_site_id from landintel.site_amenity_context
        union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
        union all select document.source_key, link.source_family, link.canonical_site_id
        from landintel.site_planning_document_links as link
        join landintel.planning_document_records as document on document.id = link.planning_document_record_id
        union all select event.source_key, link.source_family, link.canonical_site_id
        from landintel.site_intelligence_links as link
        join landintel.intelligence_event_records as event on event.id = link.intelligence_event_record_id
    ) as links
    where canonical_site_id is not null
    group by source_key, source_family
),
measured_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as measured_site_count
    from (
        select source_key, source_family, canonical_site_id from landintel.site_power_context
        union all select source_key, source_family, canonical_site_id from landintel.site_ground_risk_context
        union all select source_key, source_family, canonical_site_id from landintel.site_terrain_metrics
        union all select source_key, source_family, canonical_site_id from landintel.site_amenity_context
        union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
    ) as measurements
    group by source_key, source_family
),
evidence_rollup as (
    select
        source_family,
        metadata ->> 'source_key' as source_key,
        count(*)::bigint as evidence_count
    from landintel.evidence_references
    where metadata ? 'source_key'
    group by source_family, metadata ->> 'source_key'
),
signal_rollup as (
    select
        source_family,
        metadata ->> 'source_key' as source_key,
        count(*)::bigint as signal_count
    from landintel.site_signals
    where metadata ? 'source_key'
    group by source_family, metadata ->> 'source_key'
),
freshness as (
    select distinct on (source_family, source_dataset)
        source_family,
        source_dataset,
        freshness_status,
        live_access_status,
        last_success_at,
        records_observed,
        check_summary
    from landintel.source_freshness_states
    order by source_family, source_dataset, last_checked_at desc nulls last, updated_at desc
),
event_rollup as (
    select
        source_family,
        source_key,
        max(created_at) filter (where status in ('success', 'source_registered', 'raw_data_landed', 'evidence_generated', 'signals_generated')) as last_successful_run
    from landintel.source_expansion_events
    group by source_family, source_key
),
matrix_base as (
    select
        registry.source_key,
        registry.source_family,
        registry.source_name,
        coalesce(registry.geography, registry.source_group, 'unknown') as authority_geography,
        registry.module_key,
        registry.programme_phase,
        registry.access_status,
        registry.ingest_status,
        registry.normalisation_status,
        registry.site_link_status,
        registry.measurement_status,
        registry.evidence_status,
        registry.signal_status,
        registry.assessment_status,
        registry.trusted_for_review as registry_trusted_for_review,
        coalesce(freshness.freshness_status, 'source_registered') as freshness_status,
        event_rollup.last_successful_run,
        coalesce(source_row_rollup.row_count, 0)::bigint as row_count,
        coalesce(linked_rollup.linked_site_count, 0)::bigint as linked_site_count,
        coalesce(measured_rollup.measured_site_count, 0)::bigint as measured_site_count,
        coalesce(evidence_rollup.evidence_count, 0)::bigint as evidence_count,
        coalesce(signal_rollup.signal_count, 0)::bigint as signal_count,
        registry.limitation_notes,
        registry.next_action
    from landintel.source_estate_registry as registry
    left join source_row_rollup
      on source_row_rollup.source_key = registry.source_key
     and source_row_rollup.source_family = registry.source_family
    left join linked_rollup on linked_rollup.source_family = registry.source_family and linked_rollup.source_key = registry.source_key
    left join measured_rollup on measured_rollup.source_family = registry.source_family and measured_rollup.source_key = registry.source_key
    left join evidence_rollup on evidence_rollup.source_family = registry.source_family and evidence_rollup.source_key = registry.source_key
    left join signal_rollup on signal_rollup.source_family = registry.source_family and signal_rollup.source_key = registry.source_key
    left join freshness on freshness.source_family = registry.source_family and freshness.source_dataset = registry.source_name
    left join event_rollup on event_rollup.source_family = registry.source_family and event_rollup.source_key = registry.source_key
)
select
    matrix_base.*,
    case
        when registry_trusted_for_review
         and row_count > 0
         and linked_site_count > 0
         and evidence_count > 0
         and signal_count > 0
         and freshness_status not in ('failed', 'stale', 'access_required', 'gated')
            then true
        else false
    end as trusted_for_review,
    case
        when registry_trusted_for_review
         and row_count > 0
         and linked_site_count > 0
         and evidence_count > 0
         and signal_count > 0
         and freshness_status not in ('failed', 'stale', 'access_required', 'gated')
            then 'trusted_for_review'
        when assessment_status = 'assessment_ready' then 'assessment_ready'
        when signal_count > 0 or signal_status = 'signals_generated' then 'signals_generated'
        when evidence_count > 0 or evidence_status = 'evidence_generated' then 'evidence_generated'
        when measured_site_count > 0 or measurement_status = 'measured' then 'measured'
        when linked_site_count > 0 or site_link_status = 'linked_to_site' then 'linked_to_site'
        when normalisation_status = 'normalised' then 'normalised'
        when row_count > 0 or ingest_status = 'raw_data_landed' then 'raw_data_landed'
        when access_status = 'access_confirmed' then 'access_confirmed'
        when access_status in ('access_required', 'gated', 'failed', 'stale') then access_status
        else 'source_registered'
    end as current_lifecycle_stage
from matrix_base;

create or replace view analytics.v_landintel_source_lifecycle_stage_counts
with (security_invoker = true) as
with lifecycle_stage(stage_name) as (
    values
        ('source_registered'::text),
        ('access_confirmed'::text),
        ('raw_data_landed'::text),
        ('normalised'::text),
        ('linked_to_site'::text),
        ('measured'::text),
        ('evidence_generated'::text),
        ('signals_generated'::text),
        ('assessment_ready'::text),
        ('trusted_for_review'::text)
)
select
    lifecycle_stage.stage_name,
    count(matrix.source_key)::bigint as source_count
from lifecycle_stage
left join analytics.v_landintel_source_estate_matrix as matrix
  on matrix.current_lifecycle_stage = lifecycle_stage.stage_name
group by lifecycle_stage.stage_name
order by array_position(array[
    'source_registered',
    'access_confirmed',
    'raw_data_landed',
    'normalised',
    'linked_to_site',
    'measured',
    'evidence_generated',
    'signals_generated',
    'assessment_ready',
    'trusted_for_review'
]::text[], lifecycle_stage.stage_name);

do $$
declare
    table_name text;
begin
    foreach table_name in array array[
        'planning_appeal_records',
        'planning_appeal_documents',
        'site_planning_appeal_links',
        'appeal_issue_tags',
        'title_order_workflow',
        'title_review_records',
        'ownership_control_signals',
        'corporate_owner_links',
        'known_controlled_sites',
        'power_assets',
        'power_capacity_zones',
        'site_power_context',
        'infrastructure_friction_facts',
        'site_terrain_metrics',
        'site_slope_profiles',
        'site_cut_fill_risk',
        'site_ground_risk_context',
        'abnormal_cost_benchmarks',
        'site_abnormal_cost_flags',
        'market_transactions',
        'epc_property_attributes',
        'market_area_metrics',
        'site_market_context',
        'internal_comparable_evidence',
        'buyer_bid_evidence',
        'amenity_assets',
        'site_amenity_context',
        'location_strength_facts',
        'open_location_spine_features',
        'site_open_location_spine_context',
        'demographic_area_metrics',
        'site_demographic_context',
        'housing_demand_context',
        'planning_document_records',
        'planning_document_extractions',
        'section75_obligation_records',
        'site_planning_document_links',
        'intelligence_event_records',
        'site_intelligence_links',
        'settlement_intelligence_links'
    ]
    loop
        execute format('alter table landintel.%%I enable row level security', table_name);
        execute format('grant select on landintel.%%I to authenticated', table_name);
        execute format('drop policy if exists phase2_select_authenticated on landintel.%%I', table_name);
        execute format('create policy phase2_select_authenticated on landintel.%%I for select to authenticated using (true)', table_name);
    end loop;
end $$;

grant select on analytics.v_planning_appeal_coverage to authenticated;
grant select on analytics.v_site_planning_appeal_context to authenticated;
grant select on analytics.v_title_readiness to authenticated;
grant select on analytics.v_site_control_signals to authenticated;
grant select on analytics.v_site_power_context to authenticated;
grant select on analytics.v_site_abnormal_risk_context to authenticated;
grant select on analytics.v_site_market_context to authenticated;
grant select on analytics.v_site_amenity_context to authenticated;
grant select on analytics.v_open_location_spine_coverage to authenticated;
grant select on analytics.v_site_open_location_spine_context to authenticated;
grant select on analytics.v_site_demographic_context to authenticated;
grant select on analytics.v_site_planning_document_context to authenticated;
grant select on analytics.v_site_intelligence_events to authenticated;
grant select on analytics.v_landintel_source_estate_matrix to authenticated;
grant select on analytics.v_landintel_source_lifecycle_stage_counts to authenticated;

comment on view analytics.v_landintel_source_estate_matrix
    is 'Phase 2 source estate proof matrix. A source is shown as trusted_for_review only when rows, linked sites, evidence, signals and freshness are all proven.';

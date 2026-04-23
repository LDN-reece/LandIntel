alter table public.source_registry
    add column if not exists source_domain text,
    add column if not exists source_role text,
    add column if not exists scope text,
    add column if not exists developer_page_url text,
    add column if not exists access_pattern text,
    add column if not exists auth_type text,
    add column if not exists primary_landintel_use text,
    add column if not exists primary_output_object text,
    add column if not exists primary_join_method text,
    add column if not exists secondary_join_method text,
    add column if not exists refresh_cadence text,
    add column if not exists is_distress_source boolean not null default false,
    add column if not exists processor_name text;

create table if not exists public.site_reviews (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    verdict text not null
        check (verdict in ('proceed', 'hold', 'reject')),
    reviewer text not null default 'manual_reviewer',
    note text,
    fact_basis jsonb not null default '{}'::jsonb,
    inference_basis jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_review_checks (
    id uuid primary key default gen_random_uuid(),
    site_review_id uuid not null references public.site_reviews(id) on delete cascade,
    check_code text not null,
    check_label text not null,
    result text not null
        check (result in ('pass', 'flag', 'fail', 'na')),
    note text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (site_review_id, check_code)
);

create table if not exists public.site_appraisal_assumption_sets (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    site_review_id uuid references public.site_reviews(id) on delete set null,
    created_by text not null default 'manual_reviewer',
    note text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_appraisal_assumptions (
    id uuid primary key default gen_random_uuid(),
    assumption_set_id uuid not null references public.site_appraisal_assumption_sets(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    assumption_key text not null,
    scenario_code text not null
        check (scenario_code in ('downside', 'base', 'upside', 'all')),
    value_numeric numeric,
    value_text text,
    unit text,
    display_label text not null,
    value_origin text not null
        check (value_origin in ('fact', 'imported_default', 'inferred', 'manual_override')),
    evidence_reference_id uuid references public.evidence_references(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (assumption_set_id, assumption_key, scenario_code)
);

create table if not exists public.site_appraisal_runs (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    assumption_set_id uuid not null references public.site_appraisal_assumption_sets(id) on delete cascade,
    site_review_id uuid references public.site_reviews(id) on delete set null,
    status text not null
        check (status in ('running', 'completed', 'failed')),
    planning_risk_level text not null
        check (planning_risk_level in ('low', 'medium', 'high')),
    structure_classification text not null
        check (structure_classification in ('option_funding', 'conditional_purchase_funding', 'spv_equity', 'hybrid')),
    estimated_early_stage_capital_need_low_gbp numeric not null default 0,
    estimated_early_stage_capital_need_base_gbp numeric not null default 0,
    estimated_early_stage_capital_need_high_gbp numeric not null default 0,
    triggered_by text not null default 'manual_reviewer',
    error_message text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    completed_at timestamptz
);

create table if not exists public.site_appraisal_scenarios (
    id uuid primary key default gen_random_uuid(),
    appraisal_run_id uuid not null references public.site_appraisal_runs(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    scenario_code text not null
        check (scenario_code in ('downside', 'base', 'upside')),
    gross_acres numeric not null,
    density_per_acre numeric not null,
    sales_value_per_unit numeric not null,
    build_cost_per_sqm numeric not null,
    abnormal_cost_per_unit numeric not null,
    s75_cost_per_unit numeric not null,
    professional_fees_pct numeric not null,
    finance_pct numeric not null,
    developer_margin_pct numeric not null,
    average_unit_size_sqm numeric not null,
    unit_count numeric not null,
    total_gdv numeric not null,
    total_build_cost numeric not null,
    total_abnormal_cost numeric not null,
    total_s75_cost numeric not null,
    total_professional_fees numeric not null,
    total_finance_cost numeric not null,
    total_cost numeric not null,
    developer_profit numeric not null,
    residual_land_value numeric not null,
    residual_land_value_per_plot numeric not null,
    residual_land_value_per_acre numeric not null,
    option_deposit_proxy_gbp numeric not null default 0,
    legal_cost_allowance_gbp numeric not null default 0,
    early_dd_allowance_gbp numeric not null default 0,
    early_abnormal_exposure_gbp numeric not null default 0,
    early_s75_exposure_gbp numeric not null default 0,
    early_stage_capital_need_gbp numeric not null default 0,
    created_at timestamptz not null default now(),
    unique (appraisal_run_id, scenario_code)
);

create table if not exists public.site_appraisal_summaries (
    id uuid primary key default gen_random_uuid(),
    appraisal_run_id uuid not null unique references public.site_appraisal_runs(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    structure_classification text not null
        check (structure_classification in ('option_funding', 'conditional_purchase_funding', 'spv_equity', 'hybrid')),
    estimated_early_stage_capital_need_low_gbp numeric not null default 0,
    estimated_early_stage_capital_need_base_gbp numeric not null default 0,
    estimated_early_stage_capital_need_high_gbp numeric not null default 0,
    downside_residual_land_value numeric not null default 0,
    base_residual_land_value numeric not null default 0,
    upside_residual_land_value numeric not null default 0,
    base_residual_land_value_per_plot numeric not null default 0,
    base_residual_land_value_per_acre numeric not null default 0,
    fact_basis jsonb not null default '{}'::jsonb,
    inference_basis jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.buyer_entities (
    id uuid primary key default gen_random_uuid(),
    buyer_code text not null unique,
    entity_name text not null,
    parent_brand text,
    buyer_family text not null,
    buyer_type_detail text,
    market_focus text,
    corporate_scale text,
    geography_scope text,
    scotland_focus boolean not null default true,
    website text,
    active_buying_status text,
    principal_or_agent text,
    keep_or_bin text,
    source_seed text not null,
    confidence_level text,
    provisional_seed boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.buyer_region_rules (
    id uuid primary key default gen_random_uuid(),
    buyer_entity_id uuid not null references public.buyer_entities(id) on delete cascade,
    region_name text not null,
    rule_type text not null default 'authority'
        check (rule_type in ('authority', 'region', 'division', 'settlement')),
    fit_weight numeric not null default 1,
    is_active boolean not null default true,
    notes text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (buyer_entity_id, region_name, rule_type)
);

create table if not exists public.buyer_residential_profiles (
    id uuid primary key default gen_random_uuid(),
    buyer_entity_id uuid not null unique references public.buyer_entities(id) on delete cascade,
    min_units numeric,
    max_units numeric,
    stage_focus text[] not null default '{}'::text[],
    abnormal_tolerance text,
    product_focus text[] not null default '{}'::text[],
    notes text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.buyer_evidence (
    id uuid primary key default gen_random_uuid(),
    buyer_entity_id uuid not null references public.buyer_entities(id) on delete cascade,
    evidence_type text not null,
    source_name text not null,
    source_url text,
    evidence_date date,
    geography_text text,
    summary text not null,
    confidence_tier text,
    created_at timestamptz not null default now()
);

create table if not exists public.buyer_contacts (
    id uuid primary key default gen_random_uuid(),
    buyer_entity_id uuid not null references public.buyer_entities(id) on delete cascade,
    organisation text not null,
    contact_name text not null,
    role text,
    email text,
    phone text,
    notes text,
    route_in_quality_label text not null default 'medium'
        check (route_in_quality_label in ('low', 'medium', 'high')),
    route_in_quality_score numeric not null default 50,
    likely_buyer_route boolean not null default true,
    source_sheet text,
    sector_bucket text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.site_buyer_matches
    alter column buyer_profile_id drop not null;

alter table public.site_buyer_matches
    add column if not exists buyer_entity_id uuid references public.buyer_entities(id) on delete cascade,
    add column if not exists appraisal_run_id uuid references public.site_appraisal_runs(id) on delete cascade,
    add column if not exists match_rank integer,
    add column if not exists match_score numeric,
    add column if not exists geography_score numeric,
    add column if not exists unit_scale_score numeric,
    add column if not exists stage_score numeric,
    add column if not exists abnormal_tolerance_score numeric,
    add column if not exists evidence_recency_score numeric,
    add column if not exists route_in_quality_score numeric,
    add column if not exists fit_band text
        check (fit_band in ('strong', 'moderate', 'weak', 'excluded')),
    add column if not exists rationale text,
    add column if not exists confidence_level text,
    add column if not exists provisional_flag boolean not null default false,
    add column if not exists latest_evaluated_at timestamptz;

create table if not exists public.site_buyer_match_evidence (
    id uuid primary key default gen_random_uuid(),
    site_buyer_match_id uuid not null references public.site_buyer_matches(id) on delete cascade,
    evidence_kind text not null
        check (evidence_kind in ('buyer', 'site')),
    buyer_evidence_id uuid references public.buyer_evidence(id) on delete cascade,
    evidence_reference_id uuid references public.evidence_references(id) on delete cascade,
    created_at timestamptz not null default now(),
    check (
        (
            evidence_kind = 'buyer'
            and buyer_evidence_id is not null
            and evidence_reference_id is null
        )
        or
        (
            evidence_kind = 'site'
            and buyer_evidence_id is null
            and evidence_reference_id is not null
        )
    )
);

create table if not exists public.investor_entities (
    id uuid primary key default gen_random_uuid(),
    investor_code text not null unique,
    entity_name text not null,
    investor_type text not null
        check (investor_type in ('hnwi', 'family_office', 'private_fund', 'public_fund')),
    sub_type text,
    geography_scope text,
    scotland_focus boolean not null default false,
    residential_focus boolean not null default false,
    early_stage_focus boolean not null default false,
    planning_risk_appetite text not null
        check (planning_risk_appetite in ('low', 'medium', 'high')),
    typical_structure text not null
        check (typical_structure in ('option_funding', 'conditional_purchase_funding', 'spv_equity', 'hybrid', 'flexible')),
    typical_cheque_min numeric,
    typical_cheque_max numeric,
    target_return_profile text,
    decision_speed text not null
        check (decision_speed in ('slow', 'medium', 'fast')),
    relationship_type text not null
        check (relationship_type in ('direct', 'intermediary', 'internal_network')),
    source_seed text not null,
    confidence_level text,
    provisional_seed boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.investor_evidence (
    id uuid primary key default gen_random_uuid(),
    investor_entity_id uuid not null references public.investor_entities(id) on delete cascade,
    evidence_type text not null,
    source_name text not null,
    source_url text,
    evidence_date date,
    geography_text text,
    structure_signal text,
    capital_signal numeric,
    summary text not null,
    confidence_tier text,
    created_at timestamptz not null default now()
);

create table if not exists public.investor_source_systems (
    id uuid primary key default gen_random_uuid(),
    system_code text not null unique,
    source_name text not null,
    project_name text,
    geography_scope text,
    developer_page_url text,
    access_type text,
    credential_status text,
    env_var_reference text,
    secret_manager_reference text,
    notes text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_investor_matches (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    appraisal_run_id uuid not null references public.site_appraisal_runs(id) on delete cascade,
    investor_entity_id uuid not null references public.investor_entities(id) on delete cascade,
    match_rank integer,
    match_score numeric not null,
    cheque_size_score numeric not null,
    planning_risk_score numeric not null,
    structure_fit_score numeric not null,
    geography_score numeric not null,
    residential_focus_score numeric not null,
    evidence_recency_score numeric not null,
    fit_band text not null
        check (fit_band in ('strong', 'moderate', 'weak', 'excluded')),
    rationale text not null,
    confidence_level text,
    provisional_flag boolean not null default false,
    hard_fail_reasons jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    unique (appraisal_run_id, investor_entity_id)
);

create table if not exists public.site_investor_match_evidence (
    id uuid primary key default gen_random_uuid(),
    site_investor_match_id uuid not null references public.site_investor_matches(id) on delete cascade,
    evidence_kind text not null
        check (evidence_kind in ('investor', 'site')),
    investor_evidence_id uuid references public.investor_evidence(id) on delete cascade,
    evidence_reference_id uuid references public.evidence_references(id) on delete cascade,
    created_at timestamptz not null default now(),
    check (
        (
            evidence_kind = 'investor'
            and investor_evidence_id is not null
            and evidence_reference_id is null
        )
        or
        (
            evidence_kind = 'site'
            and investor_evidence_id is null
            and evidence_reference_id is not null
        )
    )
);

create table if not exists public.site_strategy_recommendations (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    appraisal_run_id uuid not null unique references public.site_appraisal_runs(id) on delete cascade,
    target_ldn_price_per_plot numeric not null default 0,
    absolute_max_price_per_plot numeric not null default 0,
    walk_away_price_per_plot numeric not null default 0,
    deal_attractiveness text not null
        check (deal_attractiveness in ('high', 'medium', 'low', 'reject')),
    investor_strategy_flag text not null
        check (investor_strategy_flag in ('investor_required', 'investor_optional', 'investor_not_required')),
    deal_route_bias text not null
        check (deal_route_bias in ('buyer_first', 'investor_first', 'dual_track', 'broker_only')),
    buyer_summary text not null,
    investor_summary text not null,
    rationale text not null,
    sensitivity_flags jsonb not null default '[]'::jsonb,
    fact_basis jsonb not null default '{}'::jsonb,
    inference_basis jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_infrastructure_records (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    infrastructure_type text not null,
    burden_level text not null
        check (burden_level in ('none', 'low', 'medium', 'high', 'critical', 'unknown')),
    status text,
    description text,
    source_dataset text not null,
    source_record_id text,
    source_url text,
    import_version text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_control_records (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    control_type text not null,
    control_level text not null
        check (control_level in ('none', 'low', 'medium', 'high', 'critical', 'unknown', 'single', 'multiple', 'many')),
    status text,
    description text,
    source_dataset text not null,
    source_record_id text,
    source_url text,
    import_version text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.site_assessments (
    id uuid primary key default gen_random_uuid(),
    analysis_run_id uuid not null unique references public.site_analysis_runs(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    jurisdiction text not null default 'scotland',
    assessment_version text not null,
    bucket_code text not null
        check (bucket_code in ('A', 'B', 'C', 'D', 'E', 'F')),
    bucket_label text not null,
    likely_opportunity_type text not null,
    monetisation_horizon text not null,
    horizon_year_band text not null,
    dominant_blocker text not null,
    blocker_themes jsonb not null default '[]'::jsonb,
    primary_reason text not null,
    secondary_reasons jsonb not null default '[]'::jsonb,
    buyer_profile_guess text,
    likely_buyer_profiles jsonb not null default '[]'::jsonb,
    cost_to_control_band text,
    human_review_required boolean not null default false,
    hard_fail_flags jsonb not null default '[]'::jsonb,
    review_flags jsonb not null default '[]'::jsonb,
    explanation_text text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists public.site_assessment_scores (
    id uuid primary key default gen_random_uuid(),
    site_assessment_id uuid not null references public.site_assessments(id) on delete cascade,
    site_id uuid not null references public.sites(id) on delete cascade,
    score_code text not null
        check (score_code in ('P', 'G', 'I', 'R', 'F', 'K', 'B')),
    score_label text not null,
    score_value integer not null
        check (score_value between 1 and 5),
    confidence_label text not null
        check (confidence_label in ('high', 'medium', 'low')),
    score_summary text not null,
    score_reasoning text not null,
    blocker_theme text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (site_assessment_id, score_code)
);

create table if not exists public.site_assessment_evidence (
    site_assessment_id uuid not null references public.site_assessments(id) on delete cascade,
    evidence_reference_id uuid not null references public.evidence_references(id) on delete cascade,
    created_at timestamptz not null default now(),
    primary key (site_assessment_id, evidence_reference_id)
);

create table if not exists public.site_assessment_score_evidence (
    site_assessment_score_id uuid not null references public.site_assessment_scores(id) on delete cascade,
    evidence_reference_id uuid not null references public.evidence_references(id) on delete cascade,
    created_at timestamptz not null default now(),
    primary key (site_assessment_score_id, evidence_reference_id)
);

create table if not exists public.site_assessment_overrides (
    id uuid primary key default gen_random_uuid(),
    site_id uuid not null references public.sites(id) on delete cascade,
    status text not null default 'active'
        check (status in ('active', 'superseded')),
    bucket_code text
        check (bucket_code in ('A', 'B', 'C', 'D', 'E', 'F')),
    monetisation_horizon text,
    buyer_profile_guess text,
    dominant_blocker text,
    override_summary text not null,
    review_flags jsonb not null default '[]'::jsonb,
    overridden_by text not null default 'manual_reviewer',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table analytics.site_search_cache
    add column if not exists opportunity_bucket text,
    add column if not exists bucket_label text,
    add column if not exists monetisation_horizon text,
    add column if not exists dominant_blocker text,
    add column if not exists cost_to_control_band text,
    add column if not exists human_review_required boolean not null default false,
    add column if not exists planning_score integer,
    add column if not exists ground_score integer,
    add column if not exists infrastructure_score integer,
    add column if not exists prior_progress_score integer,
    add column if not exists fixability_score integer,
    add column if not exists control_cost_score integer,
    add column if not exists buyer_depth_score integer;


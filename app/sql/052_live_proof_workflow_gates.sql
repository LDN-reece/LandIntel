alter table landintel.site_assessments
    add column if not exists source_key text not null default 'site_assessment_refresh',
    add column if not exists source_family text not null default 'site_assessment',
    add column if not exists review_tier text,
    add column if not exists site_review_status text,
    add column if not exists why_site_surfaced text,
    add column if not exists top_positives text[] not null default '{}'::text[],
    add column if not exists top_warnings text[] not null default '{}'::text[],
    add column if not exists missing_critical_evidence text[] not null default '{}'::text[],
    add column if not exists title_required_flag boolean not null default true,
    add column if not exists review_next_action text,
    add column if not exists evidence_completeness_tier text,
    add column if not exists source_limitation_notes text[] not null default '{}'::text[],
    add column if not exists source_record_signature text;

create unique index if not exists site_assessments_current_uidx
    on landintel.site_assessments (canonical_site_id, source_key, assessment_version);

create table if not exists landintel.planning_decision_facts (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    planning_application_record_id uuid references landintel.planning_application_records(id) on delete cascade,
    source_key text not null default 'planning_decision_engine',
    source_family text not null default 'planning_decisions',
    source_record_id text not null,
    authority_name text,
    planning_reference text,
    application_status text,
    decision_raw text,
    decision_status text not null default 'decision_unknown',
    decision_date date,
    proposal_text text,
    refusal_themes text[] not null default '{}'::text[],
    event_type text not null default 'planning_decision_recorded',
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_planning_decision_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'planning_decision_engine',
    source_family text not null default 'planning_decisions',
    latest_planning_reference text,
    latest_decision_status text,
    latest_decision_date date,
    approved_count integer not null default 0,
    refused_count integer not null default 0,
    withdrawn_count integer not null default 0,
    live_count integer not null default 0,
    decision_record_count integer not null default 0,
    planning_decision_summary text,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    measured_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (canonical_site_id)
);

create table if not exists landintel.corporate_entity_enrichments (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    source_key text not null,
    source_family text not null default 'corporate_control',
    source_record_id text not null,
    company_name text not null,
    company_number text,
    entity_status text,
    entity_type text,
    registered_address text,
    source_url text,
    enrichment_basis text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.corporate_charge_records (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    corporate_entity_enrichment_id uuid references landintel.corporate_entity_enrichments(id) on delete cascade,
    source_key text not null default 'companies_house_charges',
    source_family text not null default 'corporate_control',
    company_number text not null,
    charge_code text not null,
    charge_status text,
    lender_name text,
    created_on date,
    delivered_on date,
    source_url text,
    source_record_signature text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists planning_decision_facts_source_uidx
    on landintel.planning_decision_facts (source_key, source_record_id);
create index if not exists planning_decision_facts_application_idx
    on landintel.planning_decision_facts (planning_application_record_id);
create unique index if not exists corporate_entity_enrichments_source_uidx
    on landintel.corporate_entity_enrichments (source_key, source_record_id);
create unique index if not exists corporate_charge_records_source_uidx
    on landintel.corporate_charge_records (source_key, company_number, charge_code);

create index if not exists planning_decision_facts_site_idx
    on landintel.planning_decision_facts (canonical_site_id, decision_date desc nulls last);
create index if not exists site_planning_decision_context_site_idx
    on landintel.site_planning_decision_context (canonical_site_id);
create index if not exists corporate_entity_enrichments_site_idx
    on landintel.corporate_entity_enrichments (canonical_site_id, company_number);
create index if not exists corporate_charge_records_company_idx
    on landintel.corporate_charge_records (company_number, charge_status);

alter table landintel.canonical_site_refresh_queue
    drop constraint if exists canonical_site_refresh_queue_family_check;

alter table landintel.canonical_site_refresh_queue
    add constraint canonical_site_refresh_queue_family_check
    check (
        source_family is null
        or source_family = any (array[
            'planning',
            'planning_decisions',
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
            'local_intelligence',
            'site_assessment'
        ]::text[])
    );

drop view if exists analytics.v_landintel_source_lifecycle_stage_counts;
drop view if exists analytics.v_landintel_source_estate_matrix;
drop view if exists analytics.v_site_assessment_context;
drop view if exists analytics.v_site_planning_decision_context;
drop view if exists analytics.v_planning_decision_coverage;

create or replace view analytics.v_planning_decision_coverage
with (security_invoker = true) as
select
    count(*)::bigint as decision_fact_count,
    count(distinct canonical_site_id)::bigint as linked_site_count,
    count(*) filter (where decision_status = 'approved')::bigint as approved_count,
    count(*) filter (where decision_status = 'refused')::bigint as refused_count,
    count(*) filter (where decision_status = 'withdrawn')::bigint as withdrawn_count,
    count(*) filter (where decision_status = 'live')::bigint as live_count,
    max(updated_at) as latest_updated_at
from landintel.planning_decision_facts;

create or replace view analytics.v_site_planning_decision_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    context.latest_planning_reference,
    context.latest_decision_status,
    context.latest_decision_date,
    context.approved_count,
    context.refused_count,
    context.withdrawn_count,
    context.live_count,
    context.decision_record_count,
    context.planning_decision_summary,
    context.measured_at
from landintel.site_planning_decision_context as context
join landintel.canonical_sites as site on site.id = context.canonical_site_id;

create or replace view analytics.v_site_assessment_context
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    assessment.review_tier,
    assessment.site_review_status,
    assessment.why_site_surfaced,
    assessment.top_positives,
    assessment.top_warnings,
    assessment.missing_critical_evidence,
    assessment.title_required_flag,
    assessment.review_next_action,
    assessment.evidence_completeness_tier,
    assessment.source_limitation_notes,
    assessment.explanation_text,
    assessment.updated_at
from landintel.site_assessments as assessment
join landintel.canonical_sites as site on site.id = assessment.canonical_site_id
where assessment.source_key = 'site_assessment_refresh';

create or replace view analytics.v_landintel_source_estate_matrix
with (security_invoker = true) as
with source_rows as (
    select source_key, source_family, count(*)::bigint as row_count from landintel.planning_appeal_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.planning_decision_facts group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_planning_decision_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.title_order_workflow group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.title_review_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.ownership_control_signals group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_owner_links group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_entity_enrichments group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.corporate_charge_records group by source_key, source_family
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
    union all select source_key, source_family, count(*)::bigint from landintel.demographic_area_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_demographic_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.housing_demand_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.planning_document_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.section75_obligation_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.intelligence_event_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_assessments group by source_key, source_family
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
        union all select source_key, source_family, canonical_site_id from landintel.planning_decision_facts where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.site_planning_decision_context
        union all select source_key, source_family, canonical_site_id from landintel.title_order_workflow
        union all select source_key, source_family, canonical_site_id from landintel.ownership_control_signals where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.corporate_owner_links where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.corporate_entity_enrichments where canonical_site_id is not null
        union all select source_key, source_family, canonical_site_id from landintel.site_power_context
        union all select source_key, source_family, canonical_site_id from landintel.site_ground_risk_context
        union all select source_key, source_family, canonical_site_id from landintel.site_market_context
        union all select source_key, source_family, canonical_site_id from landintel.site_amenity_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
        union all select document.source_key, link.source_family, link.canonical_site_id
        from landintel.site_planning_document_links as link
        join landintel.planning_document_records as document on document.id = link.planning_document_record_id
        union all select event.source_key, link.source_family, link.canonical_site_id
        from landintel.site_intelligence_links as link
        join landintel.intelligence_event_records as event on event.id = link.intelligence_event_record_id
        union all select source_key, source_family, canonical_site_id from landintel.site_assessments
    ) as links
    where canonical_site_id is not null
    group by source_key, source_family
),
measured_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as measured_site_count
    from (
        select source_key, source_family, canonical_site_id from landintel.site_planning_decision_context
        union all select source_key, source_family, canonical_site_id from landintel.site_power_context
        union all select source_key, source_family, canonical_site_id from landintel.site_ground_risk_context
        union all select source_key, source_family, canonical_site_id from landintel.site_terrain_metrics
        union all select source_key, source_family, canonical_site_id from landintel.site_amenity_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
    ) as measurements
    group by source_key, source_family
),
assessment_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as assessment_ready_count
    from landintel.site_assessments
    where review_next_action is not null
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
    select distinct on (source_family, source_key)
        source_family,
        replace(source_scope_key, 'phase2:', '') as source_key,
        freshness_status,
        live_access_status,
        last_success_at,
        records_observed,
        check_summary
    from landintel.source_freshness_states
    where source_scope_key like 'phase2:%%'
    order by source_family, replace(source_scope_key, 'phase2:', ''), last_checked_at desc nulls last, updated_at desc
),
event_rollup as (
    select
        source_family,
        source_key,
        max(created_at) filter (where status in ('success', 'source_registered', 'raw_data_landed', 'evidence_generated', 'signals_generated', 'assessment_ready')) as last_successful_run
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
        coalesce(freshness.records_observed, 0)::bigint as freshness_record_count,
        event_rollup.last_successful_run,
        coalesce(source_row_rollup.row_count, 0)::bigint as row_count,
        coalesce(linked_rollup.linked_site_count, 0)::bigint as linked_site_count,
        coalesce(measured_rollup.measured_site_count, 0)::bigint as measured_site_count,
        coalesce(assessment_rollup.assessment_ready_count, 0)::bigint as assessment_ready_count,
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
    left join assessment_rollup on assessment_rollup.source_family = registry.source_family and assessment_rollup.source_key = registry.source_key
    left join evidence_rollup on evidence_rollup.source_family = registry.source_family and evidence_rollup.source_key = registry.source_key
    left join signal_rollup on signal_rollup.source_family = registry.source_family and signal_rollup.source_key = registry.source_key
    left join freshness on freshness.source_family = registry.source_family and freshness.source_key = registry.source_key
    left join event_rollup on event_rollup.source_family = registry.source_family and event_rollup.source_key = registry.source_key
),
matrix_gates as (
    select
        matrix_base.*,
        (
            access_status in ('access_required', 'gated', 'failed', 'stale')
            or freshness_status in ('failed', 'stale', 'access_required', 'gated')
            or limitation_notes ilike any (array[
                '%%has not yet%%',
                '%%not yet%%',
                '%%requires%%',
                '%%required%%',
                '%%must be confirmed%%',
                '%%before use%%',
                '%%adapter%%'
            ])
        ) as critical_limitation_blocking_review
    from matrix_base
)
select
    matrix_gates.*,
    case
        when registry_trusted_for_review
         and row_count > 0
         and linked_site_count > 0
         and evidence_count > 0
         and signal_count > 0
         and assessment_ready_count > 0
         and freshness_record_count > 0
         and freshness_status not in ('failed', 'stale', 'access_required', 'gated')
         and not critical_limitation_blocking_review
            then true
        else false
    end as trusted_for_review,
    case
        when registry_trusted_for_review
         and row_count > 0
         and linked_site_count > 0
         and evidence_count > 0
         and signal_count > 0
         and assessment_ready_count > 0
         and freshness_record_count > 0
         and freshness_status not in ('failed', 'stale', 'access_required', 'gated')
         and not critical_limitation_blocking_review
            then 'trusted_for_review'
        when assessment_ready_count > 0 then 'assessment_ready'
        when signal_count > 0 then 'signals_generated'
        when evidence_count > 0 then 'evidence_generated'
        when measured_site_count > 0 then 'measured'
        when linked_site_count > 0 then 'linked_to_site'
        when row_count > 0 and normalisation_status = 'normalised' then 'normalised'
        when row_count > 0 then 'raw_data_landed'
        when access_status = 'access_confirmed' then 'access_confirmed'
        else 'source_registered'
    end as current_lifecycle_stage,
    case
        when row_count = 0 then 'no_source_rows'
        when linked_site_count = 0 then 'no_linked_sites'
        when evidence_count = 0 then 'no_evidence_rows'
        when signal_count = 0 then 'no_signal_rows'
        when freshness_record_count = 0 then 'no_freshness_state'
        when critical_limitation_blocking_review then 'critical_limitation_blocks_review'
        when assessment_ready_count = 0 then 'not_assessment_ready'
        else null
    end as trust_block_reason
from matrix_gates;

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

grant select on analytics.v_planning_decision_coverage to authenticated;
grant select on analytics.v_site_planning_decision_context to authenticated;
grant select on analytics.v_site_assessment_context to authenticated;
grant select on analytics.v_landintel_source_estate_matrix to authenticated;
grant select on analytics.v_landintel_source_lifecycle_stage_counts to authenticated;

alter table landintel.planning_decision_facts enable row level security;
alter table landintel.site_planning_decision_context enable row level security;
alter table landintel.corporate_entity_enrichments enable row level security;
alter table landintel.corporate_charge_records enable row level security;

drop policy if exists planning_decision_facts_select_authenticated on landintel.planning_decision_facts;
create policy planning_decision_facts_select_authenticated on landintel.planning_decision_facts for select to authenticated using (true);

drop policy if exists site_planning_decision_context_select_authenticated on landintel.site_planning_decision_context;
create policy site_planning_decision_context_select_authenticated on landintel.site_planning_decision_context for select to authenticated using (true);

drop policy if exists corporate_entity_enrichments_select_authenticated on landintel.corporate_entity_enrichments;
create policy corporate_entity_enrichments_select_authenticated on landintel.corporate_entity_enrichments for select to authenticated using (true);

drop policy if exists corporate_charge_records_select_authenticated on landintel.corporate_charge_records;
create policy corporate_charge_records_select_authenticated on landintel.corporate_charge_records for select to authenticated using (true);

grant select on landintel.planning_decision_facts to authenticated;
grant select on landintel.site_planning_decision_context to authenticated;
grant select on landintel.corporate_entity_enrichments to authenticated;
grant select on landintel.corporate_charge_records to authenticated;

comment on view analytics.v_landintel_source_estate_matrix
    is 'Live source estate proof matrix. Trusted review status is blocked unless rows, site links, evidence, signals, freshness and assessment readiness are all proven with no blocking limitation.';

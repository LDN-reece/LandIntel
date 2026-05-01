create table if not exists landintel.site_prove_it_assessments (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    assessment_version integer not null default 1,
    source_key text not null default 'prove_it_conviction_layer',
    source_family text not null default 'site_conviction',
    claim_statement text,
    prove_it_drivers text[] not null default '{}'::text[],
    proof_points jsonb not null default '[]'::jsonb,
    interpretation_text text,
    top_positives text[] not null default '{}'::text[],
    top_warnings text[] not null default '{}'::text[],
    missing_critical_evidence text[] not null default '{}'::text[],
    title_spend_recommendation text,
    title_spend_reason text,
    constraint_position text,
    planning_journey_type text,
    market_position text,
    control_position text,
    evidence_confidence text,
    verdict text,
    review_next_action text,
    review_ready_flag boolean not null default false,
    source_record_signature text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_verdict_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_verdict_check
    check (
        verdict is null
        or verdict = any (array['ignore', 'monitor', 'review', 'pursue']::text[])
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_title_spend_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_title_spend_check
    check (
        title_spend_recommendation is null
        or title_spend_recommendation = any (array[
            'do_not_order',
            'manual_review_before_order',
            'order_title',
            'order_title_urgently'
        ]::text[])
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_confidence_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_confidence_check
    check (
        evidence_confidence is null
        or evidence_confidence = any (array['high', 'medium', 'low', 'mixed', 'insufficient']::text[])
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_planning_journey_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_planning_journey_check
    check (
        planning_journey_type is null
        or planning_journey_type = any (array[
            'allocated_or_recognised',
            'adjacent_precedent',
            'refusal_repair',
            'policy_momentum',
            'brownfield_regeneration',
            'settlement_edge_expansion',
            'infrastructure_led',
            'no_clear_journey'
        ]::text[])
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_positions_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_positions_check
    check (
        (constraint_position is null or constraint_position = any (array[
            'terminal',
            'major_review',
            'priceable_design_led',
            'context_only',
            'unknown'
        ]::text[]))
        and (market_position is null or market_position = any (array[
            'strong',
            'credible',
            'weak',
            'unproven',
            'unknown'
        ]::text[]))
        and (control_position is null or control_position = any (array[
            'known_and_attractive',
            'known_and_unattractive',
            'known_blocked',
            'likely_controlled_by_housebuilder_promoter',
            'likely_controlled_by_small_private_company',
            'likely_local_trading_company',
            'unknown_but_worth_title_spend',
            'unknown_not_worth_title_spend',
            'ownership_not_confirmed'
        ]::text[]))
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_review_ready_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_review_ready_check
    check (
        (
            (verdict is null or verdict <> all (array['review', 'pursue']::text[]))
            and review_ready_flag = false
        )
        or (
            verdict = any (array['review', 'pursue']::text[])
            and review_ready_flag = true
            and nullif(trim(claim_statement), '') is not null
            and cardinality(prove_it_drivers) > 0
            and jsonb_typeof(proof_points) = 'array'
            and jsonb_array_length(proof_points) > 0
            and nullif(trim(interpretation_text), '') is not null
            and cardinality(top_warnings) > 0
            and cardinality(missing_critical_evidence) > 0
            and title_spend_recommendation is not null
            and nullif(trim(review_next_action), '') is not null
        )
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_pursue_gate_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_pursue_gate_check
    check (
        verdict is distinct from 'pursue'
        or (
            planning_journey_type is distinct from 'no_clear_journey'
            and evidence_confidence = any (array['high', 'medium', 'mixed']::text[])
            and constraint_position is distinct from 'terminal'
            and constraint_position is distinct from 'unknown'
            and control_position is distinct from 'known_blocked'
            and control_position is distinct from 'likely_controlled_by_housebuilder_promoter'
            and market_position is distinct from 'weak'
            and market_position is distinct from 'unknown'
            and review_next_action <> all (array[
                'Ignore until new evidence appears.',
                'Monitor only. Do not spend time or title money yet.'
            ]::text[])
        )
    );

alter table landintel.site_prove_it_assessments
    drop constraint if exists site_prove_it_assessments_proof_array_check;

alter table landintel.site_prove_it_assessments
    add constraint site_prove_it_assessments_proof_array_check
    check (jsonb_typeof(proof_points) = 'array');

create unique index if not exists site_prove_it_assessments_current_uidx
    on landintel.site_prove_it_assessments (canonical_site_id, source_key, assessment_version);

create index if not exists site_prove_it_assessments_site_idx
    on landintel.site_prove_it_assessments (canonical_site_id, verdict, review_ready_flag);

create index if not exists site_prove_it_assessments_action_idx
    on landintel.site_prove_it_assessments (verdict, title_spend_recommendation, evidence_confidence);

drop view if exists analytics.v_site_prove_it_coverage;
drop view if exists analytics.v_site_prove_it_assessments;

create or replace view analytics.v_site_prove_it_coverage
with (security_invoker = true) as
select
    count(*)::bigint as prove_it_assessment_count,
    count(distinct canonical_site_id)::bigint as assessed_site_count,
    count(*) filter (where review_ready_flag)::bigint as review_ready_site_count,
    count(*) filter (where verdict = 'ignore')::bigint as ignore_count,
    count(*) filter (where verdict = 'monitor')::bigint as monitor_count,
    count(*) filter (where verdict = 'review')::bigint as review_count,
    count(*) filter (where verdict = 'pursue')::bigint as pursue_count,
    count(*) filter (where title_spend_recommendation = 'do_not_order')::bigint as do_not_order_title_count,
    count(*) filter (where title_spend_recommendation = 'manual_review_before_order')::bigint as manual_review_before_title_count,
    count(*) filter (where title_spend_recommendation = 'order_title')::bigint as order_title_count,
    count(*) filter (where title_spend_recommendation = 'order_title_urgently')::bigint as order_title_urgently_count,
    count(*) filter (where evidence_confidence = 'insufficient')::bigint as insufficient_evidence_count,
    max(updated_at) as latest_updated_at
from landintel.site_prove_it_assessments
where source_key = 'prove_it_conviction_layer';

create or replace view analytics.v_site_prove_it_assessments
with (security_invoker = true) as
select
    site.id as canonical_site_id,
    site.site_name_primary,
    site.authority_name,
    assessment.claim_statement,
    assessment.prove_it_drivers,
    assessment.proof_points,
    assessment.interpretation_text,
    assessment.top_positives,
    assessment.top_warnings,
    assessment.missing_critical_evidence,
    assessment.title_spend_recommendation,
    assessment.title_spend_reason,
    assessment.constraint_position,
    assessment.planning_journey_type,
    assessment.market_position,
    assessment.control_position,
    assessment.evidence_confidence,
    assessment.verdict,
    assessment.review_next_action,
    assessment.review_ready_flag,
    assessment.updated_at
from landintel.site_prove_it_assessments as assessment
join landintel.canonical_sites as site on site.id = assessment.canonical_site_id
where assessment.source_key = 'prove_it_conviction_layer';

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
    union all select source_key, source_family, count(*)::bigint from landintel.open_location_spine_features group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_open_location_spine_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.demographic_area_metrics group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_demographic_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.housing_demand_context group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.planning_document_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.section75_obligation_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.intelligence_event_records group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_assessments group by source_key, source_family
    union all select source_key, source_family, count(*)::bigint from landintel.site_prove_it_assessments group by source_key, source_family
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
        union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
        union all select document.source_key, link.source_family, link.canonical_site_id
        from landintel.site_planning_document_links as link
        join landintel.planning_document_records as document on document.id = link.planning_document_record_id
        union all select event.source_key, link.source_family, link.canonical_site_id
        from landintel.site_intelligence_links as link
        join landintel.intelligence_event_records as event on event.id = link.intelligence_event_record_id
        union all select source_key, source_family, canonical_site_id from landintel.site_assessments
        union all select source_key, source_family, canonical_site_id from landintel.site_prove_it_assessments
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
        union all select source_key, source_family, canonical_site_id from landintel.site_open_location_spine_context
        union all select source_key, source_family, canonical_site_id from landintel.site_demographic_context
    ) as measurements
    group by source_key, source_family
),
assessment_rollup as (
    select source_key, source_family, count(distinct canonical_site_id)::bigint as assessment_ready_count
    from (
        select source_key, source_family, canonical_site_id
        from landintel.site_assessments
        where review_next_action is not null
        union all
        select source_key, source_family, canonical_site_id
        from landintel.site_prove_it_assessments
        where review_ready_flag = true
    ) as assessments
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
        source_key,
        freshness_status,
        live_access_status,
        last_success_at,
        records_observed,
        check_summary
    from (
        select
            source_family,
            replace(replace(source_scope_key, 'phase2:', ''), 'source_expansion:', '') as source_key,
            freshness_status,
            live_access_status,
            last_success_at,
            records_observed,
            check_summary,
            last_checked_at,
            updated_at
        from landintel.source_freshness_states
        where source_scope_key like 'phase2:%%'
           or source_scope_key like 'source_expansion:%%'
    ) as freshness_rows
    order by source_family, source_key, last_checked_at desc nulls last, updated_at desc
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

alter table landintel.site_prove_it_assessments enable row level security;
grant select on landintel.site_prove_it_assessments to authenticated;
drop policy if exists site_prove_it_assessments_select_authenticated on landintel.site_prove_it_assessments;
create policy site_prove_it_assessments_select_authenticated
    on landintel.site_prove_it_assessments
    for select
    to authenticated
    using (true);

grant select on analytics.v_site_prove_it_coverage to authenticated;
grant select on analytics.v_site_prove_it_assessments to authenticated;

comment on table landintel.site_prove_it_assessments
    is 'LandIntel conviction layer: converts source evidence into claim, proof, interpretation, risk, gap, action and verdict for human land-review attention allocation.';

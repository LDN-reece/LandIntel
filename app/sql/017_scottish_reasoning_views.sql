drop view if exists analytics.v_site_current_assessment_scores;
drop view if exists analytics.v_site_current_assessments;

create or replace view analytics.v_site_current_assessments
with (security_invoker = true) as
select
    assessment.*
from public.site_assessments as assessment
join analytics.v_site_current_analysis_runs as run
    on run.analysis_run_id = assessment.analysis_run_id;

create or replace view analytics.v_site_current_assessment_scores
with (security_invoker = true) as
select
    score.*
from public.site_assessment_scores as score
join analytics.v_site_current_assessments as assessment
    on assessment.id = score.site_assessment_id;

create or replace function analytics.upsert_site_search_cache_row(target_site_id uuid)
returns void
language sql
set search_path = pg_catalog, public, analytics
as $$
    with signal_rollup as (
        select
            site_id,
            bool_or(coalesce(bool_value, false)) filter (where signal_key = 'within_settlement_boundary') as within_settlement_boundary,
            max(numeric_value) filter (where signal_key = 'distance_to_settlement_boundary_m') as distance_to_settlement_boundary_m,
            bool_or(coalesce(bool_value, false)) filter (where signal_key = 'previous_application_exists') as previous_application_exists,
            max(text_value) filter (where signal_key = 'previous_application_outcome') as previous_application_outcome,
            max(text_value) filter (where signal_key = 'allocation_status') as allocation_status,
            max(text_value) filter (where signal_key = 'flood_risk') as flood_risk,
            max(text_value) filter (where signal_key = 'mining_risk') as mining_risk,
            max(text_value) filter (where signal_key = 'access_status') as access_status,
            max(numeric_value) filter (where signal_key = 'critical_constraint_count') as critical_constraint_count,
            max(text_value) filter (where signal_key = 'new_build_comparable_strength') as new_build_comparable_strength,
            max(numeric_value) filter (where signal_key = 'comparable_sale_count') as comparable_sale_count,
            max(numeric_value) filter (where signal_key = 'buyer_fit_count') as buyer_fit_count
        from analytics.v_site_current_signals
        where site_id = target_site_id
        group by site_id
    ),
    interpretation_rollup as (
        select
            site_id,
            count(*) filter (where category = 'positive')::bigint as positive_count,
            count(*) filter (where category = 'risk')::bigint as risk_count,
            count(*) filter (where category = 'possible_fatal')::bigint as possible_fatal_count,
            count(*) filter (where category = 'unknown')::bigint as unknown_count
        from analytics.v_site_current_interpretations
        where site_id = target_site_id
        group by site_id
    ),
    score_rollup as (
        select
            site_id,
            max(score_value) filter (where score_code = 'P') as planning_score,
            max(score_value) filter (where score_code = 'G') as ground_score,
            max(score_value) filter (where score_code = 'I') as infrastructure_score,
            max(score_value) filter (where score_code = 'R') as prior_progress_score,
            max(score_value) filter (where score_code = 'F') as fixability_score,
            max(score_value) filter (where score_code = 'K') as control_cost_score,
            max(score_value) filter (where score_code = 'B') as buyer_depth_score
        from analytics.v_site_current_assessment_scores
        where site_id = target_site_id
        group by site_id
    )
    insert into analytics.site_search_cache (
        site_id,
        site_code,
        site_name,
        workflow_status,
        authority_name,
        nearest_settlement,
        settlement_relationship,
        area_acres,
        parcel_count,
        component_count,
        primary_title_number,
        within_settlement_boundary,
        distance_to_settlement_boundary_m,
        previous_application_exists,
        previous_application_outcome,
        allocation_status,
        supportive_nearby_growth_context,
        flood_risk,
        mining_risk,
        access_status,
        critical_constraint_count,
        new_build_comparable_strength,
        comparable_sale_count,
        buyer_fit_count,
        positive_count,
        risk_count,
        possible_fatal_count,
        unknown_count,
        surfaced_reason,
        current_analysis_run_id,
        current_ruleset_version,
        opportunity_bucket,
        bucket_label,
        monetisation_horizon,
        dominant_blocker,
        cost_to_control_band,
        human_review_required,
        planning_score,
        ground_score,
        infrastructure_score,
        prior_progress_score,
        fixability_score,
        control_cost_score,
        buyer_depth_score,
        updated_at
    )
    select
        fact.site_id,
        fact.site_code,
        fact.site_name,
        fact.workflow_status,
        fact.authority_name,
        fact.nearest_settlement,
        fact.settlement_relationship,
        fact.area_acres,
        fact.parcel_count,
        fact.component_count,
        fact.primary_title_number,
        coalesce(signal.within_settlement_boundary, fact.within_settlement_boundary) as within_settlement_boundary,
        coalesce(signal.distance_to_settlement_boundary_m, fact.distance_to_settlement_boundary_m) as distance_to_settlement_boundary_m,
        coalesce(signal.previous_application_exists, coalesce(fact.planning_record_count, 0) > 0) as previous_application_exists,
        signal.previous_application_outcome,
        signal.allocation_status,
        coalesce(fact.supportive_context_count, 0) > 0 as supportive_nearby_growth_context,
        signal.flood_risk,
        signal.mining_risk,
        signal.access_status,
        signal.critical_constraint_count,
        signal.new_build_comparable_strength,
        coalesce(signal.comparable_sale_count, fact.new_build_comparable_count::numeric) as comparable_sale_count,
        coalesce(signal.buyer_fit_count, fact.buyer_fit_count::numeric) as buyer_fit_count,
        coalesce(interpretation.positive_count, 0) as positive_count,
        coalesce(interpretation.risk_count, 0) as risk_count,
        coalesce(interpretation.possible_fatal_count, 0) as possible_fatal_count,
        coalesce(interpretation.unknown_count, 0) as unknown_count,
        coalesce(assessment.primary_reason, fact.surfaced_reason, 'Further structured review recommended') as surfaced_reason,
        fact.current_analysis_run_id,
        fact.current_ruleset_version,
        assessment.bucket_code,
        assessment.bucket_label,
        assessment.monetisation_horizon,
        assessment.dominant_blocker,
        assessment.cost_to_control_band,
        coalesce(assessment.human_review_required, false) as human_review_required,
        score.planning_score,
        score.ground_score,
        score.infrastructure_score,
        score.prior_progress_score,
        score.fixability_score,
        score.control_cost_score,
        score.buyer_depth_score,
        now()
    from analytics.v_site_fact_summary as fact
    left join signal_rollup as signal
        on signal.site_id = fact.site_id
    left join interpretation_rollup as interpretation
        on interpretation.site_id = fact.site_id
    left join analytics.v_site_current_assessments as assessment
        on assessment.site_id = fact.site_id
    left join score_rollup as score
        on score.site_id = fact.site_id
    where fact.site_id = target_site_id
    on conflict (site_id) do update set
        site_code = excluded.site_code,
        site_name = excluded.site_name,
        workflow_status = excluded.workflow_status,
        authority_name = excluded.authority_name,
        nearest_settlement = excluded.nearest_settlement,
        settlement_relationship = excluded.settlement_relationship,
        area_acres = excluded.area_acres,
        parcel_count = excluded.parcel_count,
        component_count = excluded.component_count,
        primary_title_number = excluded.primary_title_number,
        within_settlement_boundary = excluded.within_settlement_boundary,
        distance_to_settlement_boundary_m = excluded.distance_to_settlement_boundary_m,
        previous_application_exists = excluded.previous_application_exists,
        previous_application_outcome = excluded.previous_application_outcome,
        allocation_status = excluded.allocation_status,
        supportive_nearby_growth_context = excluded.supportive_nearby_growth_context,
        flood_risk = excluded.flood_risk,
        mining_risk = excluded.mining_risk,
        access_status = excluded.access_status,
        critical_constraint_count = excluded.critical_constraint_count,
        new_build_comparable_strength = excluded.new_build_comparable_strength,
        comparable_sale_count = excluded.comparable_sale_count,
        buyer_fit_count = excluded.buyer_fit_count,
        positive_count = excluded.positive_count,
        risk_count = excluded.risk_count,
        possible_fatal_count = excluded.possible_fatal_count,
        unknown_count = excluded.unknown_count,
        surfaced_reason = excluded.surfaced_reason,
        current_analysis_run_id = excluded.current_analysis_run_id,
        current_ruleset_version = excluded.current_ruleset_version,
        opportunity_bucket = excluded.opportunity_bucket,
        bucket_label = excluded.bucket_label,
        monetisation_horizon = excluded.monetisation_horizon,
        dominant_blocker = excluded.dominant_blocker,
        cost_to_control_band = excluded.cost_to_control_band,
        human_review_required = excluded.human_review_required,
        planning_score = excluded.planning_score,
        ground_score = excluded.ground_score,
        infrastructure_score = excluded.infrastructure_score,
        prior_progress_score = excluded.prior_progress_score,
        fixability_score = excluded.fixability_score,
        control_cost_score = excluded.control_cost_score,
        buyer_depth_score = excluded.buyer_depth_score,
        updated_at = excluded.updated_at;
$$;

create or replace view analytics.v_site_search_summary
with (security_invoker = true) as
select
    site_id,
    site_code,
    site_name,
    workflow_status,
    authority_name,
    nearest_settlement,
    settlement_relationship,
    area_acres,
    parcel_count,
    primary_title_number,
    within_settlement_boundary,
    distance_to_settlement_boundary_m,
    previous_application_exists,
    previous_application_outcome,
    allocation_status,
    supportive_nearby_growth_context,
    flood_risk,
    mining_risk,
    access_status,
    critical_constraint_count,
    new_build_comparable_strength,
    comparable_sale_count,
    buyer_fit_count,
    positive_count,
    risk_count,
    possible_fatal_count,
    unknown_count,
    surfaced_reason,
    current_analysis_run_id,
    current_ruleset_version,
    opportunity_bucket,
    bucket_label,
    monetisation_horizon,
    dominant_blocker,
    cost_to_control_band,
    human_review_required,
    planning_score,
    ground_score,
    infrastructure_score,
    prior_progress_score,
    fixability_score,
    control_cost_score,
    buyer_depth_score,
    updated_at
from analytics.site_search_cache;

revoke all on function analytics.upsert_site_search_cache_row(uuid) from public, anon, authenticated;


drop view if exists analytics.v_site_search_summary;
drop view if exists analytics.v_site_fact_summary;
drop view if exists analytics.v_site_current_interpretations;
drop view if exists analytics.v_site_current_signals;
drop view if exists analytics.v_site_current_analysis_runs;
drop materialized view if exists analytics.mv_site_search_summary;

create or replace view analytics.v_site_current_analysis_runs
with (security_invoker = true) as
select distinct on (site_id)
    id as analysis_run_id,
    site_id,
    run_type,
    ruleset_version,
    status,
    created_at,
    completed_at,
    metadata
from public.site_analysis_runs
where status = 'completed'
order by site_id, coalesce(completed_at, created_at) desc, created_at desc;

create or replace view analytics.v_site_current_signals
with (security_invoker = true) as
select
    signal.id,
    signal.analysis_run_id,
    signal.site_id,
    signal.signal_key,
    signal.signal_label,
    signal.signal_group,
    signal.value_type,
    signal.signal_state,
    signal.bool_value,
    signal.numeric_value,
    signal.text_value,
    signal.json_value,
    signal.reasoning,
    signal.created_at
from public.site_signals as signal
join analytics.v_site_current_analysis_runs as run
    on run.analysis_run_id = signal.analysis_run_id;

create or replace view analytics.v_site_current_interpretations
with (security_invoker = true) as
select
    interpretation.id,
    interpretation.analysis_run_id,
    interpretation.site_id,
    interpretation.interpretation_key,
    interpretation.category,
    interpretation.title,
    interpretation.summary,
    interpretation.reasoning,
    interpretation.rule_code,
    interpretation.priority,
    interpretation.created_at
from public.site_interpretations as interpretation
join analytics.v_site_current_analysis_runs as run
    on run.analysis_run_id = interpretation.analysis_run_id;

create or replace view analytics.v_site_fact_summary
with (security_invoker = true) as
with parcel_rollup as (
    select
        site_parcel.site_id,
        count(*)::bigint as parcel_count,
        coalesce(
            max(site_parcel.title_number) filter (where site_parcel.is_primary),
            max(site_parcel.title_number)
        ) as primary_title_number,
        round(sum(coalesce(land_object.area_acres, ros_parcel.area_acres, 0)), 3) as area_acres,
        round(sum(coalesce(land_object.area_ha, ros_parcel.area_ha, 0)), 3) as area_ha,
        string_agg(
            distinct coalesce(site_parcel.parcel_reference, ros_parcel.ros_inspire_id, land_object.source_key),
            ', '
        ) as parcel_references
    from public.site_parcels as site_parcel
    left join public.land_objects as land_object
        on land_object.id = site_parcel.land_object_id
    left join public.ros_cadastral_parcels as ros_parcel
        on ros_parcel.id = site_parcel.ros_parcel_id
    group by site_parcel.site_id
),
component_rollup as (
    select
        site_id,
        count(*)::bigint as component_count
    from public.site_geometry_components
    group by site_id
),
planning_rollup as (
    select
        site_id,
        count(*)::bigint as planning_record_count,
        count(*) filter (where lower(coalesce(application_outcome, '')) in ('approved', 'permission granted'))::bigint as approved_planning_count,
        count(*) filter (where lower(coalesce(application_outcome, '')) in ('refused', 'dismissed'))::bigint as refused_planning_count
    from public.planning_records
    group by site_id
),
context_rollup as (
    select
        site_id,
        count(*)::bigint as planning_context_count,
        count(*) filter (
            where lower(coalesce(context_status, '')) in ('allocated', 'supportive', 'approved', 'emerging')
        )::bigint as supportive_context_count
    from public.planning_context_records
    group by site_id
),
constraint_rollup as (
    select
        site_id,
        count(*)::bigint as constraint_count,
        count(*) filter (where severity = 'high')::bigint as high_constraint_count
    from public.site_constraints
    group by site_id
),
comparable_rollup as (
    select
        site_id,
        count(*)::bigint as comparable_count,
        count(*) filter (where comparable_type = 'new_build')::bigint as new_build_comparable_count,
        round(avg(price_per_sqft_gbp), 2) as average_price_per_sqft_gbp
    from public.comparable_market_records
    group by site_id
),
buyer_rollup as (
    select
        site_id,
        count(*) filter (where fit_rating in ('strong', 'moderate'))::bigint as buyer_fit_count,
        count(*) filter (where fit_rating = 'strong')::bigint as strong_buyer_fit_count
    from public.site_buyer_matches
    group by site_id
)
select
    site.id as site_id,
    site.site_code,
    site.site_name,
    site.workflow_status,
    site.source_method,
    site.surfaced_for_review,
    site.surfaced_reason,
    site.metadata,
    location.authority_name,
    location.county,
    location.postcode,
    location.nearest_settlement,
    location.settlement_relationship,
    location.within_settlement_boundary,
    location.distance_to_settlement_boundary_m,
    parcel.parcel_count,
    coalesce(component.component_count, 0) as component_count,
    parcel.primary_title_number,
    parcel.parcel_references,
    parcel.area_acres,
    parcel.area_ha,
    coalesce(planning.planning_record_count, 0) as planning_record_count,
    coalesce(planning.approved_planning_count, 0) as approved_planning_count,
    coalesce(planning.refused_planning_count, 0) as refused_planning_count,
    coalesce(context.planning_context_count, 0) as planning_context_count,
    coalesce(context.supportive_context_count, 0) as supportive_context_count,
    coalesce(site_constraint.constraint_count, 0) as constraint_count,
    coalesce(site_constraint.high_constraint_count, 0) as high_constraint_count,
    coalesce(comparable.comparable_count, 0) as comparable_count,
    coalesce(comparable.new_build_comparable_count, 0) as new_build_comparable_count,
    comparable.average_price_per_sqft_gbp,
    coalesce(buyer.buyer_fit_count, 0) as buyer_fit_count,
    coalesce(buyer.strong_buyer_fit_count, 0) as strong_buyer_fit_count,
    current_run.analysis_run_id as current_analysis_run_id,
    current_run.ruleset_version as current_ruleset_version,
    site.created_at,
    site.updated_at
from public.sites as site
left join public.site_locations as location
    on location.site_id = site.id
left join parcel_rollup as parcel
    on parcel.site_id = site.id
left join component_rollup as component
    on component.site_id = site.id
left join planning_rollup as planning
    on planning.site_id = site.id
left join context_rollup as context
    on context.site_id = site.id
left join constraint_rollup as site_constraint
    on site_constraint.site_id = site.id
left join comparable_rollup as comparable
    on comparable.site_id = site.id
left join buyer_rollup as buyer
    on buyer.site_id = site.id
left join analytics.v_site_current_analysis_runs as current_run
    on current_run.site_id = site.id;

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
            bool_or(coalesce(bool_value, false)) filter (where signal_key = 'supportive_nearby_growth_context') as supportive_nearby_growth_context,
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
            count(*) filter (where category = 'unknown')::bigint as unknown_count,
            string_agg(title, '; ' order by priority) filter (where category = 'positive') as positive_titles,
            string_agg(title, '; ' order by priority) filter (where category = 'unknown') as unknown_titles
        from analytics.v_site_current_interpretations
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
        coalesce(signal.supportive_nearby_growth_context, false) as supportive_nearby_growth_context,
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
        coalesce(
            nullif(fact.surfaced_reason, ''),
            interpretation.positive_titles,
            interpretation.unknown_titles,
            'Further structured review recommended'
        ) as surfaced_reason,
        fact.current_analysis_run_id,
        fact.current_ruleset_version,
        now()
    from analytics.v_site_fact_summary as fact
    left join signal_rollup as signal
        on signal.site_id = fact.site_id
    left join interpretation_rollup as interpretation
        on interpretation.site_id = fact.site_id
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
    updated_at
from analytics.site_search_cache;

revoke all on function analytics.refresh_cached_outputs() from public, anon, authenticated;
revoke all on function analytics.upsert_site_search_cache_row(uuid) from public, anon, authenticated;

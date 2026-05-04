drop view if exists analytics.v_live_opportunity_queue;
drop view if exists analytics.v_live_priority_alerts;
drop view if exists analytics.v_live_site_change_log;
drop view if exists analytics.v_live_site_review_state;
drop view if exists analytics.v_live_site_assessment;
drop view if exists analytics.v_live_site_constraints;
drop view if exists analytics.v_live_site_title;
drop view if exists analytics.v_constraints_tab_overview;
drop view if exists analytics.v_constraints_tab_group_summaries;
drop view if exists analytics.v_constraints_tab_measurements;
drop view if exists analytics.v_constraints_tab_commercial_friction;
drop view if exists analytics.v_live_site_sources;
drop view if exists analytics.v_live_site_readiness;
drop view if exists analytics.v_live_site_summary;
drop view if exists analytics.v_live_site_summary_base;

create or replace view analytics.v_constraints_tab_measurements
with (security_invoker = true) as
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    measurement.buffer_distance_m,
    measurement.constraint_feature_id,
    feature.source_feature_key,
    feature.feature_name,
    feature.source_reference,
    feature.severity_label,
    measurement.measurement_source,
    measurement.intersects,
    measurement.within_buffer,
    measurement.site_inside_feature,
    measurement.feature_inside_site,
    measurement.overlap_area_sqm,
    measurement.overlap_pct_of_site,
    measurement.overlap_pct_of_feature,
    measurement.nearest_distance_m,
    measurement.measured_at
from public.site_constraint_measurements as measurement
join public.constraints_site_anchor() as anchor
  on anchor.site_id = measurement.site_id
 and anchor.site_location_id = measurement.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = measurement.constraint_layer_id
left join public.constraint_source_features as feature
  on feature.id = measurement.constraint_feature_id;

create or replace view analytics.v_constraints_tab_group_summaries
with (security_invoker = true) as
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    summary.constraint_group,
    summary.summary_scope,
    summary.intersecting_feature_count,
    summary.buffered_feature_count,
    summary.total_overlap_area_sqm,
    summary.max_overlap_pct_of_site,
    summary.min_distance_m,
    summary.nearest_feature_id,
    summary.nearest_feature_name,
    summary.measured_at
from public.site_constraint_group_summaries as summary
join public.constraints_site_anchor() as anchor
  on anchor.site_id = summary.site_id
 and anchor.site_location_id = summary.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = summary.constraint_layer_id;

create or replace view analytics.v_constraints_tab_commercial_friction
with (security_invoker = true) as
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    friction.constraint_group,
    friction.fact_key,
    friction.fact_label,
    friction.fact_value_text,
    friction.fact_value_numeric,
    friction.fact_unit,
    friction.fact_basis,
    friction.created_at
from public.site_commercial_friction_facts as friction
join public.constraints_site_anchor() as anchor
  on anchor.site_id = friction.site_id
 and anchor.site_location_id = friction.site_location_id
left join public.constraint_layer_registry as layer
  on layer.id = friction.constraint_layer_id;

create or replace view analytics.v_constraints_tab_overview
with (security_invoker = true) as
with measurement_rollup as (
    select
        measurement.site_id,
        measurement.site_location_id,
        count(distinct measurement.constraint_group)::bigint as constraint_groups_measured,
        count(distinct measurement.constraint_group) filter (where measurement.intersects or measurement.within_buffer)::bigint as constraint_groups_intersecting,
        array_agg(distinct measurement.layer_key order by measurement.layer_key) as measured_layer_keys,
        max(measurement.measured_at) as latest_measurement_at
    from analytics.v_constraints_tab_measurements as measurement
    group by measurement.site_id, measurement.site_location_id
),
friction_rollup as (
    select
        friction.site_id,
        friction.site_location_id,
        count(*)::bigint as friction_fact_count,
        array_agg(distinct friction.fact_label order by friction.fact_label) as friction_fact_labels
    from analytics.v_constraints_tab_commercial_friction as friction
    group by friction.site_id, friction.site_location_id
)
select
    anchor.site_id,
    anchor.site_location_id,
    anchor.site_name,
    anchor.authority_name,
    anchor.area_sqm as site_area_sqm,
    anchor.area_acres as site_area_acres,
    anchor.location_label,
    anchor.location_role,
    coalesce(measurement_rollup.constraint_groups_measured, 0) as constraint_groups_measured,
    coalesce(measurement_rollup.constraint_groups_intersecting, 0) as constraint_groups_intersecting,
    coalesce(measurement_rollup.measured_layer_keys, '{}'::text[]) as measured_layer_keys,
    coalesce(friction_rollup.friction_fact_count, 0) as friction_fact_count,
    coalesce(friction_rollup.friction_fact_labels, '{}'::text[]) as friction_fact_labels,
    measurement_rollup.latest_measurement_at
from public.constraints_site_anchor() as anchor
left join measurement_rollup
  on measurement_rollup.site_id = anchor.site_id
 and measurement_rollup.site_location_id = anchor.site_location_id
left join friction_rollup
  on friction_rollup.site_id = anchor.site_id
 and friction_rollup.site_location_id = anchor.site_location_id;

create or replace view analytics.v_live_site_title
with (security_invoker = true) as
with parcel_links as (
    select
        spatial.site_id::uuid as canonical_site_id,
        count(*) filter (where spatial.linked_record_table = 'public.ros_cadastral_parcels')::bigint as parcel_link_count,
        array_agg(distinct spatial.linked_record_id order by spatial.linked_record_id) filter (where spatial.linked_record_table = 'public.ros_cadastral_parcels') as linked_parcel_ids,
        max(spatial.updated_at) as latest_spatial_update_at
    from public.site_spatial_links as spatial
    where spatial.site_id ~* '^[0-9a-f-]{36}$'
    group by spatial.site_id::uuid
),
title_rollup as (
    select
        title.site_id::uuid as canonical_site_id,
        count(*)::bigint as title_validation_count,
        count(*) filter (where lower(coalesce(title.validation_status, '')) in ('title_reviewed', 'validated', 'confirmed'))::bigint as title_reviewed_count,
        array_agg(distinct coalesce(title.matched_title_number, title.title_number) order by coalesce(title.matched_title_number, title.title_number)) filter (
            where coalesce(title.matched_title_number, title.title_number) is not null
        ) as title_numbers,
        max(title.updated_at) as latest_title_update_at
    from public.site_title_validation as title
    where title.site_id ~* '^[0-9a-f-]{36}$'
    group by title.site_id::uuid
),
latest_title as (
    select
        ranked.canonical_site_id,
        ranked.validation_status,
        ranked.validation_method,
        ranked.title_source,
        ranked.confidence
    from (
        select
            title.site_id::uuid as canonical_site_id,
            title.validation_status,
            title.validation_method,
            title.title_source,
            title.confidence,
            row_number() over (
                partition by title.site_id::uuid
                order by title.updated_at desc nulls last, title.created_at desc, title.id desc
            ) as row_number
        from public.site_title_validation as title
        where title.site_id ~* '^[0-9a-f-]{36}$'
    ) as ranked
    where ranked.row_number = 1
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    coalesce(parcel_links.parcel_link_count, case when site.primary_ros_parcel_id is not null then 1 else 0 end, 0) as parcel_link_count,
    coalesce(parcel_links.linked_parcel_ids, case when site.primary_ros_parcel_id is not null then array[site.primary_ros_parcel_id::text] else '{}'::text[] end) as linked_parcel_ids,
    coalesce(title_rollup.title_validation_count, 0) as title_validation_count,
    coalesce(title_rollup.title_numbers, '{}'::text[]) as title_numbers,
    coalesce(title_rollup.title_reviewed_count, 0) > 0 as title_reviewed_flag,
    case
        when coalesce(title_rollup.title_reviewed_count, 0) > 0 then 'title_reviewed'
        else 'commercial_inference'
    end as title_state,
    case
        when coalesce(title_rollup.title_reviewed_count, 0) > 0 then 'title_reviewed'
        else 'commercial_inference'
    end as ownership_control_fact_label,
    latest_title.validation_status,
    latest_title.validation_method,
    latest_title.title_source,
    latest_title.confidence,
    greatest(
        coalesce(parcel_links.latest_spatial_update_at, site.updated_at),
        coalesce(title_rollup.latest_title_update_at, site.updated_at)
    ) as latest_title_update_at,
    case
        when coalesce(title_rollup.title_reviewed_count, 0) > 0 then 'Legal ownership certainty is only shown after title review.'
        when coalesce(title_rollup.title_validation_count, 0) > 0 then 'Title-related evidence exists but ownership remains a commercial inference until title is reviewed.'
        when site.primary_ros_parcel_id is not null then 'Parcel linkage exists, but ownership remains a commercial inference until title is reviewed.'
        else 'No title review has been completed yet; ownership remains a commercial inference.'
    end as title_summary
from landintel.canonical_sites as site
left join parcel_links
  on parcel_links.canonical_site_id = site.id
left join title_rollup
  on title_rollup.canonical_site_id = site.id
left join latest_title
  on latest_title.canonical_site_id = site.id;

create or replace view analytics.v_live_site_constraints
with (security_invoker = true) as
with overview as (
    select
        overview.site_id::uuid as canonical_site_id,
        overview.constraint_groups_measured,
        overview.constraint_groups_intersecting,
        overview.measured_layer_keys,
        overview.friction_fact_count,
        overview.friction_fact_labels,
        overview.latest_measurement_at
    from analytics.v_constraints_tab_overview as overview
    where overview.site_id ~* '^[0-9a-f-]{36}$'
),
group_rollup as (
    select
        summary.site_id::uuid as canonical_site_id,
        count(*)::bigint as group_summary_count,
        count(*) filter (where coalesce(summary.intersecting_feature_count, 0) > 0)::bigint as active_group_count,
        coalesce(max(summary.max_overlap_pct_of_site), 0) as max_overlap_pct_of_site,
        coalesce(min(summary.min_distance_m), 0) as nearest_constraint_distance_m,
        array_agg(distinct summary.constraint_group order by summary.constraint_group) as constraint_groups_present
    from analytics.v_constraints_tab_group_summaries as summary
    where summary.site_id ~* '^[0-9a-f-]{36}$'
    group by summary.site_id::uuid
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    coalesce(overview.constraint_groups_measured, 0) as constraint_groups_measured,
    coalesce(overview.constraint_groups_intersecting, 0) as constraint_groups_intersecting,
    coalesce(group_rollup.group_summary_count, 0) as group_summary_count,
    coalesce(group_rollup.active_group_count, 0) as active_group_count,
    coalesce(group_rollup.constraint_groups_present, '{}'::text[]) as constraint_groups_present,
    coalesce(overview.measured_layer_keys, '{}'::text[]) as measured_layer_keys,
    coalesce(overview.friction_fact_count, 0) as friction_fact_count,
    coalesce(overview.friction_fact_labels, '{}'::text[]) as friction_fact_labels,
    coalesce(group_rollup.max_overlap_pct_of_site, 0) as max_overlap_pct_of_site,
    coalesce(group_rollup.nearest_constraint_distance_m, 0) as nearest_constraint_distance_m,
    case
        when coalesce(group_rollup.active_group_count, 0) = 0 and coalesce(overview.constraint_groups_measured, 0) = 0 then 'unknown'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 25 then 'severe'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 10 then 'material'
        when coalesce(group_rollup.active_group_count, 0) > 0 then 'conditional'
        else 'limited'
    end as constraint_severity,
    case
        when coalesce(group_rollup.active_group_count, 0) = 0 and coalesce(overview.constraint_groups_measured, 0) = 0 then 'No measured constraints have been linked yet.'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 25 then 'A material part of the site overlaps severe constraints.'
        when coalesce(group_rollup.max_overlap_pct_of_site, 0) >= 10 then 'Constraints overlap a meaningful part of the site and need review.'
        when coalesce(group_rollup.active_group_count, 0) > 0 then 'Constraints are present but may still be workable.'
        else 'Measured constraints are currently limited.'
    end as constraint_summary,
    overview.latest_measurement_at as latest_constraint_update_at
from landintel.canonical_sites as site
left join overview
  on overview.canonical_site_id = site.id
left join group_rollup
  on group_rollup.canonical_site_id = site.id;

create or replace view analytics.v_live_site_assessment
with (security_invoker = true) as
with latest_assessment as (
    select
        ranked.*
    from (
        select
            assessment.*,
            row_number() over (
                partition by assessment.canonical_site_id
                order by coalesce(assessment.latest_assessment_at, assessment.created_at) desc,
                    assessment.assessment_version desc,
                    assessment.created_at desc,
                    assessment.id desc
            ) as row_number
        from landintel.site_assessments as assessment
    ) as ranked
    where ranked.row_number = 1
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    assessment.id as site_assessment_id,
    coalesce(assessment.overall_tier, 'Unranked') as overall_tier,
    assessment.overall_rank_score,
    coalesce(assessment.queue_recommendation, 'New Candidates') as queue_recommendation,
    assessment.why_it_surfaced,
    assessment.why_it_survived,
    coalesce(assessment.good_items, '[]'::jsonb) as good_items,
    coalesce(assessment.bad_items, '[]'::jsonb) as bad_items,
    coalesce(assessment.ugly_items, '[]'::jsonb) as ugly_items,
    coalesce(assessment.subrank_summary, '{}'::jsonb) as subrank_summary,
    assessment.subrank_summary ->> 'size_rank' as size_rank,
    assessment.subrank_summary ->> 'planning_context_rank' as planning_context_rank,
    assessment.subrank_summary ->> 'location_rank' as location_rank,
    assessment.subrank_summary ->> 'constraints_rank' as constraints_rank,
    assessment.subrank_summary ->> 'access_rank' as access_rank,
    assessment.subrank_summary ->> 'geometry_rank' as geometry_rank,
    assessment.subrank_summary ->> 'ownership_control_rank' as ownership_control_rank,
    assessment.subrank_summary ->> 'utilities_burden_rank' as utilities_burden_rank,
    assessment.subrank_summary ->> 'redevelopment_angle_rank' as redevelopment_angle_rank,
    assessment.subrank_summary ->> 'stalled_site_angle_rank' as stalled_site_angle_rank,
    assessment.subrank_summary ->> 'settlement_position' as settlement_position,
    assessment.subrank_summary ->> 'location_band' as location_band,
    assessment.subrank_summary ->> 'access_strength' as access_strength,
    assessment.subrank_summary ->> 'geometry_quality' as geometry_quality,
    assessment.subrank_summary ->> 'ownership_control_state' as ownership_control_state,
    assessment.subrank_summary ->> 'constraint_severity' as constraint_severity,
    assessment.subrank_summary ->> 'planning_context_band' as planning_context_band,
    coalesce(assessment.title_state, 'commercial_inference') as title_state,
    coalesce(assessment.ownership_control_fact_label, 'commercial_inference') as ownership_control_fact_label,
    coalesce(assessment.resurfaced_reason, '') as resurfaced_reason,
    coalesce(assessment.human_review_required, false) as human_review_required,
    assessment.dominant_blocker,
    assessment.explanation_text,
    coalesce(assessment.latest_assessment_at, assessment.created_at) as latest_assessment_at
from landintel.canonical_sites as site
left join latest_assessment as assessment
  on assessment.canonical_site_id = site.id;

create or replace view analytics.v_live_site_review_state
with (security_invoker = true) as
with latest_status as (
    select
        ranked.canonical_site_id,
        ranked.review_status,
        ranked.actor_name,
        ranked.reason_text,
        ranked.created_at
    from (
        select
            event.canonical_site_id,
            event.review_status,
            event.actor_name,
            event.reason_text,
            event.created_at,
            row_number() over (
                partition by event.canonical_site_id
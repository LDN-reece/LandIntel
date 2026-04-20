drop view if exists analytics.v_constraints_tab_overview;
drop view if exists analytics.v_constraints_tab_commercial_friction;
drop view if exists analytics.v_constraints_tab_group_summaries;
drop view if exists analytics.v_constraints_tab_measurements;

create or replace view analytics.v_constraints_tab_measurements
with (security_invoker = true) as
with site_anchor as (
    select *
    from public.constraints_site_anchor()
)
select
    site_anchor.site_id,
    site_anchor.site_location_id,
    site_anchor.site_name,
    site_anchor.authority_name,
    site_anchor.area_acres as site_area_acres,
    site_anchor.location_label,
    site_anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    layer.buffer_distance_m,
    feature.id as constraint_feature_id,
    feature.source_feature_key,
    feature.feature_name,
    feature.source_reference,
    feature.severity_label,
    measurements.measurement_source,
    measurements.intersects,
    measurements.within_buffer,
    measurements.site_inside_feature,
    measurements.feature_inside_site,
    measurements.overlap_area_sqm,
    measurements.overlap_pct_of_site,
    measurements.overlap_pct_of_feature,
    measurements.nearest_distance_m,
    measurements.measured_at
from public.site_constraint_measurements as measurements
join site_anchor
  on site_anchor.site_id = measurements.site_id
 and site_anchor.site_location_id = measurements.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = measurements.constraint_layer_id
left join public.constraint_source_features as feature
  on feature.id = measurements.constraint_feature_id
order by site_anchor.site_name, layer.constraint_group, layer.layer_name, feature.feature_name nulls last, feature.source_feature_key;

create or replace view analytics.v_constraints_tab_group_summaries
with (security_invoker = true) as
with site_anchor as (
    select *
    from public.constraints_site_anchor()
)
select
    site_anchor.site_id,
    site_anchor.site_location_id,
    site_anchor.site_name,
    site_anchor.authority_name,
    site_anchor.area_acres as site_area_acres,
    site_anchor.location_label,
    site_anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    layer.constraint_group,
    summaries.summary_scope,
    summaries.intersecting_feature_count,
    summaries.buffered_feature_count,
    summaries.total_overlap_area_sqm,
    summaries.max_overlap_pct_of_site,
    summaries.min_distance_m,
    summaries.nearest_feature_id,
    summaries.nearest_feature_name,
    summaries.measured_at
from public.site_constraint_group_summaries as summaries
join site_anchor
  on site_anchor.site_id = summaries.site_id
 and site_anchor.site_location_id = summaries.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = summaries.constraint_layer_id
order by site_anchor.site_name, layer.constraint_group, layer.layer_name;

create or replace view analytics.v_constraints_tab_commercial_friction
with (security_invoker = true) as
with site_anchor as (
    select *
    from public.constraints_site_anchor()
)
select
    site_anchor.site_id,
    site_anchor.site_location_id,
    site_anchor.site_name,
    site_anchor.authority_name,
    site_anchor.area_acres as site_area_acres,
    site_anchor.location_label,
    site_anchor.location_role,
    layer.layer_key,
    layer.layer_name,
    facts.constraint_group,
    facts.fact_key,
    facts.fact_label,
    facts.fact_value_text,
    facts.fact_value_numeric,
    facts.fact_unit,
    facts.fact_basis,
    facts.created_at
from public.site_commercial_friction_facts as facts
join site_anchor
  on site_anchor.site_id = facts.site_id
 and site_anchor.site_location_id = facts.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = facts.constraint_layer_id
order by site_anchor.site_name, facts.constraint_group, facts.fact_label;

create or replace view analytics.v_constraints_tab_overview
with (security_invoker = true) as
with site_anchor as (
    select *
    from public.constraints_site_anchor()
),
group_rollup as (
    select
        summaries.site_id,
        summaries.site_location_id,
        count(distinct layer.constraint_group) as constraint_groups_measured,
        count(distinct layer.constraint_group) filter (where summaries.intersecting_feature_count > 0) as constraint_groups_intersecting,
        array_agg(distinct layer.layer_key order by layer.layer_key) as measured_layer_keys,
        max(summaries.measured_at) as latest_measurement_at
    from public.site_constraint_group_summaries as summaries
    join public.constraint_layer_registry as layer
      on layer.id = summaries.constraint_layer_id
    group by summaries.site_id, summaries.site_location_id
),
fact_rollup as (
    select
        facts.site_id,
        facts.site_location_id,
        count(*)::bigint as friction_fact_count,
        array_agg(distinct facts.fact_label order by facts.fact_label) as friction_fact_labels
    from public.site_commercial_friction_facts as facts
    group by facts.site_id, facts.site_location_id
)
select
    site_anchor.site_id,
    site_anchor.site_location_id,
    site_anchor.site_name,
    site_anchor.authority_name,
    site_anchor.area_sqm as site_area_sqm,
    site_anchor.area_acres as site_area_acres,
    site_anchor.location_label,
    site_anchor.location_role,
    coalesce(group_rollup.constraint_groups_measured, 0) as constraint_groups_measured,
    coalesce(group_rollup.constraint_groups_intersecting, 0) as constraint_groups_intersecting,
    coalesce(group_rollup.measured_layer_keys, '{}'::text[]) as measured_layer_keys,
    coalesce(fact_rollup.friction_fact_count, 0) as friction_fact_count,
    coalesce(fact_rollup.friction_fact_labels, '{}'::text[]) as friction_fact_labels,
    group_rollup.latest_measurement_at
from site_anchor
left join group_rollup
  on group_rollup.site_id = site_anchor.site_id
 and group_rollup.site_location_id = site_anchor.site_location_id
left join fact_rollup
  on fact_rollup.site_id = site_anchor.site_id
 and fact_rollup.site_location_id = site_anchor.site_location_id
order by site_anchor.site_name, site_anchor.site_location_id;

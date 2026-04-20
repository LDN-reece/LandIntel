create or replace function public.measure_constraint_feature(
    site_geometry geometry,
    feature_geometry geometry,
    buffer_distance_m numeric default 0
)
returns table (
    intersects boolean,
    within_buffer boolean,
    site_inside_feature boolean,
    feature_inside_site boolean,
    overlap_area_sqm numeric,
    overlap_pct_of_site numeric,
    overlap_pct_of_feature numeric,
    nearest_distance_m numeric
)
language sql
immutable
set search_path = pg_catalog, public
as $$
    with cleaned as (
        select
            case when site_geometry is null then null else st_makevalid(site_geometry) end as site_geometry,
            case when feature_geometry is null then null else st_makevalid(feature_geometry) end as feature_geometry,
            greatest(coalesce(buffer_distance_m, 0), 0) as buffer_distance_m
    ),
    metrics as (
        select
            st_intersects(site_geometry, feature_geometry) as intersects,
            case
                when buffer_distance_m > 0 then st_dwithin(site_geometry, feature_geometry, buffer_distance_m)
                else st_intersects(site_geometry, feature_geometry)
            end as within_buffer,
            st_coveredby(site_geometry, feature_geometry) as site_inside_feature,
            st_coveredby(feature_geometry, site_geometry) as feature_inside_site,
            case
                when st_intersects(site_geometry, feature_geometry)
                    then st_area(st_intersection(site_geometry, feature_geometry))
                else 0::double precision
            end as overlap_area_sqm,
            st_distance(site_geometry, feature_geometry) as nearest_distance_m,
            nullif(st_area(site_geometry), 0) as site_area_sqm,
            nullif(st_area(feature_geometry), 0) as feature_area_sqm
        from cleaned
        where site_geometry is not null
          and feature_geometry is not null
    )
    select
        metrics.intersects,
        metrics.within_buffer,
        metrics.site_inside_feature,
        metrics.feature_inside_site,
        round(coalesce(metrics.overlap_area_sqm, 0)::numeric, 2) as overlap_area_sqm,
        round(coalesce((metrics.overlap_area_sqm / metrics.site_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_site,
        round(coalesce((metrics.overlap_area_sqm / metrics.feature_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_feature,
        round(metrics.nearest_distance_m::numeric, 2) as nearest_distance_m
    from metrics

    union all

    select
        false,
        false,
        false,
        false,
        0::numeric,
        0::numeric,
        0::numeric,
        null::numeric
    where not exists (select 1 from metrics)
    limit 1;
$$;

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

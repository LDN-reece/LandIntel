create index if not exists site_constraint_group_summaries_layer_only_idx
    on public.site_constraint_group_summaries (constraint_layer_id);

create index if not exists site_commercial_friction_facts_layer_only_idx
    on public.site_commercial_friction_facts (constraint_layer_id);

drop view if exists analytics.v_constraint_measurement_layer_coverage;
drop view if exists analytics.v_constraint_measurement_coverage;

create or replace view analytics.v_constraint_measurement_layer_coverage
with (security_invoker = true) as
with feature_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as source_feature_count
    from public.constraint_source_features
    group by constraint_layer_id
),
measurement_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as measured_row_count,
        count(distinct site_location_id)::bigint as measured_site_count,
        count(*) filter (where overlap_character is null)::bigint as missing_overlap_character_count,
        count(distinct overlap_character) filter (where overlap_character is not null)::bigint as overlap_character_type_count,
        max(measured_at) as latest_measured_at
    from public.site_constraint_measurements
    group by constraint_layer_id
),
summary_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as summary_row_count
    from public.site_constraint_group_summaries
    group by constraint_layer_id
),
fact_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as commercial_friction_fact_count
    from public.site_commercial_friction_facts
    group by constraint_layer_id
),
scan_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as scan_state_row_count,
        count(distinct site_location_id)::bigint as scanned_site_count
    from public.site_constraint_measurement_scan_state
    group by constraint_layer_id
)
select
    layer.layer_key,
    layer.layer_name,
    layer.source_family,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    layer.buffer_distance_m,
    layer.is_active,
    coalesce(feature_counts.source_feature_count, 0::bigint) as source_feature_count,
    coalesce(measurement_counts.measured_row_count, 0::bigint) as measured_row_count,
    coalesce(measurement_counts.measured_site_count, 0::bigint) as measured_site_count,
    coalesce(summary_counts.summary_row_count, 0::bigint) as summary_row_count,
    coalesce(fact_counts.commercial_friction_fact_count, 0::bigint) as commercial_friction_fact_count,
    coalesce(scan_counts.scan_state_row_count, 0::bigint) as scan_state_row_count,
    coalesce(scan_counts.scanned_site_count, 0::bigint) as scanned_site_count,
    coalesce(measurement_counts.missing_overlap_character_count, 0::bigint) as missing_overlap_character_count,
    coalesce(measurement_counts.overlap_character_type_count, 0::bigint) as overlap_character_type_count,
    measurement_counts.latest_measured_at
from public.constraint_layer_registry as layer
left join feature_counts
  on feature_counts.constraint_layer_id = layer.id
left join measurement_counts
  on measurement_counts.constraint_layer_id = layer.id
left join summary_counts
  on summary_counts.constraint_layer_id = layer.id
left join fact_counts
  on fact_counts.constraint_layer_id = layer.id
left join scan_counts
  on scan_counts.constraint_layer_id = layer.id;

create or replace view analytics.v_constraint_measurement_coverage
with (security_invoker = true) as
with canonical_sites as (
    select id::text as site_location_id
    from landintel.canonical_sites
    where geometry is not null
),
active_layers_with_features as (
    select layer.id
    from public.constraint_layer_registry as layer
    where layer.is_active = true
      and exists (
          select 1
          from public.constraint_source_features as feature
          where feature.constraint_layer_id = layer.id
      )
),
measured_sites as (
    select distinct site_location_id
    from public.site_constraint_measurements
),
scanned_sites as (
    select distinct site_location_id
    from public.site_constraint_measurement_scan_state
),
scalar_counts as (
    select
        (select count(*)::bigint from public.constraint_layer_registry where is_active = true) as active_constraint_layer_count,
        (select count(*)::bigint from public.constraint_source_features) as source_constraint_feature_count,
        (select count(*)::bigint from public.site_constraint_measurements) as measured_site_constraint_row_count,
        (select count(*)::bigint from public.site_constraint_group_summaries) as grouped_summary_row_count,
        (select count(*)::bigint from public.site_commercial_friction_facts) as commercial_friction_fact_count,
        (select count(*)::bigint from public.site_constraint_measurement_scan_state) as constraint_scan_state_row_count,
        (select count(*)::bigint from public.site_constraint_measurements where overlap_character is null) as missing_overlap_character_count,
        (
            select count(distinct layer.source_family)::bigint
            from public.site_constraint_measurements as measurement
            join public.constraint_layer_registry as layer
              on layer.id = measurement.constraint_layer_id
        ) as measured_constraint_source_family_count,
        (select count(*)::bigint from landintel.flood_records) as flood_record_count,
        (
            select count(*)::bigint
            from public.constraint_source_features as feature
            join public.constraint_layer_registry as layer
              on layer.id = feature.constraint_layer_id
            where layer.source_family = 'sepa_flood'
        ) as sepa_flood_source_feature_count,
        (
            select count(*)::bigint
            from public.site_constraint_measurements as measurement
            join public.constraint_layer_registry as layer
              on layer.id = measurement.constraint_layer_id
            where layer.source_family = 'sepa_flood'
        ) as sepa_flood_measurement_count,
        (select count(*)::numeric from active_layers_with_features) as active_layer_with_feature_count
)
select
    scalar_counts.active_constraint_layer_count,
    scalar_counts.source_constraint_feature_count,
    scalar_counts.measured_site_constraint_row_count,
    scalar_counts.grouped_summary_row_count,
    scalar_counts.commercial_friction_fact_count,
    scalar_counts.constraint_scan_state_row_count,
    scalar_counts.missing_overlap_character_count,
    scalar_counts.measured_constraint_source_family_count,
    scalar_counts.flood_record_count,
    scalar_counts.sepa_flood_source_feature_count,
    scalar_counts.sepa_flood_measurement_count,
    count(canonical_sites.site_location_id)::bigint as canonical_site_geometry_count,
    count(canonical_sites.site_location_id) filter (where measured_sites.site_location_id is not null)::bigint
        as canonical_sites_with_measured_constraints,
    count(canonical_sites.site_location_id) filter (where measured_sites.site_location_id is null)::bigint
        as canonical_sites_without_measured_constraints,
    count(canonical_sites.site_location_id) filter (where scanned_sites.site_location_id is not null)::bigint
        as canonical_sites_with_constraint_scan_state,
    (
        count(canonical_sites.site_location_id)::numeric
        * greatest(scalar_counts.active_layer_with_feature_count, 1)
    )::bigint as canonical_site_layer_pair_count,
    scalar_counts.constraint_scan_state_row_count as scanned_site_layer_pair_count,
    round(
        (
            scalar_counts.constraint_scan_state_row_count::numeric
            / nullif(
                count(canonical_sites.site_location_id)::numeric
                * scalar_counts.active_layer_with_feature_count,
                0
            )
        ) * 100,
        4
    ) as scanned_site_layer_pair_pct,
    (select max(measured_at) from public.site_constraint_measurements) as latest_measured_at
from scalar_counts
cross join canonical_sites
left join measured_sites
  on measured_sites.site_location_id = canonical_sites.site_location_id
left join scanned_sites
  on scanned_sites.site_location_id = canonical_sites.site_location_id
group by
    scalar_counts.active_constraint_layer_count,
    scalar_counts.source_constraint_feature_count,
    scalar_counts.measured_site_constraint_row_count,
    scalar_counts.grouped_summary_row_count,
    scalar_counts.commercial_friction_fact_count,
    scalar_counts.constraint_scan_state_row_count,
    scalar_counts.missing_overlap_character_count,
    scalar_counts.measured_constraint_source_family_count,
    scalar_counts.flood_record_count,
    scalar_counts.sepa_flood_source_feature_count,
    scalar_counts.sepa_flood_measurement_count,
    scalar_counts.active_layer_with_feature_count;

grant select on analytics.v_constraint_measurement_layer_coverage to authenticated;
grant select on analytics.v_constraint_measurement_coverage to authenticated;

comment on view analytics.v_constraint_measurement_layer_coverage
    is 'Performance-safe Priority Zero layer coverage proof view. Counts are pre-aggregated by layer before joining to the registry to avoid multiplicative joins across feature, measurement, summary, fact and scan-state tables.';

comment on view analytics.v_constraint_measurement_coverage
    is 'Performance-safe Priority Zero estate coverage proof view. Uses direct canonical site counts and pre-aggregated measurement state so audit workflows do not consume spatial compute.';

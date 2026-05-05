create schema if not exists landintel_sourced;

comment on schema landintel_sourced is
    'LandIntel Sourced Sites, the polished commercial opportunity register for LDN review.';

create or replace view landintel_sourced.v_site_location_context
with (security_invoker = true) as
with base_sites as (
    select *
    from landintel_sourced.v_site_title_area_access_context
), raw_context as (
    select
        context.canonical_site_id,
        context.source_key,
        context.source_family,
        context.feature_type,
        context.nearest_feature_name,
        context.nearest_distance_m,
        context.count_within_400m,
        context.count_within_800m,
        context.count_within_1600m,
        context.measured_at,
        lower(concat_ws(
            ' ',
            context.source_key,
            context.source_family,
            context.feature_type,
            context.nearest_feature_name
        )) as context_text
    from landintel.site_open_location_spine_context as context
), classified_context as (
    select
        raw_context.*,
        case
            when raw_context.context_text like '%%road%%'
              or raw_context.context_text like '%%street%%'
              or raw_context.context_text like '%%highway%%'
                then 'road_access'
            when raw_context.context_text like '%%school%%'
              or raw_context.context_text like '%%education%%'
              or raw_context.context_text like '%%catchment%%'
              or raw_context.context_text like '%%college%%'
              or raw_context.context_text like '%%nursery%%'
                then 'education'
            when raw_context.context_text like '%%health%%'
              or raw_context.context_text like '%%medical%%'
              or raw_context.context_text like '%%hospital%%'
              or raw_context.context_text like '%%doctor%%'
              or raw_context.context_text like '%%general practice%%'
              or raw_context.context_text like '%%dentist%%'
                then 'healthcare'
            when raw_context.context_text like '%%transport%%'
              or raw_context.context_text like '%%naptan%%'
              or raw_context.context_text like '%%bus%%'
              or raw_context.context_text like '%%rail%%'
              or raw_context.context_text like '%%station%%'
              or raw_context.context_text like '%%tram%%'
                then 'transport'
            when raw_context.context_text like '%%greenspace%%'
              or raw_context.context_text like '%%open space%%'
              or raw_context.context_text like '%%open_space%%'
              or raw_context.context_text like '%%park%%'
              or raw_context.context_text like '%%recreation%%'
              or raw_context.context_text like '%%sports%%'
                then 'open_space'
            when raw_context.context_text like '%%water%%'
              or raw_context.context_text like '%%river%%'
              or raw_context.context_text like '%%burn%%'
              or raw_context.context_text like '%%loch%%'
              or raw_context.context_text like '%%canal%%'
                then 'water'
            when raw_context.context_text like '%%boundary%%'
              or raw_context.context_text like '%%local authority%%'
              or raw_context.context_text like '%%local_authority%%'
              or raw_context.context_text like '%%council%%'
                then 'authority_boundary'
            else 'other_location_context'
        end as context_family
    from raw_context
), nearest_context as (
    select distinct on (classified_context.canonical_site_id, classified_context.context_family)
        classified_context.canonical_site_id,
        classified_context.context_family,
        classified_context.source_key,
        classified_context.source_family,
        classified_context.feature_type,
        classified_context.nearest_feature_name,
        classified_context.nearest_distance_m,
        classified_context.count_within_400m,
        classified_context.count_within_800m,
        classified_context.count_within_1600m,
        classified_context.measured_at
    from classified_context
    order by
        classified_context.canonical_site_id,
        classified_context.context_family,
        classified_context.nearest_distance_m nulls last,
        classified_context.measured_at desc nulls last
), context_rollup as (
    select
        classified_context.canonical_site_id,
        count(*)::integer as location_context_row_count,
        count(distinct classified_context.context_family)::integer as measured_location_context_family_count,
        array_remove(
            array_agg(distinct classified_context.context_family order by classified_context.context_family),
            null
        ) as measured_location_context_families,
        max(classified_context.measured_at) as latest_location_context_measured_at
    from classified_context
    group by classified_context.canonical_site_id
), context_pivot as (
    select
        nearest_context.canonical_site_id,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'road_access') as nearest_road_context_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'road_access') as nearest_road_context_name,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'education') as nearest_education_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'education') as nearest_education_name,
        max(nearest_context.count_within_1600m) filter (where nearest_context.context_family = 'education') as education_count_within_1600m,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'healthcare') as nearest_healthcare_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'healthcare') as nearest_healthcare_name,
        max(nearest_context.count_within_1600m) filter (where nearest_context.context_family = 'healthcare') as healthcare_count_within_1600m,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'transport') as nearest_transport_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'transport') as nearest_transport_name,
        max(nearest_context.count_within_1600m) filter (where nearest_context.context_family = 'transport') as transport_count_within_1600m,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'open_space') as nearest_open_space_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'open_space') as nearest_open_space_name,
        max(nearest_context.count_within_1600m) filter (where nearest_context.context_family = 'open_space') as open_space_count_within_1600m,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'water') as nearest_water_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'water') as nearest_water_name,
        max(nearest_context.nearest_distance_m) filter (where nearest_context.context_family = 'authority_boundary') as nearest_authority_boundary_distance_m,
        max(nearest_context.nearest_feature_name) filter (where nearest_context.context_family = 'authority_boundary') as nearest_authority_boundary_name
    from nearest_context
    group by nearest_context.canonical_site_id
), operator_context as (
    select
        base_sites.*,
        coalesce(context_rollup.location_context_row_count, 0) as location_context_row_count,
        coalesce(context_rollup.measured_location_context_family_count, 0) as measured_location_context_family_count,
        coalesce(context_rollup.measured_location_context_families, '{}'::text[]) as measured_location_context_families,
        context_rollup.latest_location_context_measured_at,
        context_pivot.nearest_education_name,
        context_pivot.nearest_education_distance_m,
        coalesce(context_pivot.education_count_within_1600m, 0)::integer as education_count_within_1600m,
        context_pivot.nearest_healthcare_name,
        context_pivot.nearest_healthcare_distance_m,
        coalesce(context_pivot.healthcare_count_within_1600m, 0)::integer as healthcare_count_within_1600m,
        context_pivot.nearest_transport_name,
        context_pivot.nearest_transport_distance_m,
        coalesce(context_pivot.transport_count_within_1600m, 0)::integer as transport_count_within_1600m,
        context_pivot.nearest_open_space_name,
        context_pivot.nearest_open_space_distance_m,
        coalesce(context_pivot.open_space_count_within_1600m, 0)::integer as open_space_count_within_1600m,
        context_pivot.nearest_water_name,
        context_pivot.nearest_water_distance_m,
        context_pivot.nearest_authority_boundary_name,
        context_pivot.nearest_authority_boundary_distance_m,
        (
            coalesce(context_pivot.education_count_within_1600m, 0)
          + coalesce(context_pivot.healthcare_count_within_1600m, 0)
          + coalesce(context_pivot.transport_count_within_1600m, 0)
        )::integer as service_anchor_count_within_1600m,
        least(
            context_pivot.nearest_education_distance_m,
            context_pivot.nearest_healthcare_distance_m,
            context_pivot.nearest_transport_distance_m
        ) as nearest_service_anchor_distance_m
    from base_sites
    left join context_rollup
      on context_rollup.canonical_site_id = base_sites.canonical_site_id
    left join context_pivot
      on context_pivot.canonical_site_id = base_sites.canonical_site_id
)
select
    operator_context.canonical_site_id,
    operator_context.site_label,
    operator_context.legal_title_number,
    operator_context.legal_title_numbers,
    operator_context.legal_title_number_status,
    operator_context.address,
    operator_context.local_area_or_settlement_name,
    operator_context.local_authority,
    operator_context.local_council,
    operator_context.site_area_acres,
    operator_context.title_area_acres,
    operator_context.title_area_status,
    operator_context.developable_geometry_status,
    operator_context.developable_geometry_flag,
    operator_context.road_access_context_status,
    operator_context.landlocked_context_risk,
    operator_context.nearest_road_name,
    operator_context.nearest_road_distance_m,
    operator_context.location_context_row_count,
    operator_context.measured_location_context_family_count,
    operator_context.measured_location_context_families,
    operator_context.latest_location_context_measured_at,
    operator_context.nearest_education_name,
    operator_context.nearest_education_distance_m,
    operator_context.education_count_within_1600m,
    operator_context.nearest_healthcare_name,
    operator_context.nearest_healthcare_distance_m,
    operator_context.healthcare_count_within_1600m,
    operator_context.nearest_transport_name,
    operator_context.nearest_transport_distance_m,
    operator_context.transport_count_within_1600m,
    operator_context.nearest_open_space_name,
    operator_context.nearest_open_space_distance_m,
    operator_context.open_space_count_within_1600m,
    operator_context.nearest_water_name,
    operator_context.nearest_water_distance_m,
    operator_context.service_anchor_count_within_1600m,
    operator_context.nearest_service_anchor_distance_m,
    case
        when operator_context.location_context_row_count = 0
            then 'location_context_not_measured'
        when operator_context.measured_location_context_families @> array['road_access', 'education', 'healthcare', 'transport']::text[]
            then 'core_location_context_measured'
        else 'partial_location_context_measured'
    end as location_context_status,
    case
        when operator_context.local_area_or_settlement_name is not null
            then 'settlement_or_local_area_named'
        else 'settlement_or_local_area_not_held'
    end as settlement_context_status,
    case
        when operator_context.nearest_education_distance_m is null
            then 'education_context_not_measured'
        when operator_context.nearest_education_distance_m <= 1600
          or operator_context.education_count_within_1600m > 0
            then 'education_anchor_within_1600m_context'
        else 'education_anchor_beyond_1600m_context'
    end as education_context_status,
    case
        when operator_context.nearest_healthcare_distance_m is null
            then 'healthcare_context_not_measured'
        when operator_context.nearest_healthcare_distance_m <= 1600
          or operator_context.healthcare_count_within_1600m > 0
            then 'healthcare_anchor_within_1600m_context'
        else 'healthcare_anchor_beyond_1600m_context'
    end as healthcare_context_status,
    case
        when operator_context.nearest_transport_distance_m is null
            then 'transport_context_not_measured'
        when operator_context.nearest_transport_distance_m <= 1600
          or operator_context.transport_count_within_1600m > 0
            then 'transport_anchor_within_1600m_context'
        else 'transport_anchor_beyond_1600m_context'
    end as transport_context_status,
    case
        when operator_context.nearest_open_space_distance_m is null
            then 'open_space_context_not_measured'
        when operator_context.nearest_open_space_distance_m <= 1600
          or operator_context.open_space_count_within_1600m > 0
            then 'open_space_within_1600m_context'
        else 'open_space_beyond_1600m_context'
    end as open_space_context_status,
    case
        when operator_context.service_anchor_count_within_1600m > 0
          or operator_context.nearest_service_anchor_distance_m <= 1600
            then 'service_anchor_context_present'
        when operator_context.location_context_row_count = 0
            then 'service_anchor_context_not_measured'
        else 'service_anchor_context_weak_or_absent'
    end as npf4_service_anchor_context_status,
    case
        when operator_context.location_context_row_count = 0
            then 'No open-location context has been measured for this site yet. Run bounded context refresh before drawing a location conclusion.'
        when operator_context.service_anchor_count_within_1600m > 0
          or operator_context.nearest_service_anchor_distance_m <= 1600
            then 'Measured location context shows nearby service-anchor evidence. This supports location DD but does not prove planning acceptability or buyer demand.'
        else 'Some location context exists, but service-anchor evidence is weak or not yet measured. This is a gap, not a site kill.'
    end as location_context_summary,
    'Location context is contextual DD evidence only. It does not prove NPF4 compliance, adopted/legal access, planning acceptability, buyer demand or net developable area.'::text as location_context_caveat
from operator_context;

grant usage on schema landintel_sourced to authenticated;
grant select on landintel_sourced.v_site_location_context to authenticated;

comment on view landintel_sourced.v_site_location_context is
    'Operator surface for site location context using existing measured open-location spine rows. This is contextual DD evidence, not planning proof, legal access proof or buyer demand proof.';

insert into landintel_store.object_ownership_registry (
    schema_name,
    object_name,
    object_type,
    current_status,
    owner_layer,
    canonical_role,
    source_family_or_module,
    exists_in_github,
    exists_in_supabase,
    represented_in_repo,
    safe_to_read,
    safe_to_write,
    safe_for_operator,
    safe_to_retire,
    replacement_object,
    risk_summary,
    recommended_action,
    metadata,
    updated_at
)
values (
    'landintel_sourced',
    'v_site_location_context',
    'view',
    'reporting_surface',
    'landintel_sourced',
    'site location context and service-anchor DD surface',
    'open_location_spine',
    true,
    true,
    true,
    true,
    false,
    true,
    false,
    null,
    'Context view only. Uses already measured open-location spine rows and must not be read as NPF4 compliance, legal access or buyer demand proof.',
    'Use after title/location/access identity to see whether location anchors are measured and what remains a DD gap.',
    '{"not_planning_truth":true,"not_legal_access_truth":true,"not_buyer_demand_truth":true,"uses_existing_context_only":true}'::jsonb,
    now()
)
on conflict (schema_name, object_name, object_type) do update set
    current_status = excluded.current_status,
    owner_layer = excluded.owner_layer,
    canonical_role = excluded.canonical_role,
    source_family_or_module = excluded.source_family_or_module,
    exists_in_github = excluded.exists_in_github,
    exists_in_supabase = excluded.exists_in_supabase,
    represented_in_repo = excluded.represented_in_repo,
    safe_to_read = excluded.safe_to_read,
    safe_to_write = excluded.safe_to_write,
    safe_for_operator = excluded.safe_for_operator,
    safe_to_retire = excluded.safe_to_retire,
    replacement_object = excluded.replacement_object,
    risk_summary = excluded.risk_summary,
    recommended_action = excluded.recommended_action,
    metadata = excluded.metadata,
    updated_at = now();

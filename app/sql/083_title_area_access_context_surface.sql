create schema if not exists landintel_sourced;

comment on schema landintel_sourced is
    'LandIntel Sourced Sites, the polished commercial opportunity register for LDN review.';

create or replace view landintel_sourced.v_site_title_area_access_context
with (security_invoker = true) as
with identity as (
    select *
    from landintel_sourced.v_site_legal_title_location_identity
), linked_parcels as (
    select distinct
        parcel_link.site_id::uuid as canonical_site_id,
        parcel.id as ros_parcel_id,
        parcel.ros_inspire_id,
        parcel.area_acres,
        parcel.normalized_title_number,
        parcel.title_number
    from public.site_ros_parcel_link_candidates as parcel_link
    join public.ros_cadastral_parcels as parcel
      on parcel.id = parcel_link.ros_parcel_id
    where parcel_link.link_status <> 'rejected'
      and parcel_link.site_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    union

    select distinct
        site.id as canonical_site_id,
        parcel.id as ros_parcel_id,
        parcel.ros_inspire_id,
        parcel.area_acres,
        parcel.normalized_title_number,
        parcel.title_number
    from landintel.canonical_sites as site
    join public.ros_cadastral_parcels as parcel
      on parcel.id = site.primary_ros_parcel_id
), parcel_rollup as (
    select
        linked_parcels.canonical_site_id,
        count(distinct linked_parcels.ros_parcel_id)::integer as ros_parcel_count,
        round(sum(linked_parcels.area_acres)::numeric, 4) as ros_parcel_area_acres,
        round(
            sum(linked_parcels.area_acres) filter (
                where public.is_scottish_title_number_candidate(linked_parcels.normalized_title_number)
            )::numeric,
            4
        ) as legal_title_area_acres,
        array_remove(array_agg(distinct linked_parcels.ros_inspire_id order by linked_parcels.ros_inspire_id), null) as ros_parcel_references
    from linked_parcels
    group by linked_parcels.canonical_site_id
), road_context_candidates as (
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
        context.measured_at
    from landintel.site_open_location_spine_context as context
    where lower(concat_ws(
        ' ',
        context.source_key,
        context.source_family,
        context.feature_type,
        context.nearest_feature_name
    )) like '%%road%%'
       or lower(concat_ws(
        ' ',
        context.source_key,
        context.source_family,
        context.feature_type,
        context.nearest_feature_name
    )) like '%%street%%'
       or lower(concat_ws(
        ' ',
        context.source_key,
        context.source_family,
        context.feature_type,
        context.nearest_feature_name
    )) like '%%highway%%'
), nearest_road_context as (
    select distinct on (road_context_candidates.canonical_site_id)
        road_context_candidates.*
    from road_context_candidates
    order by
        road_context_candidates.canonical_site_id,
        road_context_candidates.nearest_distance_m nulls last,
        road_context_candidates.measured_at desc nulls last
), site_geometry as (
    select
        site.id as canonical_site_id,
        site.geometry is not null as site_geometry_present,
        case
            when site.geometry is null then false
            else not st_isempty(site.geometry)
        end as site_geometry_not_empty,
        case
            when site.geometry is null then null::boolean
            else st_isvalid(site.geometry)
        end as site_geometry_valid,
        site.area_acres as canonical_site_area_acres
    from landintel.canonical_sites as site
)
select
    identity.canonical_site_id,
    identity.site_label,
    identity.legal_title_number,
    identity.legal_title_numbers,
    identity.legal_title_number_status,
    identity.address,
    identity.local_area_or_settlement_name,
    identity.local_authority,
    identity.local_council,
    site_geometry.canonical_site_area_acres as site_area_acres,
    parcel_rollup.legal_title_area_acres as title_area_acres,
    parcel_rollup.ros_parcel_area_acres,
    coalesce(parcel_rollup.ros_parcel_count, 0) as ros_parcel_count,
    coalesce(parcel_rollup.ros_parcel_references, '{}'::text[]) as ros_parcel_references,
    case
        when parcel_rollup.legal_title_area_acres is not null then 'legal_title_area_from_ros_parcel_attributes'
        when parcel_rollup.ros_parcel_area_acres is not null then 'ros_parcel_area_candidate_no_legal_title_number'
        else 'title_or_parcel_area_not_held'
    end as title_area_status,
    case
        when parcel_rollup.legal_title_area_acres is not null
          and site_geometry.canonical_site_area_acres is not null
            then round((parcel_rollup.legal_title_area_acres - site_geometry.canonical_site_area_acres)::numeric, 4)
        else null::numeric
    end as title_site_area_delta_acres,
    case
        when parcel_rollup.legal_title_area_acres is not null
          and site_geometry.canonical_site_area_acres is not null
          and site_geometry.canonical_site_area_acres > 0
            then round((((parcel_rollup.legal_title_area_acres - site_geometry.canonical_site_area_acres) / site_geometry.canonical_site_area_acres) * 100)::numeric, 2)
        else null::numeric
    end as title_site_area_delta_pct,
    site_geometry.site_geometry_present,
    site_geometry.site_geometry_not_empty,
    site_geometry.site_geometry_valid,
    case
        when not coalesce(site_geometry.site_geometry_present, false) then 'geometry_missing'
        when not coalesce(site_geometry.site_geometry_not_empty, false) then 'geometry_empty'
        when site_geometry.site_geometry_valid = false then 'geometry_invalid'
        when coalesce(site_geometry.canonical_site_area_acres, 0) < 4 then 'below_4_acre_ldn_threshold'
        else 'measurement_ready_geometry'
    end as developable_geometry_status,
    case
        when site_geometry.site_geometry_present
          and site_geometry.site_geometry_not_empty
          and coalesce(site_geometry.site_geometry_valid, false)
          and coalesce(site_geometry.canonical_site_area_acres, 0) >= 4
            then true
        else false
    end as developable_geometry_flag,
    nearest_road_context.nearest_feature_name as nearest_road_name,
    nearest_road_context.nearest_distance_m as nearest_road_distance_m,
    nearest_road_context.count_within_400m as road_count_within_400m,
    nearest_road_context.count_within_800m as road_count_within_800m,
    nearest_road_context.count_within_1600m as road_count_within_1600m,
    nearest_road_context.source_key as road_context_source_key,
    nearest_road_context.feature_type as road_context_feature_type,
    nearest_road_context.measured_at as road_context_measured_at,
    case
        when nearest_road_context.canonical_site_id is null then 'road_context_not_measured'
        when coalesce(nearest_road_context.nearest_distance_m, 999999) <= 25
          or coalesce(nearest_road_context.count_within_400m, 0) > 0 then 'road_context_nearby'
        when nearest_road_context.nearest_distance_m <= 100 then 'road_context_possible_access'
        when nearest_road_context.nearest_distance_m <= 250 then 'road_context_distant_manual_access_review'
        else 'potentially_landlocked_or_remote_from_road_context'
    end as road_access_context_status,
    case
        when nearest_road_context.canonical_site_id is null then 'unknown_not_measured'
        when coalesce(nearest_road_context.nearest_distance_m, 999999) <= 25
          or coalesce(nearest_road_context.count_within_400m, 0) > 0 then 'low_contextual_landlock_risk'
        when nearest_road_context.nearest_distance_m <= 250 then 'manual_access_review_required'
        else 'possible_landlocked_or_field_isolated'
    end as landlocked_context_risk,
    case
        when nearest_road_context.canonical_site_id is null
            then 'Road/access context has not been measured for this site. Run bounded OS Open Roads/open-location context before judging access.'
        when coalesce(nearest_road_context.nearest_distance_m, 999999) <= 25
          or coalesce(nearest_road_context.count_within_400m, 0) > 0
            then 'Road context is close to the site, but this does not prove legal access, adopted access or ransom-free access.'
        when nearest_road_context.nearest_distance_m <= 250
            then 'A road feature is nearby but access still needs manual review for frontage, adopted status and ransom risk.'
        else 'The site may be field-isolated or landlocked on current road context. Manual access review is required before spend.'
    end as access_caveat,
    'Developable geometry means measurement-ready geometry for DD. It is not a net developable area, planning conclusion, legal access conclusion or abnormal-cost assessment.'::text as geometry_caveat
from identity
left join parcel_rollup
  on parcel_rollup.canonical_site_id = identity.canonical_site_id
left join site_geometry
  on site_geometry.canonical_site_id = identity.canonical_site_id
left join nearest_road_context
  on nearest_road_context.canonical_site_id = identity.canonical_site_id;

grant usage on schema landintel_sourced to authenticated;
grant select on landintel_sourced.v_site_title_area_access_context to authenticated;

comment on view landintel_sourced.v_site_title_area_access_context is
    'Operator surface for title area, site area, measurement-ready geometry and road/access context. Road proximity is not legal access proof and developable geometry is not net developable area.';

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
    'v_site_title_area_access_context',
    'view',
    'reporting_surface',
    'landintel_sourced',
    'title area and road access context',
    'title_number',
    true,
    true,
    true,
    true,
    false,
    true,
    false,
    null,
    'Operator context view only. Road proximity is not legal access proof and developable geometry is not a net developable area conclusion.',
    'Use after legal title/location identity to check area basis and whether bounded road/access context exists.',
    '{"not_ownership_truth":true,"not_legal_access_truth":true,"not_net_developable_area":true}'::jsonb,
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

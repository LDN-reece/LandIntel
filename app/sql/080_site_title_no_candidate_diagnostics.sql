create schema if not exists landintel_reporting;

create or replace view landintel_reporting.v_site_title_no_candidate_diagnostics as
with no_candidate_state as (
    select
        state.*
    from landintel_store.site_title_traceability_scan_state as state
    where state.scan_status = 'no_candidate'
),
prepared_sites as (
    select
        state.canonical_site_id,
        site.site_name_primary as site_label,
        state.scan_scope,
        state.site_priority_band,
        state.site_priority_rank,
        state.priority_source,
        coalesce(nullif(state.authority_name, ''), site.authority_name) as authority_name,
        coalesce(state.area_acres, site.area_acres) as area_acres,
        state.scanned_at,
        state.updated_at,
        site.geometry is not null as site_geometry_present,
        case
            when site.geometry is null then false
            else not st_isempty(site.geometry)
        end as site_geometry_not_empty,
        case
            when site.geometry is null then null::boolean
            else st_isvalid(site.geometry)
        end as site_geometry_valid,
        case
            when site.geometry is null or st_isempty(site.geometry) then null::geometry(Geometry, 27700)
            when st_isvalid(site.geometry) then site.geometry
            else st_multi(st_collectionextract(st_makevalid(site.geometry), 3))
        end as working_geometry
    from no_candidate_state as state
    join landintel.canonical_sites as site
      on site.id = state.canonical_site_id
),
diagnostics as (
    select
        prepared_sites.*,
        coverage.authority_has_ros_coverage,
        nearest.ros_parcel_id as nearest_ros_parcel_id,
        nearest.ros_inspire_id as nearest_ros_inspire_id,
        nearest.title_number as nearest_raw_title_number,
        nearest.normalized_title_number as nearest_normalized_title_number,
        nearest.nearest_parcel_area_acres,
        nearest.nearest_centroid_distance_m,
        nearest.nearest_geometry_distance_m,
        nearest.nearest_geometry_intersects_site,
        nearest.nearest_parcel_centroid_inside_site,
        coalesce(candidate_window.centroid_bbox_hits_250m, 0) as parcel_centroid_bbox_hits_250m,
        coalesce(candidate_window.geometry_bbox_hits_250m, 0) as parcel_geometry_bbox_hits_250m,
        coalesce(candidate_window.geometry_within_250m, 0) as parcel_geometry_within_250m,
        coalesce(candidate_window.geometry_intersects_site_count, 0) as parcel_geometry_intersects_site_count,
        coalesce(candidate_window.centroid_inside_site_count, 0) as parcel_centroid_inside_site_count
    from prepared_sites
    left join lateral (
        select exists (
            select 1
            from public.ros_cadastral_parcels as parcel
            where parcel.authority_name = prepared_sites.authority_name
              and parcel.geometry is not null
            limit 1
        ) as authority_has_ros_coverage
    ) as coverage on true
    left join lateral (
        select
            parcel.id as ros_parcel_id,
            parcel.ros_inspire_id,
            parcel.title_number,
            parcel.normalized_title_number,
            parcel.area_acres as nearest_parcel_area_acres,
            round(
                st_distance(
                    st_pointonsurface(prepared_sites.working_geometry),
                    coalesce(parcel.centroid, st_pointonsurface(parcel.geometry))
                )::numeric,
                2
            ) as nearest_centroid_distance_m,
            round(st_distance(prepared_sites.working_geometry, parcel.geometry)::numeric, 2) as nearest_geometry_distance_m,
            st_intersects(prepared_sites.working_geometry, parcel.geometry) as nearest_geometry_intersects_site,
            st_contains(prepared_sites.working_geometry, coalesce(parcel.centroid, st_pointonsurface(parcel.geometry))) as nearest_parcel_centroid_inside_site
        from public.ros_cadastral_parcels as parcel
        where prepared_sites.working_geometry is not null
          and parcel.authority_name = prepared_sites.authority_name
          and parcel.geometry is not null
        order by
            coalesce(parcel.centroid, st_pointonsurface(parcel.geometry)) OPERATOR(extensions.<->) st_pointonsurface(prepared_sites.working_geometry),
            parcel.id
        limit 1
    ) as nearest on true
    left join lateral (
        select
            count(*) filter (
                where coalesce(parcel.centroid, st_pointonsurface(parcel.geometry)) OPERATOR(extensions.&&) st_expand(prepared_sites.working_geometry, 250)
            )::integer as centroid_bbox_hits_250m,
            count(*) filter (
                where parcel.geometry OPERATOR(extensions.&&) st_expand(prepared_sites.working_geometry, 250)
            )::integer as geometry_bbox_hits_250m,
            count(*) filter (
                where st_dwithin(parcel.geometry, prepared_sites.working_geometry, 250)
            )::integer as geometry_within_250m,
            count(*) filter (
                where st_intersects(parcel.geometry, prepared_sites.working_geometry)
            )::integer as geometry_intersects_site_count,
            count(*) filter (
                where st_contains(prepared_sites.working_geometry, coalesce(parcel.centroid, st_pointonsurface(parcel.geometry)))
            )::integer as centroid_inside_site_count
        from public.ros_cadastral_parcels as parcel
        where prepared_sites.working_geometry is not null
          and parcel.authority_name = prepared_sites.authority_name
          and parcel.geometry is not null
          and parcel.geometry OPERATOR(extensions.&&) st_expand(prepared_sites.working_geometry, 1000)
    ) as candidate_window on true
)
select
    canonical_site_id,
    site_label,
    scan_scope,
    site_priority_band,
    site_priority_rank,
    priority_source,
    authority_name,
    area_acres,
    scanned_at,
    updated_at,
    site_geometry_present,
    site_geometry_not_empty,
    site_geometry_valid,
    authority_has_ros_coverage,
    nearest_ros_parcel_id,
    nearest_ros_inspire_id,
    nearest_raw_title_number,
    nearest_normalized_title_number,
    nearest_parcel_area_acres,
    nearest_centroid_distance_m,
    nearest_geometry_distance_m,
    nearest_geometry_intersects_site,
    nearest_parcel_centroid_inside_site,
    parcel_centroid_bbox_hits_250m,
    parcel_geometry_bbox_hits_250m,
    parcel_geometry_within_250m,
    parcel_geometry_intersects_site_count,
    parcel_centroid_inside_site_count,
    case
        when not site_geometry_present or not site_geometry_not_empty then 'site_geometry_missing_or_empty'
        when site_geometry_valid = false then 'site_geometry_invalid_repaired_for_diagnostic'
        when authority_has_ros_coverage = false then 'authority_ros_coverage_missing'
        when nearest_ros_parcel_id is null then 'no_same_authority_ros_parcel_found'
        when parcel_centroid_bbox_hits_250m = 0 and parcel_geometry_within_250m > 0 then 'parcel_geometry_nearby_but_centroid_outside_candidate_window'
        when parcel_centroid_bbox_hits_250m = 0 then 'nearest_parcel_outside_candidate_window'
        when parcel_geometry_intersects_site_count = 0 then 'candidate_window_has_centroids_but_no_intersecting_parcel'
        else 'candidate_function_needs_review'
    end as diagnostic_reason,
    case
        when not site_geometry_present or not site_geometry_not_empty then 'Repair or replace the canonical site geometry before re-running title traceability.'
        when site_geometry_valid = false then 'Review canonical geometry validity; diagnostic used repaired geometry but traceability function may need clean geometry.'
        when authority_has_ros_coverage = false then 'Load or repair RoS cadastral parcel coverage for this authority before re-running.'
        when nearest_ros_parcel_id is null then 'Check authority naming and RoS coverage alignment.'
        when parcel_centroid_bbox_hits_250m = 0 and parcel_geometry_within_250m > 0 then 'Candidate window is too centroid-led for this geometry; review whether geometry-overlap fallback is needed.'
        when parcel_centroid_bbox_hits_250m = 0 then 'Do not keep re-running title traceability. The nearest parcel centroid is outside the current candidate window.'
        when parcel_geometry_intersects_site_count = 0 then 'Review source geometry. Parcel centroids are nearby but no parcel geometry intersects the site.'
        else 'Review the title traceability function for this site; candidate-window evidence exists but no row was produced.'
    end as recommended_action,
    'Diagnostic only. This does not prove ownership, title, availability or rejection of the site.'::text as caveat
from diagnostics;

create or replace view landintel_reporting.v_site_title_no_candidate_diagnostic_summary as
select
    scan_scope,
    site_priority_band,
    diagnostic_reason,
    count(*)::integer as site_count,
    min(scanned_at) as first_scanned_at,
    max(scanned_at) as latest_scanned_at,
    min(nearest_centroid_distance_m) as min_nearest_centroid_distance_m,
    percentile_cont(0.5) within group (order by nearest_centroid_distance_m) as median_nearest_centroid_distance_m,
    max(nearest_centroid_distance_m) as max_nearest_centroid_distance_m
from landintel_reporting.v_site_title_no_candidate_diagnostics
group by scan_scope, site_priority_band, diagnostic_reason
order by scan_scope, site_priority_band, site_count desc, diagnostic_reason;

comment on view landintel_reporting.v_site_title_no_candidate_diagnostics is
    'Diagnostic surface for sites where bounded title traceability found no RoS parcel candidate. This is not title or ownership evidence.';

comment on view landintel_reporting.v_site_title_no_candidate_diagnostic_summary is
    'Summary of no-candidate title traceability diagnostics by scan scope, priority band and likely blocker.';

grant select on landintel_reporting.v_site_title_no_candidate_diagnostics to authenticated;
grant select on landintel_reporting.v_site_title_no_candidate_diagnostic_summary to authenticated;

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
values
    (
        'landintel_reporting',
        'v_site_title_no_candidate_diagnostics',
        'view',
        'reporting_surface',
        'landintel_reporting',
        'title traceability no-candidate diagnostics',
        'title_number',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'landintel_store.site_title_traceability_scan_state',
        'Diagnostic surface only. It explains no-hit title indexing and must not be treated as legal ownership evidence.',
        'Use before increasing title traceability batch size or changing candidate-window logic.',
        '{"not_ownership_truth":true,"diagnostic_only":true}'::jsonb,
        now()
    ),
    (
        'landintel_reporting',
        'v_site_title_no_candidate_diagnostic_summary',
        'view',
        'reporting_surface',
        'landintel_reporting',
        'title traceability no-candidate summary',
        'title_number',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'landintel_reporting.v_site_title_no_candidate_diagnostics',
        'Aggregated diagnostic surface only.',
        'Use to decide whether the next fix is geometry, coverage, candidate-window or function review.',
        '{"not_ownership_truth":true,"diagnostic_only":true}'::jsonb,
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

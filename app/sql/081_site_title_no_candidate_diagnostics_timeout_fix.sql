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
        end as site_geometry_valid
    from no_candidate_state as state
    join landintel.canonical_sites as site
      on site.id = state.canonical_site_id
)
select
    prepared_sites.canonical_site_id,
    prepared_sites.site_label,
    prepared_sites.scan_scope,
    prepared_sites.site_priority_band,
    prepared_sites.site_priority_rank,
    prepared_sites.priority_source,
    prepared_sites.authority_name,
    prepared_sites.area_acres,
    prepared_sites.scanned_at,
    prepared_sites.updated_at,
    prepared_sites.site_geometry_present,
    prepared_sites.site_geometry_not_empty,
    prepared_sites.site_geometry_valid,
    coalesce(coverage.authority_has_ros_coverage, false) as authority_has_ros_coverage,
    null::uuid as nearest_ros_parcel_id,
    null::text as nearest_ros_inspire_id,
    null::text as nearest_raw_title_number,
    null::text as nearest_normalized_title_number,
    null::numeric as nearest_parcel_area_acres,
    null::numeric as nearest_centroid_distance_m,
    null::numeric as nearest_geometry_distance_m,
    null::boolean as nearest_geometry_intersects_site,
    null::boolean as nearest_parcel_centroid_inside_site,
    null::integer as parcel_centroid_bbox_hits_250m,
    null::integer as parcel_geometry_bbox_hits_250m,
    null::integer as parcel_geometry_within_250m,
    null::integer as parcel_geometry_intersects_site_count,
    null::integer as parcel_centroid_inside_site_count,
    case
        when not prepared_sites.site_geometry_present or not prepared_sites.site_geometry_not_empty
            then 'site_geometry_missing_or_empty'
        when prepared_sites.site_geometry_valid = false
            then 'site_geometry_invalid_needs_repair_before_traceability'
        when coalesce(coverage.authority_has_ros_coverage, false) = false
            then 'authority_ros_coverage_missing'
        else 'candidate_window_or_geometry_overlap_needs_bounded_spot_check'
    end as diagnostic_reason,
    case
        when not prepared_sites.site_geometry_present or not prepared_sites.site_geometry_not_empty
            then 'Repair or replace the canonical site geometry before re-running title traceability.'
        when prepared_sites.site_geometry_valid = false
            then 'Review canonical geometry validity before widening title traceability candidate logic.'
        when coalesce(coverage.authority_has_ros_coverage, false) = false
            then 'Load or repair RoS cadastral parcel coverage for this authority before re-running.'
        else 'Do not repeat blind title traceability yet. Run a bounded per-site parcel candidate-window spot check or adjust candidate logic with evidence.'
    end as recommended_action,
    'Diagnostic only. This audit-safe view does not prove ownership, title, availability or rejection of the site.'::text as caveat
from prepared_sites
left join lateral (
    select exists (
        select 1
        from public.ros_cadastral_parcels as parcel
        where parcel.authority_name = prepared_sites.authority_name
          and parcel.geometry is not null
        limit 1
    ) as authority_has_ros_coverage
) as coverage on true;

create or replace view landintel_reporting.v_site_title_no_candidate_diagnostic_summary as
select
    scan_scope,
    site_priority_band,
    diagnostic_reason,
    count(*)::integer as site_count,
    min(scanned_at) as first_scanned_at,
    max(scanned_at) as latest_scanned_at,
    min(nearest_centroid_distance_m) as min_nearest_centroid_distance_m,
    null::double precision as median_nearest_centroid_distance_m,
    max(nearest_centroid_distance_m) as max_nearest_centroid_distance_m
from landintel_reporting.v_site_title_no_candidate_diagnostics
group by scan_scope, site_priority_band, diagnostic_reason
order by scan_scope, site_priority_band, site_count desc, diagnostic_reason;

comment on view landintel_reporting.v_site_title_no_candidate_diagnostics is
    'Audit-safe diagnostic surface for sites where bounded title traceability found no RoS parcel candidate. It avoids parcel-nearest scans by default and is not title or ownership evidence.';

comment on view landintel_reporting.v_site_title_no_candidate_diagnostic_summary is
    'Audit-safe summary of no-candidate title traceability diagnostics by scan scope, priority band and likely blocker.';

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
        'Audit-safe diagnostic surface only. It explains no-hit title indexing without running parcel-nearest scans and must not be treated as legal ownership evidence.',
        'Use to decide whether the blocker is geometry, RoS authority coverage, or candidate-window logic needing a bounded per-site spot check.',
        '{"not_ownership_truth":true,"diagnostic_only":true,"audit_safe":true,"no_default_parcel_nearest_scan":true}'::jsonb,
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
        'Aggregated audit-safe diagnostic surface only.',
        'Use in audit-site-dd-orchestration without triggering broad RoS parcel-nearest analysis.',
        '{"not_ownership_truth":true,"diagnostic_only":true,"audit_safe":true,"no_default_parcel_nearest_scan":true}'::jsonb,
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

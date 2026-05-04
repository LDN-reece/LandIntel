create schema if not exists landintel_reporting;

create or replace view landintel_reporting.v_parcel_model_status
with (security_invoker = true) as
with parcel_objects as (
    select *
    from (
        values
            (
                'public',
                'ros_cadastral_parcels',
                'table',
                'current_but_expensive_scale_risk',
                'canonical RoS cadastral parcel source',
                'Canonical source table for RoS parcel geometry, parcel IDs and title-candidate enrichment. This remains the parcel anchor until a later approved physical migration exists.',
                array[
                    'app/src/loaders/supabase_loader.py::upsert_ros_cadastral_parcels',
                    'app/sql/047_title_resolution_bridge.sql',
                    'app/sql/048_site_ros_parcel_linking.sql',
                    'app/sql/056_urgent_site_address_title_pack.sql',
                    'app/sql/060_scotland_parcel_use_spine.sql',
                    'app/sql/061_ldn_sourced_site_briefs.sql',
                    'app/sql/064_title_control_operator_safety_views.sql'
                ]::text[],
                array[
                    'app/src/loaders/supabase_loader.py::upsert_ros_cadastral_parcels',
                    'app/src/source_expansion_runner.py::audit-title-number-control title clean-up updates'
                ]::text[],
                'Keep as canonical RoS parcel source. Read via landintel_store.ros_cadastral_parcels where possible.',
                false,
                'RoS parcel references are parcel/source identifiers, not ownership proof and not title-review confirmation.'
            ),
            (
                'public',
                'land_objects',
                'table',
                'duplicate_candidate',
                'legacy normalised parcel/object cache',
                'Created from the same RoS enriched parcel frame by upsert_land_objects with object_type ros_cadastral_parcel and source_system ros_inspire. It is still an active dependency for urgent address/title address linkage.',
                array[
                    'app/sql/056_urgent_site_address_title_pack.sql::join public.land_objects to public.land_object_address_links',
                    'app/sql/062_object_ownership_schema_clarity.sql::landintel_store.land_objects compatibility view'
                ]::text[],
                array[
                    'app/src/main.py::ingest-ros-cadastral calls loader.upsert_land_objects',
                    'app/src/loaders/supabase_loader.py::upsert_land_objects'
                ]::text[],
                'Do not retire yet. Treat as duplicate_candidate until address-link dependencies are replaced or proven unused.',
                false,
                'This is not the canonical parcel truth and should not be used for ownership certainty.'
            ),
            (
                'public',
                'land_parcels',
                'table',
                'legacy_candidate_retire',
                'legacy empty parcel stub',
                'Older public parcel table listed by audit as empty/stub. It should remain dependency-checked before any retirement PR.',
                array[]::text[],
                array[]::text[],
                'Keep labelled as legacy_candidate_retire. Do not delete without dedicated dependency proof and approval.',
                false,
                'Retirement label is not deletion approval.'
            ),
            (
                'public',
                'site_ros_parcel_link_candidates',
                'table',
                'current_keep',
                'site-to-RoS parcel candidate bridge',
                'Current bounded candidate bridge from canonical sites to public.ros_cadastral_parcels. Candidate links are not ownership proof.',
                array[
                    'app/sql/048_site_ros_parcel_linking.sql',
                    'app/sql/064_title_control_operator_safety_views.sql',
                    'app/sql/063_operator_safe_sourced_site_views.sql'
                ]::text[],
                array[
                    'app/src/source_expansion_runner.py::link-sites-to-ros-parcels',
                    'public.refresh_site_ros_parcel_link_candidates_for_sites'
                ]::text[],
                'Keep as candidate bridge. Continue exposing through operator-safe reporting views.',
                false,
                'Parcel linkage is candidate/context evidence, not legal ownership.'
            ),
            (
                'public',
                'site_title_resolution_candidates',
                'table',
                'current_keep',
                'title-candidate resolution bridge',
                'Current bridge for title-shaped candidates from RoS parcel context. Rejected SCT-like values remain audit-only.',
                array[
                    'app/sql/047_title_resolution_bridge.sql',
                    'app/sql/064_title_control_operator_safety_views.sql',
                    'app/sql/063_operator_safe_sourced_site_views.sql'
                ]::text[],
                array[
                    'app/src/source_expansion_runner.py::resolve-title-numbers',
                    'public.refresh_site_title_resolution_bridge_for_sites'
                ]::text[],
                'Keep. Operator surfaces must filter rejected SCT-like parcel references from title-number display.',
                false,
                'Title candidates are not reviewed ownership outcomes.'
            ),
            (
                'public',
                'site_title_validation',
                'table',
                'current_keep',
                'title-candidate validation audit',
                'Current title candidate validation/audit table. It stores valid and rejected outcomes but does not confirm ownership without human title review.',
                array[
                    'app/sql/047_title_resolution_bridge.sql',
                    'app/sql/064_title_control_operator_safety_views.sql',
                    'app/sql/063_operator_safe_sourced_site_views.sql'
                ]::text[],
                array[
                    'app/src/source_expansion_runner.py::resolve-title-numbers',
                    'public.refresh_site_title_resolution_bridge_for_sites'
                ]::text[],
                'Keep as audit evidence. Use landintel_reporting title/control views for operators.',
                false,
                'title_review_records remains the human ownership confirmation layer.'
            )
    ) as object_rows (
        schema_name,
        object_name,
        object_type,
        current_status,
        parcel_model_role,
        dependency_summary,
        active_read_paths,
        active_write_paths,
        recommended_action,
        safe_to_retire,
        caveat
    )
),
relation_stats as (
    select
        parcel_objects.*,
        to_regclass(parcel_objects.schema_name || '.' || parcel_objects.object_name) as relation_oid
    from parcel_objects
)
select
    schema_name,
    object_name,
    object_type,
    current_status,
    parcel_model_role,
    case
        when relation_oid is null then null::bigint
        else greatest(coalesce(class_stats.reltuples, 0), 0)::bigint
    end as row_count_estimate,
    case
        when relation_oid is null then null::bigint
        else pg_total_relation_size(relation_oid)
    end as total_relation_bytes,
    active_read_paths,
    active_write_paths,
    case
        when schema_name = 'public' and object_name = 'ros_cadastral_parcels' then true
        else false
    end as recommended_canonical_parcel_source,
    dependency_summary,
    recommended_action,
    safe_to_retire,
    caveat
from relation_stats
left join pg_class as class_stats
    on class_stats.oid = relation_stats.relation_oid;

comment on view landintel_reporting.v_parcel_model_status
    is 'Parcel model decision surface. Shows RoS parcel truth, legacy duplicate/cache tables, active dependencies, safe-to-retire status and operator caveats without moving or deleting data.';

create or replace view landintel_reporting.v_parcel_model_lightweight_overlap_audit
with (security_invoker = true) as
with status as (
    select *
    from landintel_reporting.v_parcel_model_status
    where object_name in ('ros_cadastral_parcels', 'land_objects')
)
select
    'public.land_objects versus public.ros_cadastral_parcels'::text as comparison_name,
    max(row_count_estimate) filter (where object_name = 'ros_cadastral_parcels') as ros_cadastral_parcels_estimated_rows,
    max(row_count_estimate) filter (where object_name = 'land_objects') as land_objects_estimated_rows,
    abs(
        coalesce(max(row_count_estimate) filter (where object_name = 'ros_cadastral_parcels'), 0)
        - coalesce(max(row_count_estimate) filter (where object_name = 'land_objects'), 0)
    )::bigint as estimated_row_delta,
    'Repository evidence shows land_objects is loaded from the same enriched RoS parcel frame using source_system ros_inspire and source_key ros_inspire_id:authority_name.'::text as repo_overlap_evidence,
    'No broad spatial overlap query is run in this view. Exact retirement proof should use bounded source-key checks in a later approved PR.'::text as audit_limitation,
    'Keep public.ros_cadastral_parcels as canonical RoS parcel source. Keep public.land_objects as duplicate_candidate until address-link dependencies are replaced or proven unnecessary.'::text as recommendation
from status;

comment on view landintel_reporting.v_parcel_model_lightweight_overlap_audit
    is 'Lightweight, non-spatial parcel duplicate audit based on relation estimates and repo evidence. It deliberately avoids broad parcel geometry scans.';

do $$
begin
    if to_regclass('landintel_store.object_ownership_registry') is not null then
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
            metadata
        )
        values
            (
                'public',
                'ros_cadastral_parcels',
                'table',
                'current_but_expensive_scale_risk',
                'public_legacy_compatibility',
                'canonical RoS parcel source',
                'title_parcel',
                true,
                true,
                true,
                true,
                false,
                false,
                false,
                'landintel_store.ros_cadastral_parcels',
                'Large RoS cadastral parcel source table. It remains the canonical parcel source but is not ownership proof.',
                'Keep as canonical RoS parcel source; use bounded linking and operator-safe title/control views.',
                '{"parcel_model_decision":"canonical_ros_parcel_source","ownership_proof":false}'::jsonb
            ),
            (
                'public',
                'land_objects',
                'table',
                'duplicate_candidate',
                'public_legacy_compatibility',
                'legacy normalised parcel/object cache',
                'title_parcel',
                true,
                true,
                true,
                true,
                false,
                false,
                false,
                'public.ros_cadastral_parcels',
                'Near-duplicate RoS-derived parcel cache. Still actively written by ingest-ros-cadastral and read by urgent address/title address linkage.',
                'Do not retire yet. Replace or prove address-link dependencies first, then run a dedicated retirement-readiness PR.',
                '{"parcel_model_decision":"duplicate_candidate_not_retire_now","active_read_dependency":"app/sql/056_urgent_site_address_title_pack.sql","active_write_dependency":"app/src/loaders/supabase_loader.py::upsert_land_objects"}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_parcel_model_status',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'parcel model decision surface',
                'title_parcel',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Operator and audit view for parcel model ownership and dependency status.',
                'Use before any parcel retirement or physical schema move decision.',
                '{"operator_safe":true,"broad_spatial_scan":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_parcel_model_lightweight_overlap_audit',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'lightweight parcel duplicate audit',
                'title_parcel',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Non-spatial relation-estimate and repo-evidence audit for RoS parcel duplicate risk.',
                'Use as a lightweight audit only; exact overlap proof belongs in a later bounded PR.',
                '{"operator_safe":true,"exact_overlap_not_run":true}'::jsonb
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
            metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
            reviewed_at = now(),
            updated_at = now();
    end if;
end $$;

grant usage on schema landintel_reporting to authenticated;
grant select on landintel_reporting.v_parcel_model_status to authenticated;
grant select on landintel_reporting.v_parcel_model_lightweight_overlap_audit to authenticated;

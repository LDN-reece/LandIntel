create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

do $$
begin
    if to_regclass('landintel.bgs_borehole_master') is null then
        raise notice 'Skipping BGS borehole clean normalisation views because landintel.bgs_borehole_master is not present in this database.';
    else
        execute $view$
            create or replace view landintel_store.v_bgs_borehole_master_clean
            with (security_invoker = true) as
            select
                bgs_id,
                source_upload_id,
                source_snapshot_date,
                source_file_name,
                source_row_number,
                source_storage_bucket,
                source_storage_path,
                qs,
                numb_raw,
                numb,
                bsuff,
                regno as registration_number,
                rt as source_record_type,
                grid_reference,
                easting,
                northing,
                geom_27700,
                geom_wgs84,
                is_confidential,
                start_height_m,
                nullif(name_normalised, '') as borehole_name,
                depth_m,
                depth_status,
                has_ags_log as has_log_available,
                ags_log_url,
                date_known_year,
                nullif(date_known_type_raw, '') as date_known_type,
                date_entered,
                api_last_seen_at,
                created_at,
                updated_at,
                (geom_27700 is not null and easting is not null and northing is not null) as has_valid_geometry,
                (depth_m is not null) as has_depth,
                (start_height_m is not null) as has_start_height,
                (date_known_year is not null) as has_known_year,
                case
                    when is_confidential then 'confidential_limited_use'
                    when geom_27700 is null then 'metadata_only_missing_geometry'
                    when has_ags_log then 'usable_for_proximity_and_log_availability'
                    else 'usable_for_proximity_only'
                end as operator_use_status,
                case
                    when is_confidential then
                        'BGS borehole index row is marked confidential. Use only as limited coverage context unless source records are reviewed.'
                    when geom_27700 is null then
                        'BGS borehole index row has no usable geometry. It is not safe for site proximity measurement.'
                    else
                        'BGS borehole index is safe for borehole proximity, density and log-availability intelligence only. It is not final ground-condition interpretation, piling, grouting, remediation or abnormal-cost evidence.'
                end as safe_use_caveat,
                'BGS Single Onshore Borehole Index'::text as source_name,
                'known_origin_manual_bulk_upload'::text as source_governance_status,
                'high_value_governance_incomplete'::text as risk_classification,
                'safe_for_proximity_density_and_log_availability_not_ground_condition_interpretation'::text as safe_use_classification
            from landintel.bgs_borehole_master
        $view$;

        execute $view$
            create or replace view landintel_reporting.v_bgs_borehole_operator_index
            with (security_invoker = true) as
            select
                bgs_id,
                source_snapshot_date,
                source_file_name,
                source_row_number,
                registration_number,
                source_record_type,
                grid_reference,
                easting,
                northing,
                geom_27700,
                geom_wgs84,
                is_confidential,
                borehole_name,
                depth_m,
                depth_status,
                has_log_available,
                ags_log_url,
                date_known_year,
                date_entered,
                has_valid_geometry,
                has_depth,
                has_start_height,
                has_known_year,
                operator_use_status,
                safe_use_caveat,
                source_name,
                source_governance_status,
                risk_classification,
                safe_use_classification,
                updated_at
            from landintel_store.v_bgs_borehole_master_clean
        $view$;

        execute $view$
            create or replace view landintel_reporting.v_bgs_borehole_data_quality
            with (security_invoker = true) as
            select
                source_snapshot_date,
                source_file_name,
                count(*)::bigint as row_count,
                count(*) filter (where has_valid_geometry)::bigint as rows_with_valid_geometry,
                count(*) filter (where not has_valid_geometry)::bigint as rows_missing_valid_geometry,
                count(*) filter (where is_confidential)::bigint as confidential_rows,
                count(*) filter (where has_depth)::bigint as rows_with_depth,
                count(*) filter (where has_start_height)::bigint as rows_with_start_height,
                count(*) filter (where has_known_year)::bigint as rows_with_known_year,
                count(*) filter (where has_log_available)::bigint as rows_with_log_available,
                min(depth_m) filter (where has_depth) as min_depth_m,
                max(depth_m) filter (where has_depth) as max_depth_m,
                round((avg(depth_m) filter (where has_depth))::numeric, 2) as avg_depth_m,
                min(date_known_year) filter (where has_known_year) as earliest_known_year,
                max(date_known_year) filter (where has_known_year) as latest_known_year,
                max(updated_at) as latest_row_updated_at,
                'BGS borehole index is governed as known-origin manual bulk upload. Use for proximity, density and log-availability context only, not final ground-condition interpretation.'::text as safe_use_caveat
            from landintel_store.v_bgs_borehole_master_clean
            group by source_snapshot_date, source_file_name
        $view$;

        comment on view landintel_store.v_bgs_borehole_master_clean
            is 'Clean governed view over the known-origin manual BGS Single Onshore Borehole Index bulk upload. Safe for proximity, density and log-availability context only.';

        comment on view landintel_reporting.v_bgs_borehole_operator_index
            is 'Operator-safe BGS borehole index surface. Does not provide legal, engineering, piling, grouting, remediation or abnormal-cost conclusions.';

        comment on view landintel_reporting.v_bgs_borehole_data_quality
            is 'BGS borehole master data quality and governance summary for source coverage, parse rate and safe-use caveats.';
    end if;
end $$;

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
                'landintel',
                'bgs_borehole_master',
                'table',
                'known_origin_manual_bulk_upload',
                'landintel_store',
                'BGS borehole master warehouse asset',
                'ground_abnormal',
                false,
                true,
                false,
                true,
                false,
                false,
                false,
                'landintel_store.v_bgs_borehole_master_clean',
                'High-value BGS Single Onshore Borehole Index manual bulk upload. Governance and enrichment are incomplete.',
                'Govern through clean views and later bounded site-to-borehole proximity workflow. Do not re-upload or treat as final ground-condition interpretation.',
                '{"risk_classification":"high_value_governance_incomplete","source":"BGS Single Onshore Borehole Index","manual_bulk_upload":true,"trusted_interpreted_ground_evidence":false}'::jsonb
            ),
            (
                'landintel_store',
                'v_bgs_borehole_master_clean',
                'view',
                'current_keep',
                'landintel_store',
                'clean governed BGS borehole warehouse view',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                false,
                false,
                null,
                'Read-only normalised view over manual BGS master upload. Not an interpreted ground-condition surface.',
                'Use as the source for future bounded proximity and evidence-generation workflows.',
                '{"safe_use":"proximity_density_log_availability_only"}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_bgs_borehole_operator_index',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'operator-safe BGS borehole lookup surface',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Operator view exposes useful borehole context with explicit caveats. It is not engineering evidence.',
                'Use for manual review and future Pre-SI triage only.',
                '{"operator_safe":true,"final_ground_condition_interpretation":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_bgs_borehole_data_quality',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'BGS borehole source quality summary',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Aggregated source quality and parse-rate surface over the manual BGS borehole master.',
                'Use to prove coverage and governance before scaling site enrichment.',
                '{"operator_safe":true,"source_quality_summary":true}'::jsonb
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

grant usage on schema landintel_store to authenticated;
grant usage on schema landintel_reporting to authenticated;

do $$
begin
    if to_regclass('landintel_store.v_bgs_borehole_master_clean') is not null then
        grant select on landintel_store.v_bgs_borehole_master_clean to authenticated;
    end if;

    if to_regclass('landintel_reporting.v_bgs_borehole_operator_index') is not null then
        grant select on landintel_reporting.v_bgs_borehole_operator_index to authenticated;
    end if;

    if to_regclass('landintel_reporting.v_bgs_borehole_data_quality') is not null then
        grant select on landintel_reporting.v_bgs_borehole_data_quality to authenticated;
    end if;
end $$;

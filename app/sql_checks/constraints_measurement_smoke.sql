begin;
set local transaction read only;

-- Expected measurement tables, functions, and views must exist.
do $$
declare
    expected_object text;
begin
    foreach expected_object in array[
        'public.site_spatial_links',
        'public.site_title_validation',
        'public.constraint_layer_registry',
        'public.constraint_source_features',
        'public.site_constraint_measurements',
        'public.site_constraint_group_summaries',
        'public.site_commercial_friction_facts',
        'analytics.v_constraint_measurement_coverage',
        'analytics.v_constraint_measurement_layer_coverage',
        'analytics.v_constraints_tab_overview',
        'analytics.v_constraints_tab_measurements',
        'analytics.v_constraints_tab_group_summaries',
        'analytics.v_constraints_tab_commercial_friction'
    ] loop
        if to_regclass(expected_object) is null then
            raise exception 'Missing expected constraints measurement object: %', expected_object;
        end if;
    end loop;

    if to_regprocedure('public.constraints_site_anchor()') is null then
        raise exception 'Missing expected function: public.constraints_site_anchor()';
    end if;

    if to_regprocedure('public.measure_constraint_feature(geometry,geometry,numeric)') is null then
        raise exception 'Missing expected function: public.measure_constraint_feature(geometry,geometry,numeric)';
    end if;

    if to_regprocedure('public.refresh_constraint_measurements_for_layer_sites(text,text[],numeric,numeric)') is null then
        raise exception 'Missing expected function: public.refresh_constraint_measurements_for_layer_sites(text,text[],numeric,numeric)';
    end if;
end $$;

-- Overview columns must exist.
do $$
declare
    required_column text;
begin
    foreach required_column in array[
        'site_id',
        'site_location_id',
        'site_name',
        'authority_name',
        'site_area_sqm',
        'site_area_acres',
        'location_label',
        'location_role',
        'constraint_groups_measured',
        'constraint_groups_intersecting',
        'measured_layer_keys',
        'friction_fact_count',
        'friction_fact_labels',
        'latest_measurement_at'
    ] loop
        if not exists (
            select 1
            from information_schema.columns
            where table_schema = 'analytics'
              and table_name = 'v_constraints_tab_overview'
              and column_name = required_column
        ) then
            raise exception 'Missing expected column on analytics.v_constraints_tab_overview: %', required_column;
        end if;
    end loop;
end $$;

-- Measurement detail columns must exist.
do $$
declare
    required_column text;
begin
    foreach required_column in array[
        'site_id',
        'site_location_id',
        'site_name',
        'authority_name',
        'site_area_acres',
        'location_label',
        'layer_key',
        'layer_name',
        'constraint_group',
        'constraint_type',
        'measurement_mode',
        'buffer_distance_m',
        'constraint_feature_id',
        'source_feature_key',
        'feature_name',
        'source_reference',
        'severity_label',
        'measurement_source',
        'intersects',
        'within_buffer',
        'site_inside_feature',
        'feature_inside_site',
        'overlap_area_sqm',
        'overlap_pct_of_site',
        'overlap_pct_of_feature',
        'nearest_distance_m',
        'measured_at',
        'overlap_character'
    ] loop
        if not exists (
            select 1
            from information_schema.columns
            where table_schema = 'analytics'
              and table_name = 'v_constraints_tab_measurements'
              and column_name = required_column
        ) then
            raise exception 'Missing expected column on analytics.v_constraints_tab_measurements: %', required_column;
        end if;
    end loop;
end $$;

-- Group summary columns must exist.
do $$
declare
    required_column text;
begin
    foreach required_column in array[
        'site_id',
        'site_location_id',
        'site_name',
        'authority_name',
        'site_area_acres',
        'location_label',
        'layer_key',
        'layer_name',
        'constraint_group',
        'summary_scope',
        'intersecting_feature_count',
        'buffered_feature_count',
        'total_overlap_area_sqm',
        'max_overlap_pct_of_site',
        'min_distance_m',
        'nearest_feature_id',
        'nearest_feature_name',
        'measured_at',
        'constraint_character'
    ] loop
        if not exists (
            select 1
            from information_schema.columns
            where table_schema = 'analytics'
              and table_name = 'v_constraints_tab_group_summaries'
              and column_name = required_column
        ) then
            raise exception 'Missing expected column on analytics.v_constraints_tab_group_summaries: %', required_column;
        end if;
    end loop;
end $$;

-- Commercial friction columns must exist.
do $$
declare
    required_column text;
begin
    foreach required_column in array[
        'site_id',
        'site_location_id',
        'site_name',
        'authority_name',
        'site_area_acres',
        'location_label',
        'layer_key',
        'layer_name',
        'constraint_group',
        'fact_key',
        'fact_label',
        'fact_value_text',
        'fact_value_numeric',
        'fact_unit',
        'fact_basis',
        'created_at',
        'evidence_state_signature'
    ] loop
        if not exists (
            select 1
            from information_schema.columns
            where table_schema = 'analytics'
              and table_name = 'v_constraints_tab_commercial_friction'
              and column_name = required_column
        ) then
            raise exception 'Missing expected column on analytics.v_constraints_tab_commercial_friction: %', required_column;
        end if;
    end loop;
end $$;

-- Overview should stay one row per live site geometry anchor.
do $$
declare
    overview_count bigint;
    anchor_count bigint;
begin
    select count(*) into overview_count from analytics.v_constraints_tab_overview;
    select count(*) into anchor_count from public.constraints_site_anchor();

    if overview_count <> anchor_count then
        raise exception 'analytics.v_constraints_tab_overview row count (%) does not match public.constraints_site_anchor() (%)', overview_count, anchor_count;
    end if;
end $$;

-- Constraints tab surfaces must stay measurement-only.
do $$
begin
    if exists (
        select 1
        from information_schema.columns
        where table_schema = 'analytics'
          and table_name in (
              'v_constraints_tab_overview',
              'v_constraints_tab_measurements',
              'v_constraints_tab_group_summaries',
              'v_constraints_tab_commercial_friction'
          )
          and (
              column_name like '%score%'
              or column_name like '%rag%'
              or column_name like '%pass_fail%'
          )
    ) then
        raise exception 'Constraints tab views contain unsupported scoring or pass/fail fields';
    end if;
end $$;

-- Legacy site_constraints should be clearly marked if it still exists.
do $$
declare
    legacy_comment text;
begin
    if to_regclass('public.site_constraints') is not null then
        select obj_description('public.site_constraints'::regclass, 'pg_class') into legacy_comment;
        if legacy_comment is null or position('legacy' in lower(legacy_comment)) = 0 then
            raise exception 'public.site_constraints must be marked as legacy';
        end if;
    end if;
end $$;

rollback;

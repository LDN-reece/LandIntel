alter table public.site_constraint_measurements
    add column if not exists feature_geometry_dimension integer,
    add column if not exists overlap_character text,
    add column if not exists measurement_signature text;

alter table public.site_constraint_group_summaries
    add column if not exists constraint_character text,
    add column if not exists summary_signature text;

alter table public.site_commercial_friction_facts
    add column if not exists evidence_state_signature text;

alter table public.site_constraint_measurements
    drop constraint if exists site_constraint_measurements_overlap_character_check;

alter table public.site_constraint_measurements
    add constraint site_constraint_measurements_overlap_character_check
    check (
        overlap_character is null
        or overlap_character in (
            'edge-based',
            'core-based',
            'central',
            'fragmented',
            'linear',
            'proximity-only',
            'unknown'
        )
    );

alter table public.site_constraint_group_summaries
    drop constraint if exists site_constraint_group_summaries_character_check;

alter table public.site_constraint_group_summaries
    add constraint site_constraint_group_summaries_character_check
    check (
        constraint_character is null
        or constraint_character in (
            'edge-based',
            'core-based',
            'central',
            'fragmented',
            'linear',
            'proximity-only',
            'unknown'
        )
    );

create index if not exists site_constraint_measurements_signature_idx
    on public.site_constraint_measurements (constraint_layer_id, site_location_id, measurement_signature);

create index if not exists site_constraint_group_summaries_signature_idx
    on public.site_constraint_group_summaries (constraint_layer_id, site_location_id, summary_signature);

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
set search_path = pg_catalog, public, extensions
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
                 and st_dimension(feature_geometry) = 2
                    then st_area(st_collectionextract(st_intersection(site_geometry, feature_geometry), 3))
                else 0::double precision
            end as overlap_area_sqm,
            st_distance(site_geometry, feature_geometry) as nearest_distance_m,
            nullif(st_area(site_geometry), 0) as site_area_sqm,
            case
                when st_dimension(feature_geometry) = 2 then nullif(st_area(feature_geometry), 0)
                else null::double precision
            end as feature_area_sqm
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

create or replace function public.classify_constraint_overlap(
    p_intersects boolean,
    p_within_buffer boolean,
    p_overlap_pct_of_site numeric,
    p_feature_geometry geometry
)
returns text
language sql
immutable
set search_path = pg_catalog, public, extensions
as $$
    select case
        when coalesce(p_intersects, false) = false and coalesce(p_within_buffer, false) then 'proximity-only'
        when coalesce(p_intersects, false) = false then 'unknown'
        when p_feature_geometry is not null and st_dimension(p_feature_geometry) = 1 then 'linear'
        when coalesce(p_overlap_pct_of_site, 0) >= 50 then 'central'
        when coalesce(p_overlap_pct_of_site, 0) >= 15 then 'core-based'
        when coalesce(p_overlap_pct_of_site, 0) > 0 then 'edge-based'
        else 'edge-based'
    end;
$$;

create or replace function public.constraint_summary_signature(
    p_intersecting_feature_count integer,
    p_buffered_feature_count integer,
    p_total_overlap_area_sqm numeric,
    p_max_overlap_pct_of_site numeric,
    p_min_distance_m numeric,
    p_nearest_feature_id uuid,
    p_constraint_character text
)
returns text
language sql
immutable
set search_path = pg_catalog
as $$
    select md5(concat_ws(
        '|',
        coalesce(p_intersecting_feature_count, 0)::text,
        coalesce(p_buffered_feature_count, 0)::text,
        round(coalesce(p_total_overlap_area_sqm, 0), 2)::text,
        round(coalesce(p_max_overlap_pct_of_site, 0), 4)::text,
        round(coalesce(p_min_distance_m, -1), 2)::text,
        coalesce(p_nearest_feature_id::text, ''),
        coalesce(p_constraint_character, 'unknown')
    ));
$$;

create or replace function public.refresh_constraint_measurements_for_layer_sites(
    p_layer_key text,
    p_site_location_ids text[],
    p_overlap_delta_pct numeric default 1,
    p_distance_delta_m numeric default 25
)
returns table (
    layer_key text,
    site_batch_count integer,
    measurement_count integer,
    summary_count integer,
    friction_fact_count integer,
    evidence_count integer,
    signal_count integer,
    change_event_count integer,
    affected_site_count integer,
    material_change_count integer
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_layer_id uuid;
    v_source_family text;
    v_constraint_group text;
    v_layer_name text;
    v_site_batch_count integer := 0;
    v_measurement_count integer := 0;
    v_summary_count integer := 0;
    v_friction_fact_count integer := 0;
    v_evidence_count integer := 0;
    v_signal_count integer := 0;
    v_change_event_count integer := 0;
    v_affected_site_count integer := 0;
    v_material_change_count integer := 0;
begin
    if coalesce(array_length(p_site_location_ids, 1), 0) = 0 then
        return query select p_layer_key, 0, 0, 0, 0, 0, 0, 0, 0, 0;
        return;
    end if;

    select layer_row.id, layer_row.source_family, layer_row.constraint_group, layer_row.layer_name
      into v_layer_id, v_source_family, v_constraint_group, v_layer_name
    from public.constraint_layer_registry as layer_row
    where layer_row.layer_key = p_layer_key
      and layer_row.is_active = true;

    if v_layer_id is null then
        raise exception using message = concat('Unknown active constraint layer key: ', p_layer_key);
    end if;

    select count(distinct site_location_id)
      into v_site_batch_count
    from unnest(p_site_location_ids) as input(site_location_id);

    create temporary table if not exists tmp_constraint_previous_summary (
        site_id text,
        site_location_id text,
        intersecting_feature_count integer,
        buffered_feature_count integer,
        total_overlap_area_sqm numeric,
        max_overlap_pct_of_site numeric,
        min_distance_m numeric,
        nearest_feature_id uuid,
        nearest_feature_name text,
        constraint_character text,
        summary_signature text
    ) on commit drop;

    truncate tmp_constraint_previous_summary;

    insert into tmp_constraint_previous_summary (
        site_id,
        site_location_id,
        intersecting_feature_count,
        buffered_feature_count,
        total_overlap_area_sqm,
        max_overlap_pct_of_site,
        min_distance_m,
        nearest_feature_id,
        nearest_feature_name,
        constraint_character,
        summary_signature
    )
    select
        summary.site_id,
        summary.site_location_id,
        summary.intersecting_feature_count,
        summary.buffered_feature_count,
        summary.total_overlap_area_sqm,
        summary.max_overlap_pct_of_site,
        summary.min_distance_m,
        summary.nearest_feature_id,
        summary.nearest_feature_name,
        coalesce(summary.constraint_character, summary.metadata ->> 'constraint_character', 'unknown'),
        coalesce(
            summary.summary_signature,
            public.constraint_summary_signature(
                summary.intersecting_feature_count,
                summary.buffered_feature_count,
                summary.total_overlap_area_sqm,
                summary.max_overlap_pct_of_site,
                summary.min_distance_m,
                summary.nearest_feature_id,
                coalesce(summary.constraint_character, summary.metadata ->> 'constraint_character', 'unknown')
            )
        )
    from public.site_constraint_group_summaries as summary
    where summary.constraint_layer_id = v_layer_id
      and summary.site_location_id = any(p_site_location_ids);

    delete from public.site_constraint_measurements
    where constraint_layer_id = v_layer_id
      and site_location_id = any(p_site_location_ids);

    with inserted_measurements as (
        insert into public.site_constraint_measurements (
            site_id,
            site_location_id,
            constraint_layer_id,
            constraint_feature_id,
            measurement_source,
            intersects,
            within_buffer,
            site_inside_feature,
            feature_inside_site,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_feature,
            nearest_distance_m,
            buffer_distance_m,
            feature_geometry_dimension,
            overlap_character,
            measurement_signature,
            metadata
        )
        select
            anchor.site_id,
            anchor.site_location_id,
            layer_row.id,
            feature.id,
            'canonical_site_geometry',
            metric.intersects,
            metric.within_buffer,
            metric.site_inside_feature,
            metric.feature_inside_site,
            metric.overlap_area_sqm,
            metric.overlap_pct_of_site,
            metric.overlap_pct_of_feature,
            metric.nearest_distance_m,
            layer_row.buffer_distance_m,
            st_dimension(feature.geometry),
            public.classify_constraint_overlap(
                metric.intersects,
                metric.within_buffer,
                metric.overlap_pct_of_site,
                feature.geometry
            ) as overlap_character,
            md5(concat_ws(
                '|',
                anchor.site_location_id,
                feature.id::text,
                metric.intersects::text,
                metric.within_buffer::text,
                round(metric.overlap_pct_of_site, 4)::text,
                round(coalesce(metric.nearest_distance_m, -1), 2)::text,
                public.classify_constraint_overlap(
                    metric.intersects,
                    metric.within_buffer,
                    metric.overlap_pct_of_site,
                    feature.geometry
                )
            )) as measurement_signature,
            jsonb_build_object(
                'constraint_layer_key', layer_row.layer_key,
                'source_feature_key', feature.source_feature_key,
                'source_expansion_constraint', true,
                'feature_geometry_dimension', st_dimension(feature.geometry),
                'constraint_character', public.classify_constraint_overlap(
                    metric.intersects,
                    metric.within_buffer,
                    metric.overlap_pct_of_site,
                    feature.geometry
                )
            )
        from public.constraint_layer_registry as layer_row
        join public.constraint_source_features as feature
          on feature.constraint_layer_id = layer_row.id
        join public.constraints_site_anchor() as anchor
          on anchor.site_location_id = any(p_site_location_ids)
         and anchor.geometry is not null
         and feature.geometry is not null
         and anchor.geometry OPERATOR(extensions.&&) st_expand(feature.geometry, greatest(layer_row.buffer_distance_m, 0))
         and (
                (
                    layer_row.buffer_distance_m > 0
                    and st_dwithin(anchor.geometry, feature.geometry, layer_row.buffer_distance_m)
                )
                or (
                    layer_row.buffer_distance_m = 0
                    and st_intersects(anchor.geometry, feature.geometry)
                )
             )
        cross join lateral public.measure_constraint_feature(
            anchor.geometry,
            feature.geometry,
            layer_row.buffer_distance_m
        ) as metric
        where layer_row.id = v_layer_id
          and (metric.intersects or metric.within_buffer)
        returning id
    )
    select count(*) into v_measurement_count from inserted_measurements;

    create temporary table if not exists tmp_constraint_new_summary (
        site_id text,
        site_location_id text,
        intersecting_feature_count integer,
        buffered_feature_count integer,
        total_overlap_area_sqm numeric,
        max_overlap_pct_of_site numeric,
        min_distance_m numeric,
        nearest_feature_id uuid,
        nearest_feature_name text,
        constraint_character text,
        summary_signature text
    ) on commit drop;

    truncate tmp_constraint_new_summary;

    insert into tmp_constraint_new_summary (
        site_id,
        site_location_id,
        intersecting_feature_count,
        buffered_feature_count,
        total_overlap_area_sqm,
        max_overlap_pct_of_site,
        min_distance_m,
        nearest_feature_id,
        nearest_feature_name,
        constraint_character,
        summary_signature
    )
    select
        measurement.site_id,
        measurement.site_location_id,
        count(*) filter (where measurement.intersects)::integer as intersecting_feature_count,
        count(*) filter (where measurement.within_buffer)::integer as buffered_feature_count,
        coalesce(sum(measurement.overlap_area_sqm), 0) as total_overlap_area_sqm,
        coalesce(max(measurement.overlap_pct_of_site), 0) as max_overlap_pct_of_site,
        min(measurement.nearest_distance_m) as min_distance_m,
        (array_agg(feature.id order by measurement.nearest_distance_m nulls last, measurement.overlap_area_sqm desc))[1],
        (array_agg(feature.feature_name order by measurement.nearest_distance_m nulls last, measurement.overlap_area_sqm desc))[1],
        case
            when count(*) filter (where measurement.intersects) = 0
             and count(*) filter (where measurement.within_buffer) > 0 then 'proximity-only'
            when bool_or(measurement.overlap_character = 'linear') then 'linear'
            when count(*) filter (where measurement.intersects) > 1
             and coalesce(sum(measurement.overlap_area_sqm), 0) > coalesce(max(measurement.overlap_area_sqm), 0) * 1.15 then 'fragmented'
            when coalesce(max(measurement.overlap_pct_of_site), 0) >= 50 then 'central'
            when coalesce(max(measurement.overlap_pct_of_site), 0) >= 15 then 'core-based'
            when coalesce(max(measurement.overlap_pct_of_site), 0) > 0 then 'edge-based'
            when count(*) filter (where measurement.intersects) > 0 then 'edge-based'
            else 'unknown'
        end as constraint_character,
        public.constraint_summary_signature(
            count(*) filter (where measurement.intersects)::integer,
            count(*) filter (where measurement.within_buffer)::integer,
            coalesce(sum(measurement.overlap_area_sqm), 0),
            coalesce(max(measurement.overlap_pct_of_site), 0),
            min(measurement.nearest_distance_m),
            (array_agg(feature.id order by measurement.nearest_distance_m nulls last, measurement.overlap_area_sqm desc))[1],
            case
                when count(*) filter (where measurement.intersects) = 0
                 and count(*) filter (where measurement.within_buffer) > 0 then 'proximity-only'
                when bool_or(measurement.overlap_character = 'linear') then 'linear'
                when count(*) filter (where measurement.intersects) > 1
                 and coalesce(sum(measurement.overlap_area_sqm), 0) > coalesce(max(measurement.overlap_area_sqm), 0) * 1.15 then 'fragmented'
                when coalesce(max(measurement.overlap_pct_of_site), 0) >= 50 then 'central'
                when coalesce(max(measurement.overlap_pct_of_site), 0) >= 15 then 'core-based'
                when coalesce(max(measurement.overlap_pct_of_site), 0) > 0 then 'edge-based'
                when count(*) filter (where measurement.intersects) > 0 then 'edge-based'
                else 'unknown'
            end
        ) as summary_signature
    from public.site_constraint_measurements as measurement
    join public.constraint_source_features as feature
      on feature.id = measurement.constraint_feature_id
    where measurement.constraint_layer_id = v_layer_id
      and measurement.site_location_id = any(p_site_location_ids)
    group by measurement.site_id, measurement.site_location_id;

    create temporary table if not exists tmp_constraint_changed_sites (
        site_id text,
        site_location_id text,
        previous_signature text,
        current_signature text,
        material_reason text
    ) on commit drop;

    truncate tmp_constraint_changed_sites;

    insert into tmp_constraint_changed_sites (
        site_id,
        site_location_id,
        previous_signature,
        current_signature,
        material_reason
    )
    select
        coalesce(new_summary.site_id, previous.site_id),
        coalesce(new_summary.site_location_id, previous.site_location_id),
        previous.summary_signature,
        new_summary.summary_signature,
        case
            when previous.site_location_id is null then 'constraint_relationship_added'
            when new_summary.site_location_id is null then 'constraint_relationship_removed'
            when previous.intersecting_feature_count is distinct from new_summary.intersecting_feature_count then 'intersection_count_changed'
            when previous.buffered_feature_count is distinct from new_summary.buffered_feature_count then 'buffered_count_changed'
            when previous.nearest_feature_id is distinct from new_summary.nearest_feature_id then 'nearest_feature_changed'
            when previous.constraint_character is distinct from new_summary.constraint_character then 'constraint_character_changed'
            when abs(coalesce(previous.max_overlap_pct_of_site, 0) - coalesce(new_summary.max_overlap_pct_of_site, 0))
                    >= greatest(coalesce(p_overlap_delta_pct, 1), 0) then 'overlap_pct_changed'
            when previous.min_distance_m is distinct from new_summary.min_distance_m
             and abs(coalesce(previous.min_distance_m, 0) - coalesce(new_summary.min_distance_m, 0))
                    >= greatest(coalesce(p_distance_delta_m, 25), 0) then 'nearest_distance_changed'
            else 'summary_signature_changed'
        end as material_reason
    from tmp_constraint_previous_summary as previous
    full outer join tmp_constraint_new_summary as new_summary
      on new_summary.site_location_id = previous.site_location_id
    where previous.site_location_id is null
       or new_summary.site_location_id is null
       or previous.intersecting_feature_count is distinct from new_summary.intersecting_feature_count
       or previous.buffered_feature_count is distinct from new_summary.buffered_feature_count
       or previous.nearest_feature_id is distinct from new_summary.nearest_feature_id
       or previous.constraint_character is distinct from new_summary.constraint_character
       or abs(coalesce(previous.max_overlap_pct_of_site, 0) - coalesce(new_summary.max_overlap_pct_of_site, 0))
            >= greatest(coalesce(p_overlap_delta_pct, 1), 0)
       or (
            previous.min_distance_m is distinct from new_summary.min_distance_m
            and abs(coalesce(previous.min_distance_m, 0) - coalesce(new_summary.min_distance_m, 0))
                >= greatest(coalesce(p_distance_delta_m, 25), 0)
       );

    select count(*) into v_material_change_count from tmp_constraint_changed_sites;

    delete from public.site_constraint_group_summaries as summary
    using tmp_constraint_changed_sites as changed
    where summary.constraint_layer_id = v_layer_id
      and summary.site_location_id = changed.site_location_id;

    with inserted_summaries as (
        insert into public.site_constraint_group_summaries (
            site_id,
            site_location_id,
            constraint_layer_id,
            constraint_group,
            summary_scope,
            intersecting_feature_count,
            buffered_feature_count,
            total_overlap_area_sqm,
            max_overlap_pct_of_site,
            min_distance_m,
            nearest_feature_id,
            nearest_feature_name,
            constraint_character,
            summary_signature,
            metadata
        )
        select
            new_summary.site_id,
            new_summary.site_location_id,
            v_layer_id,
            v_constraint_group,
            'canonical_site_geometry',
            new_summary.intersecting_feature_count,
            new_summary.buffered_feature_count,
            new_summary.total_overlap_area_sqm,
            new_summary.max_overlap_pct_of_site,
            new_summary.min_distance_m,
            new_summary.nearest_feature_id,
            new_summary.nearest_feature_name,
            new_summary.constraint_character,
            new_summary.summary_signature,
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_character', new_summary.constraint_character,
                'source_expansion_constraint', true
            )
        from tmp_constraint_new_summary as new_summary
        join tmp_constraint_changed_sites as changed
          on changed.site_location_id = new_summary.site_location_id
        returning id
    )
    select count(*) into v_summary_count from inserted_summaries;

    delete from public.site_commercial_friction_facts as fact
    using tmp_constraint_changed_sites as changed
    where fact.constraint_layer_id = v_layer_id
      and fact.site_location_id = changed.site_location_id;

    with inserted_facts as (
        insert into public.site_commercial_friction_facts (
            site_id,
            site_location_id,
            constraint_group,
            constraint_layer_id,
            fact_key,
            fact_label,
            fact_value_text,
            fact_value_numeric,
            fact_unit,
            fact_basis,
            source_summary_id,
            evidence_state_signature,
            metadata
        )
        select
            summary.site_id,
            summary.site_location_id,
            summary.constraint_group,
            summary.constraint_layer_id,
            v_source_family || '_constraint_evidence',
            case
                when summary.max_overlap_pct_of_site > 0 then
                    v_layer_name || ' intersects ' || round(summary.max_overlap_pct_of_site, 1)::text || '%% of site'
                when summary.min_distance_m is not null then
                    'Nearest ' || v_layer_name || ' is ' || round(summary.min_distance_m, 0)::text || 'm from site'
                else
                    v_layer_name || ' constraint evidence present'
            end,
            summary.constraint_character,
            case
                when summary.max_overlap_pct_of_site > 0 then summary.max_overlap_pct_of_site
                else summary.min_distance_m
            end,
            case
                when summary.max_overlap_pct_of_site > 0 then 'pct_of_site'
                else 'm'
            end,
            'measured_site_constraint_delta',
            summary.id,
            summary.summary_signature,
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_character', summary.constraint_character,
                'source_expansion_constraint', true
            )
        from public.site_constraint_group_summaries as summary
        join tmp_constraint_changed_sites as changed
          on changed.site_location_id = summary.site_location_id
        where summary.constraint_layer_id = v_layer_id
          and (summary.intersecting_feature_count > 0 or summary.buffered_feature_count > 0)
        returning id
    )
    select count(*) into v_friction_fact_count from inserted_facts;

    delete from landintel.evidence_references as evidence
    using tmp_constraint_changed_sites as changed
    where evidence.canonical_site_id = changed.site_id::uuid
      and evidence.source_family = v_source_family
      and evidence.metadata ->> 'constraint_layer_key' = p_layer_key
      and evidence.metadata ->> 'source_expansion_constraint' = 'true';

    with inserted_evidence as (
        insert into landintel.evidence_references (
            canonical_site_id,
            source_family,
            source_dataset,
            source_record_id,
            source_reference,
            confidence,
            source_registry_id,
            ingest_run_id,
            metadata
        )
        select distinct
            measurement.site_id::uuid,
            v_source_family,
            v_layer_name,
            feature.source_feature_key,
            coalesce(feature.source_reference, feature.feature_name, feature.source_feature_key),
            'medium',
            null::uuid,
            null::uuid,
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_group', v_constraint_group,
                'constraint_character', measurement.overlap_character,
                'source_expansion_constraint', true,
                'overlap_pct_of_site', measurement.overlap_pct_of_site,
                'nearest_distance_m', measurement.nearest_distance_m
            )
        from public.site_constraint_measurements as measurement
        join public.constraint_source_features as feature
          on feature.id = measurement.constraint_feature_id
        join tmp_constraint_changed_sites as changed
          on changed.site_location_id = measurement.site_location_id
        where measurement.constraint_layer_id = v_layer_id
        returning id
    )
    select count(*) into v_evidence_count from inserted_evidence;

    delete from landintel.site_signals as signal
    using tmp_constraint_changed_sites as changed
    where signal.canonical_site_id = changed.site_id::uuid
      and signal.source_family = v_source_family
      and signal.metadata ->> 'constraint_layer_key' = p_layer_key;

    with inserted_signals as (
        insert into landintel.site_signals (
            canonical_site_id,
            signal_family,
            signal_name,
            signal_value_text,
            signal_value_numeric,
            confidence,
            source_family,
            source_record_id,
            fact_label,
            evidence_metadata,
            metadata,
            current_flag
        )
        select
            summary.site_id::uuid,
            'constraints',
            'constraint_presence',
            summary.constraint_character,
            summary.max_overlap_pct_of_site,
            0.75,
            v_source_family,
            p_layer_key,
            'measured_constraint_fact',
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_group', v_constraint_group,
                'intersecting_feature_count', summary.intersecting_feature_count,
                'buffered_feature_count', summary.buffered_feature_count,
                'constraint_character', summary.constraint_character
            ),
            jsonb_build_object('constraint_layer_key', p_layer_key, 'source_expansion_constraint', true),
            true
        from public.site_constraint_group_summaries as summary
        join tmp_constraint_changed_sites as changed
          on changed.site_location_id = summary.site_location_id
        where summary.constraint_layer_id = v_layer_id
        returning id
    )
    select count(*) into v_signal_count from inserted_signals;

    with inserted_events as (
        insert into landintel.site_change_events (
            canonical_site_id,
            source_family,
            source_record_id,
            change_type,
            change_summary,
            previous_signature,
            current_signature,
            triggered_refresh,
            metadata
        )
        select
            changed.site_id::uuid,
            v_source_family,
            p_layer_key,
            'constraint_evidence_state_changed',
            v_layer_name || ' measured constraint evidence changed for canonical site.',
            changed.previous_signature,
            changed.current_signature,
            true,
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_group', v_constraint_group,
                'material_reason', changed.material_reason,
                'source_expansion_constraint', true
            )
        from tmp_constraint_changed_sites as changed
        returning id
    )
    select count(*) into v_change_event_count from inserted_events;

    with enqueued as (
        insert into landintel.canonical_site_refresh_queue (
            canonical_site_id,
            refresh_scope,
            trigger_source,
            source_family,
            source_record_id,
            status,
            metadata,
            updated_at
        )
        select
            changed.site_id::uuid,
            'site_outputs',
            'constraint_measurement_engine',
            v_source_family,
            p_layer_key,
            'pending',
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_group', v_constraint_group,
                'material_reason', changed.material_reason,
                'source_expansion_constraint', true
            ),
            now()
        from tmp_constraint_changed_sites as changed
        on conflict (canonical_site_id, refresh_scope) do update set
            trigger_source = excluded.trigger_source,
            source_family = excluded.source_family,
            source_record_id = excluded.source_record_id,
            status = 'pending',
            claimed_by = null,
            claimed_at = null,
            lease_expires_at = null,
            attempt_count = 0,
            next_attempt_at = null,
            processed_at = null,
            error_message = null,
            metadata = excluded.metadata,
            updated_at = now()
        returning id
    )
    select count(*) into v_affected_site_count from enqueued;

    return query select
        p_layer_key,
        v_site_batch_count,
        v_measurement_count,
        v_summary_count,
        v_friction_fact_count,
        v_evidence_count,
        v_signal_count,
        v_change_event_count,
        v_affected_site_count,
        v_material_change_count;
end;
$$;

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
    measurements.measured_at,
    measurements.overlap_character,
    measurements.feature_geometry_dimension
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
    summaries.measured_at,
    summaries.constraint_character
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
    facts.created_at,
    facts.evidence_state_signature
from public.site_commercial_friction_facts as facts
join site_anchor
  on site_anchor.site_id = facts.site_id
 and site_anchor.site_location_id = facts.site_location_id
join public.constraint_layer_registry as layer
  on layer.id = facts.constraint_layer_id
order by site_anchor.site_name, facts.constraint_group, facts.fact_label;

drop view if exists analytics.v_constraint_measurement_layer_coverage;
drop view if exists analytics.v_constraint_measurement_coverage;

create or replace view analytics.v_constraint_measurement_layer_coverage
with (security_invoker = true) as
select
    layer.layer_key,
    layer.layer_name,
    layer.source_family,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    layer.buffer_distance_m,
    layer.is_active,
    count(distinct feature.id)::bigint as source_feature_count,
    count(distinct measurement.id)::bigint as measured_row_count,
    count(distinct measurement.site_location_id)::bigint as measured_site_count,
    count(distinct summary.id)::bigint as summary_row_count,
    count(distinct fact.id)::bigint as commercial_friction_fact_count,
    max(measurement.measured_at) as latest_measured_at
from public.constraint_layer_registry as layer
left join public.constraint_source_features as feature
  on feature.constraint_layer_id = layer.id
left join public.site_constraint_measurements as measurement
  on measurement.constraint_layer_id = layer.id
left join public.site_constraint_group_summaries as summary
  on summary.constraint_layer_id = layer.id
left join public.site_commercial_friction_facts as fact
  on fact.constraint_layer_id = layer.id
group by
    layer.layer_key,
    layer.layer_name,
    layer.source_family,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    layer.buffer_distance_m,
    layer.is_active;

create or replace view analytics.v_constraint_measurement_coverage
with (security_invoker = true) as
with site_anchor as (
    select *
    from public.constraints_site_anchor()
),
measured_sites as (
    select distinct site_location_id
    from public.site_constraint_measurements
)
select
    (select count(*)::bigint from public.constraint_layer_registry where is_active = true) as active_constraint_layer_count,
    (select count(*)::bigint from public.constraint_source_features) as source_constraint_feature_count,
    (select count(*)::bigint from public.site_constraint_measurements) as measured_site_constraint_row_count,
    (select count(*)::bigint from public.site_constraint_group_summaries) as grouped_summary_row_count,
    (select count(*)::bigint from public.site_commercial_friction_facts) as commercial_friction_fact_count,
    count(*)::bigint as canonical_site_geometry_count,
    count(*) filter (where measured_sites.site_location_id is not null)::bigint as canonical_sites_with_measured_constraints,
    count(*) filter (where measured_sites.site_location_id is null)::bigint as canonical_sites_without_measured_constraints,
    (select max(measured_at) from public.site_constraint_measurements) as latest_measured_at
from site_anchor
left join measured_sites
  on measured_sites.site_location_id = site_anchor.site_location_id;

grant select on analytics.v_constraint_measurement_layer_coverage to authenticated;
grant select on analytics.v_constraint_measurement_coverage to authenticated;

comment on function public.refresh_constraint_measurements_for_layer_sites(text, text[], numeric, numeric)
    is 'Batch-safe constraint measurement refresh. Measurements are refreshed idempotently, while evidence/signals/change events/refresh queue rows are emitted only where the site-layer evidence state materially changes.';

comment on view analytics.v_constraint_measurement_coverage
    is 'Proof view for Priority Zero constraint measurement coverage across live site geometries, measured rows, summaries and descriptive commercial friction facts.';

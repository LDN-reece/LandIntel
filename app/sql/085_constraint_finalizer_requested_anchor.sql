-- Optimise the existing constraint finalizer for bounded proof runs.
--
-- The previous scan-state write called public.constraints_site_anchor(), which expands every
-- canonical site before filtering back to the requested batch. That made heavy NatureScot
-- one-site proof chunks time out even though the workflow was correctly bounded.
--
-- This migration redefines the existing function only. It does not execute measurement, move
-- data, create a second constraint truth table, or broaden the source/cohort scope.

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
        join lateral (
            select
                site.id::text as site_id,
                site.id::text as site_location_id,
                site.geometry
            from landintel.canonical_sites as site
            join (
                select distinct input.site_location_id
                from unnest(p_site_location_ids) as input(site_location_id)
            ) as requested
              on requested.site_location_id = site.id::text
            where site.geometry is not null
        ) as anchor on true
        join lateral (
            select feature.*
            from public.constraint_source_features as feature
            where feature.constraint_layer_id = layer_row.id
              and feature.geometry is not null
              and feature.geometry OPERATOR(extensions.&&)
                    st_expand(anchor.geometry, greatest(layer_row.buffer_distance_m, 0)::double precision)
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
        ) as feature on true
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

    insert into public.site_constraint_measurement_scan_state (
        site_id,
        site_location_id,
        constraint_layer_id,
        scan_scope,
        latest_measurement_count,
        latest_summary_signature,
        has_constraint_relationship,
        scanned_at,
        updated_at,
        metadata
    )
    select
        anchor.site_id,
        anchor.site_location_id,
        v_layer_id,
        'canonical_site_geometry',
        coalesce(measurement_counts.measurement_count, 0),
        new_summary.summary_signature,
        new_summary.site_location_id is not null,
        now(),
        now(),
        jsonb_build_object(
            'constraint_layer_key', p_layer_key,
            'constraint_group', v_constraint_group,
            'source_expansion_constraint', true,
            'has_constraint_relationship', new_summary.site_location_id is not null,
            'latest_summary_signature', new_summary.summary_signature
        )
    from (
        select
            site.id::text as site_id,
            site.id::text as site_location_id
        from landintel.canonical_sites as site
        join (
            select distinct input.site_location_id
            from unnest(p_site_location_ids) as input(site_location_id)
        ) as requested
          on requested.site_location_id = site.id::text
        where site.geometry is not null
    ) as anchor
    left join tmp_constraint_new_summary as new_summary
      on new_summary.site_location_id = anchor.site_location_id
    left join (
        select
            measurement.site_location_id,
            count(*)::integer as measurement_count
        from public.site_constraint_measurements as measurement
        where measurement.constraint_layer_id = v_layer_id
          and measurement.site_location_id = any(p_site_location_ids)
        group by measurement.site_location_id
    ) as measurement_counts
      on measurement_counts.site_location_id = anchor.site_location_id
    on conflict (constraint_layer_id, site_location_id, scan_scope)
    do update set
        site_id = excluded.site_id,
        latest_measurement_count = excluded.latest_measurement_count,
        latest_summary_signature = excluded.latest_summary_signature,
        has_constraint_relationship = excluded.has_constraint_relationship,
        scanned_at = excluded.scanned_at,
        updated_at = now(),
        metadata = excluded.metadata;

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

comment on function public.refresh_constraint_measurements_for_layer_sites(text, text[], numeric, numeric)
    is 'Refreshes measured constraint relationships for one layer and an explicit requested canonical-site batch. Scan-state anchor is restricted to the requested site ids for bounded GitHub proof runs.';

create table if not exists landintel.ela_site_records (
    id uuid primary key default gen_random_uuid(),
    source_key text not null default 'employment_land_supply_spatialhub',
    source_family text not null default 'ela',
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    site_reference text,
    site_name text,
    status_text text,
    geometry geometry(Geometry, 27700),
    source_estate_registry_id uuid references landintel.source_estate_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    source_record_signature text,
    geometry_hash text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.vdl_site_records (
    id uuid primary key default gen_random_uuid(),
    source_key text not null default 'vacant_derelict_land_spatialhub',
    source_family text not null default 'vdl',
    source_record_id text not null,
    canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    authority_name text,
    site_reference text,
    site_name text,
    status_text text,
    geometry geometry(Geometry, 27700),
    source_estate_registry_id uuid references landintel.source_estate_registry(id) on delete set null,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    source_record_signature text,
    geometry_hash text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.source_expansion_events (
    id uuid primary key default gen_random_uuid(),
    command_name text not null,
    source_key text,
    source_family text not null,
    status text not null,
    raw_rows bigint not null default 0,
    linked_rows bigint not null default 0,
    measured_rows bigint not null default 0,
    evidence_rows bigint not null default 0,
    signal_rows bigint not null default 0,
    change_event_rows bigint not null default 0,
    summary text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create table if not exists landintel.site_signals (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    signal_family text not null,
    signal_name text not null,
    signal_value_text text,
    signal_value_numeric numeric,
    confidence numeric,
    source_family text,
    source_record_id text,
    fact_label text not null default 'evidence_fact',
    evidence_metadata jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    current_flag boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists landintel.site_change_events (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    source_family text,
    source_record_id text,
    change_type text not null,
    change_summary text not null,
    previous_signature text,
    current_signature text,
    triggered_refresh boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

alter table landintel.site_signals
    add column if not exists canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    add column if not exists signal_family text,
    add column if not exists signal_name text,
    add column if not exists signal_value_text text,
    add column if not exists signal_value_numeric numeric,
    add column if not exists confidence numeric,
    add column if not exists source_family text,
    add column if not exists source_record_id text,
    add column if not exists fact_label text not null default 'evidence_fact',
    add column if not exists evidence_metadata jsonb not null default '{}'::jsonb,
    add column if not exists metadata jsonb not null default '{}'::jsonb,
    add column if not exists current_flag boolean not null default true,
    add column if not exists created_at timestamptz not null default now(),
    add column if not exists updated_at timestamptz not null default now();

alter table landintel.site_change_events
    add column if not exists canonical_site_id uuid references landintel.canonical_sites(id) on delete cascade,
    add column if not exists source_family text,
    add column if not exists source_record_id text,
    add column if not exists change_type text,
    add column if not exists change_summary text,
    add column if not exists previous_signature text,
    add column if not exists current_signature text,
    add column if not exists triggered_refresh boolean not null default false,
    add column if not exists metadata jsonb not null default '{}'::jsonb,
    add column if not exists created_at timestamptz not null default now();

create unique index if not exists ela_site_records_source_uidx
    on landintel.ela_site_records (source_family, source_record_id);

create unique index if not exists vdl_site_records_source_uidx
    on landintel.vdl_site_records (source_family, source_record_id);

create index if not exists ela_site_records_site_idx
    on landintel.ela_site_records (canonical_site_id)
    where canonical_site_id is not null;

create index if not exists vdl_site_records_site_idx
    on landintel.vdl_site_records (canonical_site_id)
    where canonical_site_id is not null;

create index if not exists ela_site_records_geometry_gix
    on landintel.ela_site_records using gist (geometry);

create index if not exists vdl_site_records_geometry_gix
    on landintel.vdl_site_records using gist (geometry);

create index if not exists source_expansion_events_family_idx
    on landintel.source_expansion_events (source_family, created_at desc);

create index if not exists site_signals_site_family_idx
    on landintel.site_signals (canonical_site_id, signal_family, signal_name)
    where canonical_site_id is not null and current_flag = true;

create index if not exists site_change_events_site_idx
    on landintel.site_change_events (canonical_site_id, created_at desc)
    where canonical_site_id is not null;

alter table landintel.canonical_site_refresh_queue
    drop constraint if exists canonical_site_refresh_queue_family_check;

alter table landintel.canonical_site_refresh_queue
    add constraint canonical_site_refresh_queue_family_check
    check (
        source_family is null
        or source_family = any (array[
            'planning',
            'hla',
            'ela',
            'vdl',
            'sepa_flood',
            'coal_authority',
            'hes',
            'naturescot',
            'contaminated_land',
            'tpo',
            'culverts',
            'conservation_areas',
            'greenbelt',
            'topography',
            'os_places',
            'os_features',
            'local_landscape_areas',
            'local_nature',
            'forestry_woodland',
            'sgn_assets'
        ]::text[])
    );

create unique index if not exists constraint_source_features_layer_key_uidx
    on public.constraint_source_features (constraint_layer_id, source_feature_key);

create or replace function public.constraints_site_anchor()
returns table (
    site_id text,
    site_location_id text,
    site_name text,
    authority_name text,
    geometry geometry(Geometry, 27700),
    area_sqm numeric,
    area_acres numeric,
    location_label text,
    location_role text
)
language sql
stable
set search_path = pg_catalog, public, landintel
as $$
    select
        site.id::text as site_id,
        site.id::text as site_location_id,
        coalesce(
            nullif(btrim(to_jsonb(site) ->> 'site_name_primary'), ''),
            nullif(btrim(to_jsonb(site) ->> 'site_name'), ''),
            nullif(btrim(to_jsonb(site) ->> 'site_code'), ''),
            site.id::text
        ) as site_name,
        nullif(btrim(to_jsonb(site) ->> 'authority_name'), '') as authority_name,
        site.geometry,
        round(st_area(site.geometry)::numeric, 2) as area_sqm,
        public.calculate_area_acres(st_area(site.geometry)::numeric) as area_acres,
        'Canonical site geometry'::text as location_label,
        'canonical_site'::text as location_role
    from landintel.canonical_sites as site
    where site.geometry is not null;
$$;

insert into public.constraint_layer_registry (
    layer_key,
    layer_name,
    source_name,
    source_family,
    constraint_group,
    constraint_type,
    geometry_type,
    measurement_mode,
    buffer_distance_m,
    is_active,
    metadata
)
values
    ('sepa_flood', 'SEPA flood maps', 'SEPA Flood Maps', 'sepa_flood', 'flood', 'flood_risk', 'mixed', 'intersection', 0, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('coal_authority', 'Coal Authority mining constraints', 'Coal Authority mining constraints', 'coal_authority', 'mining', 'mining_risk', 'mixed', 'intersection', 0, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('hes', 'Historic Environment Scotland designations', 'Historic Environment Scotland designations', 'hes', 'heritage', 'heritage_designation', 'mixed', 'intersection_and_distance', 50, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('naturescot', 'NatureScot protected areas', 'NatureScot protected areas', 'naturescot', 'environmental', 'environmental_designation', 'mixed', 'intersection_and_distance', 50, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('contaminated_land', 'Contaminated land', 'Contaminated Land - Scotland', 'contaminated_land', 'contamination', 'contamination', 'mixed', 'intersection', 0, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('tpo', 'Tree Preservation Orders', 'Tree Preservation Orders - Scotland', 'tpo', 'trees', 'tree_preservation_order', 'mixed', 'intersection_and_distance', 25, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('culverts', 'Culverts', 'Culverts - Scotland', 'culverts', 'drainage', 'culvert', 'mixed', 'intersection_and_distance', 25, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('conservation_areas', 'Conservation areas', 'Conservation Areas - Scotland', 'conservation_areas', 'heritage', 'conservation_area', 'mixed', 'intersection', 0, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('greenbelt', 'Green belt', 'Green Belt - Scotland', 'greenbelt', 'policy_constraint', 'greenbelt', 'polygon', 'intersection', 0, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('topography', 'Topography and slope', 'OS Terrain and Scottish LiDAR', 'topography', 'topography', 'slope', 'raster_derived', 'intersection', 0, true, '{"phase_one_source_expansion": true, "derived_area_label": "indicative_only"}'::jsonb),
    ('os_features', 'OS NGD and OS Features', 'Ordnance Survey Features API', 'os_features', 'location_context', 'os_feature_context', 'mixed', 'intersection_and_distance', 25, true, '{"phase_one_source_expansion": true}'::jsonb),
    ('os_places', 'OS Places', 'Ordnance Survey Places API', 'os_places', 'location_context', 'address_place_context', 'point', 'distance', 250, true, '{"phase_one_source_expansion": true}'::jsonb)
on conflict (layer_key) do update set
    layer_name = excluded.layer_name,
    source_name = excluded.source_name,
    source_family = excluded.source_family,
    constraint_group = excluded.constraint_group,
    constraint_type = excluded.constraint_type,
    geometry_type = excluded.geometry_type,
    measurement_mode = excluded.measurement_mode,
    buffer_distance_m = excluded.buffer_distance_m,
    is_active = excluded.is_active,
    metadata = public.constraint_layer_registry.metadata || excluded.metadata,
    updated_at = now();

create or replace function public.refresh_constraint_measurements_for_layer(p_layer_key text)
returns table (
    measurement_count integer,
    summary_count integer,
    friction_fact_count integer,
    evidence_count integer,
    signal_count integer,
    affected_site_count integer
)
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
declare
    v_layer_id uuid;
    v_source_family text;
    v_constraint_group text;
    v_layer_name text;
    v_measurement_count integer := 0;
    v_summary_count integer := 0;
    v_friction_fact_count integer := 0;
    v_evidence_count integer := 0;
    v_signal_count integer := 0;
    v_affected_site_count integer := 0;
begin
    select id, source_family, constraint_group, layer_name
      into v_layer_id, v_source_family, v_constraint_group, v_layer_name
    from public.constraint_layer_registry
    where layer_key = p_layer_key;

    if v_layer_id is null then
        raise exception using message = concat('Unknown constraint layer key: ', p_layer_key);
    end if;

    delete from public.site_commercial_friction_facts where constraint_layer_id = v_layer_id;
    delete from public.site_constraint_group_summaries where constraint_layer_id = v_layer_id;
    delete from public.site_constraint_measurements where constraint_layer_id = v_layer_id;
    delete from landintel.evidence_references
    where source_family = v_source_family
      and metadata ->> 'constraint_layer_key' = p_layer_key
      and metadata ->> 'source_expansion_constraint' = 'true';
    delete from landintel.site_signals
    where source_family = v_source_family
      and metadata ->> 'constraint_layer_key' = p_layer_key;

    with measured as (
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
            jsonb_build_object(
                'constraint_layer_key', layer_row.layer_key,
                'source_feature_key', feature.source_feature_key,
                'source_expansion_constraint', true
            )
        from public.constraint_layer_registry as layer_row
        join public.constraint_source_features as feature on feature.constraint_layer_id = layer_row.id
        join public.constraints_site_anchor() as anchor on st_dwithin(anchor.geometry, feature.geometry, greatest(layer_row.buffer_distance_m, 0))
        cross join lateral public.measure_constraint_feature(anchor.geometry, feature.geometry, layer_row.buffer_distance_m) as metric
        where layer_row.id = v_layer_id
          and (metric.intersects or metric.within_buffer)
        returning id
    )
    select count(*) into v_measurement_count from measured;

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
            metadata
        )
        select
            measurement.site_id,
            measurement.site_location_id,
            v_layer_id,
            v_constraint_group,
            'canonical_site_geometry',
            count(*) filter (where measurement.intersects)::integer,
            count(*) filter (where measurement.within_buffer)::integer,
            coalesce(sum(measurement.overlap_area_sqm), 0),
            coalesce(max(measurement.overlap_pct_of_site), 0),
            min(measurement.nearest_distance_m),
            (array_agg(feature.id order by measurement.nearest_distance_m nulls last, measurement.overlap_area_sqm desc))[1],
            (array_agg(feature.feature_name order by measurement.nearest_distance_m nulls last, measurement.overlap_area_sqm desc))[1],
            jsonb_build_object('constraint_layer_key', p_layer_key, 'source_expansion_constraint', true)
        from public.site_constraint_measurements as measurement
        join public.constraint_source_features as feature on feature.id = measurement.constraint_feature_id
        where measurement.constraint_layer_id = v_layer_id
        group by measurement.site_id, measurement.site_location_id
        returning id
    )
    select count(*) into v_summary_count from inserted_summaries;

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
            metadata
        )
        select
            summary.site_id,
            summary.site_location_id,
            summary.constraint_group,
            summary.constraint_layer_id,
            v_source_family || '_constraint_overlap',
            v_layer_name || ' constraint overlap',
            case
                when summary.max_overlap_pct_of_site >= 25 then 'high'
                when summary.max_overlap_pct_of_site > 0 then 'medium'
                when summary.buffered_feature_count > 0 then 'nearby'
                else 'low'
            end,
            summary.max_overlap_pct_of_site,
            'pct_of_site',
            'canonical_site_constraint_overlay',
            summary.id,
            jsonb_build_object('constraint_layer_key', p_layer_key, 'source_expansion_constraint', true)
        from public.site_constraint_group_summaries as summary
        where summary.constraint_layer_id = v_layer_id
          and (summary.intersecting_feature_count > 0 or summary.buffered_feature_count > 0)
        returning id
    )
    select count(*) into v_friction_fact_count from inserted_facts;

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
                'source_expansion_constraint', true,
                'overlap_pct_of_site', measurement.overlap_pct_of_site,
                'nearest_distance_m', measurement.nearest_distance_m
            )
        from public.site_constraint_measurements as measurement
        join public.constraint_source_features as feature on feature.id = measurement.constraint_feature_id
        where measurement.constraint_layer_id = v_layer_id
        returning id
    )
    select count(*) into v_evidence_count from inserted_evidence;

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
            'constraint_severity',
            case
                when summary.max_overlap_pct_of_site >= 25 then 'high'
                when summary.max_overlap_pct_of_site > 0 then 'medium'
                when summary.buffered_feature_count > 0 then 'nearby'
                else 'low'
            end,
            summary.max_overlap_pct_of_site,
            0.75,
            v_source_family,
            p_layer_key,
            'evidence_fact',
            jsonb_build_object(
                'constraint_layer_key', p_layer_key,
                'constraint_group', v_constraint_group,
                'intersecting_feature_count', summary.intersecting_feature_count,
                'buffered_feature_count', summary.buffered_feature_count
            ),
            jsonb_build_object('constraint_layer_key', p_layer_key, 'source_expansion_constraint', true),
            true
        from public.site_constraint_group_summaries as summary
        where summary.constraint_layer_id = v_layer_id
        returning id
    )
    select count(*) into v_signal_count from inserted_signals;

    with affected_sites as (
        select distinct summary.site_id::uuid as canonical_site_id
        from public.site_constraint_group_summaries as summary
        where summary.constraint_layer_id = v_layer_id
    ), inserted_events as (
        insert into landintel.site_change_events (
            canonical_site_id,
            source_family,
            source_record_id,
            change_type,
            change_summary,
            triggered_refresh,
            metadata
        )
        select
            affected_sites.canonical_site_id,
            v_source_family,
            p_layer_key,
            'constraint_refresh',
            v_layer_name || ' constraint data refreshed for canonical site.',
            true,
            jsonb_build_object('constraint_layer_key', p_layer_key, 'source_expansion_constraint', true)
        from affected_sites
        returning id
    ), enqueued as (
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
            affected_sites.canonical_site_id,
            'site_outputs',
            'source_expansion_constraints',
            v_source_family,
            p_layer_key,
            'pending',
            jsonb_build_object('constraint_layer_key', p_layer_key, 'source_expansion_constraint', true),
            now()
        from affected_sites
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
        v_measurement_count,
        v_summary_count,
        v_friction_fact_count,
        v_evidence_count,
        v_signal_count,
        v_affected_site_count;
end;
$$;

drop view if exists analytics.v_phase_one_source_expansion_readiness;

create or replace view analytics.v_phase_one_source_expansion_readiness
with (security_invoker = true) as
with expected_sources(source_family, command_name, target_table, source_role) as (
    values
        ('ela', 'ingest-ela', 'landintel.ela_site_records', 'future_context'),
        ('vdl', 'ingest-vdl', 'landintel.vdl_site_records', 'future_context'),
        ('sepa_flood', 'ingest-sepa-flood', 'public.constraint_source_features', 'constraint'),
        ('coal_authority', 'ingest-coal-authority', 'public.constraint_source_features', 'constraint'),
        ('hes', 'ingest-hes-designations', 'public.constraint_source_features', 'constraint'),
        ('naturescot', 'ingest-naturescot', 'public.constraint_source_features', 'constraint'),
        ('contaminated_land', 'ingest-contaminated-land', 'public.constraint_source_features', 'constraint'),
        ('tpo', 'ingest-tpo', 'public.constraint_source_features', 'constraint'),
        ('culverts', 'ingest-culverts', 'public.constraint_source_features', 'constraint'),
        ('conservation_areas', 'ingest-conservation-areas', 'public.constraint_source_features', 'constraint'),
        ('greenbelt', 'ingest-greenbelt', 'public.constraint_source_features', 'constraint'),
        ('topography', 'ingest-os-topography', 'public.constraint_source_features', 'constraint'),
        ('os_places', 'ingest-os-places', 'public.constraint_source_features', 'location_context'),
        ('os_features', 'ingest-os-features', 'public.constraint_source_features', 'location_context'),
        ('ldp', 'ingest-ldp', 'landintel.ldp_site_records', 'policy_deferred'),
        ('settlement', 'promote-settlement-authority-source', 'landintel.authority_source_registry', 'policy_deferred')
), raw_counts as (
    select 'ela'::text as source_family, count(*)::bigint as raw_row_count from landintel.ela_site_records
    union all
    select 'vdl'::text as source_family, count(*)::bigint as raw_row_count from landintel.vdl_site_records
    union all
    select 'ldp'::text as source_family, count(*)::bigint as raw_row_count from landintel.ldp_site_records
    union all
    select layer.source_family, count(feature.id)::bigint as raw_row_count
    from public.constraint_layer_registry as layer
    left join public.constraint_source_features as feature on feature.constraint_layer_id = layer.id
    group by layer.source_family
), linked_counts as (
    select 'ela'::text as source_family, count(*)::bigint as linked_row_count from landintel.ela_site_records where canonical_site_id is not null
    union all
    select 'vdl'::text as source_family, count(*)::bigint as linked_row_count from landintel.vdl_site_records where canonical_site_id is not null
    union all
    select layer.source_family, count(distinct measurement.site_id)::bigint as linked_row_count
    from public.constraint_layer_registry as layer
    left join public.site_constraint_measurements as measurement on measurement.constraint_layer_id = layer.id
    group by layer.source_family
), evidence_counts as (
    select source_family, count(*)::bigint as evidence_row_count
    from landintel.evidence_references
    where source_family in (select source_family from expected_sources)
    group by source_family
), signal_counts as (
    select source_family, count(*)::bigint as signal_row_count
    from landintel.site_signals
    where source_family in (select source_family from expected_sources)
      and current_flag = true
    group by source_family
), change_counts as (
    select source_family, count(*)::bigint as change_event_count
    from landintel.site_change_events
    where source_family in (select source_family from expected_sources)
    group by source_family
), review_counts as (
    select source_family, count(*)::bigint as review_output_row_count
    from landintel.site_source_links
    where source_family in ('ela', 'vdl')
    group by source_family
    union all
    select layer.source_family, count(summary.id)::bigint as review_output_row_count
    from public.constraint_layer_registry as layer
    left join public.site_constraint_group_summaries as summary on summary.constraint_layer_id = layer.id
    group by layer.source_family
), latest_events as (
    select distinct on (event.source_family)
        event.source_family,
        event.status as latest_event_status,
        event.summary as latest_event_summary,
        event.created_at as latest_event_at
    from landintel.source_expansion_events as event
    order by event.source_family, event.created_at desc
)
select
    expected.source_family,
    expected.command_name,
    expected.target_table,
    expected.source_role,
    coalesce(raw_counts.raw_row_count, 0) as raw_or_feature_rows,
    coalesce(linked_counts.linked_row_count, 0) as linked_or_measured_rows,
    coalesce(evidence_counts.evidence_row_count, 0) as evidence_rows,
    coalesce(signal_counts.signal_row_count, 0) as signal_rows,
    coalesce(change_counts.change_event_count, 0) as change_event_rows,
    coalesce(review_counts.review_output_row_count, 0) as review_output_rows,
    latest_events.latest_event_status,
    latest_events.latest_event_summary,
    latest_events.latest_event_at,
    case
        when expected.source_family in ('ldp', 'settlement') then 'explicitly_deferred_until_authority_adapter_validated'
        when coalesce(raw_counts.raw_row_count, 0) > 0
         and coalesce(linked_counts.linked_row_count, 0) > 0
         and coalesce(evidence_counts.evidence_row_count, 0) > 0
         and coalesce(signal_counts.signal_row_count, 0) > 0
         and coalesce(change_counts.change_event_count, 0) > 0
         and coalesce(review_counts.review_output_row_count, 0) > 0 then 'live_wired_proven'
        when coalesce(raw_counts.raw_row_count, 0) > 0 then 'populated_not_fully_proven'
        when latest_events.latest_event_status is not null then latest_events.latest_event_status
        else 'not_yet_populated'
    end as live_proof_status
from expected_sources as expected
left join raw_counts on raw_counts.source_family = expected.source_family
left join linked_counts on linked_counts.source_family = expected.source_family
left join evidence_counts on evidence_counts.source_family = expected.source_family
left join signal_counts on signal_counts.source_family = expected.source_family
left join change_counts on change_counts.source_family = expected.source_family
left join review_counts on review_counts.source_family = expected.source_family
left join latest_events on latest_events.source_family = expected.source_family
order by expected.source_role, expected.source_family;

alter table if exists landintel.ela_site_records enable row level security;
alter table if exists landintel.vdl_site_records enable row level security;
alter table if exists landintel.source_expansion_events enable row level security;
alter table if exists landintel.site_signals enable row level security;
alter table if exists landintel.site_change_events enable row level security;

revoke all on table landintel.ela_site_records from anon, authenticated;
revoke all on table landintel.vdl_site_records from anon, authenticated;
revoke all on table landintel.source_expansion_events from anon, authenticated;
revoke all on table landintel.site_signals from anon, authenticated;
revoke all on table landintel.site_change_events from anon, authenticated;
revoke all on table analytics.v_phase_one_source_expansion_readiness from anon, authenticated;

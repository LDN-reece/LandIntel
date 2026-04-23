create or replace function public.bgs_normalise_name(value text)
returns text
language sql
immutable
set search_path = pg_catalog
as $$
    select nullif(
        trim(
            regexp_replace(
                upper(
                    regexp_replace(coalesce(value, ''), '[^A-Za-z0-9]+', ' ', 'g')
                ),
                '\s+',
                ' ',
                'g'
            )
        ),
        ''
    );
$$;

create or replace function public.bgs_name_tokens(value text)
returns text[]
language sql
immutable
set search_path = pg_catalog
as $$
    select coalesce(
        (
            select array_agg(token order by token)
            from (
                select distinct token
                from unnest(regexp_split_to_array(coalesce(public.bgs_normalise_name(value), ''), '\s+')) as token
                where token <> ''
            ) as deduped
        ),
        '{}'::text[]
    );
$$;

create or replace function public.refresh_bgs_boreholes(p_ingest_run_id uuid)
returns table (
    ingest_run_id uuid,
    source_snapshot_date date,
    raw_rows bigint,
    normalised_rows bigint,
    valid_geometry_rows bigint,
    scotland_rows bigint,
    ags_link_rows bigint,
    confidential_rows bigint,
    known_depth_rows bigint,
    invalid_or_quarantined_rows bigint,
    deleted_from_master_rows bigint
)
language plpgsql
set search_path = public, extensions, pg_catalog
as $$
declare
    v_deleted_from_master_rows bigint := 0;
begin
    if p_ingest_run_id is null then
        raise exception 'p_ingest_run_id is required';
    end if;

    if not exists (
        select 1
        from public.bgs_boreholes_raw
        where ingest_run_id = p_ingest_run_id
    ) then
        raise exception 'No raw BGS borehole rows were found for ingest run %', p_ingest_run_id;
    end if;

    if not exists (
        select 1
        from public.bgs_boreholes_raw
        where ingest_run_id = p_ingest_run_id
          and bgs_id is not null
          and geom is not null
    ) then
        raise exception 'The BGS ingest run % did not contain any valid master rows.', p_ingest_run_id;
    end if;

    with current_run_ids as (
        select distinct raw.bgs_id
        from public.bgs_boreholes_raw as raw
        where raw.ingest_run_id = p_ingest_run_id
          and raw.bgs_id is not null
          and raw.geom is not null
    ),
    deleted as (
        delete from public.bgs_boreholes as master
        where master.source_dataset_key = 'bgs_single_onshore_borehole_index'
          and not exists (
              select 1
              from current_run_ids
              where current_run_ids.bgs_id = master.bgs_id
          )
        returning 1
    )
    select count(*) into v_deleted_from_master_rows
    from deleted;

    with shaped as (
        select distinct on (raw.bgs_id)
            raw.bgs_id,
            nullif(btrim(raw.regno), '') as regno,
            nullif(btrim(raw.qs), '') as qs,
            case
                when raw.numb is null then null
                else raw.numb::integer
            end as numb,
            nullif(btrim(raw.bsuff), '') as bsuff,
            nullif(btrim(raw.rt), '') as rt,
            nullif(btrim(raw.grid_refer), '') as grid_reference,
            raw.easting as source_easting_raw,
            raw.northing as source_northing_raw,
            coalesce(
                raw.x,
                nullif(regexp_replace(coalesce(raw.easting, ''), '[^0-9\.-]', '', 'g'), '')::numeric
            ) as easting,
            coalesce(
                raw.y,
                nullif(regexp_replace(coalesce(raw.northing, ''), '[^0-9\.-]', '', 'g'), '')::numeric
            ) as northing,
            raw.geom,
            case
                when raw.geom is null then null::geometry(point, 4326)
                else ST_Transform(raw.geom, 4326)::geometry(point, 4326)
            end as geom_wgs84,
            nullif(btrim(raw.confidenti), '') as confidentiality_code,
            upper(coalesce(raw.confidenti, '')) = 'Y' as is_confidential,
            case
                when raw.strtheight is null then null
                else raw.strtheight::text
            end as start_height_raw,
            raw.strtheight as start_height_m,
            nullif(btrim(raw.name), '') as name_original,
            public.bgs_normalise_name(raw.name) as name_normalised,
            public.bgs_name_tokens(raw.name) as name_tokens,
            case
                when raw.length is null then null
                else raw.length::text
            end as depth_raw,
            case
                when raw.length is not null and raw.length > 0 then raw.length
                else null
            end as depth_m,
            case
                when raw.length is null then 'missing'
                when raw.length = 0 then 'zero'
                when raw.length > 0 then 'positive'
                else 'invalid'
            end as depth_status,
            nullif(btrim(raw.ags_log_ur), '') as ags_log_url,
            nullif(btrim(raw.ags_log_ur), '') is not null as has_ags_log,
            case
                when raw.date_known is null then null
                else raw.date_known::text
            end as date_known_raw,
            case
                when raw.date_known between 1000 and 2100 then raw.date_known
                else null
            end as date_known_year,
            nullif(btrim(raw.date_k_typ), '') as date_known_type,
            raw.date_enter as date_entered,
            scotland.authority_name as scotland_authority_name,
            scotland.authority_name is not null as is_scotland,
            raw.source_archive_name,
            raw.source_file_name,
            raw.source_snapshot_date,
            raw.ingest_run_id as source_ingest_run_id,
            raw.source_row_number
        from public.bgs_boreholes_raw as raw
        left join lateral (
            select authority.authority_name
            from public.authority_aoi as authority
            where authority.active = true
              and raw.geom is not null
              and ST_Intersects(authority.geometry, raw.geom)
            order by authority.authority_name
            limit 1
        ) as scotland on true
        where raw.ingest_run_id = p_ingest_run_id
          and raw.bgs_id is not null
          and raw.geom is not null
        order by raw.bgs_id, raw.source_row_number desc
    )
    insert into public.bgs_boreholes (
        bgs_id,
        regno,
        qs,
        numb,
        bsuff,
        rt,
        grid_reference,
        source_easting_raw,
        source_northing_raw,
        easting,
        northing,
        geom,
        geom_wgs84,
        confidentiality_code,
        is_confidential,
        start_height_raw,
        start_height_m,
        name_original,
        name_normalised,
        name_tokens,
        depth_raw,
        depth_m,
        depth_status,
        ags_log_url,
        has_ags_log,
        date_known_raw,
        date_known_year,
        date_known_type,
        date_entered,
        is_scotland,
        scotland_authority_name,
        source_dataset_key,
        source_dataset_name,
        source_archive_name,
        source_file_name,
        source_snapshot_date,
        source_ingest_run_id,
        source_row_number
    )
    select
        shaped.bgs_id,
        shaped.regno,
        shaped.qs,
        shaped.numb,
        shaped.bsuff,
        shaped.rt,
        shaped.grid_reference,
        shaped.source_easting_raw,
        shaped.source_northing_raw,
        shaped.easting,
        shaped.northing,
        shaped.geom,
        shaped.geom_wgs84,
        shaped.confidentiality_code,
        shaped.is_confidential,
        shaped.start_height_raw,
        shaped.start_height_m,
        shaped.name_original,
        shaped.name_normalised,
        shaped.name_tokens,
        shaped.depth_raw,
        shaped.depth_m,
        shaped.depth_status,
        shaped.ags_log_url,
        shaped.has_ags_log,
        shaped.date_known_raw,
        shaped.date_known_year,
        shaped.date_known_type,
        shaped.date_entered,
        shaped.is_scotland,
        shaped.scotland_authority_name,
        'bgs_single_onshore_borehole_index',
        'BGS Single Onshore Borehole Index',
        shaped.source_archive_name,
        shaped.source_file_name,
        shaped.source_snapshot_date,
        shaped.source_ingest_run_id,
        shaped.source_row_number
    from shaped
    on conflict (bgs_id)
    do update set
        regno = excluded.regno,
        qs = excluded.qs,
        numb = excluded.numb,
        bsuff = excluded.bsuff,
        rt = excluded.rt,
        grid_reference = excluded.grid_reference,
        source_easting_raw = excluded.source_easting_raw,
        source_northing_raw = excluded.source_northing_raw,
        easting = excluded.easting,
        northing = excluded.northing,
        geom = excluded.geom,
        geom_wgs84 = excluded.geom_wgs84,
        confidentiality_code = excluded.confidentiality_code,
        is_confidential = excluded.is_confidential,
        start_height_raw = excluded.start_height_raw,
        start_height_m = excluded.start_height_m,
        name_original = excluded.name_original,
        name_normalised = excluded.name_normalised,
        name_tokens = excluded.name_tokens,
        depth_raw = excluded.depth_raw,
        depth_m = excluded.depth_m,
        depth_status = excluded.depth_status,
        ags_log_url = excluded.ags_log_url,
        has_ags_log = excluded.has_ags_log,
        date_known_raw = excluded.date_known_raw,
        date_known_year = excluded.date_known_year,
        date_known_type = excluded.date_known_type,
        date_entered = excluded.date_entered,
        is_scotland = excluded.is_scotland,
        scotland_authority_name = excluded.scotland_authority_name,
        source_archive_name = excluded.source_archive_name,
        source_file_name = excluded.source_file_name,
        source_snapshot_date = excluded.source_snapshot_date,
        source_ingest_run_id = excluded.source_ingest_run_id,
        source_row_number = excluded.source_row_number,
        updated_at = now();

    return query
    with raw_summary as (
        select
            raw.ingest_run_id,
            max(raw.source_snapshot_date) as source_snapshot_date,
            count(*)::bigint as raw_rows
        from public.bgs_boreholes_raw as raw
        where raw.ingest_run_id = p_ingest_run_id
        group by raw.ingest_run_id
    ),
    master_summary as (
        select
            master.source_ingest_run_id as ingest_run_id,
            max(master.source_snapshot_date) as source_snapshot_date,
            count(*)::bigint as normalised_rows,
            count(*) filter (where master.geom is not null)::bigint as valid_geometry_rows,
            count(*) filter (where master.is_scotland)::bigint as scotland_rows,
            count(*) filter (where master.has_ags_log)::bigint as ags_link_rows,
            count(*) filter (where master.is_confidential)::bigint as confidential_rows,
            count(*) filter (where master.depth_m is not null)::bigint as known_depth_rows
        from public.bgs_boreholes as master
        where master.source_ingest_run_id = p_ingest_run_id
        group by master.source_ingest_run_id
    )
    select
        raw_summary.ingest_run_id,
        coalesce(master_summary.source_snapshot_date, raw_summary.source_snapshot_date),
        raw_summary.raw_rows,
        coalesce(master_summary.normalised_rows, 0),
        coalesce(master_summary.valid_geometry_rows, 0),
        coalesce(master_summary.scotland_rows, 0),
        coalesce(master_summary.ags_link_rows, 0),
        coalesce(master_summary.confidential_rows, 0),
        coalesce(master_summary.known_depth_rows, 0),
        greatest(raw_summary.raw_rows - coalesce(master_summary.normalised_rows, 0), 0),
        v_deleted_from_master_rows
    from raw_summary
    left join master_summary
        on master_summary.ingest_run_id = raw_summary.ingest_run_id;
end;
$$;

create or replace function public.refresh_bgs_site_constraints(
    p_source_ingest_run_id uuid default null,
    p_site_ids uuid[] default null
)
returns table (
    source_ingest_run_id uuid,
    affected_site_count bigint,
    borehole_rows_refreshed bigint,
    site_investigation_rows_refreshed bigint,
    queued_site_refreshes bigint
)
language plpgsql
set search_path = public, extensions, pg_catalog
as $$
declare
    v_source_ingest_run_id uuid;
    v_borehole_rows bigint := 0;
    v_site_investigation_rows bigint := 0;
    v_queue_rows bigint := 0;
begin
    v_source_ingest_run_id := coalesce(
        p_source_ingest_run_id,
        (
            select master.source_ingest_run_id
            from public.bgs_boreholes as master
            order by master.source_snapshot_date desc, master.updated_at desc
            limit 1
        )
    );

    if v_source_ingest_run_id is null then
        return;
    end if;

    create temp table tmp_bgs_target_sites on commit drop as
    select
        site.id as site_id,
        location.geometry as site_geometry,
        location.centroid as site_centroid
    from public.sites as site
    join public.site_locations as location
        on location.site_id = site.id
    where coalesce(location.geometry, location.centroid) is not null
      and (p_site_ids is null or site.id = any (p_site_ids));

    create temp table tmp_bgs_previous_sites on commit drop as
    select distinct constraint_row.site_id
    from public.site_constraints as constraint_row
    join tmp_bgs_target_sites as target
        on target.site_id = constraint_row.site_id
    where constraint_row.source_dataset = 'bgs_boreholes'
      and lower(constraint_row.constraint_type) in ('borehole', 'site_investigation');

    create temp table tmp_bgs_site_metrics on commit drop as
    select
        target.site_id,
        coalesce(metrics.count_site, 0)::bigint as count_site,
        coalesce(metrics.count_100m, 0)::bigint as count_100m,
        coalesce(metrics.count_250m, 0)::bigint as count_250m,
        coalesce(metrics.count_500m, 0)::bigint as count_500m,
        coalesce(metrics.count_confidential_site, 0)::bigint as count_confidential_site,
        coalesce(metrics.count_confidential_500m, 0)::bigint as count_confidential_500m,
        coalesce(metrics.count_ags_site, 0)::bigint as count_ags_site,
        coalesce(metrics.count_ags_500m, 0)::bigint as count_ags_500m,
        coalesce(metrics.count_positive_depth_site, 0)::bigint as count_positive_depth_site,
        coalesce(metrics.count_positive_depth_500m, 0)::bigint as count_positive_depth_500m,
        metrics.nearest_distance_m
    from tmp_bgs_target_sites as target
    left join lateral (
        select
            count(*) filter (
                where target.site_geometry is not null
                  and ST_Intersects(target.site_geometry, master.geom)
            ) as count_site,
            count(*) filter (
                where ST_DWithin(coalesce(target.site_geometry, target.site_centroid), master.geom, 100)
            ) as count_100m,
            count(*) filter (
                where ST_DWithin(coalesce(target.site_geometry, target.site_centroid), master.geom, 250)
            ) as count_250m,
            count(*) filter (
                where ST_DWithin(coalesce(target.site_geometry, target.site_centroid), master.geom, 500)
            ) as count_500m,
            count(*) filter (
                where target.site_geometry is not null
                  and ST_Intersects(target.site_geometry, master.geom)
                  and master.is_confidential
            ) as count_confidential_site,
            count(*) filter (
                where master.is_confidential
            ) as count_confidential_500m,
            count(*) filter (
                where target.site_geometry is not null
                  and ST_Intersects(target.site_geometry, master.geom)
                  and master.has_ags_log
            ) as count_ags_site,
            count(*) filter (
                where master.has_ags_log
            ) as count_ags_500m,
            count(*) filter (
                where target.site_geometry is not null
                  and ST_Intersects(target.site_geometry, master.geom)
                  and master.depth_m is not null
            ) as count_positive_depth_site,
            count(*) filter (
                where master.depth_m is not null
            ) as count_positive_depth_500m,
            min(
                ST_Distance(
                    coalesce(target.site_geometry, target.site_centroid),
                    master.geom
                )
            ) as nearest_distance_m
        from public.bgs_boreholes as master
        where master.source_ingest_run_id = v_source_ingest_run_id
          and master.geom is not null
          and master.geom && ST_Expand(coalesce(target.site_geometry, target.site_centroid), 500)
          and ST_DWithin(coalesce(target.site_geometry, target.site_centroid), master.geom, 500)
    ) as metrics on true;

    delete from public.site_constraints as constraint_row
    using tmp_bgs_target_sites as target
    where constraint_row.site_id = target.site_id
      and constraint_row.source_dataset = 'bgs_boreholes'
      and lower(constraint_row.constraint_type) in ('borehole', 'site_investigation');

    with inserted as (
        insert into public.site_constraints (
            site_id,
            constraint_type,
            severity,
            status,
            distance_m,
            description,
            source_dataset,
            source_record_id,
            source_url,
            import_version,
            raw_payload
        )
        select
            metrics.site_id,
            'borehole',
            case
                when metrics.count_site > 0 or metrics.count_100m >= 5 then 'medium'
                else 'low'
            end,
            'present',
            metrics.nearest_distance_m,
            case
                when metrics.count_site > 0 then
                    format(
                        '%s BGS boreholes sit within 500m of the site, including %s on the site footprint.',
                        metrics.count_500m,
                        metrics.count_site
                    )
                else
                    format('%s BGS boreholes sit within 500m of the site.', metrics.count_500m)
            end,
            'bgs_boreholes',
            concat(metrics.site_id::text, ':borehole'),
            null,
            v_source_ingest_run_id::text,
            jsonb_build_object(
                'count_site', metrics.count_site,
                'count_100m', metrics.count_100m,
                'count_250m', metrics.count_250m,
                'count_500m', metrics.count_500m,
                'count_confidential_site', metrics.count_confidential_site,
                'count_confidential_500m', metrics.count_confidential_500m,
                'count_ags_site', metrics.count_ags_site,
                'count_ags_500m', metrics.count_ags_500m,
                'count_positive_depth_site', metrics.count_positive_depth_site,
                'count_positive_depth_500m', metrics.count_positive_depth_500m,
                'nearest_distance_m', metrics.nearest_distance_m,
                'source_ingest_run_id', v_source_ingest_run_id::text
            )
        from tmp_bgs_site_metrics as metrics
        where metrics.count_500m > 0
        returning 1
    )
    select count(*) into v_borehole_rows
    from inserted;

    with inserted as (
        insert into public.site_constraints (
            site_id,
            constraint_type,
            severity,
            status,
            distance_m,
            description,
            source_dataset,
            source_record_id,
            source_url,
            import_version,
            raw_payload
        )
        select
            metrics.site_id,
            'site_investigation',
            'medium',
            'present',
            0,
            format('%s BGS boreholes overlap the current site footprint.', metrics.count_site),
            'bgs_boreholes',
            concat(metrics.site_id::text, ':site_investigation'),
            null,
            v_source_ingest_run_id::text,
            jsonb_build_object(
                'count_site', metrics.count_site,
                'count_100m', metrics.count_100m,
                'count_250m', metrics.count_250m,
                'count_500m', metrics.count_500m,
                'count_confidential_site', metrics.count_confidential_site,
                'count_ags_site', metrics.count_ags_site,
                'count_positive_depth_site', metrics.count_positive_depth_site,
                'source_ingest_run_id', v_source_ingest_run_id::text
            )
        from tmp_bgs_site_metrics as metrics
        where metrics.count_site > 0
        returning 1
    )
    select count(*) into v_site_investigation_rows
    from inserted;

    create temp table tmp_bgs_queue_sites on commit drop as
    select distinct site_id
    from (
        select site_id
        from tmp_bgs_previous_sites
        union
        select metrics.site_id
        from tmp_bgs_site_metrics as metrics
        where metrics.count_500m > 0 or metrics.count_site > 0
    ) as queue_candidates;

    with inserted as (
        insert into public.site_refresh_queue (
            site_id,
            trigger_source,
            source_table,
            source_record_id,
            refresh_scope,
            status,
            metadata
        )
        select
            queue_sites.site_id,
            'bgs_boreholes',
            'public.bgs_boreholes',
            v_source_ingest_run_id::text,
            'signals_and_interpretations',
            'pending',
            jsonb_build_object(
                'reason', 'bgs_borehole_refresh',
                'source_ingest_run_id', v_source_ingest_run_id::text
            )
        from tmp_bgs_queue_sites as queue_sites
        where not exists (
            select 1
            from public.site_refresh_queue as refresh_queue
            where refresh_queue.site_id = queue_sites.site_id
              and refresh_queue.refresh_scope = 'signals_and_interpretations'
              and refresh_queue.status in ('pending', 'processing')
        )
        returning 1
    )
    select count(*) into v_queue_rows
    from inserted;

    return query
    select
        v_source_ingest_run_id,
        (select count(*)::bigint from tmp_bgs_queue_sites),
        v_borehole_rows,
        v_site_investigation_rows,
        v_queue_rows;
end;
$$;

drop trigger if exists bgs_boreholes_touch_updated_at on public.bgs_boreholes;
create trigger bgs_boreholes_touch_updated_at
before update on public.bgs_boreholes
for each row execute function public.touch_updated_at();

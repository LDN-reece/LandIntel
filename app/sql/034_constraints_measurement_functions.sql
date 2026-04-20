create or replace function public.normalize_site_title_number(raw_title text)
returns text
language sql
immutable
set search_path = pg_catalog
as $$
    select nullif(upper(regexp_replace(coalesce(raw_title, ''), '[^A-Za-z0-9]+', '', 'g')), '');
$$;

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
set search_path = pg_catalog, public
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
                    then st_area(st_intersection(site_geometry, feature_geometry))
                else 0::double precision
            end as overlap_area_sqm,
            st_distance(site_geometry, feature_geometry) as nearest_distance_m,
            nullif(st_area(site_geometry), 0) as site_area_sqm,
            nullif(st_area(feature_geometry), 0) as feature_area_sqm
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
language plpgsql
stable
set search_path = pg_catalog, public
as $$
begin
    if to_regclass('public.sites') is null or to_regclass('public.site_locations') is null then
        return;
    end if;

    return query execute $sql$
        with ranked_locations as (
            select
                site_locations.id::text as site_location_id,
                site_locations.site_id::text as site_id,
                site_locations.geometry,
                to_jsonb(site_locations) as location_row,
                row_number() over (
                    partition by site_locations.site_id
                    order by
                        case
                            when lower(coalesce(to_jsonb(site_locations) ->> 'is_primary', 'false')) in ('true', 't', '1', 'yes')
                                then 0
                            else 1
                        end,
                        case lower(coalesce(to_jsonb(site_locations) ->> 'location_role', ''))
                            when 'primary' then 0
                            when 'site' then 1
                            when 'boundary' then 2
                            else 3
                        end,
                        site_locations.id::text
                ) as row_number
            from public.site_locations
            where site_locations.geometry is not null
        )
        select
            sites.id::text as site_id,
            ranked_locations.site_location_id,
            coalesce(
                nullif(btrim(to_jsonb(sites) ->> 'site_name'), ''),
                nullif(btrim(to_jsonb(sites) ->> 'name'), ''),
                nullif(btrim(to_jsonb(sites) ->> 'site_code'), ''),
                sites.id::text
            ) as site_name,
            coalesce(
                nullif(btrim(to_jsonb(sites) ->> 'authority_name'), ''),
                nullif(btrim(to_jsonb(sites) ->> 'planning_authority'), ''),
                nullif(btrim(to_jsonb(sites) ->> 'authority_code'), ''),
                nullif(btrim(ranked_locations.location_row ->> 'authority_name'), '')
            ) as authority_name,
            ranked_locations.geometry,
            round(st_area(ranked_locations.geometry)::numeric, 2) as area_sqm,
            public.calculate_area_acres(st_area(ranked_locations.geometry)::numeric) as area_acres,
            coalesce(
                nullif(btrim(ranked_locations.location_row ->> 'location_label'), ''),
                nullif(btrim(ranked_locations.location_row ->> 'location_name'), ''),
                'Live site geometry'
            ) as location_label,
            coalesce(
                nullif(btrim(ranked_locations.location_row ->> 'location_role'), ''),
                'site'
            ) as location_role
        from public.sites as sites
        join ranked_locations
          on ranked_locations.site_id = sites.id::text
         and ranked_locations.row_number = 1
    $sql$;
end;
$$;

drop trigger if exists trg_touch_updated_at_site_spatial_links on public.site_spatial_links;
create trigger trg_touch_updated_at_site_spatial_links
before update on public.site_spatial_links
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_title_validation on public.site_title_validation;
create trigger trg_touch_updated_at_site_title_validation
before update on public.site_title_validation
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_constraint_layer_registry on public.constraint_layer_registry;
create trigger trg_touch_updated_at_constraint_layer_registry
before update on public.constraint_layer_registry
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_constraint_source_features on public.constraint_source_features;
create trigger trg_touch_updated_at_constraint_source_features
before update on public.constraint_source_features
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_constraint_measurements on public.site_constraint_measurements;
create trigger trg_touch_updated_at_site_constraint_measurements
before update on public.site_constraint_measurements
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_constraint_group_summaries on public.site_constraint_group_summaries;
create trigger trg_touch_updated_at_site_constraint_group_summaries
before update on public.site_constraint_group_summaries
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_commercial_friction_facts on public.site_commercial_friction_facts;
create trigger trg_touch_updated_at_site_commercial_friction_facts
before update on public.site_commercial_friction_facts
for each row
execute function public.touch_updated_at();

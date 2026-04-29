create table if not exists public.site_ros_parcel_link_candidates (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    ros_parcel_id uuid not null references public.ros_cadastral_parcels(id) on delete cascade,
    ros_inspire_id text,
    authority_name text,
    link_source text not null default 'ros_cadastral_site_parcel_link',
    link_status text not null check (
        link_status in (
            'primary',
            'candidate',
            'manual_review',
            'rejected'
        )
    ),
    match_method text not null check (
        match_method in (
            'exact_overlap_candidate',
            'centroid_within_site',
            'nearest_centroid',
            'existing_primary_ros_parcel'
        )
    ),
    confidence numeric,
    candidate_rank integer not null,
    overlap_area_sqm numeric,
    overlap_pct_of_site numeric,
    overlap_pct_of_parcel numeric,
    nearest_distance_m numeric,
    centroid_inside_site boolean,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (confidence is null or (confidence >= 0 and confidence <= 1))
);

create unique index if not exists site_ros_parcel_link_candidates_site_parcel_uidx
    on public.site_ros_parcel_link_candidates (site_location_id, ros_parcel_id, link_source);

create index if not exists site_ros_parcel_link_candidates_site_idx
    on public.site_ros_parcel_link_candidates (site_id);

create index if not exists site_ros_parcel_link_candidates_parcel_idx
    on public.site_ros_parcel_link_candidates (ros_parcel_id);

create index if not exists site_ros_parcel_link_candidates_status_idx
    on public.site_ros_parcel_link_candidates (link_status, match_method, confidence desc);

alter table public.site_ros_parcel_link_candidates enable row level security;

revoke all on table public.site_ros_parcel_link_candidates from anon;
revoke all on table public.site_ros_parcel_link_candidates from authenticated;
grant select on table public.site_ros_parcel_link_candidates to authenticated;

drop policy if exists site_ros_parcel_link_candidates_authenticated_select on public.site_ros_parcel_link_candidates;
create policy site_ros_parcel_link_candidates_authenticated_select
on public.site_ros_parcel_link_candidates
for select
to authenticated
using (true);

create or replace function public.refresh_site_ros_parcel_link_candidates_for_sites(
    max_candidates_per_site integer default 10,
    max_distance_m numeric default 250,
    min_overlap_sqm numeric default 1,
    site_location_ids jsonb default '[]'::jsonb
)
returns table (
    candidate_rows bigint,
    candidate_site_count bigint,
    primary_link_rows bigint,
    exact_overlap_candidate_rows bigint,
    nearest_candidate_rows bigint
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_max_candidates_per_site integer := greatest(coalesce(max_candidates_per_site, 10), 1);
    v_candidate_pool_size integer := greatest(v_max_candidates_per_site * 5, 25);
    v_max_distance_m numeric := greatest(coalesce(max_distance_m, 250), 0);
    v_min_overlap_sqm numeric := greatest(coalesce(min_overlap_sqm, 1), 0);
    v_site_location_ids jsonb := case
        when jsonb_typeof(coalesce(site_location_ids, '[]'::jsonb)) = 'array'
            then coalesce(site_location_ids, '[]'::jsonb)
        else '[]'::jsonb
    end;
begin
    perform set_config('statement_timeout', '10min', true);

    with requested_sites as (
        select distinct value as site_location_id
        from jsonb_array_elements_text(v_site_location_ids)
    ), anchor_page as (
        select
            anchor.*,
            nullif(st_area(anchor.geometry), 0) as site_area_sqm,
            st_pointonsurface(anchor.geometry) as anchor_point
        from public.constraints_site_anchor() as anchor
        join requested_sites as requested
          on requested.site_location_id = anchor.site_location_id
        where anchor.geometry is not null
          and anchor.authority_name is not null
    ), candidate_pool as (
        select
            anchor.site_id,
            anchor.site_location_id,
            anchor.site_name,
            anchor.authority_name as site_authority_name,
            anchor.geometry as site_geometry,
            anchor.site_area_sqm,
            parcel.id as ros_parcel_id,
            parcel.ros_inspire_id,
            parcel.authority_name as parcel_authority_name,
            parcel.geometry as parcel_geometry,
            parcel.centroid as parcel_centroid,
            row_number() over (
                partition by anchor.site_location_id
                order by parcel.centroid OPERATOR(extensions.<->) anchor.anchor_point,
                         parcel.id
            ) as pool_rank
        from anchor_page as anchor
        join lateral (
            select parcel.*
            from public.ros_cadastral_parcels as parcel
            where parcel.geometry is not null
              and parcel.centroid is not null
              and parcel.authority_name = anchor.authority_name
              and parcel.centroid OPERATOR(extensions.&&) st_expand(anchor.geometry, v_max_distance_m::double precision)
            order by parcel.centroid OPERATOR(extensions.<->) anchor.anchor_point,
                     parcel.id
            limit v_candidate_pool_size
        ) as parcel on true
    ), measured_candidates as (
        select
            pool.site_id,
            pool.site_location_id,
            pool.site_name,
            pool.site_authority_name,
            pool.ros_parcel_id,
            pool.ros_inspire_id,
            pool.parcel_authority_name,
            public.extract_ros_cadastral_identifier(parcel.raw_attributes, parcel.ros_inspire_id) as cadastral_unit_identifier,
            metrics.overlap_area_sqm,
            metrics.overlap_pct_of_site,
            metrics.overlap_pct_of_parcel,
            metrics.nearest_distance_m,
            metrics.centroid_inside_site,
            row_number() over (
                partition by pool.site_location_id
                order by
                    (metrics.overlap_area_sqm >= v_min_overlap_sqm) desc,
                    metrics.centroid_inside_site desc,
                    metrics.overlap_pct_of_site desc nulls last,
                    metrics.nearest_distance_m asc nulls last,
                    pool.ros_parcel_id
            ) as candidate_rank
        from candidate_pool as pool
        join public.ros_cadastral_parcels as parcel
          on parcel.id = pool.ros_parcel_id
        cross join lateral (
            select
                case
                    when st_isvalid(pool.site_geometry) then pool.site_geometry
                    else st_makevalid(pool.site_geometry)
                end as site_geometry,
                case
                    when st_isvalid(pool.parcel_geometry) then pool.parcel_geometry
                    else st_makevalid(pool.parcel_geometry)
                end as parcel_geometry
        ) as cleaned
        cross join lateral (
            select
                st_intersects(cleaned.site_geometry, cleaned.parcel_geometry) as intersects_site,
                st_covers(cleaned.site_geometry, pool.parcel_centroid) as centroid_inside_site,
                st_distance(cleaned.site_geometry, cleaned.parcel_geometry) as nearest_distance_m,
                nullif(st_area(cleaned.parcel_geometry), 0) as parcel_area_sqm
        ) as raw_metrics
        cross join lateral (
            select
                case
                    when raw_metrics.intersects_site
                        then st_area(st_intersection(cleaned.site_geometry, cleaned.parcel_geometry))
                    else 0::double precision
                end as overlap_area_sqm,
                raw_metrics.parcel_area_sqm,
                raw_metrics.nearest_distance_m,
                raw_metrics.centroid_inside_site
        ) as measured
        cross join lateral (
            select
                round(measured.overlap_area_sqm::numeric, 2) as overlap_area_sqm,
                round(coalesce((measured.overlap_area_sqm / pool.site_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_site,
                round(coalesce((measured.overlap_area_sqm / measured.parcel_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_parcel,
                round(measured.nearest_distance_m::numeric, 2) as nearest_distance_m,
                measured.centroid_inside_site
        ) as metrics
        where metrics.overlap_area_sqm >= v_min_overlap_sqm
           or metrics.centroid_inside_site
           or metrics.nearest_distance_m <= v_max_distance_m
    ), bounded_candidates as (
        select *
        from measured_candidates
        where candidate_rank <= v_max_candidates_per_site
    ), prepared_candidates as (
        select
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            parcel_authority_name as authority_name,
            'ros_cadastral_site_parcel_link'::text as link_source,
            case
                when candidate_rank = 1
                 and (
                    overlap_area_sqm >= v_min_overlap_sqm
                    or centroid_inside_site
                    or nearest_distance_m <= 25
                 )
                    then 'primary'
                when overlap_area_sqm >= v_min_overlap_sqm
                  or centroid_inside_site
                  or nearest_distance_m <= v_max_distance_m
                    then 'candidate'
                else 'manual_review'
            end as link_status,
            case
                when overlap_area_sqm >= v_min_overlap_sqm then 'exact_overlap_candidate'
                when centroid_inside_site then 'centroid_within_site'
                else 'nearest_centroid'
            end as match_method,
            case
                when overlap_pct_of_site >= 80 then 0.95
                when overlap_pct_of_site >= 50 then 0.88
                when overlap_pct_of_site >= 25 then 0.78
                when overlap_area_sqm >= v_min_overlap_sqm then 0.68
                when centroid_inside_site then 0.64
                when nearest_distance_m <= 25 then 0.6
                when nearest_distance_m <= 100 then 0.45
                else 0.35
            end::numeric as confidence,
            candidate_rank,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            centroid_inside_site,
            jsonb_build_object(
                'bridge', 'canonical_site_to_ros_cadastral_parcel',
                'site_name', site_name,
                'site_authority_name', site_authority_name,
                'parcel_authority_name', parcel_authority_name,
                'cadastral_unit_identifier', cadastral_unit_identifier,
                'candidate_rank', candidate_rank,
                'candidate_pool_size', v_candidate_pool_size,
                'max_distance_m', v_max_distance_m,
                'min_overlap_sqm', v_min_overlap_sqm,
                'note', 'Batched RoS parcel linker narrows candidates with the centroid GiST index, measures exact overlap only inside the small candidate pool, and promotes high-confidence primary parcel links for site intelligence.'
            ) as metadata
        from bounded_candidates
    ), inserted_candidates as (
        insert into public.site_ros_parcel_link_candidates (
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            authority_name,
            link_source,
            link_status,
            match_method,
            confidence,
            candidate_rank,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            centroid_inside_site,
            metadata
        )
        select
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            authority_name,
            link_source,
            link_status,
            match_method,
            confidence,
            candidate_rank,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            centroid_inside_site,
            metadata
        from prepared_candidates
        on conflict (site_location_id, ros_parcel_id, link_source)
        do update set
            ros_inspire_id = excluded.ros_inspire_id,
            authority_name = excluded.authority_name,
            link_status = excluded.link_status,
            match_method = excluded.match_method,
            confidence = excluded.confidence,
            candidate_rank = excluded.candidate_rank,
            overlap_area_sqm = excluded.overlap_area_sqm,
            overlap_pct_of_site = excluded.overlap_pct_of_site,
            overlap_pct_of_parcel = excluded.overlap_pct_of_parcel,
            nearest_distance_m = excluded.nearest_distance_m,
            centroid_inside_site = excluded.centroid_inside_site,
            metadata = excluded.metadata,
            updated_at = now()
        returning *
    ), top_primary as (
        select distinct on (candidate.site_id)
            candidate.site_id,
            candidate.ros_parcel_id
        from inserted_candidates as candidate
        where candidate.candidate_rank = 1
          and candidate.link_status = 'primary'
          and candidate.confidence >= 0.6
        order by candidate.site_id,
                 candidate.confidence desc nulls last,
                 candidate.overlap_area_sqm desc nulls last,
                 candidate.nearest_distance_m asc nulls last,
                 candidate.ros_parcel_id
    ), updated_sites as (
        update landintel.canonical_sites as site
        set primary_ros_parcel_id = top_primary.ros_parcel_id,
            updated_at = now()
        from top_primary
        where site.id::text = top_primary.site_id
          and site.primary_ros_parcel_id is distinct from top_primary.ros_parcel_id
        returning site.id
    )
    select
        count(*)::bigint,
        count(distinct site_id)::bigint,
        count(*) filter (where link_status = 'primary')::bigint,
        count(*) filter (where match_method = 'exact_overlap_candidate')::bigint,
        count(*) filter (where match_method = 'nearest_centroid')::bigint
    into
        candidate_rows,
        candidate_site_count,
        primary_link_rows,
        exact_overlap_candidate_rows,
        nearest_candidate_rows
    from inserted_candidates;

    return next;
end;
$$;

revoke all on function public.refresh_site_ros_parcel_link_candidates_for_sites(integer, numeric, numeric, jsonb) from anon, authenticated;

comment on table public.site_ros_parcel_link_candidates is
    'Batched candidate links between LandIntel canonical sites and Registers of Scotland cadastral parcels. Stores scored candidate parcels and supports promotion of the primary RoS parcel for site intelligence and title resolution.';

comment on function public.refresh_site_ros_parcel_link_candidates_for_sites(integer, numeric, numeric, jsonb) is
    'Refreshes RoS parcel link candidates for a supplied batch of site_location_ids, using indexed centroid search plus exact overlap measurement within a bounded candidate pool.';

drop trigger if exists trg_touch_updated_at_site_ros_parcel_link_candidates on public.site_ros_parcel_link_candidates;
create trigger trg_touch_updated_at_site_ros_parcel_link_candidates
before update on public.site_ros_parcel_link_candidates
for each row execute function public.touch_updated_at();

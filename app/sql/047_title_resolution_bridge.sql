create table if not exists public.site_title_resolution_candidates (
    id uuid primary key default gen_random_uuid(),
    site_id text not null,
    site_location_id text not null,
    ros_parcel_id uuid references public.ros_cadastral_parcels(id) on delete cascade,
    ros_inspire_id text,
    cadastral_unit_identifier text,
    candidate_title_number text,
    normalized_title_number text,
    candidate_source text not null default 'ros_cadastral_spatial_intersection',
    resolution_status text not null check (
        resolution_status in (
            'probable_title',
            'validated_title',
            'needs_licensed_bridge',
            'manual_review',
            'rejected'
        )
    ),
    match_method text not null check (
        match_method in (
            'toid_geometry_to_ros_cadastral',
            'site_geometry_to_ros_cadastral',
            'licensed_ros_bridge',
            'manual'
        )
    ),
    confidence numeric,
    overlap_area_sqm numeric,
    overlap_pct_of_site numeric,
    overlap_pct_of_parcel numeric,
    nearest_distance_m numeric,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (confidence is null or (confidence >= 0 and confidence <= 1))
);

create unique index if not exists site_title_resolution_candidates_site_parcel_uidx
    on public.site_title_resolution_candidates (site_location_id, ros_parcel_id, candidate_source)
    where ros_parcel_id is not null;

create index if not exists site_title_resolution_candidates_site_idx
    on public.site_title_resolution_candidates (site_id);

create index if not exists site_title_resolution_candidates_title_idx
    on public.site_title_resolution_candidates (normalized_title_number)
    where normalized_title_number is not null;

create index if not exists site_title_resolution_candidates_status_idx
    on public.site_title_resolution_candidates (resolution_status, candidate_source);

alter table public.site_title_resolution_candidates enable row level security;

revoke all on table public.site_title_resolution_candidates from anon;
revoke all on table public.site_title_resolution_candidates from authenticated;
grant select on table public.site_title_resolution_candidates to authenticated;

drop policy if exists site_title_resolution_candidates_authenticated_select on public.site_title_resolution_candidates;
create policy site_title_resolution_candidates_authenticated_select
on public.site_title_resolution_candidates
for select
to authenticated
using (true);

create or replace function public.extract_ros_title_number_candidate(raw_attributes jsonb, ros_inspire_id text)
returns text
language sql
immutable
set search_path = pg_catalog, public
as $$
    with candidate_values(raw_value, priority_rank) as (
        values
            (raw_attributes ->> 'titleNumber', 1),
            (raw_attributes ->> 'title_number', 2),
            (raw_attributes ->> 'titlenumber', 3),
            (raw_attributes ->> 'title_no', 4),
            (raw_attributes ->> 'title', 5),
            (raw_attributes ->> 'cadastralUnit', 6),
            (raw_attributes ->> 'cadastral_unit', 7),
            (raw_attributes ->> 'label', 8),
            (raw_attributes ->> 'nationalca', 9),
            (raw_attributes ->> 'nationalCadastralReference', 10),
            (regexp_replace(coalesce(ros_inspire_id, ''), '^.*\.', ''), 11)
    ), normalized as (
        select
            priority_rank,
            public.normalize_site_title_number(raw_value) as title_number
        from candidate_values
    )
    select title_number
    from normalized
    where title_number ~ '^[A-Z]{3}[0-9]{1,8}$'
    order by priority_rank
    limit 1;
$$;

create or replace function public.extract_ros_cadastral_identifier(raw_attributes jsonb, ros_inspire_id text)
returns text
language sql
immutable
set search_path = pg_catalog, public
as $$
    select coalesce(
        nullif(btrim(raw_attributes ->> 'label'), ''),
        nullif(btrim(raw_attributes ->> 'nationalca'), ''),
        nullif(btrim(raw_attributes ->> 'nationalCadastralReference'), ''),
        nullif(btrim(raw_attributes ->> 'cadastralUnit'), ''),
        nullif(btrim(regexp_replace(coalesce(ros_inspire_id, ''), '^.*\.', '')), '')
    );
$$;

alter table public.ros_cadastral_parcels
    add column if not exists title_number text;

alter table public.ros_cadastral_parcels
    add column if not exists normalized_title_number text;

create index if not exists ros_cadastral_parcels_title_number_idx
    on public.ros_cadastral_parcels (title_number)
    where title_number is not null;

create index if not exists ros_cadastral_parcels_normalized_title_idx
    on public.ros_cadastral_parcels (normalized_title_number)
    where normalized_title_number is not null;

create or replace function public.refresh_site_title_resolution_bridge(
    max_candidates_per_site integer default 10,
    min_overlap_sqm numeric default 1
)
returns table (
    candidate_rows bigint,
    candidate_site_count bigint,
    promoted_title_rows bigint,
    probable_title_rows bigint,
    licensed_bridge_required_rows bigint,
    ros_parcel_count bigint,
    canonical_site_count bigint
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_max_candidates_per_site integer := greatest(coalesce(max_candidates_per_site, 10), 1);
    v_min_overlap_sqm numeric := greatest(coalesce(min_overlap_sqm, 1), 0);
begin
    perform set_config('statement_timeout', '15min', true);

    delete from public.site_title_resolution_candidates
    where candidate_source = 'ros_cadastral_spatial_intersection';

    with ranked_candidates as (
        select
            anchor.site_id,
            anchor.site_location_id,
            anchor.site_name,
            anchor.authority_name as site_authority_name,
            parcel.id as ros_parcel_id,
            parcel.ros_inspire_id,
            parcel.authority_name as parcel_authority_name,
            public.extract_ros_cadastral_identifier(parcel.raw_attributes, parcel.ros_inspire_id) as cadastral_unit_identifier,
            coalesce(
                parcel.title_number,
                public.extract_ros_title_number_candidate(parcel.raw_attributes, parcel.ros_inspire_id)
            ) as candidate_title_number,
            coalesce(
                parcel.normalized_title_number,
                public.normalize_site_title_number(
                    coalesce(
                        parcel.title_number,
                        public.extract_ros_title_number_candidate(parcel.raw_attributes, parcel.ros_inspire_id)
                    )
                )
            ) as candidate_normalized_title_number,
            metrics.overlap_area_sqm,
            metrics.overlap_pct_of_site,
            metrics.overlap_pct_of_parcel,
            metrics.nearest_distance_m,
            row_number() over (
                partition by anchor.site_location_id
                order by metrics.overlap_pct_of_site desc nulls last,
                         metrics.overlap_area_sqm desc nulls last,
                         parcel.id
            ) as candidate_rank
        from public.constraints_site_anchor() as anchor
        join public.ros_cadastral_parcels as parcel
          on anchor.geometry is not null
         and parcel.geometry is not null
         and parcel.authority_name = anchor.authority_name
         and parcel.geometry OPERATOR(extensions.&&) anchor.geometry
         and st_intersects(parcel.geometry, anchor.geometry)
        cross join lateral (
            with cleaned as (
                select
                    case
                        when st_isvalid(anchor.geometry) then anchor.geometry
                        else st_makevalid(anchor.geometry)
                    end as site_geometry,
                    case
                        when st_isvalid(parcel.geometry) then parcel.geometry
                        else st_makevalid(parcel.geometry)
                    end as parcel_geometry
            ), measured as (
                select
                    st_area(st_intersection(site_geometry, parcel_geometry)) as overlap_area_sqm,
                    nullif(st_area(site_geometry), 0) as site_area_sqm,
                    nullif(st_area(parcel_geometry), 0) as parcel_area_sqm,
                    st_distance(site_geometry, parcel_geometry) as nearest_distance_m
                from cleaned
            )
            select
                round(overlap_area_sqm::numeric, 2) as overlap_area_sqm,
                round(coalesce((overlap_area_sqm / site_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_site,
                round(coalesce((overlap_area_sqm / parcel_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_parcel,
                round(nearest_distance_m::numeric, 2) as nearest_distance_m
            from measured
        ) as metrics
        where metrics.overlap_area_sqm >= v_min_overlap_sqm
    ), bounded_candidates as (
        select *
        from ranked_candidates
        where candidate_rank <= v_max_candidates_per_site
    ), prepared_candidates as (
        select
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            cadastral_unit_identifier,
            candidate_title_number,
            coalesce(candidate_normalized_title_number, public.normalize_site_title_number(candidate_title_number)) as normalized_title_number,
            'ros_cadastral_spatial_intersection'::text as candidate_source,
            case
                when candidate_title_number is not null then 'probable_title'
                when cadastral_unit_identifier is not null then 'needs_licensed_bridge'
                else 'manual_review'
            end as resolution_status,
            'site_geometry_to_ros_cadastral'::text as match_method,
            case
                when candidate_title_number is not null and overlap_pct_of_site >= 80 then 0.9
                when candidate_title_number is not null and overlap_pct_of_site >= 25 then 0.75
                when candidate_title_number is not null then 0.6
                when overlap_pct_of_site >= 80 then 0.55
                when overlap_pct_of_site >= 25 then 0.45
                else 0.35
            end::numeric as confidence,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            jsonb_build_object(
                'bridge', 'site_or_toid_geometry_to_ros_cadastral',
                'site_name', site_name,
                'site_authority_name', site_authority_name,
                'parcel_authority_name', parcel_authority_name,
                'candidate_rank', candidate_rank,
                'ros_title_candidate_present', candidate_title_number is not null,
                'requires_licensed_title_bridge', candidate_title_number is null,
                'note', 'RoS Land Register API is title-number-first; this bridge creates spatial title candidates before API validation.'
            ) as metadata
        from bounded_candidates
    ), inserted_candidates as (
        insert into public.site_title_resolution_candidates (
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            cadastral_unit_identifier,
            candidate_title_number,
            normalized_title_number,
            candidate_source,
            resolution_status,
            match_method,
            confidence,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            metadata
        )
        select
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            cadastral_unit_identifier,
            candidate_title_number,
            normalized_title_number,
            candidate_source,
            resolution_status,
            match_method,
            confidence,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            metadata
        from prepared_candidates
        on conflict (site_location_id, ros_parcel_id, candidate_source) where ros_parcel_id is not null
        do update set
            ros_inspire_id = excluded.ros_inspire_id,
            cadastral_unit_identifier = excluded.cadastral_unit_identifier,
            candidate_title_number = excluded.candidate_title_number,
            normalized_title_number = excluded.normalized_title_number,
            resolution_status = excluded.resolution_status,
            match_method = excluded.match_method,
            confidence = excluded.confidence,
            overlap_area_sqm = excluded.overlap_area_sqm,
            overlap_pct_of_site = excluded.overlap_pct_of_site,
            overlap_pct_of_parcel = excluded.overlap_pct_of_parcel,
            nearest_distance_m = excluded.nearest_distance_m,
            metadata = excluded.metadata,
            updated_at = now()
        returning *
    ), promoted_titles as (
        insert into public.site_title_validation (
            site_id,
            site_location_id,
            title_number,
            normalized_title_number,
            matched_title_number,
            validation_status,
            validation_method,
            confidence,
            title_source,
            metadata
        )
        select
            candidate.site_id,
            candidate.site_location_id,
            candidate.candidate_title_number,
            candidate.normalized_title_number,
            candidate.candidate_title_number,
            case
                when coalesce(candidate.confidence, 0) >= 0.75 then 'probable'
                else 'manual_review'
            end as validation_status,
            'spatial_intersection'::text as validation_method,
            candidate.confidence,
            'ros_cadastral_spatial_intersection'::text as title_source,
            candidate.metadata || jsonb_build_object(
                'ros_parcel_id', candidate.ros_parcel_id,
                'ros_inspire_id', candidate.ros_inspire_id,
                'cadastral_unit_identifier', candidate.cadastral_unit_identifier,
                'title_bridge_candidate_id', candidate.id
            ) as metadata
        from inserted_candidates as candidate
        where candidate.candidate_title_number is not null
          and candidate.normalized_title_number is not null
        on conflict (site_location_id, normalized_title_number, validation_method)
        do update set
            title_number = excluded.title_number,
            matched_title_number = excluded.matched_title_number,
            validation_status = excluded.validation_status,
            confidence = excluded.confidence,
            title_source = excluded.title_source,
            metadata = excluded.metadata,
            updated_at = now()
        returning *
    ), top_parcels as (
        select distinct on (candidate.site_id)
            candidate.site_id,
            candidate.ros_parcel_id
        from inserted_candidates as candidate
        where candidate.ros_parcel_id is not null
        order by candidate.site_id,
                 candidate.confidence desc nulls last,
                 candidate.overlap_area_sqm desc nulls last,
                 candidate.ros_parcel_id
    ), updated_sites as (
        update landintel.canonical_sites as site
        set primary_ros_parcel_id = top_parcels.ros_parcel_id,
            updated_at = now()
        from top_parcels
        where site.id::text = top_parcels.site_id
          and site.primary_ros_parcel_id is distinct from top_parcels.ros_parcel_id
        returning site.id
    )
    select
        count(*)::bigint,
        count(distinct site_id)::bigint,
        (select count(*)::bigint from promoted_titles),
        count(*) filter (where resolution_status = 'probable_title')::bigint,
        count(*) filter (where resolution_status = 'needs_licensed_bridge')::bigint,
        (select count(*)::bigint from public.ros_cadastral_parcels),
        (select count(*)::bigint from public.constraints_site_anchor())
    into
        candidate_rows,
        candidate_site_count,
        promoted_title_rows,
        probable_title_rows,
        licensed_bridge_required_rows,
        ros_parcel_count,
        canonical_site_count
    from inserted_candidates;

    return next;
end;
$$;

create or replace function public.refresh_site_title_resolution_bridge_for_sites(
    max_candidates_per_site integer default 10,
    min_overlap_sqm numeric default 1,
    site_location_ids jsonb default '[]'::jsonb
)
returns table (
    candidate_rows bigint,
    candidate_site_count bigint,
    promoted_title_rows bigint,
    probable_title_rows bigint,
    licensed_bridge_required_rows bigint,
    ros_parcel_count bigint,
    canonical_site_count bigint
)
language plpgsql
set search_path = pg_catalog, public, landintel, extensions
as $$
declare
    v_max_candidates_per_site integer := greatest(coalesce(max_candidates_per_site, 10), 1);
    v_min_overlap_sqm numeric := greatest(coalesce(min_overlap_sqm, 1), 0);
    v_site_location_ids jsonb := case
        when jsonb_typeof(coalesce(site_location_ids, '[]'::jsonb)) = 'array'
            then coalesce(site_location_ids, '[]'::jsonb)
        else '[]'::jsonb
    end;
begin
    perform set_config('statement_timeout', '15min', true);

    with requested_sites as (
        select distinct value as site_location_id
        from jsonb_array_elements_text(v_site_location_ids)
    ), anchor_page as (
        select anchor.*
        from public.constraints_site_anchor() as anchor
        join requested_sites as requested
          on requested.site_location_id = anchor.site_location_id
        where anchor.geometry is not null
          and anchor.authority_name is not null
    ), anchor_prepared as (
        select
            anchor.*,
            nullif(st_area(anchor.geometry), 0) as site_area_sqm
        from anchor_page as anchor
    ), ranked_candidates as (
        select
            anchor.site_id,
            anchor.site_location_id,
            anchor.site_name,
            anchor.authority_name as site_authority_name,
            parcel.id as ros_parcel_id,
            parcel.ros_inspire_id,
            parcel.authority_name as parcel_authority_name,
            public.extract_ros_cadastral_identifier(parcel.raw_attributes, parcel.ros_inspire_id) as cadastral_unit_identifier,
            coalesce(
                parcel.title_number,
                public.extract_ros_title_number_candidate(parcel.raw_attributes, parcel.ros_inspire_id)
            ) as candidate_title_number,
            coalesce(
                parcel.normalized_title_number,
                public.normalize_site_title_number(
                    coalesce(
                        parcel.title_number,
                        public.extract_ros_title_number_candidate(parcel.raw_attributes, parcel.ros_inspire_id)
                    )
                )
            ) as candidate_normalized_title_number,
            metrics.overlap_area_sqm,
            metrics.overlap_pct_of_site,
            metrics.overlap_pct_of_parcel,
            metrics.nearest_distance_m,
            row_number() over (
                partition by anchor.site_location_id
                order by metrics.overlap_pct_of_site desc nulls last,
                         metrics.nearest_distance_m asc nulls last,
                         parcel.id
            ) as candidate_rank
        from anchor_prepared as anchor
        join lateral (
            select parcel.*
            from public.ros_cadastral_parcels as parcel
            where parcel.geometry is not null
              and parcel.centroid is not null
              and parcel.authority_name = anchor.authority_name
              and parcel.geometry OPERATOR(extensions.&&) anchor.geometry
              and parcel.centroid OPERATOR(extensions.&&) anchor.geometry
              and st_covers(anchor.geometry, parcel.centroid)
            order by parcel.id
            limit greatest(v_max_candidates_per_site * 25, 250)
        ) as parcel on true
        cross join lateral (
            select
                nullif(st_area(parcel.geometry), 0) as parcel_area_sqm,
                st_distance(st_pointonsurface(anchor.geometry), parcel.centroid) as nearest_distance_m
        ) as prepared_metrics
        cross join lateral (
            select
                round(least(coalesce(prepared_metrics.parcel_area_sqm, 0), coalesce(anchor.site_area_sqm, 0))::numeric, 2) as overlap_area_sqm,
                round(coalesce((least(coalesce(prepared_metrics.parcel_area_sqm, 0), coalesce(anchor.site_area_sqm, 0)) / anchor.site_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_site,
                round(coalesce((least(coalesce(prepared_metrics.parcel_area_sqm, 0), coalesce(anchor.site_area_sqm, 0)) / prepared_metrics.parcel_area_sqm) * 100, 0)::numeric, 4) as overlap_pct_of_parcel,
                round(prepared_metrics.nearest_distance_m::numeric, 2) as nearest_distance_m
        ) as metrics
        where metrics.overlap_area_sqm >= v_min_overlap_sqm
    ), bounded_candidates as (
        select *
        from ranked_candidates
        where candidate_rank <= v_max_candidates_per_site
    ), prepared_candidates as (
        select
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            cadastral_unit_identifier,
            candidate_title_number,
            coalesce(candidate_normalized_title_number, public.normalize_site_title_number(candidate_title_number)) as normalized_title_number,
            'ros_cadastral_spatial_intersection'::text as candidate_source,
            case
                when candidate_title_number is not null then 'probable_title'
                when cadastral_unit_identifier is not null then 'needs_licensed_bridge'
                else 'manual_review'
            end as resolution_status,
            'site_geometry_to_ros_cadastral_fast_candidate'::text as match_method,
            case
                when candidate_title_number is not null and overlap_pct_of_site >= 80 then 0.9
                when candidate_title_number is not null and overlap_pct_of_site >= 25 then 0.75
                when candidate_title_number is not null then 0.6
                when overlap_pct_of_site >= 80 then 0.55
                when overlap_pct_of_site >= 25 then 0.45
                else 0.35
            end::numeric as confidence,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            jsonb_build_object(
                'bridge', 'site_or_toid_geometry_to_ros_cadastral',
                'site_name', site_name,
                'site_authority_name', site_authority_name,
                'parcel_authority_name', parcel_authority_name,
                'candidate_rank', candidate_rank,
                'ros_title_candidate_present', candidate_title_number is not null,
                'requires_licensed_title_bridge', candidate_title_number is null,
                'measurement_mode', 'fast_centroid_candidate',
                'note', 'RoS Land Register API is title-number-first; this fast bridge creates title candidates before slower exact overlap enrichment.'
            ) as metadata
        from bounded_candidates
    ), inserted_candidates as (
        insert into public.site_title_resolution_candidates (
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            cadastral_unit_identifier,
            candidate_title_number,
            normalized_title_number,
            candidate_source,
            resolution_status,
            match_method,
            confidence,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            metadata
        )
        select
            site_id,
            site_location_id,
            ros_parcel_id,
            ros_inspire_id,
            cadastral_unit_identifier,
            candidate_title_number,
            normalized_title_number,
            candidate_source,
            resolution_status,
            match_method,
            confidence,
            overlap_area_sqm,
            overlap_pct_of_site,
            overlap_pct_of_parcel,
            nearest_distance_m,
            metadata
        from prepared_candidates
        on conflict (site_location_id, ros_parcel_id, candidate_source) where ros_parcel_id is not null
        do update set
            ros_inspire_id = excluded.ros_inspire_id,
            cadastral_unit_identifier = excluded.cadastral_unit_identifier,
            candidate_title_number = excluded.candidate_title_number,
            normalized_title_number = excluded.normalized_title_number,
            resolution_status = excluded.resolution_status,
            match_method = excluded.match_method,
            confidence = excluded.confidence,
            overlap_area_sqm = excluded.overlap_area_sqm,
            overlap_pct_of_site = excluded.overlap_pct_of_site,
            overlap_pct_of_parcel = excluded.overlap_pct_of_parcel,
            nearest_distance_m = excluded.nearest_distance_m,
            metadata = excluded.metadata,
            updated_at = now()
        returning *
    ), promoted_titles as (
        insert into public.site_title_validation (
            site_id,
            site_location_id,
            title_number,
            normalized_title_number,
            matched_title_number,
            validation_status,
            validation_method,
            confidence,
            title_source,
            metadata
        )
        select
            candidate.site_id,
            candidate.site_location_id,
            candidate.candidate_title_number,
            candidate.normalized_title_number,
            candidate.candidate_title_number,
            case
                when coalesce(candidate.confidence, 0) >= 0.75 then 'probable'
                else 'manual_review'
            end as validation_status,
            'spatial_intersection'::text as validation_method,
            candidate.confidence,
            'ros_cadastral_spatial_intersection'::text as title_source,
            candidate.metadata || jsonb_build_object(
                'ros_parcel_id', candidate.ros_parcel_id,
                'ros_inspire_id', candidate.ros_inspire_id,
                'cadastral_unit_identifier', candidate.cadastral_unit_identifier,
                'title_bridge_candidate_id', candidate.id
            ) as metadata
        from inserted_candidates as candidate
        where candidate.candidate_title_number is not null
          and candidate.normalized_title_number is not null
        on conflict (site_location_id, normalized_title_number, validation_method)
        do update set
            title_number = excluded.title_number,
            matched_title_number = excluded.matched_title_number,
            validation_status = excluded.validation_status,
            confidence = excluded.confidence,
            title_source = excluded.title_source,
            metadata = excluded.metadata,
            updated_at = now()
        returning *
    ), top_parcels as (
        select distinct on (candidate.site_id)
            candidate.site_id,
            candidate.ros_parcel_id
        from inserted_candidates as candidate
        where candidate.ros_parcel_id is not null
        order by candidate.site_id,
                 candidate.confidence desc nulls last,
                 candidate.overlap_area_sqm desc nulls last,
                 candidate.ros_parcel_id
    ), updated_sites as (
        update landintel.canonical_sites as site
        set primary_ros_parcel_id = top_parcels.ros_parcel_id,
            updated_at = now()
        from top_parcels
        where site.id::text = top_parcels.site_id
          and site.primary_ros_parcel_id is distinct from top_parcels.ros_parcel_id
        returning site.id
    )
    select
        count(*)::bigint,
        count(distinct site_id)::bigint,
        (select count(*)::bigint from promoted_titles),
        count(*) filter (where resolution_status = 'probable_title')::bigint,
        count(*) filter (where resolution_status = 'needs_licensed_bridge')::bigint,
        (select count(*)::bigint from public.ros_cadastral_parcels),
        (select count(*)::bigint from public.constraints_site_anchor())
    into
        candidate_rows,
        candidate_site_count,
        promoted_title_rows,
        probable_title_rows,
        licensed_bridge_required_rows,
        ros_parcel_count,
        canonical_site_count
    from inserted_candidates;

    return next;
end;
$$;

revoke all on function public.extract_ros_title_number_candidate(jsonb, text) from anon, authenticated;
revoke all on function public.extract_ros_cadastral_identifier(jsonb, text) from anon, authenticated;
revoke all on function public.refresh_site_title_resolution_bridge(integer, numeric) from anon, authenticated;
revoke all on function public.refresh_site_title_resolution_bridge_for_sites(integer, numeric, jsonb) from anon, authenticated;

comment on table public.site_title_resolution_candidates is
    'Bridge table for resolving control evidence from TOID/site geometry through RoS cadastral parcels into title-number candidates. It stores candidates first and only promotes valid title-number-shaped values into public.site_title_validation.';

comment on column public.site_title_resolution_candidates.cadastral_unit_identifier is
    'RoS cadastral map identifier observed on the parcel. This is useful bridge evidence but is not assumed to be a ScotLIS API title number unless it matches the title-number pattern.';

comment on column public.ros_cadastral_parcels.title_number is
    'Title-number-shaped value bulk-derived from the RoS INSPIRE identifier or parcel attributes. This is the open-data title spine for Scotland MVP control evidence.';

comment on column public.ros_cadastral_parcels.normalized_title_number is
    'Normalized form of the RoS cadastral title number for indexed matching and promotion into site_title_validation.';

comment on function public.refresh_site_title_resolution_bridge(integer, numeric) is
    'Refreshes site-to-RoS-cadastral candidates and promotes only valid title-number-shaped candidates into public.site_title_validation. The ScotLIS API remains title-number-first.';

comment on function public.refresh_site_title_resolution_bridge_for_sites(integer, numeric, jsonb) is
    'Fast batch-safe site-to-RoS-cadastral title candidate refresh for selected site_location_ids. Uses centroid-contained RoS parcels so sourcing is not blocked by exact overlap enrichment.';

drop trigger if exists trg_touch_updated_at_site_title_resolution_candidates on public.site_title_resolution_candidates;
create trigger trg_touch_updated_at_site_title_resolution_candidates
before update on public.site_title_resolution_candidates
for each row
execute function public.touch_updated_at();

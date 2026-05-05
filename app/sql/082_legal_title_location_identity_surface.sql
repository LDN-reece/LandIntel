create schema if not exists landintel_sourced;

comment on schema landintel_sourced is
    'LandIntel Sourced Sites, the polished commercial opportunity register for LDN review.';

create or replace view landintel_sourced.v_site_legal_title_location_identity
with (security_invoker = true) as
with latest_ldn_screen as (
    select distinct on (screen.canonical_site_id)
        screen.*
    from landintel.site_ldn_candidate_screen as screen
    order by screen.canonical_site_id, screen.updated_at desc nulls last, screen.created_at desc nulls last
), latest_pack as (
    select distinct on (pack.canonical_site_id)
        pack.*
    from landintel.site_urgent_address_title_pack as pack
    order by pack.canonical_site_id, pack.updated_at desc nulls last, pack.created_at desc nulls last
), first_address_candidate as (
    select distinct on (candidate.canonical_site_id)
        candidate.canonical_site_id,
        candidate.address_text,
        candidate.uprn,
        candidate.address_source,
        candidate.match_rank,
        candidate.distance_m
    from landintel.site_urgent_address_candidates as candidate
    where nullif(btrim(candidate.address_text), '') is not null
    order by
        candidate.canonical_site_id,
        candidate.match_rank nulls last,
        candidate.distance_m nulls last,
        candidate.updated_at desc nulls last
), title_rows as (
    select
        pack.canonical_site_id,
        coalesce(pack.normalized_title_number, pack.title_number) as raw_title_number,
        'urgent_address_title_pack'::text as source_label,
        1 as source_rank
    from latest_pack as pack
    where pack.title_candidate_status <> 'title_required'

    union all

    select
        validation.site_id::uuid as canonical_site_id,
        coalesce(validation.normalized_title_number, validation.title_number) as raw_title_number,
        coalesce(validation.title_source, 'site_title_validation') as source_label,
        2 as source_rank
    from public.site_title_validation as validation
    where validation.validation_status <> 'rejected'
      and validation.site_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    union all

    select
        candidate.site_id::uuid as canonical_site_id,
        coalesce(candidate.normalized_title_number, candidate.candidate_title_number) as raw_title_number,
        coalesce(candidate.candidate_source, 'site_title_resolution_candidates') as source_label,
        3 as source_rank
    from public.site_title_resolution_candidates as candidate
    where candidate.resolution_status <> 'rejected'
      and candidate.site_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    union all

    select
        parcel_link.site_id::uuid as canonical_site_id,
        coalesce(parcel.normalized_title_number, parcel.title_number) as raw_title_number,
        'ros_cadastral_parcel_attribute'::text as source_label,
        4 as source_rank
    from public.site_ros_parcel_link_candidates as parcel_link
    join public.ros_cadastral_parcels as parcel
      on parcel.id = parcel_link.ros_parcel_id
    where parcel_link.link_status <> 'rejected'
      and parcel_link.site_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    union all

    select
        site.id as canonical_site_id,
        coalesce(parcel.normalized_title_number, parcel.title_number) as raw_title_number,
        'primary_ros_cadastral_parcel_attribute'::text as source_label,
        5 as source_rank
    from landintel.canonical_sites as site
    join public.ros_cadastral_parcels as parcel
      on parcel.id = site.primary_ros_parcel_id
), clean_title_rows as (
    select
        title_rows.canonical_site_id,
        public.normalize_site_title_number(title_rows.raw_title_number) as legal_title_number,
        min(title_rows.source_rank) as source_rank,
        array_remove(array_agg(distinct title_rows.source_label order by title_rows.source_label), null) as source_labels
    from title_rows
    where public.is_scottish_title_number_candidate(title_rows.raw_title_number)
    group by
        title_rows.canonical_site_id,
        public.normalize_site_title_number(title_rows.raw_title_number)
), title_rollup as (
    select
        clean_title_rows.canonical_site_id,
        array_agg(clean_title_rows.legal_title_number order by clean_title_rows.source_rank, clean_title_rows.legal_title_number) as legal_title_numbers,
        count(*)::integer as legal_title_number_count,
        array_remove(array_agg(distinct source_label order by source_label), null) as title_number_sources
    from clean_title_rows
    left join lateral unnest(clean_title_rows.source_labels) as source_label on true
    group by clean_title_rows.canonical_site_id
), base_sites as (
    select
        site.id as canonical_site_id,
        coalesce(nullif(pack.site_name, ''), nullif(ldn.site_name, ''), nullif(site.site_name_primary, '')) as site_label,
        coalesce(nullif(site.authority_name, ''), nullif(ldn.authority_name, ''), nullif(pack.authority_name, '')) as authority_name,
        coalesce(
            nullif(site.metadata ->> 'settlement_name', ''),
            nullif(site.metadata ->> 'settlement', ''),
            nullif(site.metadata ->> 'local_area', ''),
            nullif(ldn.metadata ->> 'settlement_name', ''),
            nullif(ldn.metadata ->> 'settlement', ''),
            nullif(ldn.metadata ->> 'local_area', ''),
            nullif(pack.metadata ->> 'settlement_name', ''),
            nullif(pack.metadata ->> 'settlement', ''),
            nullif(pack.metadata ->> 'local_area', '')
        ) as local_area_or_settlement_name,
        coalesce(
            nullif(pack.primary_address_text, ''),
            nullif(address_candidate.address_text, ''),
            nullif(site.metadata ->> 'primary_address_text', ''),
            nullif(site.metadata ->> 'address', ''),
            nullif(ldn.metadata ->> 'primary_address_text', ''),
            nullif(ldn.metadata ->> 'address', '')
        ) as address,
        case
            when coalesce(ldn.unregistered_opportunity_signal, false) then 'unregistered_opportunity_signal'
            when coalesce(ldn.register_origin_site, false) then 'register_context_source'
            when pack.canonical_site_id is not null then 'urgent_address_title_pack'
            when nullif(site.surfaced_reason, '') is not null then site.surfaced_reason
            else 'canonical_site_spine'
        end as source_route,
        coalesce(site.area_acres, ldn.area_acres, pack.area_acres) as area_acres,
        site.metadata as site_metadata,
        coalesce(ldn.metadata, '{}'::jsonb) as ldn_metadata,
        coalesce(pack.metadata, '{}'::jsonb) as pack_metadata
    from landintel.canonical_sites as site
    left join latest_ldn_screen as ldn
      on ldn.canonical_site_id = site.id
    left join latest_pack as pack
      on pack.canonical_site_id = site.id
    left join first_address_candidate as address_candidate
      on address_candidate.canonical_site_id = site.id
), filtered_sites as (
    select
        base_sites.*
    from base_sites
    where not (
        lower(coalesce(base_sites.site_metadata ->> 'external_focus_area', '')) in ('true', 'yes', '1')
        or lower(coalesce(base_sites.ldn_metadata ->> 'external_focus_area', '')) in ('true', 'yes', '1')
        or lower(coalesce(base_sites.pack_metadata ->> 'external_focus_area', '')) in ('true', 'yes', '1')
        or lower(coalesce(base_sites.site_metadata ->> 'source_route', '')) like '%%external_focus_area%%'
        or lower(coalesce(base_sites.ldn_metadata ->> 'source_route', '')) like '%%external_focus_area%%'
        or lower(coalesce(base_sites.pack_metadata ->> 'source_route', '')) like '%%external_focus_area%%'
        or lower(coalesce(base_sites.source_route, '')) like '%%external_focus_area%%'
    )
)
select
    filtered_sites.canonical_site_id,
    filtered_sites.site_label,
    coalesce(title_rollup.legal_title_numbers[1], 'LEGAL TITLE NUMBER NOT HELD') as legal_title_number,
    coalesce(title_rollup.legal_title_numbers, '{}'::text[]) as legal_title_numbers,
    case
        when coalesce(title_rollup.legal_title_number_count, 0) = 0 then 'legal_title_number_not_held'
        when title_rollup.legal_title_number_count = 1 then 'legal_title_number_found'
        else 'multiple_legal_title_numbers_found'
    end as legal_title_number_status,
    coalesce(filtered_sites.address, 'ADDRESS NOT HELD') as address,
    coalesce(filtered_sites.local_area_or_settlement_name, 'LOCAL AREA / SETTLEMENT NOT HELD') as local_area_or_settlement_name,
    coalesce(filtered_sites.authority_name, 'LOCAL AUTHORITY NOT HELD') as local_authority,
    case
        when filtered_sites.authority_name is null then 'LOCAL COUNCIL NOT HELD'
        when filtered_sites.authority_name ilike '%%council%%' then filtered_sites.authority_name
        else filtered_sites.authority_name || ' Council'
    end as local_council,
    filtered_sites.area_acres,
    filtered_sites.source_route,
    coalesce(title_rollup.title_number_sources, '{}'::text[]) as legal_title_number_sources,
    'Legal title number means a title-number-shaped value already held in existing RoS/title evidence tables. SCT parcel references and rejected records are excluded. This is not ownership confirmation.'::text as caveat
from filtered_sites
left join title_rollup
  on title_rollup.canonical_site_id = filtered_sites.canonical_site_id;

grant usage on schema landintel_sourced to authenticated;
grant select on landintel_sourced.v_site_legal_title_location_identity to authenticated;

comment on view landintel_sourced.v_site_legal_title_location_identity is
    'Clean operator surface for site identity: legal title number if held, address, settlement/local area, local authority and local council. It excludes SCT-like parcel references and does not use title workflow as ownership truth.';

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
    metadata,
    updated_at
)
values (
    'landintel_sourced',
    'v_site_legal_title_location_identity',
    'view',
    'reporting_surface',
    'landintel_sourced',
    'legal title and location identity operator surface',
    'title_number',
    true,
    true,
    true,
    true,
    false,
    true,
    false,
    null,
    'Operator identity view only. It must not be read as ownership confirmation and deliberately excludes SCT parcel references as title numbers.',
    'Use this as the first readable site identity surface before DD interpretation.',
    '{"not_ownership_truth":true,"sct_excluded":true,"external_focus_area_excluded":true}'::jsonb,
    now()
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
    metadata = excluded.metadata,
    updated_at = now();

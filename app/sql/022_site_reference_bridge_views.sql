drop view if exists analytics.v_site_reference_index;
drop view if exists analytics.v_canonical_sites;

create or replace view analytics.v_canonical_sites
with (security_invoker = true) as
with alias_rollup as (
    select
        site_id,
        array_remove(array_agg(distinct site_name_hint), null) as site_name_aliases,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'source_ref'), null) as source_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'planning_ref'), null) as planning_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'ldp_ref'), null) as ldp_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'hla_ref'), null) as hla_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'ela_ref'), null) as ela_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'vdl_ref'), null) as vdl_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'council_ref'), null) as council_refs,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'title_number'), null) as title_numbers,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'uprn'), null) as uprns,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'usrn'), null) as usrns,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'toid'), null) as toids,
        array_remove(array_agg(distinct raw_reference_value) filter (where reference_family = 'authority_ref'), null) as authority_refs,
        count(*) filter (where status = 'matched')::integer as matched_reference_count,
        count(*) filter (where status = 'probable')::integer as probable_reference_count,
        count(*) filter (where status = 'unresolved')::integer as unresolved_reference_count,
        string_agg(distinct match_notes, '; ') filter (where match_notes is not null) as match_notes
    from public.site_reference_aliases
    group by site_id
),
geometry_rollup as (
    select
        site_id,
        array_remove(array_agg(distinct version_label order by version_label), null) as geometry_versions
    from public.site_geometry_versions
    group by site_id
)
select
    site.id as canonical_site_id,
    site.id as site_id,
    site.site_code,
    site.site_name as site_name_primary,
    coalesce(alias_rollup.site_name_aliases, '{}'::text[]) as site_name_aliases,
    coalesce(alias_rollup.source_refs, array[site.site_code]) as source_refs,
    coalesce(alias_rollup.planning_refs, '{}'::text[]) as planning_refs,
    coalesce(alias_rollup.ldp_refs, '{}'::text[]) as ldp_refs,
    coalesce(alias_rollup.hla_refs, '{}'::text[]) as hla_refs,
    coalesce(alias_rollup.ela_refs, '{}'::text[]) as ela_refs,
    coalesce(alias_rollup.vdl_refs, '{}'::text[]) as vdl_refs,
    coalesce(alias_rollup.council_refs, '{}'::text[]) as council_refs,
    coalesce(alias_rollup.title_numbers, '{}'::text[]) as title_numbers,
    coalesce(alias_rollup.uprns, '{}'::text[]) as uprns,
    coalesce(alias_rollup.usrns, '{}'::text[]) as usrns,
    coalesce(alias_rollup.toids, '{}'::text[]) as toids,
    coalesce(alias_rollup.authority_refs, '{}'::text[]) as authority_refs,
    coalesce(geometry_rollup.geometry_versions, '{}'::text[]) as geometry_versions,
    case
        when coalesce(alias_rollup.unresolved_reference_count, 0) > 0 then 'low'
        when coalesce(alias_rollup.probable_reference_count, 0) > 0 then 'medium'
        else 'high'
    end as match_confidence,
    coalesce(alias_rollup.match_notes, '') as match_notes,
    coalesce(alias_rollup.matched_reference_count, 0) as matched_reference_count,
    coalesce(alias_rollup.unresolved_reference_count, 0) as unresolved_reference_count
from public.sites as site
left join alias_rollup
    on alias_rollup.site_id = site.id
left join geometry_rollup
    on geometry_rollup.site_id = site.id;

create or replace view analytics.v_site_reference_index
with (security_invoker = true) as
select
    canonical.canonical_site_id::text as site_id,
    canonical.site_code,
    canonical.site_name_primary as site_name,
    location.authority_name,
    location.nearest_settlement,
    array(
        select distinct value
        from unnest(
            canonical.source_refs
            || canonical.ldp_refs
            || canonical.hla_refs
            || canonical.ela_refs
            || canonical.vdl_refs
            || canonical.council_refs
            || canonical.authority_refs
        ) as value
    ) as reference_values,
    canonical.planning_refs,
    canonical.title_numbers,
    canonical.site_name_aliases
from analytics.v_canonical_sites as canonical
left join public.site_locations as location
    on location.site_id = canonical.site_id;

drop view if exists analytics.v_live_site_readiness;
drop view if exists analytics.v_live_site_summary;
drop view if exists analytics.v_live_site_sources;
drop view if exists analytics.v_live_source_coverage;
drop view if exists analytics.v_live_ingest_audit;

create or replace view analytics.v_live_ingest_audit
with (security_invoker = true) as
with run_mapping as (
    select *
    from (
        values
            ('ingest_planning_history'::text, 'planning'::text, 'landintel.planning_application_records'::text),
            ('ingest_hla'::text, 'hla'::text, 'landintel.hla_site_records'::text),
            ('reconcile_canonical_sites'::text, 'canonical'::text, 'landintel.canonical_sites + landintel.site_reference_aliases + landintel.site_source_links + landintel.evidence_references'::text),
            ('ingest_bgs'::text, 'bgs'::text, 'landintel.bgs_records'::text),
            ('ingest_ldp'::text, 'ldp'::text, 'landintel.ldp_site_records'::text),
            ('ingest_ldp_sites'::text, 'ldp'::text, 'landintel.ldp_site_records'::text),
            ('ingest_settlement_boundaries'::text, 'settlement'::text, 'landintel.settlement_boundary_records'::text),
            ('ingest_settlement_boundary'::text, 'settlement'::text, 'landintel.settlement_boundary_records'::text),
            ('ingest_flood'::text, 'flood'::text, 'landintel.flood_records'::text),
            ('ingest_flood_constraints'::text, 'flood'::text, 'landintel.flood_records'::text)
    ) as mapping(run_type, source_family, destination_table)
)
select
    ingest.id as ingest_run_id,
    ingest.run_type,
    ingest.source_name,
    mapping.source_family,
    mapping.destination_table,
    case
        when jsonb_typeof(ingest.metadata -> 'target_authorities') = 'array'
            then array(select jsonb_array_elements_text(ingest.metadata -> 'target_authorities'))
        else '{}'::text[]
    end as target_authorities,
    ingest.status,
    ingest.records_fetched as fetched_count,
    ingest.records_loaded as loaded_count,
    ingest.records_retained as retained_count,
    ingest.started_at,
    ingest.finished_at,
    ingest.error_message as latest_error_message
from public.ingest_runs as ingest
join run_mapping as mapping
    on mapping.run_type = ingest.run_type
order by ingest.started_at desc;

create or replace view analytics.v_live_source_coverage
with (security_invoker = true) as
with source_rows as (
    select
        coalesce(planning.authority_name, 'Unknown') as authority_name,
        'planning'::text as source_family,
        coalesce(source_registry.source_name, 'Planning Applications: Official - Scotland') as source_dataset,
        planning.source_record_id,
        coalesce(planning.canonical_site_id, source_link.canonical_site_id) as canonical_site_id,
        planning.ingest_run_id,
        planning.updated_at
    from landintel.planning_application_records as planning
    left join lateral (
        select link.canonical_site_id
        from landintel.site_source_links as link
        where link.source_family = 'planning'
          and link.source_record_id = planning.source_record_id
        order by link.updated_at desc nulls last, link.created_at desc
        limit 1
    ) as source_link
        on true
    left join public.source_registry as source_registry
        on source_registry.id = planning.source_registry_id

    union all

    select
        coalesce(hla.authority_name, 'Unknown') as authority_name,
        'hla'::text as source_family,
        coalesce(source_registry.source_name, 'Housing Land Supply - Scotland') as source_dataset,
        hla.source_record_id,
        coalesce(hla.canonical_site_id, source_link.canonical_site_id) as canonical_site_id,
        hla.ingest_run_id,
        hla.updated_at
    from landintel.hla_site_records as hla
    left join lateral (
        select link.canonical_site_id
        from landintel.site_source_links as link
        where link.source_family = 'hla'
          and link.source_record_id = hla.source_record_id
        order by link.updated_at desc nulls last, link.created_at desc
        limit 1
    ) as source_link
        on true
    left join public.source_registry as source_registry
        on source_registry.id = hla.source_registry_id

    union all

    select
        coalesce(bgs.authority_name, 'Unknown') as authority_name,
        'bgs'::text as source_family,
        coalesce(source_registry.source_name, initcap(replace(bgs.record_type, '_', ' ')), 'BGS OpenGeoscience API') as source_dataset,
        bgs.source_record_id,
        coalesce(bgs.canonical_site_id, source_link.canonical_site_id) as canonical_site_id,
        bgs.ingest_run_id,
        bgs.updated_at
    from landintel.bgs_records as bgs
    left join lateral (
        select link.canonical_site_id
        from landintel.site_source_links as link
        where link.source_family = 'bgs'
          and link.source_record_id = bgs.source_record_id
        order by link.updated_at desc nulls last, link.created_at desc
        limit 1
    ) as source_link
        on true
    left join public.source_registry as source_registry
        on source_registry.id = bgs.source_registry_id

    union all

    select
        coalesce(ldp.authority_name, 'Unknown') as authority_name,
        'ldp'::text as source_family,
        coalesce(source_registry.source_name, ldp.plan_name, 'Local Development Plan') as source_dataset,
        ldp.source_record_id,
        coalesce(ldp.canonical_site_id, source_link.canonical_site_id) as canonical_site_id,
        ldp.ingest_run_id,
        ldp.updated_at
    from landintel.ldp_site_records as ldp
    left join lateral (
        select link.canonical_site_id
        from landintel.site_source_links as link
        where link.source_family = 'ldp'
          and link.source_record_id = ldp.source_record_id
        order by link.updated_at desc nulls last, link.created_at desc
        limit 1
    ) as source_link
        on true
    left join public.source_registry as source_registry
        on source_registry.id = ldp.source_registry_id

    union all

    select
        coalesce(flood.authority_name, 'Unknown') as authority_name,
        'flood'::text as source_family,
        coalesce(source_registry.source_name, flood.flood_source, 'Flood Constraints') as source_dataset,
        flood.source_record_id,
        coalesce(flood.canonical_site_id, source_link.canonical_site_id) as canonical_site_id,
        flood.ingest_run_id,
        flood.updated_at
    from landintel.flood_records as flood
    left join lateral (
        select link.canonical_site_id
        from landintel.site_source_links as link
        where link.source_family = 'flood'
          and link.source_record_id = flood.source_record_id
        order by link.updated_at desc nulls last, link.created_at desc
        limit 1
    ) as source_link
        on true
    left join public.source_registry as source_registry
        on source_registry.id = flood.source_registry_id

    union all

    select
        coalesce(settlement.authority_name, 'Unknown') as authority_name,
        'settlement'::text as source_family,
        coalesce(source_registry.source_name, concat('Settlement Boundary: ', settlement.boundary_role), 'Settlement Boundaries') as source_dataset,
        settlement.source_record_id,
        source_link.canonical_site_id,
        settlement.ingest_run_id,
        settlement.updated_at
    from landintel.settlement_boundary_records as settlement
    left join lateral (
        select link.canonical_site_id
        from landintel.site_source_links as link
        where link.source_family = 'settlement'
          and link.source_record_id = settlement.source_record_id
        order by link.updated_at desc nulls last, link.created_at desc
        limit 1
    ) as source_link
        on true
    left join public.source_registry as source_registry
        on source_registry.id = settlement.source_registry_id
),
coverage_rollup as (
    select
        authority_name,
        source_family,
        source_dataset,
        count(*)::bigint as raw_record_count,
        count(*) filter (where canonical_site_id is not null)::bigint as linked_source_record_count,
        count(distinct canonical_site_id) filter (where canonical_site_id is not null)::bigint as linked_canonical_site_count,
        max(updated_at) as latest_source_update_at
    from source_rows
    group by authority_name, source_family, source_dataset
),
latest_runs as (
    select
        source_rows.authority_name,
        source_rows.source_family,
        source_rows.source_dataset,
        source_rows.ingest_run_id,
        row_number() over (
            partition by source_rows.authority_name, source_rows.source_family, source_rows.source_dataset
            order by source_rows.updated_at desc nulls last, ingest.started_at desc nulls last, source_rows.ingest_run_id desc nulls last
        ) as row_number
    from source_rows
    left join public.ingest_runs as ingest
        on ingest.id = source_rows.ingest_run_id
)
select
    coverage.authority_name,
    coverage.source_family,
    coverage.source_dataset,
    coverage.raw_record_count,
    coverage.linked_canonical_site_count,
    coverage.linked_source_record_count,
    greatest(coverage.raw_record_count - coverage.linked_source_record_count, 0) as unlinked_raw_record_count,
    latest.ingest_run_id as last_ingest_run_id,
    ingest.started_at as last_ingest_started_at,
    ingest.finished_at as last_ingest_finished_at,
    ingest.status as last_ingest_status,
    coverage.latest_source_update_at
from coverage_rollup as coverage
left join latest_runs as latest
    on latest.authority_name = coverage.authority_name
   and latest.source_family = coverage.source_family
   and latest.source_dataset = coverage.source_dataset
   and latest.row_number = 1
left join public.ingest_runs as ingest
    on ingest.id = latest.ingest_run_id
order by coverage.authority_name, coverage.source_family, coverage.source_dataset;

create or replace view analytics.v_live_site_sources
with (security_invoker = true) as
with link_rollup as (
    select
        link.canonical_site_id,
        link.source_family,
        link.source_dataset,
        count(distinct link.source_record_id)::bigint as linked_source_record_count,
        array_agg(distinct link.source_record_id order by link.source_record_id) as source_record_ids,
        avg(link.confidence) as average_link_confidence,
        max(link.updated_at) as latest_source_update_at
    from landintel.site_source_links as link
    group by link.canonical_site_id, link.source_family, link.source_dataset
),
alias_rollup as (
    select
        alias.canonical_site_id,
        alias.source_family,
        alias.source_dataset,
        array_agg(distinct alias.raw_reference_value order by alias.raw_reference_value) as alias_references,
        max(alias.updated_at) as latest_alias_update_at
    from landintel.site_reference_aliases as alias
    group by alias.canonical_site_id, alias.source_family, alias.source_dataset
),
dominant_link_method as (
    select
        ranked.canonical_site_id,
        ranked.source_family,
        ranked.source_dataset,
        ranked.link_method
    from (
        select
            link.canonical_site_id,
            link.source_family,
            link.source_dataset,
            link.link_method,
            row_number() over (
                partition by link.canonical_site_id, link.source_family, link.source_dataset
                order by count(*) desc, max(link.updated_at) desc nulls last
            ) as row_number
        from landintel.site_source_links as link
        group by link.canonical_site_id, link.source_family, link.source_dataset, link.link_method
    ) as ranked
    where ranked.row_number = 1
),
latest_link_run as (
    select
        ranked.canonical_site_id,
        ranked.source_family,
        ranked.source_dataset,
        ranked.ingest_run_id
    from (
        select
            link.canonical_site_id,
            link.source_family,
            link.source_dataset,
            link.ingest_run_id,
            row_number() over (
                partition by link.canonical_site_id, link.source_family, link.source_dataset
                order by link.updated_at desc nulls last, link.created_at desc, link.ingest_run_id desc nulls last
            ) as row_number
        from landintel.site_source_links as link
    ) as ranked
    where ranked.row_number = 1
)
select
    site.id as canonical_site_id,
    site.site_code,
    site.site_name_primary as site_name,
    site.authority_name,
    link.source_family,
    link.source_dataset,
    link.linked_source_record_count,
    array(
        select distinct reference_value
        from unnest(coalesce(alias_rollup.alias_references, '{}'::text[]) || coalesce(link.source_record_ids, '{}'::text[])) as reference_value
        where nullif(btrim(reference_value), '') is not null
        order by reference_value
    ) as key_references,
    dominant.link_method,
    round(coalesce(link.average_link_confidence, 0)::numeric, 3) as average_link_confidence,
    latest_run.ingest_run_id as latest_ingest_run_id,
    greatest(link.latest_source_update_at, coalesce(alias_rollup.latest_alias_update_at, link.latest_source_update_at)) as latest_source_update_at
from link_rollup as link
join landintel.canonical_sites as site
    on site.id = link.canonical_site_id
left join alias_rollup
    on alias_rollup.canonical_site_id = link.canonical_site_id
   and alias_rollup.source_family = link.source_family
   and alias_rollup.source_dataset = link.source_dataset
left join dominant_link_method as dominant
    on dominant.canonical_site_id = link.canonical_site_id
   and dominant.source_family = link.source_family
   and dominant.source_dataset = link.source_dataset
left join latest_link_run as latest_run
    on latest_run.canonical_site_id = link.canonical_site_id
   and latest_run.source_family = link.source_family
   and latest_run.source_dataset = link.source_dataset
order by site.authority_name, site.site_name_primary, link.source_family, link.source_dataset;

create or replace view analytics.v_live_site_summary
with (security_invoker = true) as
with planning_rollup as (
    select
        canonical_site_id,
        count(*)::bigint as planning_record_count,
        max(updated_at) as latest_planning_update_at
    from landintel.planning_application_records
    where canonical_site_id is not null
    group by canonical_site_id
),
hla_rollup as (
    select
        canonical_site_id,
        count(*)::bigint as hla_record_count,
        max(updated_at) as latest_hla_update_at
    from landintel.hla_site_records
    where canonical_site_id is not null
    group by canonical_site_id
),
bgs_rollup as (
    select
        canonical_site_id,
        count(*)::bigint as bgs_record_count,
        max(updated_at) as latest_bgs_update_at
    from landintel.bgs_records
    where canonical_site_id is not null
    group by canonical_site_id
),
flood_rollup as (
    select
        canonical_site_id,
        count(*)::bigint as constraint_record_count,
        max(updated_at) as latest_flood_update_at
    from landintel.flood_records
    where canonical_site_id is not null
    group by canonical_site_id
),
evidence_rollup as (
    select
        canonical_site_id,
        count(*)::bigint as evidence_count,
        max(created_at) as latest_evidence_at
    from landintel.evidence_references
    where canonical_site_id is not null
    group by canonical_site_id
),
alias_rollup as (
    select
        canonical_site_id,
        count(*) filter (where status = 'unresolved')::bigint as unresolved_alias_count,
        max(updated_at) as latest_alias_update_at
    from landintel.site_reference_aliases
    where canonical_site_id is not null
    group by canonical_site_id
),
source_presence as (
    select canonical_site_id, 'planning'::text as source_family
    from landintel.planning_application_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, 'hla'::text as source_family
    from landintel.hla_site_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, 'bgs'::text as source_family
    from landintel.bgs_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, 'flood'::text as source_family
    from landintel.flood_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, 'ldp'::text as source_family
    from landintel.ldp_site_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, source_family
    from landintel.site_source_links
),
source_presence_rollup as (
    select
        canonical_site_id,
        array_agg(distinct source_family order by source_family) as source_families_present
    from source_presence
    where canonical_site_id is not null
    group by canonical_site_id
),
link_confidence_rollup as (
    select
        canonical_site_id,
        avg(confidence) as average_link_confidence,
        count(*)::bigint as source_link_count,
        max(updated_at) as latest_link_update_at
    from landintel.site_source_links
    where canonical_site_id is not null
    group by canonical_site_id
),
site_activity as (
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.planning_application_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.hla_site_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.bgs_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.flood_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.ldp_site_records
    where canonical_site_id is not null
    union all
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.site_source_links
    where canonical_site_id is not null
    union all
    select canonical_site_id, updated_at as event_at, ingest_run_id
    from landintel.site_reference_aliases
    where canonical_site_id is not null
    union all
    select canonical_site_id, created_at as event_at, ingest_run_id
    from landintel.evidence_references
    where canonical_site_id is not null
    union all
    select id as canonical_site_id, updated_at as event_at, null::uuid as ingest_run_id
    from landintel.canonical_sites
),
latest_source_update as (
    select
        canonical_site_id,
        max(event_at) as latest_source_update_at
    from site_activity
    group by canonical_site_id
),
latest_ingest as (
    select
        ranked.canonical_site_id,
        ranked.ingest_run_id,
        ranked.status as latest_ingest_status
    from (
        select
            activity.canonical_site_id,
            activity.ingest_run_id,
            ingest.status,
            row_number() over (
                partition by activity.canonical_site_id
                order by activity.event_at desc nulls last, ingest.started_at desc nulls last, activity.ingest_run_id desc nulls last
            ) as row_number
        from site_activity as activity
        join public.ingest_runs as ingest
            on ingest.id = activity.ingest_run_id
        where activity.ingest_run_id is not null
    ) as ranked
    where ranked.row_number = 1
),
site_rollup as (
    select
        site.id as canonical_site_id,
        site.site_code,
        site.site_name_primary as site_name,
        site.authority_name,
        null::text as settlement_name,
        site.area_acres,
        site.workflow_status,
        site.surfaced_reason,
        site.primary_ros_parcel_id as primary_parcel_id,
        coalesce(planning_rollup.planning_record_count, 0) as planning_record_count,
        coalesce(hla_rollup.hla_record_count, 0) as hla_record_count,
        coalesce(bgs_rollup.bgs_record_count, 0) as bgs_record_count,
        coalesce(flood_rollup.constraint_record_count, 0) as constraint_record_count,
        coalesce(evidence_rollup.evidence_count, 0) as evidence_count,
        coalesce(source_presence_rollup.source_families_present, '{}'::text[]) as source_families_present,
        coalesce(alias_rollup.unresolved_alias_count, 0) as unresolved_alias_count,
        latest_source_update.latest_source_update_at,
        latest_ingest.latest_ingest_status,
        coalesce(link_confidence_rollup.average_link_confidence, 0) as average_link_confidence,
        coalesce(link_confidence_rollup.source_link_count, 0) as source_link_count
    from landintel.canonical_sites as site
    left join planning_rollup
        on planning_rollup.canonical_site_id = site.id
    left join hla_rollup
        on hla_rollup.canonical_site_id = site.id
    left join bgs_rollup
        on bgs_rollup.canonical_site_id = site.id
    left join flood_rollup
        on flood_rollup.canonical_site_id = site.id
    left join evidence_rollup
        on evidence_rollup.canonical_site_id = site.id
    left join alias_rollup
        on alias_rollup.canonical_site_id = site.id
    left join source_presence_rollup
        on source_presence_rollup.canonical_site_id = site.id
    left join link_confidence_rollup
        on link_confidence_rollup.canonical_site_id = site.id
    left join latest_source_update
        on latest_source_update.canonical_site_id = site.id
    left join latest_ingest
        on latest_ingest.canonical_site_id = site.id
),
site_status as (
    select
        rollup.*,
        case
            when cardinality(rollup.source_families_present) = 0 then 'raw_only'
            when rollup.authority_name is not null
             and coalesce(rollup.area_acres, 0) > 0
             and nullif(btrim(coalesce(rollup.surfaced_reason, '')), '') is not null
             and (rollup.planning_record_count > 0 or rollup.hla_record_count > 0)
             and rollup.evidence_count >= 3
             and rollup.bgs_record_count > 0 then 'linked_enriched'
            when rollup.authority_name is not null
             and coalesce(rollup.area_acres, 0) > 0
             and nullif(btrim(coalesce(rollup.surfaced_reason, '')), '') is not null
             and (rollup.planning_record_count > 0 or rollup.hla_record_count > 0)
             and rollup.evidence_count > 0 then 'linked_core'
            else 'linked_partial'
        end as data_completeness_status,
        case
            when rollup.source_link_count = 0 or cardinality(rollup.source_families_present) = 0 then 'unresolved_links'
            when rollup.unresolved_alias_count > 0 or rollup.average_link_confidence < 0.75 then 'review_needed'
            when rollup.evidence_count > 0 then 'clear'
            else 'review_needed'
        end as traceability_status,
        case
            when rollup.planning_record_count > 0 and rollup.hla_record_count > 0 and rollup.bgs_record_count > 0 then 'planning_hla_bgs_linked'
            when rollup.planning_record_count > 0 and rollup.hla_record_count > 0 then 'planning_hla_linked'
            when rollup.planning_record_count > 0 and rollup.hla_record_count = 0 then 'planning_only'
            when rollup.hla_record_count > 0 and rollup.planning_record_count = 0 then 'hla_only'
            else null::text
        end as site_stage,
        array_remove(array[
            case when rollup.planning_record_count = 0 then 'no planning link' end,
            case when rollup.hla_record_count = 0 then 'no HLA link' end,
            case when rollup.bgs_record_count = 0 then 'no BGS/ground record' end,
            case when rollup.constraint_record_count = 0 then 'no constraints context' end,
            case when rollup.evidence_count = 0 then 'no evidence references' end,
            case when nullif(btrim(coalesce(rollup.surfaced_reason, '')), '') is null then 'no surfaced reason' end,
            case when coalesce(rollup.area_acres, 0) <= 0 then 'no area acres' end,
            case when cardinality(rollup.source_families_present) = 0 then 'no linked source families' end
        ], null) as missing_core_inputs
    from site_rollup as rollup
)
select
    site_status.canonical_site_id,
    site_status.site_code,
    site_status.site_name,
    site_status.authority_name,
    site_status.settlement_name,
    site_status.area_acres,
    site_status.workflow_status,
    site_status.surfaced_reason,
    site_status.primary_parcel_id,
    site_status.planning_record_count,
    site_status.hla_record_count,
    site_status.bgs_record_count,
    site_status.constraint_record_count,
    site_status.evidence_count,
    site_status.source_families_present,
    site_status.unresolved_alias_count,
    site_status.latest_source_update_at,
    site_status.latest_ingest_status,
    site_status.data_completeness_status,
    site_status.traceability_status,
    site_status.site_stage,
    (
        coalesce(site_status.area_acres, 0) > 0
        and nullif(btrim(coalesce(site_status.authority_name, '')), '') is not null
        and cardinality(site_status.source_families_present) > 0
        and (site_status.planning_record_count > 0 or site_status.hla_record_count > 0)
        and nullif(btrim(coalesce(site_status.surfaced_reason, '')), '') is not null
        and site_status.evidence_count > 0
        and site_status.traceability_status <> 'unresolved_links'
    ) as review_ready_flag,
    (
        (
            coalesce(site_status.area_acres, 0) > 0
            and nullif(btrim(coalesce(site_status.authority_name, '')), '') is not null
            and cardinality(site_status.source_families_present) > 0
            and (site_status.planning_record_count > 0 or site_status.hla_record_count > 0)
            and nullif(btrim(coalesce(site_status.surfaced_reason, '')), '') is not null
            and site_status.evidence_count > 0
            and site_status.traceability_status <> 'unresolved_links'
        )
        and site_status.planning_record_count > 0
        and site_status.hla_record_count > 0
        and site_status.constraint_record_count > 0
        and site_status.evidence_count >= 3
        and site_status.traceability_status = 'clear'
    ) as commercial_ready_flag,
    site_status.missing_core_inputs,
    case
        when cardinality(site_status.source_families_present) = 0 then 'No linked source families yet.'
        when coalesce(site_status.area_acres, 0) <= 0 then 'No area acres recorded on the canonical site.'
        when site_status.planning_record_count = 0 and site_status.hla_record_count = 0 then 'No planning or HLA context linked yet.'
        when nullif(btrim(coalesce(site_status.surfaced_reason, '')), '') is null then 'No surfaced reason has been recorded yet.'
        when site_status.evidence_count = 0 then 'No evidence references have been attached yet.'
        when site_status.traceability_status = 'unresolved_links' then 'Source linkage is still unresolved.'
        when (
            coalesce(site_status.area_acres, 0) > 0
            and nullif(btrim(coalesce(site_status.authority_name, '')), '') is not null
            and cardinality(site_status.source_families_present) > 0
            and (site_status.planning_record_count > 0 or site_status.hla_record_count > 0)
            and nullif(btrim(coalesce(site_status.surfaced_reason, '')), '') is not null
            and site_status.evidence_count > 0
            and site_status.traceability_status <> 'unresolved_links'
        ) and site_status.constraint_record_count = 0 then 'No constraints context linked yet.'
        when (
            coalesce(site_status.area_acres, 0) > 0
            and nullif(btrim(coalesce(site_status.authority_name, '')), '') is not null
            and cardinality(site_status.source_families_present) > 0
            and (site_status.planning_record_count > 0 or site_status.hla_record_count > 0)
            and nullif(btrim(coalesce(site_status.surfaced_reason, '')), '') is not null
            and site_status.evidence_count > 0
            and site_status.traceability_status <> 'unresolved_links'
        ) and site_status.evidence_count < 3 then 'Evidence depth is still below the commercial readiness threshold.'
        else null::text
    end as why_not_ready
from site_status
order by site_status.authority_name, site_status.site_name;

create or replace view analytics.v_live_site_readiness
with (security_invoker = true) as
select
    summary.canonical_site_id,
    summary.site_code,
    summary.site_name,
    summary.authority_name,
    summary.area_acres,
    summary.source_families_present,
    summary.planning_record_count,
    summary.hla_record_count,
    summary.bgs_record_count,
    summary.constraint_record_count,
    summary.review_ready_flag,
    summary.commercial_ready_flag,
    case
        when summary.commercial_ready_flag then 'commercial_ready'
        when summary.review_ready_flag then 'review_ready'
        else 'not_ready'
    end as minimum_readiness_band,
    summary.missing_core_inputs,
    summary.why_not_ready,
    summary.latest_source_update_at
from analytics.v_live_site_summary as summary
order by summary.authority_name, summary.site_name;

alter table landintel.canonical_site_refresh_queue
    drop constraint if exists canonical_site_refresh_queue_family_check;

alter table landintel.canonical_site_refresh_queue
    add constraint canonical_site_refresh_queue_family_check
    check (
        source_family is null
        or source_family = any (array[
            'planning',
            'hla',
            'title_number',
            'ldp',
            'settlement',
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
            'os_linked_identifiers',
            'os_openmap_local',
            'os_open_roads',
            'os_open_rivers',
            'os_boundary_line',
            'os_open_names',
            'os_open_greenspace',
            'os_open_uprn',
            'os_open_usrn',
            'osm',
            'naptan',
            'statistics_gov_scot',
            'opentopography_srtm',
            'local_landscape_areas',
            'local_nature',
            'forestry_woodland',
            'sgn_assets'
        ]::text[])
    );

delete from landintel.ldp_site_records as older
using landintel.ldp_site_records as newer
where older.ctid < newer.ctid
  and older.source_record_id = newer.source_record_id;

create unique index if not exists landintel_ldp_site_records_source_record_uidx
    on landintel.ldp_site_records (source_record_id);

delete from landintel.settlement_boundary_records as older
using landintel.settlement_boundary_records as newer
where older.ctid < newer.ctid
  and older.source_record_id = newer.source_record_id;

create unique index if not exists landintel_settlement_boundary_records_source_record_uidx
    on landintel.settlement_boundary_records (source_record_id);

drop view if exists analytics.v_phase_one_control_policy_priority;
drop view if exists analytics.v_phase_one_source_expansion_readiness;

create or replace view analytics.v_phase_one_source_expansion_readiness
with (security_invoker = true) as
with expected_sources(priority_rank, source_family, command_name, target_table, source_role) as (
    values
        (1, 'title_number', 'resolve-title-numbers', 'public.site_title_resolution_candidates -> public.site_title_validation', 'control'),
        (2, 'ldp', 'ingest-ldp', 'landintel.ldp_site_records', 'policy_core'),
        (3, 'settlement', 'ingest-settlement-boundaries', 'landintel.settlement_boundary_records', 'policy_core'),
        (10, 'ela', 'ingest-ela', 'landintel.ela_site_records', 'future_context'),
        (11, 'vdl', 'ingest-vdl', 'landintel.vdl_site_records', 'future_context'),
        (20, 'sepa_flood', 'ingest-sepa-flood', 'public.constraint_source_features', 'constraint'),
        (21, 'coal_authority', 'ingest-coal-authority', 'public.constraint_source_features', 'constraint'),
        (22, 'hes', 'ingest-hes-designations', 'public.constraint_source_features', 'constraint'),
        (23, 'naturescot', 'ingest-naturescot', 'public.constraint_source_features', 'constraint'),
        (24, 'contaminated_land', 'ingest-contaminated-land', 'public.constraint_source_features', 'constraint'),
        (25, 'tpo', 'ingest-tpo', 'public.constraint_source_features', 'constraint'),
        (26, 'culverts', 'ingest-culverts', 'public.constraint_source_features', 'constraint'),
        (27, 'conservation_areas', 'ingest-conservation-areas', 'public.constraint_source_features', 'constraint'),
        (28, 'greenbelt', 'ingest-greenbelt', 'public.constraint_source_features', 'constraint'),
        (30, 'topography', 'ingest-os-topography', 'public.constraint_source_features', 'constraint'),
        (31, 'os_places', 'ingest-os-places', 'public.constraint_source_features', 'location_context'),
        (32, 'os_features', 'ingest-os-features', 'public.constraint_source_features', 'location_context'),
        (33, 'os_linked_identifiers', 'ingest-os-linked-identifiers', 'landintel.source_estate_registry', 'location_context'),
        (34, 'os_openmap_local', 'ingest-os-openmap-local', 'landintel.source_estate_registry', 'base_geometry'),
        (35, 'os_open_roads', 'ingest-os-open-roads', 'landintel.source_estate_registry', 'access_context'),
        (36, 'os_open_rivers', 'ingest-os-open-rivers', 'landintel.source_estate_registry', 'hydrography'),
        (37, 'os_boundary_line', 'ingest-os-boundary-line', 'landintel.source_estate_registry', 'administrative_geography'),
        (38, 'os_open_names', 'ingest-os-open-names', 'landintel.source_estate_registry', 'location_naming'),
        (39, 'os_open_greenspace', 'ingest-os-open-greenspace', 'landintel.source_estate_registry', 'amenities'),
        (40, 'os_open_uprn', 'ingest-os-open-uprn', 'landintel.source_estate_registry', 'address_context'),
        (41, 'os_open_usrn', 'ingest-os-open-usrn', 'landintel.source_estate_registry', 'access_context'),
        (42, 'osm', 'ingest-osm-overpass', 'landintel.source_estate_registry', 'open_context'),
        (43, 'naptan', 'ingest-naptan', 'landintel.source_estate_registry', 'amenities'),
        (44, 'statistics_gov_scot', 'ingest-statistics-gov-scot', 'landintel.source_estate_registry', 'demographics'),
        (45, 'opentopography_srtm', 'ingest-opentopography-srtm', 'landintel.source_estate_registry', 'terrain_fallback')
), raw_counts as (
    select 'title_number'::text as source_family, count(*)::bigint as raw_row_count from public.site_title_validation
    union all
    select 'ldp'::text as source_family, count(*)::bigint as raw_row_count from landintel.ldp_site_records
    union all
    select 'settlement'::text as source_family, count(*)::bigint as raw_row_count from landintel.settlement_boundary_records
    union all
    select 'ela'::text as source_family, count(*)::bigint as raw_row_count from landintel.ela_site_records
    union all
    select 'vdl'::text as source_family, count(*)::bigint as raw_row_count from landintel.vdl_site_records
    union all
    select layer.source_family, count(feature.id)::bigint as raw_row_count
    from public.constraint_layer_registry as layer
    left join public.constraint_source_features as feature on feature.constraint_layer_id = layer.id
    group by layer.source_family
), linked_counts as (
    select
        'title_number'::text as source_family,
        count(distinct title.site_id)::bigint as linked_row_count
    from public.site_title_validation as title
    where title.validation_status in ('matched', 'probable', 'manual_review')
    union all
    select 'ldp'::text as source_family, count(*)::bigint as linked_row_count
    from landintel.ldp_site_records
    where canonical_site_id is not null
    union all
    select 'settlement'::text as source_family, count(distinct link.canonical_site_id)::bigint as linked_row_count
    from landintel.site_source_links as link
    where link.source_family = 'settlement'
    union all
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
), review_counts_raw as (
    select 'title_number'::text as source_family, count(*)::bigint as review_output_row_count
    from public.site_title_validation
    where validation_status in ('matched', 'probable', 'manual_review')
    union all
    select source_family, count(*)::bigint as review_output_row_count
    from landintel.site_source_links
    where source_family in ('ela', 'vdl', 'ldp', 'settlement', 'title_number')
    group by source_family
    union all
    select layer.source_family, count(summary.id)::bigint as review_output_row_count
    from public.constraint_layer_registry as layer
    left join public.site_constraint_group_summaries as summary on summary.constraint_layer_id = layer.id
    group by layer.source_family
), review_counts as (
    select source_family, sum(review_output_row_count)::bigint as review_output_row_count
    from review_counts_raw
    group by source_family
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
    expected.priority_rank,
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
        when expected.source_family = 'title_number'
         and coalesce(raw_counts.raw_row_count, 0) > 0
         and coalesce(linked_counts.linked_row_count, 0) > 0 then 'control_wired_proven'
        when expected.source_family = 'title_number'
         and coalesce(raw_counts.raw_row_count, 0) > 0 then 'control_title_rows_need_review'
        when expected.source_family = 'title_number' then coalesce(latest_events.latest_event_status, 'control_source_not_yet_populated')
        when expected.source_family in ('ldp', 'settlement')
         and coalesce(raw_counts.raw_row_count, 0) > 0
         and coalesce(linked_counts.linked_row_count, 0) > 0
         and coalesce(evidence_counts.evidence_row_count, 0) > 0
         and coalesce(signal_counts.signal_row_count, 0) > 0
         and coalesce(review_counts.review_output_row_count, 0) > 0 then 'core_policy_wired_proven'
        when expected.source_family = 'ldp'
         and coalesce(raw_counts.raw_row_count, 0) > 0 then 'core_policy_storage_proven_licence_gated'
        when expected.source_family = 'settlement'
         and coalesce(raw_counts.raw_row_count, 0) > 0 then 'core_policy_storage_proven_interpreter_gated'
        when expected.source_family = 'ldp' then coalesce(latest_events.latest_event_status, 'core_policy_spatialhub_package_pending')
        when expected.source_family = 'settlement' then coalesce(latest_events.latest_event_status, 'core_policy_nrs_wfs_pending')
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
order by expected.priority_rank, expected.source_role, expected.source_family;

create or replace view analytics.v_phase_one_control_policy_priority
with (security_invoker = true) as
select
    priority_rank,
    source_family,
    command_name,
    target_table,
    source_role,
    raw_or_feature_rows,
    linked_or_measured_rows,
    evidence_rows,
    signal_rows,
    change_event_rows,
    review_output_rows,
    latest_event_status,
    latest_event_summary,
    latest_event_at,
    live_proof_status
from analytics.v_phase_one_source_expansion_readiness
where priority_rank <= 3
order by priority_rank;

revoke all on table analytics.v_phase_one_source_expansion_readiness from anon, authenticated;
revoke all on table analytics.v_phase_one_control_policy_priority from anon, authenticated;

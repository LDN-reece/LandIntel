create table if not exists landintel.open_location_spine_ingest_progress (
    id uuid primary key default gen_random_uuid(),
    source_key text not null,
    source_family text not null,
    download_signature text not null,
    download_file_name text,
    download_format text,
    source_path text not null,
    source_layer text not null default '',
    next_row_offset bigint not null default 0,
    rows_landed bigint not null default 0,
    last_batch_row_count integer not null default 0,
    completion_status text not null default 'pending',
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, download_signature, source_path, source_layer),
    check (completion_status in ('pending', 'in_progress', 'exhausted', 'skipped', 'failed'))
);

create index if not exists open_location_spine_ingest_progress_source_idx
    on landintel.open_location_spine_ingest_progress (source_key, completion_status, updated_at);

drop view if exists analytics.v_open_location_spine_completion;

create or replace view analytics.v_open_location_spine_completion
with (security_invoker = true) as
with source_rows as (
    select
        registry.source_key,
        registry.source_family,
        registry.source_name,
        registry.access_status,
        registry.ingest_status,
        registry.normalisation_status,
        registry.site_link_status,
        registry.measurement_status,
        registry.evidence_status,
        registry.signal_status,
        registry.trusted_for_review,
        registry.limitation_notes,
        registry.next_action
    from landintel.source_estate_registry as registry
    where registry.source_family in (
        'os_openmap_local',
        'os_open_roads',
        'os_open_rivers',
        'os_boundary_line',
        'os_open_names',
        'os_open_greenspace',
        'os_open_zoomstack',
        'os_open_toid',
        'os_open_built_up_areas',
        'os_open_uprn',
        'os_open_usrn',
        'osm'
    )
), feature_counts as (
    select
        feature.source_key,
        count(*)::bigint as feature_count,
        count(distinct feature.feature_type)::bigint as feature_type_count,
        max(feature.updated_at) as latest_feature_updated_at
    from landintel.open_location_spine_features as feature
    group by feature.source_key
), context_counts as (
    select
        context.source_key,
        count(*)::bigint as context_row_count,
        count(distinct context.canonical_site_id)::bigint as linked_site_count,
        max(context.measured_at) as latest_context_measured_at
    from landintel.site_open_location_spine_context as context
    group by context.source_key
), progress_counts as (
    select
        progress.source_key,
        count(*)::bigint as tracked_slice_count,
        count(*) filter (where progress.completion_status = 'exhausted')::bigint as exhausted_slice_count,
        count(*) filter (where progress.completion_status = 'failed')::bigint as failed_slice_count,
        count(*) filter (where progress.completion_status in ('pending', 'in_progress'))::bigint as active_slice_count,
        coalesce(sum(progress.rows_landed), 0)::bigint as progress_rows_landed,
        max(progress.updated_at) as latest_progress_updated_at
    from landintel.open_location_spine_ingest_progress as progress
    group by progress.source_key
)
select
    source_rows.source_family,
    source_rows.source_key,
    source_rows.source_name,
    source_rows.access_status,
    source_rows.ingest_status,
    source_rows.normalisation_status,
    source_rows.site_link_status,
    source_rows.measurement_status,
    source_rows.evidence_status,
    source_rows.signal_status,
    coalesce(feature_counts.feature_count, 0) as feature_count,
    coalesce(feature_counts.feature_type_count, 0) as feature_type_count,
    coalesce(context_counts.context_row_count, 0) as context_row_count,
    coalesce(context_counts.linked_site_count, 0) as linked_site_count,
    coalesce(progress_counts.tracked_slice_count, 0) as tracked_slice_count,
    coalesce(progress_counts.exhausted_slice_count, 0) as exhausted_slice_count,
    coalesce(progress_counts.failed_slice_count, 0) as failed_slice_count,
    coalesce(progress_counts.active_slice_count, 0) as active_slice_count,
    coalesce(progress_counts.progress_rows_landed, 0) as progress_rows_landed,
    case
        when coalesce(progress_counts.failed_slice_count, 0) > 0 then 'failed'
        when coalesce(progress_counts.tracked_slice_count, 0) = 0
         and coalesce(feature_counts.feature_count, 0) = 0 then 'not_started'
        when coalesce(progress_counts.tracked_slice_count, 0) > 0
         and coalesce(progress_counts.active_slice_count, 0) = 0 then 'source_exhausted'
        when coalesce(feature_counts.feature_count, 0) > 0 then 'landing_in_progress'
        else 'registered_no_rows'
    end as corpus_completion_status,
    false as trusted_for_review,
    source_rows.limitation_notes,
    case
        when coalesce(progress_counts.failed_slice_count, 0) > 0
            then 'Review failed open-data slices and rerun with an adjusted budget or adapter fix.'
        when coalesce(progress_counts.tracked_slice_count, 0) > 0
         and coalesce(progress_counts.active_slice_count, 0) = 0
            then 'Corpus slices are exhausted for the selected download; expand context measurement coverage.'
        when coalesce(feature_counts.feature_count, 0) > 0
            then 'Continue completion pulse until all tracked slices are exhausted and site context is measured.'
        else coalesce(source_rows.next_action, 'Run open-data completion pulse for this source.')
    end as next_action,
    feature_counts.latest_feature_updated_at,
    context_counts.latest_context_measured_at,
    progress_counts.latest_progress_updated_at
from source_rows
left join feature_counts on feature_counts.source_key = source_rows.source_key
left join context_counts on context_counts.source_key = source_rows.source_key
left join progress_counts on progress_counts.source_key = source_rows.source_key;

grant select on analytics.v_open_location_spine_completion to authenticated;

comment on table landintel.open_location_spine_ingest_progress
    is 'Tracks resumable OS/OpenStreetMap open-location corpus landing by source, download, path, layer and row offset so timed GitHub Actions runs advance rather than replaying the first slice.';

comment on view analytics.v_open_location_spine_completion
    is 'Proof view for open-data corpus completion: distinguishes registered, landing, exhausted, failed and site-context coverage states without marking contextual open data trusted for review.';

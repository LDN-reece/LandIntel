create or replace view analytics.v_ingest_run_summary as
select
    id,
    run_type,
    source_name,
    status,
    started_at,
    finished_at,
    extract(epoch from (coalesce(finished_at, now()) - started_at))::numeric(18, 2) as duration_seconds,
    records_fetched,
    records_loaded,
    records_retained,
    error_message,
    metadata
from public.ingest_runs
order by started_at desc;

create or replace view analytics.v_ros_parcels_summary_by_authority_size as
select
    authority_name,
    size_bucket,
    size_bucket_label,
    count(*) as parcel_count,
    round(sum(area_sqm), 2) as total_area_sqm,
    round(sum(area_ha), 6) as total_area_ha,
    round(sum(area_acres), 6) as total_area_acres,
    round(avg(area_acres), 6) as average_area_acres
from public.ros_cadastral_parcels
group by authority_name, size_bucket, size_bucket_label
order by authority_name, size_bucket;

create or replace view analytics.v_source_registry_latest as
select
    source_name,
    source_type,
    publisher,
    metadata_uuid,
    endpoint_url,
    download_url,
    last_seen_at,
    updated_at,
    geographic_extent
from public.source_registry
order by updated_at desc, source_name asc;

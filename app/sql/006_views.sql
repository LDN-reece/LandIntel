create or replace view analytics.v_ingest_run_summary
with (security_invoker = true) as
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

drop view if exists analytics.v_frontend_authority_summary;
drop view if exists analytics.v_frontend_authority_size_summary;
drop view if exists analytics.v_ros_parcels_summary_by_authority_size;
drop materialized view if exists analytics.mv_frontend_authority_summary;
drop materialized view if exists analytics.mv_frontend_authority_size_summary;

create materialized view analytics.mv_frontend_authority_summary as
select
    authority.authority_name,
    count(parcel.id)::bigint as parcel_count,
    coalesce(round(sum(parcel.area_sqm), 2), 0) as total_area_sqm,
    coalesce(round(sum(parcel.area_ha), 6), 0) as total_area_ha,
    coalesce(round(sum(parcel.area_acres), 6), 0) as total_area_acres,
    coalesce(round(avg(parcel.area_acres), 6), 0) as average_area_acres
from public.authority_aoi as authority
left join public.ros_cadastral_parcels as parcel
    on parcel.authority_name = authority.authority_name
where authority.active = true
group by authority.authority_name;

create unique index if not exists mv_frontend_authority_summary_authority_uidx
    on analytics.mv_frontend_authority_summary (authority_name);

create materialized view analytics.mv_frontend_authority_size_summary as
select
    authority_name,
    size_bucket,
    size_bucket_label,
    count(*)::bigint as parcel_count,
    round(sum(area_sqm), 2) as total_area_sqm,
    round(sum(area_ha), 6) as total_area_ha,
    round(sum(area_acres), 6) as total_area_acres,
    round(avg(area_acres), 6) as average_area_acres
from public.ros_cadastral_parcels
group by authority_name, size_bucket, size_bucket_label;

create unique index if not exists mv_frontend_authority_size_summary_authority_size_uidx
    on analytics.mv_frontend_authority_size_summary (authority_name, size_bucket);

create or replace view analytics.v_frontend_authority_summary
with (security_invoker = true) as
select
    authority_name,
    parcel_count,
    total_area_sqm,
    total_area_ha,
    total_area_acres,
    average_area_acres
from analytics.mv_frontend_authority_summary
order by authority_name;

create or replace view analytics.v_frontend_authority_size_summary
with (security_invoker = true) as
select
    authority_name,
    size_bucket,
    size_bucket_label,
    parcel_count,
    total_area_sqm,
    total_area_ha,
    total_area_acres,
    average_area_acres
from analytics.mv_frontend_authority_size_summary
order by authority_name, size_bucket;

create or replace view analytics.v_ros_parcels_summary_by_authority_size
with (security_invoker = true) as
select
    authority_name,
    size_bucket,
    size_bucket_label,
    parcel_count,
    total_area_sqm,
    total_area_ha,
    total_area_acres,
    average_area_acres
from analytics.mv_frontend_authority_size_summary
order by authority_name, size_bucket;

create or replace view analytics.v_source_registry_latest
with (security_invoker = true) as
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

create or replace view analytics.v_relation_storage_usage
with (security_invoker = true) as
select
    namespace.nspname as schema_name,
    relation.relname as relation_name,
    case relation.relkind
        when 'r' then 'table'
        when 'm' then 'materialized_view'
        else relation.relkind::text
    end as relation_type,
    coalesce(stats.n_live_tup::bigint, relation.reltuples::bigint, 0) as estimated_rows,
    pg_relation_size(relation.oid) as table_bytes,
    pg_indexes_size(relation.oid) as index_bytes,
    pg_total_relation_size(relation.oid) as total_bytes,
    pg_size_pretty(pg_total_relation_size(relation.oid)) as total_size_pretty
from pg_class as relation
join pg_namespace as namespace
    on namespace.oid = relation.relnamespace
left join pg_stat_user_tables as stats
    on stats.relid = relation.oid
where namespace.nspname in ('public', 'staging', 'analytics')
  and relation.relkind in ('r', 'm')
order by pg_total_relation_size(relation.oid) desc, namespace.nspname, relation.relname;

create or replace view analytics.v_storage_bucket_usage
with (security_invoker = true) as
select
    storage_object.bucket_id,
    count(*)::bigint as object_count,
    coalesce(sum((storage_object.metadata ->> 'size')::bigint), 0) as total_bytes,
    pg_size_pretty(coalesce(sum((storage_object.metadata ->> 'size')::bigint), 0)) as total_size_pretty,
    coalesce(max((storage_object.metadata ->> 'size')::bigint), 0) as largest_object_bytes,
    max(storage_object.updated_at) as last_object_updated_at
from storage.objects as storage_object
group by storage_object.bucket_id
order by total_bytes desc, storage_object.bucket_id;

create or replace function analytics.refresh_cached_outputs()
returns void
language plpgsql
set search_path = pg_catalog, public, analytics
as $$
begin
    refresh materialized view analytics.mv_frontend_authority_summary;
    refresh materialized view analytics.mv_frontend_authority_size_summary;
end;
$$;

revoke all on function analytics.refresh_cached_outputs() from public, anon, authenticated;

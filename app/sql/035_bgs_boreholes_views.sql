drop view if exists analytics.v_bgs_borehole_ingest_summary;

create or replace view analytics.v_bgs_borehole_ingest_summary
with (security_invoker = true) as
with raw_summary as (
    select
        raw.ingest_run_id,
        max(raw.source_snapshot_date) as source_snapshot_date,
        max(raw.source_archive_name) as source_archive_name,
        max(raw.source_file_name) as source_file_name,
        count(*)::bigint as raw_rows
    from public.bgs_boreholes_raw as raw
    group by raw.ingest_run_id
),
master_summary as (
    select
        master.source_ingest_run_id as ingest_run_id,
        max(master.source_snapshot_date) as source_snapshot_date,
        max(master.source_archive_name) as source_archive_name,
        max(master.source_file_name) as source_file_name,
        count(*)::bigint as normalised_rows,
        count(*) filter (where master.geom is not null)::bigint as valid_geometry_rows,
        count(*) filter (where master.is_scotland)::bigint as scotland_rows,
        count(*) filter (where master.has_ags_log)::bigint as ags_link_rows,
        count(*) filter (where master.is_confidential)::bigint as confidential_rows,
        count(*) filter (where master.depth_m is not null)::bigint as known_depth_rows
    from public.bgs_boreholes as master
    group by master.source_ingest_run_id
)
select
    coalesce(raw_summary.ingest_run_id, master_summary.ingest_run_id) as ingest_run_id,
    coalesce(master_summary.source_snapshot_date, raw_summary.source_snapshot_date) as source_snapshot_date,
    coalesce(master_summary.source_archive_name, raw_summary.source_archive_name) as source_archive_name,
    coalesce(master_summary.source_file_name, raw_summary.source_file_name) as source_file_name,
    coalesce(raw_summary.raw_rows, 0) as raw_rows,
    coalesce(master_summary.normalised_rows, 0) as normalised_rows,
    coalesce(master_summary.valid_geometry_rows, 0) as valid_geometry_rows,
    coalesce(master_summary.scotland_rows, 0) as scotland_rows,
    coalesce(master_summary.ags_link_rows, 0) as ags_link_rows,
    coalesce(master_summary.confidential_rows, 0) as confidential_rows,
    coalesce(master_summary.known_depth_rows, 0) as known_depth_rows,
    greatest(coalesce(raw_summary.raw_rows, 0) - coalesce(master_summary.normalised_rows, 0), 0) as invalid_or_quarantined_rows
from raw_summary
full outer join master_summary
    on master_summary.ingest_run_id = raw_summary.ingest_run_id;

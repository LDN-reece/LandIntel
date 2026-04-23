create index if not exists bgs_boreholes_raw_ingest_row_idx
    on public.bgs_boreholes_raw (ingest_run_id, source_row_number);

create index if not exists bgs_boreholes_raw_bgs_id_idx
    on public.bgs_boreholes_raw (bgs_id);

create index if not exists bgs_boreholes_raw_snapshot_idx
    on public.bgs_boreholes_raw (source_snapshot_date desc);

create index if not exists bgs_boreholes_regno_idx
    on public.bgs_boreholes (regno);

create index if not exists bgs_boreholes_is_scotland_idx
    on public.bgs_boreholes (is_scotland);

create index if not exists bgs_boreholes_has_ags_log_idx
    on public.bgs_boreholes (has_ags_log);

create index if not exists bgs_boreholes_depth_status_idx
    on public.bgs_boreholes (depth_status);

create index if not exists bgs_boreholes_source_run_idx
    on public.bgs_boreholes (source_ingest_run_id);

create index if not exists bgs_boreholes_snapshot_idx
    on public.bgs_boreholes (source_snapshot_date desc);

create index if not exists bgs_boreholes_geom_gix
    on public.bgs_boreholes using gist (geom);

create index if not exists bgs_boreholes_name_tokens_gin
    on public.bgs_boreholes using gin (name_tokens);

create index if not exists site_constraints_source_dataset_type_site_idx
    on public.site_constraints (source_dataset, constraint_type, site_id);

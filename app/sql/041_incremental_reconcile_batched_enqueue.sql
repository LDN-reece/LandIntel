drop trigger if exists ingest_runs_incremental_reconcile_queue_trigger on public.ingest_runs;

create index if not exists planning_application_records_reconcile_ingest_idx
    on landintel.planning_application_records (ingest_run_id, authority_name, source_record_id);

create index if not exists hla_site_records_reconcile_ingest_idx
    on landintel.hla_site_records (ingest_run_id, authority_name, source_record_id);

create index if not exists source_reconcile_state_scope_seen_idx
    on landintel.source_reconcile_state (source_family, authority_name, active_flag, last_seen_ingest_run_id, source_record_id);

comment on function landintel.handle_successful_reconcile_enqueue()
    is 'Legacy trigger path retired. Incremental reconcile enqueue is now workflow-led and batched.';

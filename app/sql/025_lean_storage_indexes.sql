create index if not exists source_artifacts_ingest_run_idx
    on public.source_artifacts (ingest_run_id);

create index if not exists source_artifacts_source_idx
    on public.source_artifacts (source_name, authority_name, artifact_role);

create index if not exists source_artifacts_retention_idx
    on public.source_artifacts (retention_class, expires_at, deleted_at);

create index if not exists source_artifacts_storage_idx
    on public.source_artifacts (storage_backend, storage_bucket, storage_path);

create index if not exists source_artifacts_created_idx
    on public.source_artifacts (created_at desc);

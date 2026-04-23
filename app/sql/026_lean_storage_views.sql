create or replace view analytics.v_source_artifact_storage_summary as
select
    source_name,
    coalesce(authority_name, 'ALL') as authority_name,
    artifact_role,
    storage_backend,
    retention_class,
    count(*) as artifact_count,
    count(*) filter (where deleted_at is null) as active_artifact_count,
    count(*) filter (where deleted_at is not null) as deleted_artifact_count,
    coalesce(sum(size_bytes), 0)::bigint as total_size_bytes,
    coalesce(sum(size_bytes) filter (where deleted_at is null), 0)::bigint as active_size_bytes,
    min(created_at) as first_created_at,
    max(created_at) as last_created_at,
    min(expires_at) filter (where deleted_at is null) as next_expiry_at
from public.source_artifacts
group by
    source_name,
    coalesce(authority_name, 'ALL'),
    artifact_role,
    storage_backend,
    retention_class;

create or replace view analytics.v_source_artifact_expiry_queue as
select
    id,
    ingest_run_id,
    source_name,
    authority_name,
    artifact_role,
    storage_backend,
    storage_bucket,
    storage_path,
    retention_class,
    size_bytes,
    expires_at,
    deleted_at,
    created_at
from public.source_artifacts
where deleted_at is null
  and expires_at is not null
order by expires_at asc, created_at asc;

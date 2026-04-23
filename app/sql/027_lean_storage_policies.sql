alter table public.source_artifacts enable row level security;

revoke all on table public.source_artifacts from anon, authenticated;
revoke all on table analytics.v_source_artifact_storage_summary from anon, authenticated;
revoke all on table analytics.v_source_artifact_expiry_queue from anon, authenticated;

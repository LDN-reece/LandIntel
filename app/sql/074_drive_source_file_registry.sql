create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

create table if not exists landintel_store.drive_source_file_registry (
    id uuid primary key default extensions.uuid_generate_v4(),
    root_folder_id text not null,
    root_folder_name text,
    folder_path text not null,
    folder_id text,
    parent_folder_id text,
    file_id text not null,
    file_name text not null,
    file_or_folder text not null default 'file',
    mime_type text,
    file_extension text,
    drive_url text,
    source_family text,
    asset_role text,
    ready_to_upload_flag boolean not null default false,
    ready_to_upload_reason text,
    upload_status text not null default 'metadata_only',
    download_status text not null default 'not_requested',
    storage_bucket text,
    storage_path text,
    size_bytes bigint,
    md5_checksum text,
    drive_created_at timestamptz,
    drive_modified_at timestamptz,
    manifest_seen_at timestamptz,
    live_seen_at timestamptz,
    last_synced_at timestamptz default now(),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint drive_source_file_registry_file_or_folder_check
        check (file_or_folder in ('file', 'folder')),
    constraint drive_source_file_registry_upload_status_check
        check (
            upload_status in (
                'metadata_only',
                'ready_for_controlled_upload',
                'not_ready',
                'paused',
                'uploaded_to_artifact_store',
                'download_failed',
                'blocked'
            )
        ),
    constraint drive_source_file_registry_download_status_check
        check (
            download_status in (
                'not_requested',
                'disabled',
                'skipped',
                'downloaded',
                'uploaded_to_artifact_store',
                'failed'
            )
        )
);

comment on table landintel_store.drive_source_file_registry is
    'Metadata registry for Google Drive-held Scotland source files. This is a source-control surface only; it does not ingest or interpret the datasets.';

create unique index if not exists drive_source_file_registry_root_file_uidx
    on landintel_store.drive_source_file_registry (root_folder_id, file_id);

create index if not exists drive_source_file_registry_ready_idx
    on landintel_store.drive_source_file_registry (ready_to_upload_flag, source_family, asset_role);

create index if not exists drive_source_file_registry_folder_idx
    on landintel_store.drive_source_file_registry (root_folder_id, folder_path);

create index if not exists drive_source_file_registry_metadata_gin_idx
    on landintel_store.drive_source_file_registry using gin (metadata);

create or replace view landintel_reporting.v_drive_source_ready_upload_files
with (security_invoker = true) as
select
    registry.root_folder_id,
    registry.root_folder_name,
    registry.folder_path,
    registry.folder_id,
    registry.file_id,
    registry.file_name,
    registry.mime_type,
    registry.file_extension,
    registry.drive_url,
    registry.source_family,
    registry.asset_role,
    registry.ready_to_upload_reason,
    registry.upload_status,
    registry.download_status,
    registry.storage_bucket,
    registry.storage_path,
    registry.size_bytes,
    registry.drive_modified_at,
    registry.manifest_seen_at,
    registry.live_seen_at,
    registry.last_synced_at,
    registry.updated_at
from landintel_store.drive_source_file_registry as registry
where registry.file_or_folder = 'file'
  and registry.ready_to_upload_flag is true;

create or replace view landintel_reporting.v_drive_source_sync_status
with (security_invoker = true) as
select
    registry.root_folder_id,
    max(registry.root_folder_name) as root_folder_name,
    count(*) filter (where registry.file_or_folder = 'folder')::bigint as folder_rows,
    count(*) filter (where registry.file_or_folder = 'file')::bigint as file_rows,
    count(*) filter (where registry.ready_to_upload_flag is true)::bigint as ready_to_upload_rows,
    count(*) filter (where registry.asset_role = 'documentation')::bigint as documentation_rows,
    count(*) filter (where registry.asset_role = 'loose_shapefile_component')::bigint as loose_shapefile_component_rows,
    count(*) filter (where registry.asset_role = 'known_origin_manual_bulk_upload')::bigint as known_origin_manual_bulk_upload_rows,
    count(*) filter (where registry.asset_role = 'misfiled_review')::bigint as misfiled_review_rows,
    count(distinct registry.source_family)::bigint as source_family_count,
    max(registry.drive_modified_at) as latest_drive_modified_at,
    max(registry.manifest_seen_at) as latest_manifest_seen_at,
    max(registry.live_seen_at) as latest_live_seen_at,
    max(registry.last_synced_at) as latest_synced_at
from landintel_store.drive_source_file_registry as registry
group by registry.root_folder_id;

insert into landintel_store.object_ownership_registry (
    schema_name,
    object_name,
    object_type,
    current_status,
    owner_layer,
    canonical_role,
    source_family_or_module,
    exists_in_github,
    exists_in_supabase,
    represented_in_repo,
    safe_to_read,
    safe_to_write,
    safe_for_operator,
    safe_to_retire,
    risk_summary,
    recommended_action,
    metadata,
    reviewed_at,
    updated_at
)
values
    (
        'landintel_store',
        'drive_source_file_registry',
        'table',
        'current_keep',
        'LandIntel Data Store',
        'Google Drive source-file metadata registry',
        'source_completion',
        true,
        true,
        true,
        true,
        true,
        false,
        false,
        'Metadata-only registry; do not treat as source ingestion proof or dataset interpretation.',
        'Use the dedicated Drive sync workflow to keep file awareness and ready-upload status current.',
        '{"created_by_migration":"074_drive_source_file_registry.sql"}'::jsonb,
        now(),
        now()
    ),
    (
        'landintel_reporting',
        'v_drive_source_ready_upload_files',
        'view',
        'reporting_surface',
        'LandIntel Reporting',
        'Operator-safe list of Drive-held files ready for controlled source storage',
        'source_completion',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'Ready-to-upload means file-level storage readiness only, not data ingestion or DD completion.',
        'Review before promoting any file into a parser/ingestion workflow.',
        '{"created_by_migration":"074_drive_source_file_registry.sql"}'::jsonb,
        now(),
        now()
    ),
    (
        'landintel_reporting',
        'v_drive_source_sync_status',
        'view',
        'reporting_surface',
        'LandIntel Reporting',
        'Drive source sync status rollup',
        'source_completion',
        true,
        true,
        true,
        true,
        false,
        true,
        false,
        'Status rollup only; does not prove source completion.',
        'Use with the source completion matrix to prioritise controlled source completion.',
        '{"created_by_migration":"074_drive_source_file_registry.sql"}'::jsonb,
        now(),
        now()
    )
on conflict (schema_name, object_name, object_type) do update set
    current_status = excluded.current_status,
    owner_layer = excluded.owner_layer,
    canonical_role = excluded.canonical_role,
    source_family_or_module = excluded.source_family_or_module,
    exists_in_github = excluded.exists_in_github,
    exists_in_supabase = excluded.exists_in_supabase,
    represented_in_repo = excluded.represented_in_repo,
    safe_to_read = excluded.safe_to_read,
    safe_to_write = excluded.safe_to_write,
    safe_for_operator = excluded.safe_for_operator,
    safe_to_retire = excluded.safe_to_retire,
    risk_summary = excluded.risk_summary,
    recommended_action = excluded.recommended_action,
    metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
    reviewed_at = now(),
    updated_at = now();

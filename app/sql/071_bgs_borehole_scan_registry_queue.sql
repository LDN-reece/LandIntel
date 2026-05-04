create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

create table if not exists landintel_store.bgs_borehole_scan_registry (
    id uuid primary key default gen_random_uuid(),
    source_key text not null default 'bgs_borehole_scan_registry',
    source_family text not null default 'bgs',
    bgs_id bigint not null,
    registration_number text,
    borehole_name text,
    grid_reference text,
    easting numeric,
    northing numeric,
    geom_27700 geometry(Point, 27700),
    depth_m numeric,
    has_log_available boolean not null default false,
    ags_log_url text,
    operator_use_status text,
    registry_status text not null default 'linked_not_downloaded',
    fetch_status text not null default 'not_queued',
    source_record_signature text,
    safe_use_caveat text not null default 'BGS scan/log registry stores source links only. It does not download scans, run OCR, store PDF blobs in Postgres or provide final ground-condition interpretation.',
    metadata jsonb not null default '{}'::jsonb,
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists bgs_borehole_scan_registry_bgs_uidx
    on landintel_store.bgs_borehole_scan_registry (bgs_id);

create index if not exists bgs_borehole_scan_registry_status_idx
    on landintel_store.bgs_borehole_scan_registry (registry_status, fetch_status, updated_at);

create index if not exists bgs_borehole_scan_registry_log_url_idx
    on landintel_store.bgs_borehole_scan_registry (has_log_available, ags_log_url)
    where has_log_available = true and ags_log_url is not null;

create index if not exists bgs_borehole_scan_registry_geom_gix
    on landintel_store.bgs_borehole_scan_registry using gist (geom_27700)
    where geom_27700 is not null;

create table if not exists landintel_store.bgs_borehole_scan_fetch_queue (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    registry_id uuid not null references landintel_store.bgs_borehole_scan_registry(id) on delete cascade,
    bgs_id bigint not null,
    source_key text not null default 'bgs_borehole_scan_queue',
    source_family text not null default 'bgs',
    site_priority_band text,
    site_priority_rank integer,
    priority_source text,
    borehole_distance_m numeric,
    borehole_depth_m numeric,
    queue_status text not null default 'queued',
    fetch_status text not null default 'linked_not_downloaded',
    requested_action text not null default 'manual_pre_si_log_review',
    source_record_signature text,
    safe_use_caveat text not null default 'BGS scan/log queue is for bounded candidate-site review only. Linked source records are not downloaded, OCRed or treated as engineering or abnormal-cost evidence.',
    metadata jsonb not null default '{}'::jsonb,
    queued_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists bgs_borehole_scan_fetch_queue_site_bgs_uidx
    on landintel_store.bgs_borehole_scan_fetch_queue (canonical_site_id, bgs_id);

create index if not exists bgs_borehole_scan_fetch_queue_status_idx
    on landintel_store.bgs_borehole_scan_fetch_queue (queue_status, fetch_status, updated_at);

create index if not exists bgs_borehole_scan_fetch_queue_priority_idx
    on landintel_store.bgs_borehole_scan_fetch_queue (site_priority_rank, borehole_distance_m);

do $$
begin
    if to_regclass('landintel_store.v_bgs_borehole_master_clean') is not null then
        execute $view$
            create or replace view landintel_reporting.v_bgs_scan_registry
            with (security_invoker = true) as
            select
                registry.id,
                registry.bgs_id,
                registry.registration_number,
                registry.borehole_name,
                registry.grid_reference,
                registry.easting,
                registry.northing,
                registry.geom_27700,
                registry.depth_m,
                registry.has_log_available,
                registry.ags_log_url,
                registry.operator_use_status,
                registry.registry_status,
                registry.fetch_status,
                registry.safe_use_caveat,
                registry.last_seen_at,
                registry.updated_at
            from landintel_store.bgs_borehole_scan_registry as registry
        $view$;

        execute $view$
            create or replace view landintel_reporting.v_bgs_scan_queue
            with (security_invoker = true) as
            select
                queue.id as queue_id,
                queue.canonical_site_id,
                site.site_name_primary as site_label,
                site.authority_name,
                site.area_acres,
                queue.site_priority_band,
                queue.site_priority_rank,
                queue.priority_source,
                registry.bgs_id,
                registry.registration_number,
                registry.borehole_name,
                registry.grid_reference,
                queue.borehole_distance_m,
                queue.borehole_depth_m,
                registry.ags_log_url,
                queue.queue_status,
                queue.fetch_status,
                queue.requested_action,
                queue.safe_use_caveat,
                queue.queued_at,
                queue.updated_at
            from landintel_store.bgs_borehole_scan_fetch_queue as queue
            join landintel_store.bgs_borehole_scan_registry as registry
              on registry.id = queue.registry_id
            join landintel.canonical_sites as site
              on site.id = queue.canonical_site_id
        $view$;

        comment on view landintel_reporting.v_bgs_scan_registry
            is 'BGS scan/log source-link registry. It stores source references only and does not download scans, OCR documents or provide ground-condition interpretation.';

        comment on view landintel_reporting.v_bgs_scan_queue
            is 'Candidate-site-first BGS scan/log review queue. Queue rows remain linked-not-downloaded until a separate bounded fetch workflow is approved.';
    else
        raise notice 'Skipping BGS scan/log reporting views because landintel_store.v_bgs_borehole_master_clean is not present.';
    end if;
end $$;

comment on table landintel_store.bgs_borehole_scan_registry
    is 'BGS scan/log source-link registry over the governed borehole master. Stores links and metadata only; no scan downloads or OCR.';

comment on table landintel_store.bgs_borehole_scan_fetch_queue
    is 'Bounded candidate-site queue for later manual or controlled BGS scan/log review. No assets are downloaded by this table.';

do $$
begin
    if to_regclass('landintel_store.object_ownership_registry') is not null then
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
            replacement_object,
            risk_summary,
            recommended_action,
            metadata
        )
        values
            (
                'landintel_store',
                'bgs_borehole_scan_registry',
                'table',
                'current_keep',
                'landintel_store',
                'BGS borehole scan/log source-link registry',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                true,
                false,
                false,
                'landintel_reporting.v_bgs_scan_registry',
                'Stores source links only. It is not a scan asset store and not interpreted ground-condition evidence.',
                'Refresh from the governed BGS clean master. Do not download scans or run OCR in this phase.',
                '{"phase":"G2","download_assets":false,"ocr":false,"final_ground_condition_interpretation":false}'::jsonb
            ),
            (
                'landintel_store',
                'bgs_borehole_scan_fetch_queue',
                'table',
                'current_keep',
                'landintel_store',
                'candidate-site BGS scan/log review queue',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                true,
                false,
                false,
                'landintel_reporting.v_bgs_scan_queue',
                'Queue identifies candidate-site source links for later bounded review. Rows are linked-not-downloaded until a future fetch workflow is approved.',
                'Queue candidate sites only, cap rows per run, and keep all outputs caveated as Pre-SI source availability context.',
                '{"phase":"G2","candidate_site_first":true,"download_assets":false,"ocr":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_bgs_scan_registry',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'operator-safe BGS scan/log registry surface',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Reporting surface for BGS source links only; does not expose them as interpreted ground evidence.',
                'Use to check available logs/scans and governance status before manual Pre-SI review.',
                '{"phase":"G2","operator_safe":true}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_bgs_scan_queue',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'operator-safe BGS scan/log queue surface',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Operator surface for bounded candidate-site scan/log review queue. Not evidence of ground condition.',
                'Use for deciding which source records merit a later bounded fetch or manual review.',
                '{"phase":"G2","operator_safe":true,"linked_not_downloaded":true}'::jsonb
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
            replacement_object = excluded.replacement_object,
            risk_summary = excluded.risk_summary,
            recommended_action = excluded.recommended_action,
            metadata = landintel_store.object_ownership_registry.metadata || excluded.metadata,
            reviewed_at = now(),
            updated_at = now();
    end if;
end $$;

grant usage on schema landintel_store to authenticated;
grant usage on schema landintel_reporting to authenticated;
grant select on landintel_store.bgs_borehole_scan_registry to authenticated;
grant select on landintel_store.bgs_borehole_scan_fetch_queue to authenticated;

do $$
begin
    if to_regclass('landintel_reporting.v_bgs_scan_registry') is not null then
        execute 'grant select on landintel_reporting.v_bgs_scan_registry to authenticated';
    end if;
    if to_regclass('landintel_reporting.v_bgs_scan_queue') is not null then
        execute 'grant select on landintel_reporting.v_bgs_scan_queue to authenticated';
    end if;
end $$;

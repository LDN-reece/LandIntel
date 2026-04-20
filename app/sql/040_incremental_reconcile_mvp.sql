create table if not exists landintel.source_reconcile_state (
    id uuid primary key default gen_random_uuid(),
    source_family text not null,
    source_dataset text not null,
    authority_name text not null,
    source_record_id text not null,
    active_flag boolean not null default true,
    lifecycle_status text not null default 'active',
    current_source_signature text,
    current_geometry_hash text,
    last_seen_ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    last_seen_at timestamptz,
    last_processed_at timestamptz,
    current_canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    previous_canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    match_method text,
    match_confidence numeric,
    publish_state text not null default 'blocked',
    review_required boolean not null default false,
    review_reason_code text,
    candidate_site_ids uuid[] not null default '{}'::uuid[],
    last_queue_item_id uuid,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint source_reconcile_state_family_check
        check (source_family in ('planning', 'hla')),
    constraint source_reconcile_state_lifecycle_check
        check (lifecycle_status in ('active', 'retired', 'review_required', 'blocked')),
    constraint source_reconcile_state_publish_check
        check (publish_state in ('published', 'provisional', 'blocked')),
    constraint source_reconcile_state_review_reason_check
        check (
            review_reason_code is null
            or review_reason_code = any (
                array[
                    'reference_conflict',
                    'trusted_alias_conflict',
                    'spatial_ambiguous',
                    'weak_spatial_overlap',
                    'geometry_missing_for_new_record',
                    'new_site_below_area_floor',
                    'near_existing_site_conflict',
                    'reassignment_conflict',
                    'possible_merge',
                    'possible_split',
                    'retirement_orphan_risk',
                    'data_integrity_anomaly'
                ]::text[]
            )
        )
);

create unique index if not exists source_reconcile_state_source_key_uidx
    on landintel.source_reconcile_state (source_family, authority_name, source_record_id);

create index if not exists source_reconcile_state_site_idx
    on landintel.source_reconcile_state (current_canonical_site_id, active_flag);

create index if not exists source_reconcile_state_publish_idx
    on landintel.source_reconcile_state (publish_state, review_required, lifecycle_status);

create table if not exists landintel.source_reconcile_queue (
    id uuid primary key default gen_random_uuid(),
    state_id uuid not null references landintel.source_reconcile_state(id) on delete cascade,
    source_family text not null,
    source_dataset text not null,
    authority_name text not null,
    source_record_id text not null,
    work_type text not null,
    priority integer not null default 100,
    status text not null default 'pending',
    source_signature text,
    geometry_hash text,
    previous_canonical_site_id uuid references landintel.canonical_sites(id) on delete set null,
    candidate_site_ids uuid[] not null default '{}'::uuid[],
    claimed_by text,
    claimed_at timestamptz,
    lease_expires_at timestamptz,
    attempt_count integer not null default 0,
    next_attempt_at timestamptz,
    processed_at timestamptz,
    error_code text,
    error_message text,
    review_reason_code text,
    ingest_run_id uuid references public.ingest_runs(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint source_reconcile_queue_family_check
        check (source_family in ('planning', 'hla')),
    constraint source_reconcile_queue_work_type_check
        check (work_type in ('upsert', 'retire', 'recheck')),
    constraint source_reconcile_queue_status_check
        check (status in ('pending', 'claimed', 'processing', 'completed', 'review_required', 'retryable_failed', 'dead_letter', 'cancelled', 'superseded')),
    constraint source_reconcile_queue_review_reason_check
        check (
            review_reason_code is null
            or review_reason_code = any (
                array[
                    'reference_conflict',
                    'trusted_alias_conflict',
                    'spatial_ambiguous',
                    'weak_spatial_overlap',
                    'geometry_missing_for_new_record',
                    'new_site_below_area_floor',
                    'near_existing_site_conflict',
                    'reassignment_conflict',
                    'possible_merge',
                    'possible_split',
                    'retirement_orphan_risk',
                    'data_integrity_anomaly'
                ]::text[]
            )
        )
);

create unique index if not exists source_reconcile_queue_state_uidx
    on landintel.source_reconcile_queue (state_id);

create index if not exists source_reconcile_queue_status_idx
    on landintel.source_reconcile_queue (status, next_attempt_at, priority desc, updated_at);

create table if not exists landintel.canonical_site_refresh_queue (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    refresh_scope text not null,
    trigger_source text,
    source_family text,
    source_record_id text,
    status text not null default 'pending',
    claimed_by text,
    claimed_at timestamptz,
    lease_expires_at timestamptz,
    attempt_count integer not null default 0,
    next_attempt_at timestamptz,
    processed_at timestamptz,
    error_message text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint canonical_site_refresh_queue_scope_check
        check (refresh_scope in ('site_outputs', 'parcel_only', 'full_site_refresh')),
    constraint canonical_site_refresh_queue_status_check
        check (status in ('pending', 'processing', 'completed', 'retryable_failed', 'dead_letter', 'superseded')),
    constraint canonical_site_refresh_queue_family_check
        check (source_family is null or source_family in ('planning', 'hla'))
);

create unique index if not exists canonical_site_refresh_queue_scope_uidx
    on landintel.canonical_site_refresh_queue (canonical_site_id, refresh_scope);

create index if not exists canonical_site_refresh_queue_status_idx
    on landintel.canonical_site_refresh_queue (status, next_attempt_at, updated_at);

create table if not exists landintel.canonical_site_lineage (
    id uuid primary key default gen_random_uuid(),
    lineage_event_type text not null,
    from_canonical_site_id uuid not null references landintel.canonical_sites(id) on delete restrict,
    to_canonical_site_id uuid not null references landintel.canonical_sites(id) on delete restrict,
    resolved_by text not null,
    resolved_at timestamptz not null default now(),
    resolution_notes text,
    trigger_queue_item_id uuid references landintel.source_reconcile_queue(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint canonical_site_lineage_event_check
        check (lineage_event_type in ('merge', 'split', 'manual_reassignment'))
);

alter table landintel.site_source_links
    add column if not exists reconcile_state_id uuid references landintel.source_reconcile_state(id) on delete set null,
    add column if not exists active_flag boolean not null default true,
    add column if not exists retired_at timestamptz;

alter table landintel.site_reference_aliases
    add column if not exists reconcile_state_id uuid references landintel.source_reconcile_state(id) on delete set null,
    add column if not exists active_flag boolean not null default true,
    add column if not exists retired_at timestamptz;

alter table landintel.evidence_references
    add column if not exists reconcile_state_id uuid references landintel.source_reconcile_state(id) on delete set null,
    add column if not exists active_flag boolean not null default true,
    add column if not exists retired_at timestamptz;

create unique index if not exists site_source_links_reconcile_state_uidx
    on landintel.site_source_links (reconcile_state_id);

create unique index if not exists site_reference_aliases_reconcile_state_uidx
    on landintel.site_reference_aliases (reconcile_state_id);

create unique index if not exists evidence_references_reconcile_state_uidx
    on landintel.evidence_references (reconcile_state_id);

create or replace function landintel.normalized_reconcile_reference(value text)
returns text
language sql
immutable
as $$
    select regexp_replace(lower(coalesce(value, '')), '[^a-z0-9]+', '', 'g');
$$;

create or replace function landintel.normalized_polygon_geometry(input_geometry geometry)
returns geometry
language sql
immutable
as $$
    with cleaned as (
        select
            case
                when input_geometry is null or st_isempty(input_geometry) then null::geometry
                else st_multi(st_collectionextract(st_makevalid(input_geometry), 3))
            end as geometry_value
    )
    select
        case
            when geometry_value is null or st_isempty(geometry_value) then null::geometry
            else geometry_value
        end
    from cleaned;
$$;

create or replace function landintel.normalized_geometry_hash(input_geometry geometry)
returns text
language sql
immutable
as $$
    select
        case
            when landintel.normalized_polygon_geometry(input_geometry) is null then null
            else md5(encode(st_asbinary(landintel.normalized_polygon_geometry(input_geometry)), 'hex'))
        end;
$$;

create or replace function landintel.planning_reconcile_signature(
    p_source_record_id text,
    p_authority_name text,
    p_planning_reference text,
    p_proposal_text text,
    p_application_status text,
    p_decision text,
    p_appeal_status text,
    p_raw_payload jsonb,
    p_geometry geometry
)
returns text
language sql
immutable
as $$
    select md5(
        concat_ws(
            '|',
            coalesce(p_source_record_id, ''),
            coalesce(p_authority_name, ''),
            coalesce(p_planning_reference, ''),
            coalesce(p_proposal_text, ''),
            coalesce(p_application_status, ''),
            coalesce(p_decision, ''),
            coalesce(p_appeal_status, ''),
            coalesce(p_raw_payload::text, ''),
            coalesce(landintel.normalized_geometry_hash(p_geometry), '')
        )
    );
$$;

create or replace function landintel.hla_reconcile_signature(
    p_source_record_id text,
    p_authority_name text,
    p_site_reference text,
    p_site_name text,
    p_effectiveness_status text,
    p_programming_horizon text,
    p_constraint_reasons text[],
    p_remaining_capacity integer,
    p_raw_payload jsonb,
    p_geometry geometry
)
returns text
language sql
immutable
as $$
    select md5(
        concat_ws(
            '|',
            coalesce(p_source_record_id, ''),
            coalesce(p_authority_name, ''),
            coalesce(p_site_reference, ''),
            coalesce(p_site_name, ''),
            coalesce(p_effectiveness_status, ''),
            coalesce(p_programming_horizon, ''),
            coalesce(array_to_string(coalesce(p_constraint_reasons, '{}'::text[]), '|'), ''),
            coalesce(p_remaining_capacity::text, ''),
            coalesce(p_raw_payload::text, ''),
            coalesce(landintel.normalized_geometry_hash(p_geometry), '')
        )
    );
$$;

create or replace function landintel.queue_planning_reconcile_from_ingest(p_ingest_run_id uuid)
returns integer
language plpgsql
as $$
declare
    v_now timestamptz := now();
    v_target_authorities text[] := array[]::text[];
    v_total_count integer := 0;
begin
    select coalesce(array_agg(authority_name), array[]::text[])
      into v_target_authorities
    from (
        select jsonb_array_elements_text(coalesce(ingest.metadata -> 'target_authorities', '[]'::jsonb)) as authority_name
        from public.ingest_runs as ingest
        where ingest.id = p_ingest_run_id
    ) as authority_scope;

    with prepared as (
        select
            'planning'::text as source_family,
            'Planning Applications: Official - Scotland'::text as source_dataset,
            planning.authority_name,
            planning.source_record_id,
            landintel.planning_reconcile_signature(
                planning.source_record_id,
                planning.authority_name,
                planning.planning_reference,
                planning.proposal_text,
                planning.application_status,
                planning.decision,
                planning.appeal_status,
                planning.raw_payload,
                planning.geometry
            ) as source_signature,
            landintel.normalized_geometry_hash(planning.geometry) as geometry_hash,
            planning.canonical_site_id
        from landintel.planning_application_records as planning
        where planning.ingest_run_id = p_ingest_run_id
    ),
    upserted_state as (
        insert into landintel.source_reconcile_state (
            source_family,
            source_dataset,
            authority_name,
            source_record_id,
            active_flag,
            lifecycle_status,
            current_source_signature,
            current_geometry_hash,
            last_seen_ingest_run_id,
            last_seen_at,
            current_canonical_site_id,
            previous_canonical_site_id,
            match_method,
            match_confidence,
            publish_state,
            review_required,
            review_reason_code,
            candidate_site_ids,
            metadata,
            updated_at
        )
        select
            prepared.source_family,
            prepared.source_dataset,
            prepared.authority_name,
            prepared.source_record_id,
            true,
            'active',
            prepared.source_signature,
            prepared.geometry_hash,
            p_ingest_run_id,
            v_now,
            prepared.canonical_site_id,
            prepared.canonical_site_id,
            case when prepared.canonical_site_id is not null then 'legacy_link' else null end,
            case when prepared.canonical_site_id is not null then 0.7 else null end,
            case when prepared.canonical_site_id is not null then 'published' else 'blocked' end,
            false,
            null,
            '{}'::uuid[],
            jsonb_build_object('source_table', 'landintel.planning_application_records'),
            v_now
        from prepared
        on conflict (source_family, authority_name, source_record_id) do update
        set source_dataset = excluded.source_dataset,
            active_flag = true,
            lifecycle_status = case
                when landintel.source_reconcile_state.lifecycle_status = 'retired' then 'active'
                else landintel.source_reconcile_state.lifecycle_status
            end,
            current_source_signature = excluded.current_source_signature,
            current_geometry_hash = excluded.current_geometry_hash,
            last_seen_ingest_run_id = excluded.last_seen_ingest_run_id,
            last_seen_at = excluded.last_seen_at,
            metadata = coalesce(landintel.source_reconcile_state.metadata, '{}'::jsonb)
                || jsonb_build_object('source_table', 'landintel.planning_application_records'),
            updated_at = v_now
        returning id, authority_name, source_record_id, current_source_signature, current_geometry_hash, current_canonical_site_id
    ),
    queued as (
        insert into landintel.source_reconcile_queue (
            state_id,
            source_family,
            source_dataset,
            authority_name,
            source_record_id,
            work_type,
            priority,
            status,
            source_signature,
            geometry_hash,
            previous_canonical_site_id,
            candidate_site_ids,
            claimed_by,
            claimed_at,
            lease_expires_at,
            attempt_count,
            next_attempt_at,
            processed_at,
            error_code,
            error_message,
            review_reason_code,
            ingest_run_id,
            metadata,
            updated_at
        )
        select
            state_row.id,
            'planning',
            'Planning Applications: Official - Scotland',
            state_row.authority_name,
            state_row.source_record_id,
            'upsert',
            100,
            'pending',
            state_row.current_source_signature,
            state_row.current_geometry_hash,
            state_row.current_canonical_site_id,
            '{}'::uuid[],
            null,
            null,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            p_ingest_run_id,
            '{}'::jsonb,
            v_now
        from upserted_state as state_row
        on conflict (state_id) do update
        set source_family = excluded.source_family,
            source_dataset = excluded.source_dataset,
            authority_name = excluded.authority_name,
            source_record_id = excluded.source_record_id,
            work_type = excluded.work_type,
            priority = excluded.priority,
            status = 'pending',
            source_signature = excluded.source_signature,
            geometry_hash = excluded.geometry_hash,
            previous_canonical_site_id = excluded.previous_canonical_site_id,
            candidate_site_ids = '{}'::uuid[],
            claimed_by = null,
            claimed_at = null,
            lease_expires_at = null,
            attempt_count = 0,
            next_attempt_at = null,
            processed_at = null,
            error_code = null,
            error_message = null,
            review_reason_code = null,
            ingest_run_id = excluded.ingest_run_id,
            metadata = excluded.metadata,
            updated_at = v_now
        returning id, state_id
    ),
    linked as (
        update landintel.source_reconcile_state as state_row
        set last_queue_item_id = queued.id,
            updated_at = v_now
        from queued
        where state_row.id = queued.state_id
        returning state_row.id
    )
    select count(*) into v_total_count from linked;

    with retire_candidates as (
        select state_row.id, state_row.current_canonical_site_id, state_row.authority_name, state_row.source_record_id
        from landintel.source_reconcile_state as state_row
        where state_row.source_family = 'planning'
          and state_row.active_flag = true
          and (cardinality(v_target_authorities) = 0 or state_row.authority_name = any(v_target_authorities))
          and state_row.last_seen_ingest_run_id is distinct from p_ingest_run_id
    ),
    retired as (
        update landintel.source_reconcile_state as state_row
        set active_flag = false,
            lifecycle_status = 'retired',
            publish_state = 'blocked',
            review_required = false,
            review_reason_code = null,
            candidate_site_ids = '{}'::uuid[],
            updated_at = v_now
        from retire_candidates as candidate
        where state_row.id = candidate.id
        returning state_row.id, state_row.authority_name, state_row.source_record_id, state_row.current_canonical_site_id
    ),
    retire_queue as (
        insert into landintel.source_reconcile_queue (
            state_id,
            source_family,
            source_dataset,
            authority_name,
            source_record_id,
            work_type,
            priority,
            status,
            source_signature,
            geometry_hash,
            previous_canonical_site_id,
            candidate_site_ids,
            claimed_by,
            claimed_at,
            lease_expires_at,
            attempt_count,
            next_attempt_at,
            processed_at,
            error_code,
            error_message,
            review_reason_code,
            ingest_run_id,
            metadata,
            updated_at
        )
        select
            retired.id,
            'planning',
            'Planning Applications: Official - Scotland',
            retired.authority_name,
            retired.source_record_id,
            'retire',
            110,
            'pending',
            null,
            null,
            retired.current_canonical_site_id,
            '{}'::uuid[],
            null,
            null,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            p_ingest_run_id,
            '{}'::jsonb,
            v_now
        from retired
        on conflict (state_id) do update
        set work_type = 'retire',
            priority = 110,
            status = 'pending',
            source_signature = null,
            geometry_hash = null,
            previous_canonical_site_id = excluded.previous_canonical_site_id,
            candidate_site_ids = '{}'::uuid[],
            claimed_by = null,
            claimed_at = null,
            lease_expires_at = null,
            attempt_count = 0,
            next_attempt_at = null,
            processed_at = null,
            error_code = null,
            error_message = null,
            review_reason_code = null,
            ingest_run_id = excluded.ingest_run_id,
            metadata = excluded.metadata,
            updated_at = v_now
        returning id, state_id
    )
    update landintel.source_reconcile_state as state_row
    set last_queue_item_id = retire_queue.id,
        updated_at = v_now
    from retire_queue
    where state_row.id = retire_queue.state_id;

    get diagnostics v_total_count = row_count + v_total_count;
    return v_total_count;
end;
$$;

create or replace function landintel.queue_hla_reconcile_from_ingest(p_ingest_run_id uuid)
returns integer
language plpgsql
as $$
declare
    v_now timestamptz := now();
    v_target_authorities text[] := array[]::text[];
    v_total_count integer := 0;
begin
    select coalesce(array_agg(authority_name), array[]::text[])
      into v_target_authorities
    from (
        select jsonb_array_elements_text(coalesce(ingest.metadata -> 'target_authorities', '[]'::jsonb)) as authority_name
        from public.ingest_runs as ingest
        where ingest.id = p_ingest_run_id
    ) as authority_scope;

    with prepared as (
        select
            'hla'::text as source_family,
            'Housing Land Supply - Scotland'::text as source_dataset,
            hla.authority_name,
            hla.source_record_id,
            landintel.hla_reconcile_signature(
                hla.source_record_id,
                hla.authority_name,
                hla.site_reference,
                hla.site_name,
                hla.effectiveness_status,
                hla.programming_horizon,
                hla.constraint_reasons,
                hla.remaining_capacity,
                hla.raw_payload,
                hla.geometry
            ) as source_signature,
            landintel.normalized_geometry_hash(hla.geometry) as geometry_hash,
            hla.canonical_site_id
        from landintel.hla_site_records as hla
        where hla.ingest_run_id = p_ingest_run_id
    ),
    upserted_state as (
        insert into landintel.source_reconcile_state (
            source_family,
            source_dataset,
            authority_name,
            source_record_id,
            active_flag,
            lifecycle_status,
            current_source_signature,
            current_geometry_hash,
            last_seen_ingest_run_id,
            last_seen_at,
            current_canonical_site_id,
            previous_canonical_site_id,
            match_method,
            match_confidence,
            publish_state,
            review_required,
            review_reason_code,
            candidate_site_ids,
            metadata,
            updated_at
        )
        select
            prepared.source_family,
            prepared.source_dataset,
            prepared.authority_name,
            prepared.source_record_id,
            true,
            'active',
            prepared.source_signature,
            prepared.geometry_hash,
            p_ingest_run_id,
            v_now,
            prepared.canonical_site_id,
            prepared.canonical_site_id,
            case when prepared.canonical_site_id is not null then 'legacy_link' else null end,
            case when prepared.canonical_site_id is not null then 1.0 else null end,
            case when prepared.canonical_site_id is not null then 'published' else 'blocked' end,
            false,
            null,
            '{}'::uuid[],
            jsonb_build_object('source_table', 'landintel.hla_site_records'),
            v_now
        from prepared
        on conflict (source_family, authority_name, source_record_id) do update
        set source_dataset = excluded.source_dataset,
            active_flag = true,
            lifecycle_status = case
                when landintel.source_reconcile_state.lifecycle_status = 'retired' then 'active'
                else landintel.source_reconcile_state.lifecycle_status
            end,
            current_source_signature = excluded.current_source_signature,
            current_geometry_hash = excluded.current_geometry_hash,
            last_seen_ingest_run_id = excluded.last_seen_ingest_run_id,
            last_seen_at = excluded.last_seen_at,
            metadata = coalesce(landintel.source_reconcile_state.metadata, '{}'::jsonb)
                || jsonb_build_object('source_table', 'landintel.hla_site_records'),
            updated_at = v_now
        returning id, authority_name, source_record_id, current_source_signature, current_geometry_hash, current_canonical_site_id
    ),
    queued as (
        insert into landintel.source_reconcile_queue (
            state_id,
            source_family,
            source_dataset,
            authority_name,
            source_record_id,
            work_type,
            priority,
            status,
            source_signature,
            geometry_hash,
            previous_canonical_site_id,
            candidate_site_ids,
            claimed_by,
            claimed_at,
            lease_expires_at,
            attempt_count,
            next_attempt_at,
            processed_at,
            error_code,
            error_message,
            review_reason_code,
            ingest_run_id,
            metadata,
            updated_at
        )
        select
            state_row.id,
            'hla',
            'Housing Land Supply - Scotland',
            state_row.authority_name,
            state_row.source_record_id,
            'upsert',
            100,
            'pending',
            state_row.current_source_signature,
            state_row.current_geometry_hash,
            state_row.current_canonical_site_id,
            '{}'::uuid[],
            null,
            null,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            p_ingest_run_id,
            '{}'::jsonb,
            v_now
        from upserted_state as state_row
        on conflict (state_id) do update
        set source_family = excluded.source_family,
            source_dataset = excluded.source_dataset,
            authority_name = excluded.authority_name,
            source_record_id = excluded.source_record_id,
            work_type = excluded.work_type,
            priority = excluded.priority,
            status = 'pending',
            source_signature = excluded.source_signature,
            geometry_hash = excluded.geometry_hash,
            previous_canonical_site_id = excluded.previous_canonical_site_id,
            candidate_site_ids = '{}'::uuid[],
            claimed_by = null,
            claimed_at = null,
            lease_expires_at = null,
            attempt_count = 0,
            next_attempt_at = null,
            processed_at = null,
            error_code = null,
            error_message = null,
            review_reason_code = null,
            ingest_run_id = excluded.ingest_run_id,
            metadata = excluded.metadata,
            updated_at = v_now
        returning id, state_id
    ),
    linked as (
        update landintel.source_reconcile_state as state_row
        set last_queue_item_id = queued.id,
            updated_at = v_now
        from queued
        where state_row.id = queued.state_id
        returning state_row.id
    )
    select count(*) into v_total_count from linked;

    with retire_candidates as (
        select state_row.id, state_row.current_canonical_site_id, state_row.authority_name, state_row.source_record_id
        from landintel.source_reconcile_state as state_row
        where state_row.source_family = 'hla'
          and state_row.active_flag = true
          and (cardinality(v_target_authorities) = 0 or state_row.authority_name = any(v_target_authorities))
          and state_row.last_seen_ingest_run_id is distinct from p_ingest_run_id
    ),
    retired as (
        update landintel.source_reconcile_state as state_row
        set active_flag = false,
            lifecycle_status = 'retired',
            publish_state = 'blocked',
            review_required = false,
            review_reason_code = null,
            candidate_site_ids = '{}'::uuid[],
            updated_at = v_now
        from retire_candidates as candidate
        where state_row.id = candidate.id
        returning state_row.id, state_row.authority_name, state_row.source_record_id, state_row.current_canonical_site_id
    ),
    retire_queue as (
        insert into landintel.source_reconcile_queue (
            state_id,
            source_family,
            source_dataset,
            authority_name,
            source_record_id,
            work_type,
            priority,
            status,
            source_signature,
            geometry_hash,
            previous_canonical_site_id,
            candidate_site_ids,
            claimed_by,
            claimed_at,
            lease_expires_at,
            attempt_count,
            next_attempt_at,
            processed_at,
            error_code,
            error_message,
            review_reason_code,
            ingest_run_id,
            metadata,
            updated_at
        )
        select
            retired.id,
            'hla',
            'Housing Land Supply - Scotland',
            retired.authority_name,
            retired.source_record_id,
            'retire',
            110,
            'pending',
            null,
            null,
            retired.current_canonical_site_id,
            '{}'::uuid[],
            null,
            null,
            null,
            0,
            null,
            null,
            null,
            null,
            null,
            p_ingest_run_id,
            '{}'::jsonb,
            v_now
        from retired
        on conflict (state_id) do update
        set work_type = 'retire',
            priority = 110,
            status = 'pending',
            source_signature = null,
            geometry_hash = null,
            previous_canonical_site_id = excluded.previous_canonical_site_id,
            candidate_site_ids = '{}'::uuid[],
            claimed_by = null,
            claimed_at = null,
            lease_expires_at = null,
            attempt_count = 0,
            next_attempt_at = null,
            processed_at = null,
            error_code = null,
            error_message = null,
            review_reason_code = null,
            ingest_run_id = excluded.ingest_run_id,
            metadata = excluded.metadata,
            updated_at = v_now
        returning id, state_id
    )
    update landintel.source_reconcile_state as state_row
    set last_queue_item_id = retire_queue.id,
        updated_at = v_now
    from retire_queue
    where state_row.id = retire_queue.state_id;

    get diagnostics v_total_count = row_count + v_total_count;
    return v_total_count;
end;
$$;

create or replace function landintel.handle_successful_reconcile_enqueue()
returns trigger
language plpgsql
as $$
begin
    if new.status <> 'success' or coalesce(old.status, '') = 'success' then
        return new;
    end if;

    if new.run_type = 'ingest_planning_history' then
        perform landintel.queue_planning_reconcile_from_ingest(new.id);
    elsif new.run_type = 'ingest_hla' then
        perform landintel.queue_hla_reconcile_from_ingest(new.id);
    end if;

    return new;
end;
$$;

drop trigger if exists ingest_runs_incremental_reconcile_queue_trigger on public.ingest_runs;

create trigger ingest_runs_incremental_reconcile_queue_trigger
after update of status on public.ingest_runs
for each row
execute function landintel.handle_successful_reconcile_enqueue();

alter table landintel.source_reconcile_state enable row level security;
alter table landintel.source_reconcile_queue enable row level security;
alter table landintel.canonical_site_refresh_queue enable row level security;
alter table landintel.canonical_site_lineage enable row level security;

revoke all on table landintel.source_reconcile_state from anon, authenticated;
revoke all on table landintel.source_reconcile_queue from anon, authenticated;
revoke all on table landintel.canonical_site_refresh_queue from anon, authenticated;
revoke all on table landintel.canonical_site_lineage from anon, authenticated;

grant select on table landintel.source_reconcile_state to authenticated;
grant select on table landintel.source_reconcile_queue to authenticated;
grant select on table landintel.canonical_site_refresh_queue to authenticated;
grant select on table landintel.canonical_site_lineage to authenticated;

drop policy if exists source_reconcile_state_authenticated_select on landintel.source_reconcile_state;
create policy source_reconcile_state_authenticated_select
    on landintel.source_reconcile_state
    for select
    to authenticated
    using (true);

drop policy if exists source_reconcile_queue_authenticated_select on landintel.source_reconcile_queue;
create policy source_reconcile_queue_authenticated_select
    on landintel.source_reconcile_queue
    for select
    to authenticated
    using (true);

drop policy if exists canonical_site_refresh_queue_authenticated_select on landintel.canonical_site_refresh_queue;
create policy canonical_site_refresh_queue_authenticated_select
    on landintel.canonical_site_refresh_queue
    for select
    to authenticated
    using (true);

drop policy if exists canonical_site_lineage_authenticated_select on landintel.canonical_site_lineage;
create policy canonical_site_lineage_authenticated_select
    on landintel.canonical_site_lineage
    for select
    to authenticated
    using (true);

create or replace view analytics.v_source_link_publish_status
with (security_invoker = true) as
with site_rollup as (
    select
        state_row.current_canonical_site_id as canonical_site_id,
        bool_or(state_row.publish_state = 'provisional') as has_provisional_source_links,
        bool_or(
            state_row.publish_state = 'blocked'
            and state_row.review_reason_code in ('possible_merge', 'possible_split', 'reassignment_conflict')
        ) as has_blocked_structural_links,
        case
            when bool_or(
                state_row.publish_state = 'blocked'
                and state_row.review_reason_code in ('possible_merge', 'possible_split', 'reassignment_conflict')
            ) then 'Blocked — structural review required'
            when bool_or(state_row.publish_state = 'provisional') then 'Contains provisional source links'
            else 'Confirmed source links'
        end as site_publish_state_label
    from landintel.source_reconcile_state as state_row
    where state_row.current_canonical_site_id is not null
    group by state_row.current_canonical_site_id
)
select
    state_row.id as reconcile_state_id,
    state_row.source_family,
    state_row.source_dataset,
    state_row.authority_name,
    state_row.source_record_id,
    state_row.lifecycle_status,
    state_row.publish_state,
    state_row.review_required,
    state_row.review_reason_code,
    case when state_row.publish_state = 'published' then true else false end as confirmed_link_flag,
    case
        when state_row.publish_state = 'provisional' then 'Provisional link — analyst review required'
        when state_row.publish_state = 'blocked'
            and state_row.review_reason_code in ('possible_merge', 'possible_split', 'reassignment_conflict')
            then 'Blocked — structural review required'
        when state_row.publish_state = 'blocked' then 'Blocked — analyst review required'
        else 'Confirmed link'
    end as publish_state_label,
    state_row.current_canonical_site_id as canonical_site_id,
    site.site_code,
    site.site_name_primary,
    state_row.previous_canonical_site_id,
    link_row.active_flag as active_link_flag,
    link_row.link_method,
    link_row.confidence as link_confidence,
    coalesce(site_rollup.has_provisional_source_links, false) as has_provisional_source_links,
    coalesce(site_rollup.has_blocked_structural_links, false) as has_blocked_structural_links,
    coalesce(site_rollup.site_publish_state_label, 'Confirmed source links') as site_publish_state_label,
    state_row.last_processed_at,
    state_row.updated_at
from landintel.source_reconcile_state as state_row
left join landintel.canonical_sites as site
  on site.id = state_row.current_canonical_site_id
left join landintel.site_source_links as link_row
  on link_row.reconcile_state_id = state_row.id
 and link_row.active_flag = true
left join site_rollup
  on site_rollup.canonical_site_id = state_row.current_canonical_site_id;

create or replace view analytics.v_reconcile_review_queue
with (security_invoker = true) as
select
    queue_row.id as queue_item_id,
    queue_row.work_type,
    queue_row.status,
    queue_row.priority,
    state_row.source_family,
    state_row.source_dataset,
    state_row.authority_name,
    state_row.source_record_id,
    state_row.publish_state,
    state_row.review_required,
    state_row.review_reason_code,
    case
        when state_row.publish_state = 'provisional' then 'Provisional link — analyst review required'
        when state_row.publish_state = 'blocked'
            and state_row.review_reason_code in ('possible_merge', 'possible_split', 'reassignment_conflict')
            then 'Blocked — structural review required'
        when state_row.publish_state = 'blocked' then 'Blocked — analyst review required'
        else 'Confirmed link'
    end as publish_state_label,
    state_row.current_canonical_site_id,
    current_site.site_code as current_site_code,
    current_site.site_name_primary as current_site_name,
    state_row.previous_canonical_site_id,
    previous_site.site_code as previous_site_code,
    previous_site.site_name_primary as previous_site_name,
    state_row.candidate_site_ids,
    queue_row.attempt_count,
    queue_row.error_code,
    queue_row.error_message,
    queue_row.next_attempt_at,
    queue_row.claimed_at,
    queue_row.lease_expires_at,
    queue_row.updated_at
from landintel.source_reconcile_state as state_row
join landintel.source_reconcile_queue as queue_row
  on queue_row.state_id = state_row.id
left join landintel.canonical_sites as current_site
  on current_site.id = state_row.current_canonical_site_id
left join landintel.canonical_sites as previous_site
  on previous_site.id = state_row.previous_canonical_site_id
where queue_row.status in ('review_required', 'dead_letter')
   or state_row.review_required = true
   or state_row.publish_state <> 'published';

create or replace view analytics.v_reconcile_queue_health
with (security_invoker = true) as
with reconcile_queue as (
    select
        source_family,
        count(*) filter (where status = 'pending') as pending_count,
        count(*) filter (where status = 'processing') as processing_count,
        count(*) filter (where status = 'review_required') as review_required_count,
        count(*) filter (where status = 'retryable_failed') as retryable_failed_count,
        count(*) filter (where status = 'dead_letter') as dead_letter_count,
        count(*) filter (where status = 'processing' and lease_expires_at < now()) as stale_processing_count,
        min(created_at) filter (where status = 'pending') as oldest_pending_created_at,
        max(processed_at) as last_processed_at
    from landintel.source_reconcile_queue
    group by source_family
),
refresh_queue as (
    select
        count(*) filter (where status = 'pending') as refresh_pending_count,
        count(*) filter (where status = 'processing') as refresh_processing_count,
        count(*) filter (where status = 'retryable_failed') as refresh_retryable_failed_count,
        count(*) filter (where status = 'dead_letter') as refresh_dead_letter_count,
        count(*) filter (where status = 'processing' and lease_expires_at < now()) as refresh_stale_processing_count,
        min(created_at) filter (where status = 'pending') as oldest_refresh_pending_created_at,
        max(processed_at) as last_refresh_processed_at
    from landintel.canonical_site_refresh_queue
)
select
    reconcile_queue.source_family,
    reconcile_queue.pending_count,
    reconcile_queue.processing_count,
    reconcile_queue.review_required_count,
    reconcile_queue.retryable_failed_count,
    reconcile_queue.dead_letter_count,
    reconcile_queue.stale_processing_count,
    case
        when reconcile_queue.oldest_pending_created_at is null then null
        else now() - reconcile_queue.oldest_pending_created_at
    end as oldest_pending_age,
    reconcile_queue.last_processed_at,
    refresh_queue.refresh_pending_count,
    refresh_queue.refresh_processing_count,
    refresh_queue.refresh_retryable_failed_count,
    refresh_queue.refresh_dead_letter_count,
    refresh_queue.refresh_stale_processing_count,
    case
        when refresh_queue.oldest_refresh_pending_created_at is null then null
        else now() - refresh_queue.oldest_refresh_pending_created_at
    end as oldest_refresh_pending_age,
    refresh_queue.last_refresh_processed_at
from reconcile_queue
cross join refresh_queue;

create or replace view analytics.v_reconcile_drift_summary
with (security_invoker = true) as
with planning_without_state as (
    select count(*)::bigint as count_value
    from landintel.planning_application_records as planning
    left join landintel.source_reconcile_state as state_row
      on state_row.source_family = 'planning'
     and state_row.authority_name = planning.authority_name
     and state_row.source_record_id = planning.source_record_id
    where state_row.id is null
),
hla_without_state as (
    select count(*)::bigint as count_value
    from landintel.hla_site_records as hla
    left join landintel.source_reconcile_state as state_row
      on state_row.source_family = 'hla'
     and state_row.authority_name = hla.authority_name
     and state_row.source_record_id = hla.source_record_id
    where state_row.id is null
),
active_states_without_source as (
    select count(*)::bigint as count_value
    from landintel.source_reconcile_state as state_row
    where state_row.active_flag = true
      and (
        (state_row.source_family = 'planning' and not exists (
            select 1
            from landintel.planning_application_records as planning
            where planning.authority_name = state_row.authority_name
              and planning.source_record_id = state_row.source_record_id
        ))
        or
        (state_row.source_family = 'hla' and not exists (
            select 1
            from landintel.hla_site_records as hla
            where hla.authority_name = state_row.authority_name
              and hla.source_record_id = state_row.source_record_id
        ))
      )
),
retired_states_with_live_link as (
    select count(*)::bigint as count_value
    from landintel.source_reconcile_state as state_row
    join landintel.site_source_links as link_row
      on link_row.reconcile_state_id = state_row.id
     and link_row.active_flag = true
    where state_row.lifecycle_status = 'retired'
),
active_states_without_live_link as (
    select count(*)::bigint as count_value
    from landintel.source_reconcile_state as state_row
    where state_row.active_flag = true
      and state_row.current_canonical_site_id is not null
      and not exists (
          select 1
          from landintel.site_source_links as link_row
          where link_row.reconcile_state_id = state_row.id
            and link_row.active_flag = true
      )
),
stale_claims as (
    select
        (
            select count(*)::bigint
            from landintel.source_reconcile_queue
            where status = 'processing'
              and lease_expires_at < now()
        )
        +
        (
            select count(*)::bigint
            from landintel.canonical_site_refresh_queue
            where status = 'processing'
              and lease_expires_at < now()
        ) as count_value
)
select
    planning_without_state.count_value as planning_records_without_state,
    hla_without_state.count_value as hla_records_without_state,
    active_states_without_source.count_value as active_states_without_source,
    retired_states_with_live_link.count_value as retired_states_with_live_link,
    active_states_without_live_link.count_value as active_states_without_live_link,
    stale_claims.count_value as stale_claim_count
from planning_without_state
cross join hla_without_state
cross join active_states_without_source
cross join retired_states_with_live_link
cross join active_states_without_live_link
cross join stale_claims;

grant select on table analytics.v_source_link_publish_status to authenticated;
grant select on table analytics.v_reconcile_review_queue to authenticated;
grant select on table analytics.v_reconcile_queue_health to authenticated;
grant select on table analytics.v_reconcile_drift_summary to authenticated;

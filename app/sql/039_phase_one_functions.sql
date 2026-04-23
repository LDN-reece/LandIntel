create or replace function public.constraints_site_anchor()
returns table(
    site_id text,
    site_location_id text,
    site_name text,
    authority_name text,
    geometry geometry,
    area_sqm numeric,
    area_acres numeric,
    location_label text,
    location_role text
)
language plpgsql
stable
set search_path = pg_catalog, public, landintel
as $$
begin
    if to_regclass('landintel.canonical_sites') is null then
        return;
    end if;

    return query
    select
        site.id::text as site_id,
        site.id::text as site_location_id,
        coalesce(
            nullif(btrim(site.site_name_primary), ''),
            nullif(btrim(site.site_code), ''),
            site.id::text
        ) as site_name,
        site.authority_name,
        site.geometry,
        round(st_area(site.geometry)::numeric, 2) as area_sqm,
        coalesce(site.area_acres, public.calculate_area_acres(st_area(site.geometry)::numeric)) as area_acres,
        'Canonical site geometry'::text as location_label,
        'canonical_site'::text as location_role
    from landintel.canonical_sites as site
    where site.geometry is not null;
end;
$$;

create or replace function landintel.enqueue_canonical_site_refresh(
    p_canonical_site_id uuid,
    p_refresh_scope text default 'assessment',
    p_trigger_source text default 'manual',
    p_source_family text default null,
    p_source_record_id text default null,
    p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
declare
    v_queue_id uuid;
begin
    if p_canonical_site_id is null then
        return null;
    end if;

    insert into landintel.canonical_site_refresh_queue (
        canonical_site_id,
        refresh_scope,
        trigger_source,
        source_family,
        source_record_id,
        status,
        metadata
    )
    values (
        p_canonical_site_id,
        coalesce(nullif(p_refresh_scope, ''), 'assessment'),
        coalesce(nullif(p_trigger_source, ''), 'manual'),
        p_source_family,
        p_source_record_id,
        'pending',
        coalesce(p_metadata, '{}'::jsonb)
    )
    returning id into v_queue_id;

    return v_queue_id;
end;
$$;

create or replace function landintel.record_site_change_event(
    p_canonical_site_id uuid,
    p_source_family text,
    p_change_category text,
    p_event_type text,
    p_event_summary text,
    p_source_record_id text default null,
    p_alert_priority text default 'normal',
    p_resurfaced_flag boolean default false,
    p_metadata jsonb default '{}'::jsonb,
    p_enqueue_refresh boolean default true
)
returns uuid
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
declare
    v_event_id uuid;
begin
    insert into landintel.site_change_events (
        canonical_site_id,
        source_family,
        change_category,
        event_type,
        event_summary,
        source_record_id,
        alert_priority,
        resurfaced_flag,
        metadata
    )
    values (
        p_canonical_site_id,
        coalesce(nullif(p_source_family, ''), 'system'),
        coalesce(nullif(p_change_category, ''), 'general_change'),
        coalesce(nullif(p_event_type, ''), 'general_update'),
        coalesce(nullif(p_event_summary, ''), 'Site updated.'),
        p_source_record_id,
        case
            when p_alert_priority in ('normal', 'high', 'critical') then p_alert_priority
            else 'normal'
        end,
        coalesce(p_resurfaced_flag, false),
        coalesce(p_metadata, '{}'::jsonb)
    )
    returning id into v_event_id;

    if coalesce(p_enqueue_refresh, true) and p_canonical_site_id is not null then
        perform landintel.enqueue_canonical_site_refresh(
            p_canonical_site_id,
            'assessment',
            coalesce(nullif(p_event_type, ''), 'general_update'),
            p_source_family,
            p_source_record_id,
            jsonb_build_object('change_event_id', v_event_id) || coalesce(p_metadata, '{}'::jsonb)
        );
    end if;

    return v_event_id;
end;
$$;

create or replace function landintel.record_planning_change_event(
    p_canonical_site_id uuid,
    p_event_type text,
    p_event_summary text,
    p_source_record_id text default null,
    p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
begin
    return landintel.record_site_change_event(
        p_canonical_site_id,
        'planning',
        'planning_change',
        p_event_type,
        p_event_summary,
        p_source_record_id,
        case
            when p_event_type in ('new_application', 'refusal', 'withdrawal', 'lapse') then 'high'
            else 'normal'
        end,
        true,
        p_metadata,
        true
    );
end;
$$;

create or replace function landintel.record_policy_change_event(
    p_canonical_site_id uuid,
    p_event_type text,
    p_event_summary text,
    p_source_record_id text default null,
    p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
begin
    return landintel.record_site_change_event(
        p_canonical_site_id,
        'policy',
        'policy_change',
        p_event_type,
        p_event_summary,
        p_source_record_id,
        case
            when p_event_type in ('allocation_change', 'settlement_boundary_change', 'ldp_release') then 'high'
            else 'normal'
        end,
        true,
        p_metadata,
        true
    );
end;
$$;

create or replace function landintel.record_title_review_event(
    p_canonical_site_id uuid,
    p_event_type text,
    p_actor_name text default 'system',
    p_reason_text text default null,
    p_title_number text default null,
    p_metadata jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
declare
    v_event_id uuid;
begin
    insert into landintel.site_review_events (
        canonical_site_id,
        event_type,
        review_status,
        actor_name,
        reason_text,
        title_number,
        metadata
    )
    values (
        p_canonical_site_id,
        case
            when p_event_type in ('buy_title_now', 'title_ordered', 'title_reviewed') then p_event_type
            else 'title_reviewed'
        end,
        case
            when p_event_type = 'buy_title_now' then 'Buy title now'
            when p_event_type = 'title_ordered' then 'Title ordered'
            when p_event_type = 'title_reviewed' then 'Title reviewed'
            else null
        end,
        coalesce(nullif(p_actor_name, ''), 'system'),
        p_reason_text,
        p_title_number,
        coalesce(p_metadata, '{}'::jsonb)
    )
    returning id into v_event_id;

    perform landintel.record_site_change_event(
        p_canonical_site_id,
        'title',
        'title_change',
        coalesce(nullif(p_event_type, ''), 'title_reviewed'),
        case
            when p_event_type = 'buy_title_now' then 'Title purchase requested for human follow-up.'
            when p_event_type = 'title_ordered' then 'Title has been ordered for review.'
            when p_event_type = 'title_reviewed' then 'Title review outcome recorded.'
            else 'Title review event recorded.'
        end,
        p_title_number,
        case when p_event_type = 'title_reviewed' then 'high' else 'normal' end,
        false,
        jsonb_build_object('site_review_event_id', v_event_id) || coalesce(p_metadata, '{}'::jsonb),
        true
    );

    return v_event_id;
end;
$$;

create or replace function landintel.publish_reconciled_planning_links(p_limit integer default 1000)
returns integer
language plpgsql
set search_path = pg_catalog, public, landintel
as $$
declare
    v_now timestamptz := now();
    v_published_count integer := 0;
begin
    if to_regclass('landintel.source_reconcile_state') is null then
        return 0;
    end if;

    with candidates as (
        select
            state.id as state_id,
            state.source_family,
            state.source_dataset,
            state.authority_name,
            state.source_record_id,
            state.current_canonical_site_id as canonical_site_id,
            state.match_method,
            state.match_confidence,
            state.current_geometry_hash,
            planning.id as planning_id,
            planning.planning_reference,
            planning.proposal_text,
            planning.source_registry_id,
            planning.ingest_run_id
        from landintel.source_reconcile_state as state
        join landintel.planning_application_records as planning
          on planning.source_record_id = state.source_record_id
         and planning.authority_name is not distinct from state.authority_name
        where state.source_family = 'planning'
          and state.active_flag = true
          and state.current_canonical_site_id is not null
          and coalesce(state.publish_state, 'pending') <> 'published'
        order by coalesce(state.last_processed_at, state.updated_at, state.last_seen_at) asc nulls first
        limit greatest(coalesce(p_limit, 1000), 1)
    ),
    updated_planning as (
        update landintel.planning_application_records as planning
        set canonical_site_id = candidate.canonical_site_id,
            updated_at = v_now
        from candidates as candidate
        where planning.id = candidate.planning_id
        returning candidate.*
    ),
    retired_links as (
        update landintel.site_source_links as link
        set active_flag = false,
            retired_at = v_now,
            updated_at = v_now
        where link.source_family = 'planning'
          and link.active_flag = true
          and link.source_record_id in (select source_record_id from updated_planning)
          and link.canonical_site_id not in (select canonical_site_id from updated_planning)
        returning link.id
    ),
    retired_aliases as (
        update landintel.site_reference_aliases as alias
        set active_flag = false,
            retired_at = v_now,
            updated_at = v_now
        where alias.source_family = 'planning'
          and alias.active_flag = true
          and coalesce(alias.metadata ->> 'source_record_id', alias.planning_reference, alias.raw_reference_value) in (
                select source_record_id
                from updated_planning
                union
                select planning_reference
                from updated_planning
                where planning_reference is not null
            )
          and alias.canonical_site_id not in (select canonical_site_id from updated_planning)
        returning alias.id
    ),
    retired_evidence as (
        update landintel.evidence_references as evidence
        set active_flag = false,
            retired_at = v_now
        where evidence.source_family = 'planning'
          and evidence.active_flag = true
          and evidence.source_record_id in (select source_record_id from updated_planning)
          and evidence.canonical_site_id not in (select canonical_site_id from updated_planning)
        returning evidence.id
    ),
    inserted_links as (
        insert into landintel.site_source_links (
            canonical_site_id,
            source_family,
            source_dataset,
            source_record_id,
            link_method,
            confidence,
            source_registry_id,
            ingest_run_id,
            metadata,
            created_at,
            updated_at,
            reconcile_state_id,
            active_flag,
            retired_at
        )
        select
            candidate.canonical_site_id,
            'planning',
            coalesce(candidate.source_dataset, 'Planning Applications: Official - Scotland'),
            candidate.source_record_id,
            coalesce(candidate.match_method, 'reconcile_publish'),
            candidate.match_confidence,
            candidate.source_registry_id,
            candidate.ingest_run_id,
            jsonb_build_object(
                'planning_reference', candidate.planning_reference,
                'proposal_text', candidate.proposal_text,
                'published_by', 'landintel.publish_reconciled_planning_links'
            ),
            v_now,
            v_now,
            candidate.state_id,
            true,
            null
        from updated_planning as candidate
        where not exists (
            select 1
            from landintel.site_source_links as existing
            where existing.canonical_site_id = candidate.canonical_site_id
              and existing.source_family = 'planning'
              and existing.source_dataset = coalesce(candidate.source_dataset, 'Planning Applications: Official - Scotland')
              and existing.source_record_id = candidate.source_record_id
              and existing.active_flag = true
        )
        returning canonical_site_id, source_record_id
    ),
    inserted_aliases as (
        insert into landintel.site_reference_aliases (
            canonical_site_id,
            source_family,
            source_dataset,
            authority_name,
            plan_period,
            site_name,
            raw_reference_value,
            normalized_reference_value,
            planning_reference,
            geometry_hash,
            status,
            confidence,
            source_registry_id,
            ingest_run_id,
            metadata,
            created_at,
            updated_at,
            reconcile_state_id,
            active_flag,
            retired_at
        )
        select
            candidate.canonical_site_id,
            'planning',
            coalesce(candidate.source_dataset, 'Planning Applications: Official - Scotland'),
            candidate.authority_name,
            null,
            canonical.site_name_primary,
            coalesce(candidate.planning_reference, candidate.source_record_id),
            lower(regexp_replace(coalesce(candidate.planning_reference, candidate.source_record_id), '[^a-zA-Z0-9]+', '', 'g')),
            candidate.planning_reference,
            candidate.current_geometry_hash,
            'matched',
            candidate.match_confidence,
            candidate.source_registry_id,
            candidate.ingest_run_id,
            jsonb_build_object(
                'published_by', 'landintel.publish_reconciled_planning_links',
                'source_record_id', candidate.source_record_id
            ),
            v_now,
            v_now,
            candidate.state_id,
            true,
            null
        from updated_planning as candidate
        join landintel.canonical_sites as canonical
          on canonical.id = candidate.canonical_site_id
        where coalesce(candidate.planning_reference, candidate.source_record_id) is not null
          and not exists (
            select 1
            from landintel.site_reference_aliases as existing
            where existing.canonical_site_id = candidate.canonical_site_id
              and existing.source_family = 'planning'
              and existing.source_dataset = coalesce(candidate.source_dataset, 'Planning Applications: Official - Scotland')
              and coalesce(existing.raw_reference_value, '') = coalesce(candidate.planning_reference, candidate.source_record_id)
              and existing.active_flag = true
        )
        returning canonical_site_id, raw_reference_value
    ),
    inserted_evidence as (
        insert into landintel.evidence_references (
            canonical_site_id,
            source_family,
            source_dataset,
            source_record_id,
            source_reference,
            source_url,
            confidence,
            source_registry_id,
            ingest_run_id,
            metadata,
            created_at,
            reconcile_state_id,
            active_flag,
            retired_at
        )
        select
            candidate.canonical_site_id,
            'planning',
            coalesce(candidate.source_dataset, 'Planning Applications: Official - Scotland'),
            candidate.source_record_id,
            coalesce(candidate.planning_reference, candidate.source_record_id),
            null,
            case
                when coalesce(candidate.match_confidence, 0) >= 0.85 then 'high'
                when coalesce(candidate.match_confidence, 0) >= 0.65 then 'medium'
                else 'low'
            end,
            candidate.source_registry_id,
            candidate.ingest_run_id,
            jsonb_build_object(
                'proposal_text', candidate.proposal_text,
                'published_by', 'landintel.publish_reconciled_planning_links'
            ),
            v_now,
            candidate.state_id,
            true,
            null
        from updated_planning as candidate
        where not exists (
            select 1
            from landintel.evidence_references as existing
            where existing.canonical_site_id = candidate.canonical_site_id
              and existing.source_family = 'planning'
              and existing.source_dataset = coalesce(candidate.source_dataset, 'Planning Applications: Official - Scotland')
              and coalesce(existing.source_record_id, '') = coalesce(candidate.source_record_id, '')
              and existing.active_flag = true
        )
        returning canonical_site_id
    ),
    changed_events as (
        insert into landintel.site_change_events (
            canonical_site_id,
            source_family,
            change_category,
            event_type,
            event_summary,
            source_record_id,
            alert_priority,
            resurfaced_flag,
            metadata,
            created_at
        )
        select
            candidate.canonical_site_id,
            'planning',
            'planning_change',
            'planning_published',
            'Planning evidence was linked into the canonical site spine.',
            candidate.source_record_id,
            'high',
            true,
            jsonb_build_object(
                'planning_reference', candidate.planning_reference,
                'published_by', 'landintel.publish_reconciled_planning_links'
            ),
            v_now
        from updated_planning as candidate
        returning canonical_site_id
    ),
    queued_refresh as (
        select landintel.enqueue_canonical_site_refresh(
            candidate.canonical_site_id,
            'assessment',
            'planning_publish',
            'planning',
            candidate.source_record_id,
            jsonb_build_object(
                'planning_reference', candidate.planning_reference,
                'source_record_id', candidate.source_record_id
            )
        ) as queue_id
        from updated_planning as candidate
    ),
    updated_state as (
        update landintel.source_reconcile_state as state
        set publish_state = 'published',
            review_required = false,
            review_reason_code = null,
            last_processed_at = v_now,
            updated_at = v_now
        where state.id in (select state_id from updated_planning)
        returning state.id
    )
    select count(*) into v_published_count
    from updated_state;

    return coalesce(v_published_count, 0);
end;
$$;

drop trigger if exists trg_touch_updated_at_landintel_canonical_sites on landintel.canonical_sites;
create trigger trg_touch_updated_at_landintel_canonical_sites
before update on landintel.canonical_sites
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_planning_application_records on landintel.planning_application_records;
create trigger trg_touch_updated_at_landintel_planning_application_records
before update on landintel.planning_application_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_hla_site_records on landintel.hla_site_records;
create trigger trg_touch_updated_at_landintel_hla_site_records
before update on landintel.hla_site_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_ldp_site_records on landintel.ldp_site_records;
create trigger trg_touch_updated_at_landintel_ldp_site_records
before update on landintel.ldp_site_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_settlement_boundary_records on landintel.settlement_boundary_records;
create trigger trg_touch_updated_at_landintel_settlement_boundary_records
before update on landintel.settlement_boundary_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_bgs_records on landintel.bgs_records;
create trigger trg_touch_updated_at_landintel_bgs_records
before update on landintel.bgs_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_flood_records on landintel.flood_records;
create trigger trg_touch_updated_at_landintel_flood_records
before update on landintel.flood_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_ela_site_records on landintel.ela_site_records;
create trigger trg_touch_updated_at_landintel_ela_site_records
before update on landintel.ela_site_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_vdl_site_records on landintel.vdl_site_records;
create trigger trg_touch_updated_at_landintel_vdl_site_records
before update on landintel.vdl_site_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_site_source_links on landintel.site_source_links;
create trigger trg_touch_updated_at_landintel_site_source_links
before update on landintel.site_source_links
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_site_reference_aliases on landintel.site_reference_aliases;
create trigger trg_touch_updated_at_landintel_site_reference_aliases
before update on landintel.site_reference_aliases
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_site_signals on landintel.site_signals;
create trigger trg_touch_updated_at_landintel_site_signals
before update on landintel.site_signals
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_site_assessments on landintel.site_assessments;
create trigger trg_touch_updated_at_landintel_site_assessments
before update on landintel.site_assessments
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_canonical_site_refresh_queue on landintel.canonical_site_refresh_queue;
create trigger trg_touch_updated_at_landintel_canonical_site_refresh_queue
before update on landintel.canonical_site_refresh_queue
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_source_reconcile_state on landintel.source_reconcile_state;
create trigger trg_touch_updated_at_landintel_source_reconcile_state
before update on landintel.source_reconcile_state
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_source_reconcile_queue on landintel.source_reconcile_queue;
create trigger trg_touch_updated_at_landintel_source_reconcile_queue
before update on landintel.source_reconcile_queue
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_landintel_site_geometry_diagnostics on landintel.site_geometry_diagnostics;
create trigger trg_touch_updated_at_landintel_site_geometry_diagnostics
before update on landintel.site_geometry_diagnostics
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_site_spatial_links on public.site_spatial_links;
create trigger trg_touch_updated_at_public_site_spatial_links
before update on public.site_spatial_links
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_site_title_validation on public.site_title_validation;
create trigger trg_touch_updated_at_public_site_title_validation
before update on public.site_title_validation
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_constraint_layer_registry on public.constraint_layer_registry;
create trigger trg_touch_updated_at_public_constraint_layer_registry
before update on public.constraint_layer_registry
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_constraint_source_features on public.constraint_source_features;
create trigger trg_touch_updated_at_public_constraint_source_features
before update on public.constraint_source_features
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_site_constraint_measurements on public.site_constraint_measurements;
create trigger trg_touch_updated_at_public_site_constraint_measurements
before update on public.site_constraint_measurements
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_site_constraint_group_summaries on public.site_constraint_group_summaries;
create trigger trg_touch_updated_at_public_site_constraint_group_summaries
before update on public.site_constraint_group_summaries
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_public_site_commercial_friction_facts on public.site_commercial_friction_facts;
create trigger trg_touch_updated_at_public_site_commercial_friction_facts
before update on public.site_commercial_friction_facts
for each row
execute function public.touch_updated_at();

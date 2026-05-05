create schema if not exists landintel_store;
create schema if not exists landintel_reporting;
create schema if not exists landintel_sourced;

create or replace view landintel_store.v_register_context_records_clean
with (security_invoker = true) as
with raw_registers as (
    select
        hla.id::text as source_row_id,
        'landintel.hla_site_records'::text as source_table,
        'hla'::text as source_family,
        'housing_land_supply_spatialhub'::text as source_key,
        'housing_land_supply_context'::text as source_role,
        'planning_supply_visibility'::text as evidence_role,
        'low_to_medium'::text as commercial_weight,
        true as corroboration_required,
        'HLA is register/context evidence only. It supports housing land supply visibility but does not prove availability, deliverability, clean ownership, buyer depth or commercial viability.'::text as limitation_text,
        hla.source_record_id,
        hla.canonical_site_id,
        hla.authority_name,
        hla.site_reference,
        hla.site_name,
        hla.effectiveness_status as status_text,
        hla.geometry,
        null::text as source_record_signature,
        md5(coalesce(st_astext(hla.geometry), hla.source_record_id, hla.site_reference, hla.id::text)) as geometry_hash,
        hla.raw_payload,
        hla.created_at,
        hla.updated_at,
        10::integer as source_priority
    from landintel.hla_site_records as hla

    union all

    select
        ela.id::text as source_row_id,
        'landintel.ela_site_records'::text as source_table,
        'ela'::text as source_family,
        ela.source_key,
        'emerging_land_context'::text as source_role,
        'policy_or_candidate_visibility'::text as evidence_role,
        'low_to_medium'::text as commercial_weight,
        true as corroboration_required,
        'ELA is register/context evidence only. It makes land visible as candidate/emerging context but does not prove availability, deliverability, clean ownership, buyer depth or commercial viability.'::text as limitation_text,
        ela.source_record_id,
        ela.canonical_site_id,
        ela.authority_name,
        ela.site_reference,
        ela.site_name,
        ela.status_text,
        ela.geometry,
        ela.source_record_signature,
        coalesce(ela.geometry_hash, md5(coalesce(st_astext(ela.geometry), ela.source_record_id, ela.site_reference, ela.id::text))) as geometry_hash,
        ela.raw_payload,
        ela.created_at,
        ela.updated_at,
        20::integer as source_priority
    from landintel.ela_site_records as ela

    union all

    select
        vdl.id::text as source_row_id,
        'landintel.vdl_site_records'::text as source_table,
        'vdl'::text as source_family,
        vdl.source_key,
        'vacant_derelict_land_context'::text as source_role,
        'regeneration_or_underuse_visibility'::text as evidence_role,
        'low_to_medium'::text as commercial_weight,
        true as corroboration_required,
        'VDL is register/context evidence only. It supports regeneration or underuse visibility but does not prove availability, deliverability, clean ownership, buyer depth or commercial viability.'::text as limitation_text,
        vdl.source_record_id,
        vdl.canonical_site_id,
        vdl.authority_name,
        vdl.site_reference,
        vdl.site_name,
        vdl.status_text,
        vdl.geometry,
        vdl.source_record_signature,
        coalesce(vdl.geometry_hash, md5(coalesce(st_astext(vdl.geometry), vdl.source_record_id, vdl.site_reference, vdl.id::text))) as geometry_hash,
        vdl.raw_payload,
        vdl.created_at,
        vdl.updated_at,
        30::integer as source_priority
    from landintel.vdl_site_records as vdl

    union all

    select
        ldp.id::text as source_row_id,
        'landintel.ldp_site_records'::text as source_table,
        'ldp'::text as source_family,
        'ldp_policy_context'::text as source_key,
        'local_development_plan_context'::text as source_role,
        'policy_or_allocation_visibility'::text as evidence_role,
        'medium'::text as commercial_weight,
        true as corroboration_required,
        'LDP presence is policy/context evidence. It supports planning-route review but still requires site-specific corroboration on ownership, constraints, deliverability and buyer demand.'::text as limitation_text,
        ldp.source_record_id,
        ldp.canonical_site_id,
        ldp.authority_name,
        ldp.site_reference,
        ldp.site_name,
        coalesce(ldp.allocation_status, ldp.support_level, ldp.proposed_use) as status_text,
        ldp.geometry,
        null::text as source_record_signature,
        md5(coalesce(st_astext(ldp.geometry), ldp.source_record_id, ldp.site_reference, ldp.id::text)) as geometry_hash,
        ldp.raw_payload,
        ldp.created_at,
        ldp.updated_at,
        40::integer as source_priority
    from landintel.ldp_site_records as ldp

    union all

    select
        settlement.id::text as source_row_id,
        'landintel.settlement_boundary_records'::text as source_table,
        'settlement'::text as source_family,
        'settlement_boundary_context'::text as source_key,
        'settlement_boundary_context'::text as source_role,
        'settlement_location_visibility'::text as evidence_role,
        'context_only'::text as commercial_weight,
        true as corroboration_required,
        'Settlement boundary presence is location context. It supports settlement-edge reasoning but does not prove planning support, ownership, access, constraints or commercial viability.'::text as limitation_text,
        settlement.source_record_id,
        null::uuid as canonical_site_id,
        settlement.authority_name,
        null::text as site_reference,
        settlement.settlement_name as site_name,
        coalesce(settlement.boundary_status, settlement.boundary_role) as status_text,
        settlement.geometry,
        null::text as source_record_signature,
        md5(coalesce(st_astext(settlement.geometry), settlement.source_record_id, settlement.settlement_name, settlement.id::text)) as geometry_hash,
        settlement.raw_payload,
        settlement.created_at,
        settlement.updated_at,
        50::integer as source_priority
    from landintel.settlement_boundary_records as settlement
),
cleaned as (
    select
        raw_registers.*,
        source_family || ':' || coalesce(nullif(source_record_id, ''), source_row_id) as register_record_uid,
        coalesce(
            nullif(source_record_id, ''),
            nullif(site_reference, ''),
            nullif(geometry_hash, ''),
            source_row_id
        ) as record_dedupe_key,
        case
            when geometry is null then null::geometry(MultiPolygon, 27700)
            else st_multi(st_collectionextract(st_makevalid(geometry), 3))::geometry(MultiPolygon, 27700)
        end as clean_geometry,
        case
            when geometry is null then null::boolean
            else st_isvalid(geometry)
        end as geometry_valid_flag
    from raw_registers
),
ranked as (
    select
        cleaned.*,
        row_number() over (
            partition by source_family, record_dedupe_key
            order by updated_at desc nulls last, created_at desc nulls last, source_row_id
        ) as record_rank,
        count(*) over (partition by source_family, record_dedupe_key) as duplicate_group_size
    from cleaned
)
select
    register_record_uid,
    source_row_id,
    source_table,
    source_family,
    source_key,
    source_role,
    evidence_role,
    commercial_weight,
    corroboration_required,
    limitation_text,
    source_record_id,
    canonical_site_id,
    authority_name,
    site_reference,
    site_name,
    status_text,
    source_record_signature,
    geometry_hash,
    record_dedupe_key,
    record_rank,
    duplicate_group_size,
    (record_rank = 1) as current_record_flag,
    geometry_valid_flag,
    clean_geometry,
    raw_payload,
    created_at,
    updated_at,
    source_priority
from ranked;

comment on view landintel_store.v_register_context_records_clean is
    'Clean register/context read surface for HLA, ELA, VDL, LDP and settlement records. It preserves raw source tables while exposing one caveated, deduped, geometry-repaired register context model.';

create or replace view landintel_store.v_register_context_records_current
with (security_invoker = true) as
select *
from landintel_store.v_register_context_records_clean
where current_record_flag is true;

comment on view landintel_store.v_register_context_records_current is
    'Current-row view over the register/context clean surface. Use this for sourcing/DD context instead of reading HLA, ELA, VDL, LDP and settlement raw tables directly.';

create or replace view landintel_reporting.v_register_context_merge_status
with (security_invoker = true) as
with actual as (
    select
        source_family,
        min(source_key) as source_key,
        count(*)::bigint as raw_row_count,
        count(*) filter (where current_record_flag)::bigint as current_row_count,
        count(distinct canonical_site_id) filter (where canonical_site_id is not null)::bigint as linked_site_count,
        count(*) filter (where coalesce(geometry_valid_flag, true) is false)::bigint as invalid_geometry_row_count,
        sum(greatest(duplicate_group_size - 1, 0)) filter (where record_rank = 1)::bigint as possible_duplicate_row_count,
        max(updated_at) as latest_source_updated_at
    from landintel_store.v_register_context_records_clean
    group by source_family
),
matrix as (
    select distinct on (source_family)
        source_family,
        source_key,
        current_status,
        row_count,
        linked_site_count,
        freshness_record_count,
        workflow_command,
        github_actions_command_available,
        known_blocker,
        next_action,
        matrix_generated_at
    from landintel_reporting.v_source_completion_matrix
    where source_family in ('hla', 'ela', 'vdl', 'ldp', 'settlement')
    order by source_family, row_count desc, linked_site_count desc, source_key
),
drive as (
    select
        source_family,
        count(*) filter (where file_or_folder = 'file')::bigint as drive_file_count,
        count(*) filter (where file_or_folder = 'file' and ready_to_upload_flag is true)::bigint as drive_ready_file_count,
        max(last_synced_at) as latest_drive_synced_at
    from landintel_store.drive_source_file_registry
    where source_family in ('hla', 'ela', 'vdl', 'ldp', 'settlement')
    group by source_family
)
select
    actual.source_family,
    actual.source_key,
    actual.raw_row_count,
    actual.current_row_count,
    actual.linked_site_count,
    actual.invalid_geometry_row_count,
    coalesce(actual.possible_duplicate_row_count, 0)::bigint as possible_duplicate_row_count,
    actual.latest_source_updated_at,
    coalesce(drive.drive_file_count, 0)::bigint as drive_file_count,
    coalesce(drive.drive_ready_file_count, 0)::bigint as drive_ready_file_count,
    drive.latest_drive_synced_at,
    matrix.current_status as matrix_current_status,
    matrix.row_count as matrix_row_count,
    matrix.linked_site_count as matrix_linked_site_count,
    matrix.workflow_command,
    matrix.github_actions_command_available,
    matrix.known_blocker,
    case
        when coalesce(actual.raw_row_count, 0) > 0 and coalesce(matrix.row_count, 0) = 0 then 'matrix_understates_loaded_rows'
        when coalesce(actual.linked_site_count, 0) > 0 and coalesce(matrix.linked_site_count, 0) = 0 then 'matrix_understates_linked_sites'
        when coalesce(actual.raw_row_count, 0) > 0 then 'loaded_register_context'
        else 'no_loaded_rows'
    end as source_completion_alignment_status,
    case
        when actual.source_family in ('hla', 'ela', 'vdl') then 'Use as register/context evidence only; require independent corroboration before REVIEW/PURSUE.'
        when actual.source_family = 'ldp' then 'Use as policy context; require site-specific deliverability and control evidence.'
        else 'Use as location context; do not treat as commercial proof.'
    end as operator_caveat,
    case
        when coalesce(actual.invalid_geometry_row_count, 0) > 0 then 'Keep raw rows; use clean geometry view for linking and investigate source geometry quality.'
        when coalesce(actual.raw_row_count, 0) > 0 and coalesce(matrix.row_count, 0) = 0 then 'Run audit-register-context and refresh source completion overlay; do not reload duplicate source data.'
        when coalesce(actual.raw_row_count, 0) > 0 then 'Read from landintel_store.v_register_context_records_current and landintel_sourced.v_site_register_context.'
        else coalesce(matrix.next_action, 'Complete bounded source load only if source is commercially required.')
    end as recommended_action
from actual
left join matrix
  on matrix.source_family = actual.source_family
left join drive
  on drive.source_family = actual.source_family
order by actual.source_family;

comment on view landintel_reporting.v_register_context_merge_status is
    'Operator status for register/context sources, comparing loaded HLA/ELA/VDL/LDP/settlement rows against source-completion matrix and Drive registry metadata.';

create or replace view landintel_reporting.v_register_context_duplicate_diagnostics
with (security_invoker = true) as
select
    source_family,
    record_dedupe_key,
    duplicate_group_size,
    count(*)::bigint as row_count,
    count(distinct source_record_id)::bigint as distinct_source_record_ids,
    count(distinct geometry_hash)::bigint as distinct_geometry_hashes,
    array_agg(source_record_id order by updated_at desc nulls last, source_row_id) as source_record_ids,
    array_agg(source_row_id order by updated_at desc nulls last, source_row_id) as source_row_ids,
    max(updated_at) as latest_updated_at,
    'Duplicate candidate only. Do not remove source rows; use current_record_flag in the clean surface.'::text as caveat
from landintel_store.v_register_context_records_clean
where duplicate_group_size > 1
group by source_family, record_dedupe_key, duplicate_group_size
order by duplicate_group_size desc, source_family, record_dedupe_key;

comment on view landintel_reporting.v_register_context_duplicate_diagnostics is
    'Read-only duplicate diagnostics for register/context sources. It identifies likely duplicate source records without changing raw tables.';

create or replace view landintel_reporting.v_register_context_source_completion_overlay
with (security_invoker = true) as
with register_status as (
    select
        source_family,
        raw_row_count,
        current_row_count,
        linked_site_count,
        invalid_geometry_row_count,
        possible_duplicate_row_count,
        source_completion_alignment_status,
        recommended_action
    from landintel_reporting.v_register_context_merge_status
)
select
    matrix.source_key,
    matrix.source_name,
    matrix.source_family,
    matrix.current_status as matrix_current_status,
    matrix.target_status,
    matrix.current_table_or_view,
    matrix.workflow_command,
    matrix.github_actions_command_available,
    matrix.row_count as matrix_row_count,
    matrix.linked_site_count as matrix_linked_site_count,
    matrix.freshness_record_count as matrix_freshness_record_count,
    coalesce(register_status.raw_row_count, 0)::bigint as actual_register_row_count,
    coalesce(register_status.current_row_count, 0)::bigint as actual_current_register_row_count,
    coalesce(register_status.linked_site_count, 0)::bigint as actual_linked_site_count,
    coalesce(register_status.invalid_geometry_row_count, 0)::bigint as invalid_geometry_row_count,
    coalesce(register_status.possible_duplicate_row_count, 0)::bigint as possible_duplicate_row_count,
    case
        when coalesce(register_status.raw_row_count, 0) > 0 and matrix.current_status = 'registered_only' then 'live_partial_matrix_correction_needed'
        when coalesce(register_status.raw_row_count, 0) > 0 then matrix.current_status
        else matrix.current_status
    end as corrected_status_hint,
    register_status.source_completion_alignment_status,
    coalesce(register_status.recommended_action, matrix.next_action) as recommended_action,
    matrix.matrix_generated_at
from landintel_reporting.v_source_completion_matrix as matrix
left join register_status
  on register_status.source_family = matrix.source_family
where matrix.source_family in ('hla', 'ela', 'vdl', 'ldp', 'settlement')
order by matrix.source_family, matrix.source_key;

comment on view landintel_reporting.v_register_context_source_completion_overlay is
    'Overlay showing where the source-completion matrix understates actual register/context rows already held in Supabase.';

create or replace view landintel_reporting.v_register_context_freshness
with (security_invoker = true) as
with register_status as (
    select
        source_family,
        source_key,
        raw_row_count,
        latest_source_updated_at,
        latest_drive_synced_at
    from landintel_reporting.v_register_context_merge_status
),
freshness as (
    select distinct on (source_family)
        source_family,
        source_scope_key as source_key,
        freshness_status,
        last_checked_at,
        last_success_at,
        updated_at
    from landintel.source_freshness_states
    where source_family in ('hla', 'ela', 'vdl', 'ldp', 'settlement')
    order by source_family, updated_at desc nulls last, last_checked_at desc nulls last
)
select
    register_status.source_family,
    register_status.source_key,
    register_status.raw_row_count,
    register_status.latest_source_updated_at,
    register_status.latest_drive_synced_at,
    freshness.freshness_status,
    freshness.last_checked_at,
    freshness.last_success_at,
    freshness.updated_at as freshness_updated_at,
    case
        when register_status.latest_source_updated_at is null then 'not_loaded'
        when register_status.latest_source_updated_at < now() - interval '180 days' then 'stale_review'
        when freshness.freshness_status in ('failed', 'access_required', 'gated') then 'freshness_blocked'
        else 'loaded_needs_periodic_refresh'
    end as register_freshness_status,
    case
        when register_status.source_family = 'hla' then 'Run ingest-hla through src.source_phase_runner, then audit-register-context.'
        when register_status.source_family in ('ela', 'vdl', 'ldp', 'settlement') then 'Run the targeted bounded source command, then audit-register-context.'
        else 'Audit before reload.'
    end as recommended_refresh_workflow
from register_status
left join freshness
  on freshness.source_family = register_status.source_family
order by register_status.source_family;

comment on view landintel_reporting.v_register_context_freshness is
    'Freshness and refresh guidance for register/context sources. It separates loaded source state from Drive file metadata and matrix status.';

create or replace view landintel_sourced.v_site_register_context
with (security_invoker = true) as
select
    current_records.canonical_site_id,
    count(*)::bigint as register_context_count,
    count(*) filter (where source_family = 'hla')::bigint as hla_context_count,
    count(*) filter (where source_family = 'ela')::bigint as ela_context_count,
    count(*) filter (where source_family = 'vdl')::bigint as vdl_context_count,
    count(*) filter (where source_family = 'ldp')::bigint as ldp_context_count,
    array_remove(array_agg(distinct source_family order by source_family), null::text) as source_families,
    array_remove(array_agg(distinct source_role order by source_role), null::text) as source_roles,
    array_remove(array_agg(distinct evidence_role order by evidence_role), null::text) as evidence_roles,
    array_remove(array_agg(distinct site_reference order by site_reference), null::text) as site_references,
    array_remove(array_agg(distinct site_name order by site_name), null::text) as site_names,
    bool_or(corroboration_required) as corroboration_required,
    max(updated_at) as latest_register_updated_at,
    case
        when bool_or(source_family in ('hla', 'ela', 'vdl')) then 'Register/context source hit. Useful for discovery and planning/policy/regeneration context, but independent corroboration is required before treating this as a strong sourcing opportunity.'
        when bool_or(source_family = 'ldp') then 'Policy/context source hit. Useful for planning journey review, not proof of control or deliverability.'
        else 'Context source hit. Do not treat as standalone commercial proof.'
    end as register_context_summary,
    'Register presence does not prove availability, deliverability, clean ownership, buyer demand, acceptable abnormal risk or commercial viability.'::text as caveat
from landintel_store.v_register_context_records_current as current_records
where current_records.canonical_site_id is not null
group by current_records.canonical_site_id;

comment on view landintel_sourced.v_site_register_context is
    'Site-level register/context aggregate for operator surfaces. It keeps HLA/ELA/VDL/LDP evidence visible but caveated and corroboration-led.';

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
            metadata,
            reviewed_at,
            updated_at
        )
        values
            ('landintel_store', 'v_register_context_records_clean', 'view', 'current_keep', 'landintel_store', 'clean register/context read surface', 'land_supply_policy', true, true, true, true, false, false, false, null, 'Register/context evidence can inflate conviction if read without corroboration caveats.', 'Use as the clean warehouse read surface for HLA/ELA/VDL/LDP/settlement records.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now()),
            ('landintel_store', 'v_register_context_records_current', 'view', 'current_keep', 'landintel_store', 'current register/context read surface', 'land_supply_policy', true, true, true, true, false, false, false, null, 'Raw register rows may contain duplicates or invalid geometries.', 'Use current rows for sourcing/DD context; keep raw tables as source truth.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now()),
            ('landintel_reporting', 'v_register_context_merge_status', 'view', 'reporting_surface', 'landintel_reporting', 'register source completion and merge proof', 'land_supply_policy', true, true, true, true, false, true, false, null, 'Source-completion matrix can understate loaded register rows.', 'Use for audit-register-context and operator proof before reloading source data.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now()),
            ('landintel_reporting', 'v_register_context_duplicate_diagnostics', 'view', 'reporting_surface', 'landintel_reporting', 'register duplicate diagnostics', 'land_supply_policy', true, true, true, true, false, true, false, null, 'Duplicate candidates require review before any cleanup decision.', 'Use for diagnostics only; no source rows are changed.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now()),
            ('landintel_reporting', 'v_register_context_source_completion_overlay', 'view', 'reporting_surface', 'landintel_reporting', 'register source matrix overlay', 'land_supply_policy', true, true, true, true, false, true, false, null, 'Matrix statuses may lag live source table proof.', 'Use overlay to identify live_partial corrections without replacing the matrix.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now()),
            ('landintel_reporting', 'v_register_context_freshness', 'view', 'reporting_surface', 'landintel_reporting', 'register freshness proof', 'land_supply_policy', true, true, true, true, false, true, false, null, 'Freshness and row presence are separate; stale data can still be present.', 'Use before targeted source refreshes.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now()),
            ('landintel_sourced', 'v_site_register_context', 'view', 'reporting_surface', 'landintel_sourced', 'operator site register context', 'land_supply_policy', true, true, true, true, false, true, false, null, 'Register hits are discovery/context, not commercial proof.', 'Join into sourced-site briefs only with explicit caveats and corroboration requirements.', '{"created_by_migration":"076_register_context_cleanse_merge.sql"}'::jsonb, now(), now())
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
            metadata = excluded.metadata,
            reviewed_at = excluded.reviewed_at,
            updated_at = now();
    end if;
end $$;

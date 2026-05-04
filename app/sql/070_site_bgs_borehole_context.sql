create schema if not exists landintel_store;
create schema if not exists landintel_reporting;

create table if not exists landintel_store.site_bgs_borehole_context (
    id uuid primary key default gen_random_uuid(),
    canonical_site_id uuid not null references landintel.canonical_sites(id) on delete cascade,
    source_key text not null default 'bgs_borehole_context',
    source_family text not null default 'bgs',
    site_priority_band text,
    site_priority_rank integer,
    priority_source text,
    nearest_borehole_id bigint,
    nearest_borehole_reference text,
    nearest_borehole_name text,
    nearest_borehole_distance_m numeric,
    nearest_borehole_depth_m numeric,
    nearest_borehole_has_log boolean,
    nearest_borehole_operator_use_status text,
    boreholes_inside_site integer not null default 0,
    boreholes_within_100m integer not null default 0,
    boreholes_within_250m integer not null default 0,
    boreholes_within_500m integer not null default 0,
    boreholes_within_1km integer not null default 0,
    deep_boreholes_within_500m integer not null default 0,
    deep_boreholes_within_1km integer not null default 0,
    log_available_within_500m integer not null default 0,
    log_available_within_1km integer not null default 0,
    confidential_boreholes_within_1km integer not null default 0,
    deepest_borehole_depth_m numeric,
    evidence_density_signal text not null default 'not_measured',
    ground_uncertainty_signal text not null default 'not_measured',
    source_record_signature text,
    safe_use_caveat text not null default 'BGS borehole index context is safe for proximity, density and log-availability intelligence only. It is not final ground-condition interpretation, piling, grouting, remediation or abnormal-cost evidence.',
    metadata jsonb not null default '{}'::jsonb,
    measured_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists site_bgs_borehole_context_site_uidx
    on landintel_store.site_bgs_borehole_context (canonical_site_id);

create index if not exists site_bgs_borehole_context_priority_idx
    on landintel_store.site_bgs_borehole_context (site_priority_rank, updated_at);

create index if not exists site_bgs_borehole_context_density_idx
    on landintel_store.site_bgs_borehole_context (evidence_density_signal, ground_uncertainty_signal);

do $$
begin
    if to_regclass('landintel.bgs_borehole_master') is not null then
        execute 'create index if not exists bgs_borehole_master_geom_27700_gix on landintel.bgs_borehole_master using gist (geom_27700) where geom_27700 is not null';
    end if;
end $$;

create or replace view landintel_reporting.v_site_bgs_borehole_context
with (security_invoker = true) as
select
    context.canonical_site_id,
    site.site_name_primary as site_label,
    site.authority_name,
    site.area_acres,
    context.site_priority_band,
    context.site_priority_rank,
    context.priority_source,
    context.nearest_borehole_id,
    context.nearest_borehole_reference,
    context.nearest_borehole_name,
    context.nearest_borehole_distance_m,
    context.nearest_borehole_depth_m,
    context.nearest_borehole_has_log,
    context.nearest_borehole_operator_use_status,
    context.boreholes_inside_site,
    context.boreholes_within_100m,
    context.boreholes_within_250m,
    context.boreholes_within_500m,
    context.boreholes_within_1km,
    context.deep_boreholes_within_500m,
    context.deep_boreholes_within_1km,
    context.log_available_within_500m,
    context.log_available_within_1km,
    context.confidential_boreholes_within_1km,
    context.deepest_borehole_depth_m,
    context.evidence_density_signal,
    context.ground_uncertainty_signal,
    case
        when context.boreholes_within_1km = 0 then 'No BGS borehole index records were found within 1km in the last bounded refresh.'
        when context.log_available_within_1km > 0 then 'BGS borehole index records with logs are available nearby for manual Pre-SI review.'
        when context.boreholes_within_1km >= 3 then 'BGS borehole index records are available nearby, supporting desktop evidence-density context.'
        else 'Limited BGS borehole index evidence exists nearby; ground uncertainty remains high until source records or SI are reviewed.'
    end as operator_summary,
    context.safe_use_caveat,
    context.measured_at,
    context.updated_at
from landintel_store.site_bgs_borehole_context as context
join landintel.canonical_sites as site
  on site.id = context.canonical_site_id;

comment on table landintel_store.site_bgs_borehole_context
    is 'Bounded site-to-BGS-borehole proximity and evidence-density context. Safe for Pre-SI triage only; not final ground-condition interpretation.';

comment on view landintel_reporting.v_site_bgs_borehole_context
    is 'Operator-safe site BGS borehole context view. It exposes proximity, density and log-availability context with explicit caveats and no engineering certainty.';

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
                'site_bgs_borehole_context',
                'table',
                'current_keep',
                'landintel_store',
                'bounded site-to-borehole context',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                true,
                false,
                false,
                'landintel_reporting.v_site_bgs_borehole_context',
                'Derived BGS index proximity/density context. Useful for Pre-SI triage but not engineering proof.',
                'Refresh in bounded candidate-site batches only. Do not use for piling, grouting, remediation or abnormal-cost certainty.',
                '{"phase":"G1","source":"BGS Single Onshore Borehole Index","bounded_candidate_site_workflow":true,"final_ground_condition_interpretation":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_site_bgs_borehole_context',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'operator-safe site BGS borehole context',
                'ground_abnormal',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Operator surface for BGS borehole proximity, density and log-availability context. It carries safe-use caveats.',
                'Use to decide whether bounded Pre-SI review is worth the next action. Do not treat as final ground evidence.',
                '{"phase":"G1","operator_safe":true,"final_ground_condition_interpretation":false}'::jsonb
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
grant select on landintel_store.site_bgs_borehole_context to authenticated;
grant select on landintel_reporting.v_site_bgs_borehole_context to authenticated;

create or replace view landintel_reporting.v_constraint_priority_measurement_queue
with (security_invoker = true) as
with active_layers as (
    select *
    from landintel_reporting.v_constraint_priority_layers as layer
    where layer.is_active = true
      and layer.constraint_priority_rank <= 8
      and exists (
          select 1
          from public.constraint_source_features as feature
          where feature.constraint_layer_id = layer.constraint_layer_id
      )
),
candidate_pairs as (
    select
        priority_sites.canonical_site_id,
        priority_sites.site_location_id,
        priority_sites.site_label,
        priority_sites.authority_name,
        priority_sites.area_acres,
        priority_sites.site_priority_rank,
        priority_sites.site_priority_band,
        active_layers.constraint_layer_id,
        active_layers.layer_key,
        active_layers.layer_name,
        active_layers.source_family,
        active_layers.constraint_group,
        active_layers.constraint_priority_rank,
        active_layers.constraint_priority_family
    from landintel_reporting.v_constraint_priority_sites as priority_sites
    cross join active_layers
    left join public.site_constraint_measurement_scan_state as scan_state
      on scan_state.site_location_id = priority_sites.site_location_id
     and scan_state.constraint_layer_id = active_layers.constraint_layer_id
     and scan_state.scan_scope = 'canonical_site_geometry'
    where scan_state.id is null
),
ranked as (
    select
        candidate_pairs.*,
        row_number() over (
            partition by candidate_pairs.source_family
            order by
                candidate_pairs.site_priority_rank,
                candidate_pairs.constraint_priority_rank,
                candidate_pairs.authority_name nulls last,
                candidate_pairs.area_acres desc nulls last,
                candidate_pairs.site_location_id,
                candidate_pairs.layer_key
        ) as source_family_queue_rank,
        row_number() over (
            partition by candidate_pairs.constraint_priority_family
            order by
                candidate_pairs.site_priority_rank,
                candidate_pairs.constraint_priority_rank,
                candidate_pairs.authority_name nulls last,
                candidate_pairs.area_acres desc nulls last,
                candidate_pairs.site_location_id,
                candidate_pairs.layer_key
        ) as priority_family_queue_rank
    from candidate_pairs
),
source_family_limited as (
    select *
    from ranked
    where source_family_queue_rank <= 5000
)
select
    row_number() over (
        order by
            source_family_limited.site_priority_rank,
            source_family_limited.constraint_priority_rank,
            source_family_limited.authority_name nulls last,
            source_family_limited.area_acres desc nulls last,
            source_family_limited.site_location_id,
            source_family_limited.layer_key
    ) as queue_rank,
    canonical_site_id,
    site_location_id,
    site_label,
    authority_name,
    area_acres,
    site_priority_rank,
    site_priority_band,
    constraint_priority_rank,
    constraint_priority_family,
    constraint_layer_id,
    layer_key,
    layer_name,
    source_family,
    constraint_group,
    'measure-constraints-duckdb'::text as recommended_workflow_command,
    layer_key as recommended_layer_key,
    authority_name as recommended_authority_filter,
    'Use small site batches, one source family or one layer per run, and no broad all-layer scan. Queue is capped per source family so lower-ranked constraint families are not hidden by flood backlog.'::text as bounded_run_guidance,
    source_family_queue_rank,
    priority_family_queue_rank
from source_family_limited;

comment on view landintel_reporting.v_constraint_priority_measurement_queue
    is 'Bounded unscanned site-layer queue for the existing constraint measurement engine. Capped at 5,000 pairs per source family so flood backlog does not hide coal, green belt, contamination, culvert, heritage, ecology or TPO work. Guidance only; no measurement is executed by this view.';

grant select on landintel_reporting.v_constraint_priority_measurement_queue to authenticated;

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
            risk_summary,
            recommended_action,
            metadata
        )
        values (
            'landintel_reporting',
            'v_constraint_priority_measurement_queue',
            'view',
            'reporting_surface',
            'landintel_reporting',
            'bounded constraint measurement queue',
            'constraints',
            true,
            true,
            true,
            true,
            false,
            true,
            false,
            'Guidance surface only. Earlier global 5,000-pair cap hid non-flood source families behind flood backlog; this version caps per source family.',
            'Use for bounded source-family measurement batches only; do not use as permission for broad all-layer scans.',
            jsonb_build_object(
                'migration', '073_constraint_source_family_queue_fix',
                'queue_cap', '5000_pairs_per_source_family',
                'no_measurement_executed_by_view', true
            )
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
    end if;
end $$;

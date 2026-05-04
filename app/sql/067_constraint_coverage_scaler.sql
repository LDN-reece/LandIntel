create schema if not exists landintel_reporting;

create or replace view landintel_reporting.v_constraint_priority_layers
with (security_invoker = true) as
select
    layer.id as constraint_layer_id,
    layer.layer_key,
    layer.layer_name,
    layer.source_family,
    layer.constraint_group,
    layer.constraint_type,
    layer.measurement_mode,
    layer.buffer_distance_m,
    layer.is_active,
    case
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(flood|sepa)%%' then 1
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(coal|mining|mine)%%' then 2
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(greenbelt|green belt)%%' then 3
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(contaminated|contamination)%%' then 4
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(culvert|culverts)%%' then 5
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(heritage|historic|conservation|listed|scheduled)%%' then 6
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(ecology|naturescot|nature|sssi|sac|spa|ramsar)%%' then 7
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(tpo|tree preservation|landscape)%%' then 8
        else 50
    end as constraint_priority_rank,
    case
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(flood|sepa)%%' then 'flood'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(coal|mining|mine)%%' then 'coal_mining'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(greenbelt|green belt)%%' then 'green_belt'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(contaminated|contamination)%%' then 'contaminated_land'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(culvert|culverts)%%' then 'culverts'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(heritage|historic|conservation|listed|scheduled)%%' then 'heritage_conservation'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(ecology|naturescot|nature|sssi|sac|spa|ramsar)%%' then 'ecology_naturescot'
        when lower(concat_ws(' ', layer.source_family, layer.constraint_group, layer.constraint_type, layer.layer_key, layer.layer_name)) similar to '%%(tpo|tree preservation|landscape)%%' then 'tpo_landscape'
        else 'other_constraint'
    end as constraint_priority_family
from public.constraint_layer_registry as layer;

comment on view landintel_reporting.v_constraint_priority_layers
    is 'Constraint layer priority taxonomy for bounded measurement scale-up. It maps existing registry layers to priority families without creating a second constraint engine.';

create or replace view landintel_reporting.v_constraint_priority_sites
with (security_invoker = true) as
with site_candidates as (
    select
        canonical_site_id,
        'title_spend_candidates'::text as site_priority_band,
        1::integer as site_priority_rank,
        'landintel_sourced.v_title_spend_candidates'::text as priority_source
    from landintel_sourced.v_title_spend_candidates

    union all

    select
        canonical_site_id,
        'review_queue'::text as site_priority_band,
        2::integer as site_priority_rank,
        'landintel_sourced.v_review_queue'::text as priority_source
    from landintel_sourced.v_review_queue

    union all

    select
        canonical_site_id,
        'ldn_candidate_screen'::text as site_priority_band,
        3::integer as site_priority_rank,
        'landintel.site_ldn_candidate_screen'::text as priority_source
    from landintel.site_ldn_candidate_screen
    where candidate_status <> 'not_enough_evidence'
       or ldn_target_private_no_builder = true

    union all

    select
        canonical_site_id,
        'prove_it_candidates'::text as site_priority_band,
        4::integer as site_priority_rank,
        'landintel.site_prove_it_assessments'::text as priority_source
    from landintel.site_prove_it_assessments
    where review_ready_flag = true
       or verdict in ('review', 'pursue')

    union all

    select
        id as canonical_site_id,
        'wider_canonical_sites'::text as site_priority_band,
        5::integer as site_priority_rank,
        'landintel.canonical_sites'::text as priority_source
    from landintel.canonical_sites
    where geometry is not null
),
deduplicated as (
    select distinct on (site_candidates.canonical_site_id)
        site_candidates.canonical_site_id,
        site_candidates.site_priority_band,
        site_candidates.site_priority_rank,
        site_candidates.priority_source
    from site_candidates
    order by site_candidates.canonical_site_id, site_candidates.site_priority_rank
)
select
    deduplicated.canonical_site_id,
    deduplicated.canonical_site_id::text as site_location_id,
    canonical.site_name_primary as site_label,
    canonical.authority_name,
    canonical.area_acres,
    deduplicated.site_priority_band,
    deduplicated.site_priority_rank,
    deduplicated.priority_source
from deduplicated
join landintel.canonical_sites as canonical
  on canonical.id = deduplicated.canonical_site_id
where canonical.geometry is not null;

comment on view landintel_reporting.v_constraint_priority_sites
    is 'Constraint measurement site priority spine. It ranks title spend, review queue, LDN candidate, Prove It and wider canonical sites without measuring anything.';

create or replace view landintel_reporting.v_constraint_coverage_by_layer
with (security_invoker = true) as
with priority_site_count as (
    select count(*)::bigint as site_count
    from landintel_reporting.v_constraint_priority_sites
),
feature_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as source_feature_count
    from public.constraint_source_features
    group by constraint_layer_id
),
measurement_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as measured_row_count,
        count(distinct site_location_id)::bigint as measured_site_count,
        count(*) filter (where overlap_character is null)::bigint as missing_overlap_character_count,
        count(distinct overlap_character) filter (where overlap_character is not null)::bigint as overlap_character_type_count,
        max(measured_at) as latest_measured_at
    from public.site_constraint_measurements
    group by constraint_layer_id
),
summary_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as summary_row_count
    from public.site_constraint_group_summaries
    group by constraint_layer_id
),
fact_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as commercial_friction_fact_count
    from public.site_commercial_friction_facts
    group by constraint_layer_id
),
scan_counts as (
    select
        constraint_layer_id,
        count(*)::bigint as scan_state_row_count,
        count(distinct site_location_id)::bigint as scanned_site_count,
        max(scanned_at) as latest_scanned_at
    from public.site_constraint_measurement_scan_state
    group by constraint_layer_id
)
select
    priority_layers.constraint_layer_id,
    priority_layers.layer_key,
    priority_layers.layer_name,
    priority_layers.source_family,
    priority_layers.constraint_group,
    priority_layers.constraint_type,
    priority_layers.measurement_mode,
    priority_layers.buffer_distance_m,
    priority_layers.is_active,
    priority_layers.constraint_priority_rank,
    priority_layers.constraint_priority_family,
    coalesce(feature_counts.source_feature_count, 0) as source_feature_count,
    priority_site_count.site_count as priority_site_count,
    coalesce(measurement_counts.measured_row_count, 0) as measured_row_count,
    coalesce(measurement_counts.measured_site_count, 0) as measured_site_count,
    coalesce(summary_counts.summary_row_count, 0) as summary_row_count,
    coalesce(fact_counts.commercial_friction_fact_count, 0) as commercial_friction_fact_count,
    coalesce(scan_counts.scan_state_row_count, 0) as scan_state_row_count,
    coalesce(scan_counts.scanned_site_count, 0) as scanned_site_count,
    greatest(priority_site_count.site_count - coalesce(scan_counts.scanned_site_count, 0), 0)::bigint as backlog_site_count,
    round((coalesce(measurement_counts.measured_site_count, 0)::numeric / nullif(priority_site_count.site_count, 0)) * 100, 4) as measured_site_pct,
    round((coalesce(scan_counts.scanned_site_count, 0)::numeric / nullif(priority_site_count.site_count, 0)) * 100, 4) as scanned_site_pct,
    coalesce(measurement_counts.missing_overlap_character_count, 0) as missing_overlap_character_count,
    coalesce(measurement_counts.overlap_character_type_count, 0) as overlap_character_type_count,
    measurement_counts.latest_measured_at,
    scan_counts.latest_scanned_at,
    case
        when priority_layers.is_active = false then 'inactive_layer'
        when coalesce(feature_counts.source_feature_count, 0) = 0 then 'no_source_features'
        when coalesce(scan_counts.scanned_site_count, 0) = 0 then 'not_started'
        when coalesce(scan_counts.scanned_site_count, 0) < priority_site_count.site_count then 'partial_coverage'
        else 'priority_site_coverage_recorded'
    end as coverage_state,
    'Existing constraint engine only. Use bounded layer-by-layer measurement; do not run broad all-layer scans.'::text as bounded_measurement_caveat
from landintel_reporting.v_constraint_priority_layers as priority_layers
cross join priority_site_count
left join feature_counts
  on feature_counts.constraint_layer_id = priority_layers.constraint_layer_id
left join measurement_counts
  on measurement_counts.constraint_layer_id = priority_layers.constraint_layer_id
left join summary_counts
  on summary_counts.constraint_layer_id = priority_layers.constraint_layer_id
left join fact_counts
  on fact_counts.constraint_layer_id = priority_layers.constraint_layer_id
left join scan_counts
  on scan_counts.constraint_layer_id = priority_layers.constraint_layer_id;

comment on view landintel_reporting.v_constraint_coverage_by_layer
    is 'Constraint coverage by existing layer. This is a reporting/control view only and does not create a second measurement engine.';

create or replace view landintel_reporting.v_constraint_coverage_by_site_priority
with (security_invoker = true) as
with measured_sites as (
    select distinct site_location_id
    from public.site_constraint_measurements
),
scanned_sites as (
    select distinct site_location_id
    from public.site_constraint_measurement_scan_state
),
summary_sites as (
    select distinct site_location_id
    from public.site_constraint_group_summaries
),
fact_sites as (
    select distinct site_location_id
    from public.site_commercial_friction_facts
)
select
    priority_sites.site_priority_rank,
    priority_sites.site_priority_band,
    count(*)::bigint as site_count,
    count(*) filter (where measured_sites.site_location_id is not null)::bigint as sites_with_measurements,
    count(*) filter (where scanned_sites.site_location_id is not null)::bigint as sites_with_scan_state,
    count(*) filter (where summary_sites.site_location_id is not null)::bigint as sites_with_group_summaries,
    count(*) filter (where fact_sites.site_location_id is not null)::bigint as sites_with_commercial_friction_facts,
    count(*) filter (where scanned_sites.site_location_id is null)::bigint as sites_without_scan_state,
    round((count(*) filter (where measured_sites.site_location_id is not null)::numeric / nullif(count(*), 0)) * 100, 4) as measured_site_pct,
    round((count(*) filter (where scanned_sites.site_location_id is not null)::numeric / nullif(count(*), 0)) * 100, 4) as scanned_site_pct,
    'Higher-priority sourced sites should be measured before wider canonical sites. This view only reports coverage.'::text as measurement_guidance
from landintel_reporting.v_constraint_priority_sites as priority_sites
left join measured_sites
  on measured_sites.site_location_id = priority_sites.site_location_id
left join scanned_sites
  on scanned_sites.site_location_id = priority_sites.site_location_id
left join summary_sites
  on summary_sites.site_location_id = priority_sites.site_location_id
left join fact_sites
  on fact_sites.site_location_id = priority_sites.site_location_id
group by priority_sites.site_priority_rank, priority_sites.site_priority_band;

comment on view landintel_reporting.v_constraint_coverage_by_site_priority
    is 'Constraint coverage by site priority band. It proves whether sourced/title/review sites are measured before wider canonical estate expansion.';

create or replace view landintel_reporting.v_constraint_measurement_backlog
with (security_invoker = true) as
with active_layers as (
    select *
    from landintel_reporting.v_constraint_priority_layers as layer
    where layer.is_active = true
      and exists (
          select 1
          from public.constraint_source_features as feature
          where feature.constraint_layer_id = layer.constraint_layer_id
      )
),
site_layer_pairs as (
    select
        priority_sites.site_priority_rank,
        priority_sites.site_priority_band,
        active_layers.constraint_priority_rank,
        active_layers.constraint_priority_family,
        active_layers.constraint_layer_id,
        active_layers.layer_key,
        active_layers.layer_name,
        active_layers.source_family,
        active_layers.constraint_group,
        priority_sites.site_location_id,
        scan_state.id as scan_state_id
    from landintel_reporting.v_constraint_priority_sites as priority_sites
    cross join active_layers
    left join public.site_constraint_measurement_scan_state as scan_state
      on scan_state.constraint_layer_id = active_layers.constraint_layer_id
     and scan_state.site_location_id = priority_sites.site_location_id
     and scan_state.scan_scope = 'canonical_site_geometry'
)
select
    site_priority_rank,
    site_priority_band,
    constraint_priority_rank,
    constraint_priority_family,
    constraint_layer_id,
    layer_key,
    layer_name,
    source_family,
    constraint_group,
    count(*)::bigint as target_site_layer_pairs,
    count(*) filter (where scan_state_id is not null)::bigint as scanned_site_layer_pairs,
    count(*) filter (where scan_state_id is null)::bigint as backlog_site_layer_pairs,
    'measure-constraints-duckdb'::text as recommended_workflow_command,
    layer_key as recommended_layer_key,
    'Run one layer, bounded site batches, and no broad all-layer scan.'::text as bounded_run_guidance
from site_layer_pairs
group by
    site_priority_rank,
    site_priority_band,
    constraint_priority_rank,
    constraint_priority_family,
    constraint_layer_id,
    layer_key,
    layer_name,
    source_family,
    constraint_group;

comment on view landintel_reporting.v_constraint_measurement_backlog
    is 'Aggregated backlog for existing constraint measurement engine. Use it to choose the next bounded layer/site-priority batch.';

create or replace view landintel_reporting.v_constraint_priority_measurement_queue
with (security_invoker = true) as
with active_layers as (
    select *
    from landintel_reporting.v_constraint_priority_layers as layer
    where layer.is_active = true
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
      on scan_state.constraint_layer_id = active_layers.constraint_layer_id
     and scan_state.site_location_id = priority_sites.site_location_id
     and scan_state.scan_scope = 'canonical_site_geometry'
    where scan_state.id is null
),
ranked as (
    select
        row_number() over (
            order by
                candidate_pairs.site_priority_rank,
                candidate_pairs.constraint_priority_rank,
                candidate_pairs.authority_name nulls last,
                candidate_pairs.area_acres desc nulls last,
                candidate_pairs.site_location_id,
                candidate_pairs.layer_key
        ) as queue_rank,
        candidate_pairs.*
    from candidate_pairs
)
select
    queue_rank,
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
    'Use small site batches and one layer per run. This queue is guidance only and does not perform measurement.'::text as bounded_run_guidance
from ranked
where queue_rank <= 5000;

comment on view landintel_reporting.v_constraint_priority_measurement_queue
    is 'Bounded first 5,000 unscanned site-layer pairs ordered by sourced-site priority and constraint priority. Guidance only; no measurement is executed by this view.';

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
                'landintel_reporting',
                'v_constraint_coverage_by_layer',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'constraint coverage by layer',
                'constraints',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Reports existing constraint measurement coverage by layer without running measurements.',
                'Use to select bounded layer-by-layer measurement batches.',
                '{"broad_spatial_scan":false,"second_constraint_engine":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_constraint_coverage_by_site_priority',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'constraint coverage by site priority',
                'constraints',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Shows whether title spend, review queue and LDN candidate sites have constraint coverage.',
                'Use to measure sourced sites before wider canonical expansion.',
                '{"priority_site_first":true}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_constraint_measurement_backlog',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'constraint measurement backlog',
                'constraints',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                null,
                'Aggregates unscanned site-layer pairs for the existing measurement engine.',
                'Use as backlog guidance only; run measurement through bounded GitHub Actions commands.',
                '{"guidance_only":true}'::jsonb
            ),
            (
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
                null,
                'Bounded first 5000 unscanned site-layer pairs ordered by commercial priority and constraint priority.',
                'Use to choose targeted measure-constraints-duckdb inputs; do not use as a broad scan instruction.',
                '{"bounded_queue_limit":5000,"guidance_only":true}'::jsonb
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

grant usage on schema landintel_reporting to authenticated;
grant select on landintel_reporting.v_constraint_priority_layers to authenticated;
grant select on landintel_reporting.v_constraint_priority_sites to authenticated;
grant select on landintel_reporting.v_constraint_coverage_by_layer to authenticated;
grant select on landintel_reporting.v_constraint_coverage_by_site_priority to authenticated;
grant select on landintel_reporting.v_constraint_measurement_backlog to authenticated;
grant select on landintel_reporting.v_constraint_priority_measurement_queue to authenticated;

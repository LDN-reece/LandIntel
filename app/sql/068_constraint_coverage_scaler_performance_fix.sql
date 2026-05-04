create index if not exists site_constraint_scan_state_location_layer_scope_idx
    on public.site_constraint_measurement_scan_state (site_location_id, constraint_layer_id, scan_scope);

create index if not exists site_constraint_group_summaries_location_idx
    on public.site_constraint_group_summaries (site_location_id);

create index if not exists site_commercial_friction_facts_location_idx
    on public.site_commercial_friction_facts (site_location_id);

create index if not exists site_prove_it_assessments_latest_idx
    on landintel.site_prove_it_assessments (canonical_site_id, updated_at desc, created_at desc);

create index if not exists site_urgent_address_title_pack_urgency_site_idx
    on landintel.site_urgent_address_title_pack (urgency_status, canonical_site_id);

create or replace view landintel_reporting.v_constraint_priority_sites
with (security_invoker = true) as
with latest_prove_it as (
    select distinct on (assessment.canonical_site_id)
        assessment.canonical_site_id,
        assessment.verdict,
        assessment.review_ready_flag,
        assessment.title_spend_recommendation,
        assessment.review_next_action
    from landintel.site_prove_it_assessments as assessment
    order by assessment.canonical_site_id, assessment.updated_at desc nulls last, assessment.created_at desc nulls last
),
latest_ldn_screen as (
    select distinct on (screen.canonical_site_id)
        screen.canonical_site_id,
        screen.candidate_status,
        screen.ldn_target_private_no_builder
    from landintel.site_ldn_candidate_screen as screen
    order by screen.canonical_site_id, screen.updated_at desc nulls last, screen.created_at desc nulls last
),
latest_pack as (
    select distinct on (pack.canonical_site_id)
        pack.canonical_site_id,
        pack.urgency_status
    from landintel.site_urgent_address_title_pack as pack
    order by pack.canonical_site_id, pack.updated_at desc nulls last, pack.created_at desc nulls last
),
latest_title_workflow as (
    select distinct on (workflow.canonical_site_id)
        workflow.canonical_site_id,
        workflow.title_order_status,
        workflow.next_action
    from landintel.title_order_workflow as workflow
    order by workflow.canonical_site_id, workflow.updated_at desc nulls last, workflow.created_at desc nulls last
),
site_candidates as (
    select
        latest_pack.canonical_site_id,
        'title_spend_candidates'::text as site_priority_band,
        1::integer as site_priority_rank,
        'landintel.site_urgent_address_title_pack'::text as priority_source
    from latest_pack
    where latest_pack.urgency_status = 'order_title_urgently'

    union all

    select
        latest_prove_it.canonical_site_id,
        'title_spend_candidates'::text as site_priority_band,
        1::integer as site_priority_rank,
        'landintel.site_prove_it_assessments'::text as priority_source
    from latest_prove_it
    where latest_prove_it.title_spend_recommendation in ('order_title', 'order_title_urgently')
       or (
            latest_prove_it.verdict in ('review', 'pursue')
            and coalesce(latest_prove_it.review_next_action, '') ilike '%%title%%'
       )

    union all

    select
        latest_ldn_screen.canonical_site_id,
        'title_spend_candidates'::text as site_priority_band,
        1::integer as site_priority_rank,
        'landintel.site_ldn_candidate_screen'::text as priority_source
    from latest_ldn_screen
    where latest_ldn_screen.candidate_status in (
        'true_ldn_candidate',
        'review_private_candidate',
        'review_forgotten_soul'
    )

    union all

    select
        latest_title_workflow.canonical_site_id,
        'title_spend_candidates'::text as site_priority_band,
        1::integer as site_priority_rank,
        'landintel.title_order_workflow'::text as priority_source
    from latest_title_workflow
    where latest_title_workflow.title_order_status <> 'not_ordered'
       or coalesce(latest_title_workflow.next_action, '') ilike '%%title%%'

    union all

    select
        latest_pack.canonical_site_id,
        'review_queue'::text as site_priority_band,
        2::integer as site_priority_rank,
        'landintel.site_urgent_address_title_pack'::text as priority_source
    from latest_pack

    union all

    select
        latest_prove_it.canonical_site_id,
        'review_queue'::text as site_priority_band,
        2::integer as site_priority_rank,
        'landintel.site_prove_it_assessments'::text as priority_source
    from latest_prove_it
    where latest_prove_it.review_ready_flag = true
       or latest_prove_it.verdict in ('review', 'pursue')

    union all

    select
        latest_ldn_screen.canonical_site_id,
        'review_queue'::text as site_priority_band,
        2::integer as site_priority_rank,
        'landintel.site_ldn_candidate_screen'::text as priority_source
    from latest_ldn_screen
    where latest_ldn_screen.candidate_status in (
        'true_ldn_candidate',
        'review_private_candidate',
        'review_forgotten_soul',
        'constraint_review_required'
    )

    union all

    select
        latest_ldn_screen.canonical_site_id,
        'ldn_candidate_screen'::text as site_priority_band,
        3::integer as site_priority_rank,
        'landintel.site_ldn_candidate_screen'::text as priority_source
    from latest_ldn_screen
    where latest_ldn_screen.candidate_status <> 'not_enough_evidence'
       or latest_ldn_screen.ldn_target_private_no_builder = true

    union all

    select
        latest_prove_it.canonical_site_id,
        'prove_it_candidates'::text as site_priority_band,
        4::integer as site_priority_rank,
        'landintel.site_prove_it_assessments'::text as priority_source
    from latest_prove_it
    where latest_prove_it.review_ready_flag = true
       or latest_prove_it.verdict in ('review', 'pursue')

    union all

    select
        site.id as canonical_site_id,
        'wider_canonical_sites'::text as site_priority_band,
        5::integer as site_priority_rank,
        'landintel.canonical_sites'::text as priority_source
    from landintel.canonical_sites as site
    where site.geometry is not null
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
    is 'Performance-tuned constraint measurement site priority spine. It derives priority bands from current source tables instead of expanding the heavier operator sourced-site views. It ranks title spend, review queue, LDN candidate, Prove It and wider canonical sites without measuring anything.';

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
limited_pairs as (
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
    order by
        priority_sites.site_priority_rank,
        active_layers.constraint_priority_rank,
        priority_sites.authority_name nulls last,
        priority_sites.area_acres desc nulls last,
        priority_sites.site_location_id,
        active_layers.layer_key
    limit 5000
)
select
    row_number() over (
        order by
            limited_pairs.site_priority_rank,
            limited_pairs.constraint_priority_rank,
            limited_pairs.authority_name nulls last,
            limited_pairs.area_acres desc nulls last,
            limited_pairs.site_location_id,
            limited_pairs.layer_key
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
    'Use small site batches and one layer per run. This queue is guidance only and does not perform measurement.'::text as bounded_run_guidance
from limited_pairs;

comment on view landintel_reporting.v_constraint_priority_measurement_queue
    is 'Performance-tuned bounded first 5,000 unscanned site-layer pairs ordered by sourced-site priority and priority constraint families only. Guidance only; no measurement is executed by this view.';

grant select on landintel_reporting.v_constraint_priority_sites to authenticated;
grant select on landintel_reporting.v_constraint_priority_measurement_queue to authenticated;

create schema if not exists landintel_reporting;

create or replace view landintel_reporting.v_site_title_traceability_matrix
with (security_invoker = true) as
with best_parcel_link as (
    select distinct on (link.site_id)
        link.site_id,
        link.site_location_id,
        link.ros_parcel_id,
        link.ros_inspire_id,
        link.authority_name as parcel_authority_name,
        link.link_status,
        link.match_method,
        link.confidence,
        link.candidate_rank,
        link.overlap_area_sqm,
        link.overlap_pct_of_site,
        link.overlap_pct_of_parcel,
        link.nearest_distance_m,
        link.centroid_inside_site,
        link.updated_at as parcel_link_updated_at
    from public.site_ros_parcel_link_candidates as link
    where link.link_status <> 'rejected'
    order by
        link.site_id,
        case link.link_status
            when 'primary' then 1
            when 'candidate' then 2
            when 'manual_review' then 3
            else 9
        end,
        link.candidate_rank,
        link.confidence desc nulls last,
        link.updated_at desc nulls last
),
parcel_link_counts as (
    select
        link.site_id,
        count(*) filter (where link.link_status <> 'rejected')::integer as parcel_candidate_count,
        count(*) filter (where link.link_status = 'primary')::integer as primary_parcel_candidate_count,
        count(*) filter (where link.link_status = 'rejected')::integer as rejected_parcel_candidate_count,
        max(link.updated_at) as latest_parcel_link_at
    from public.site_ros_parcel_link_candidates as link
    group by link.site_id
),
best_title_resolution as (
    select distinct on (candidate.site_id)
        candidate.site_id,
        candidate.site_location_id,
        candidate.ros_parcel_id,
        candidate.ros_inspire_id,
        candidate.cadastral_unit_identifier,
        candidate.candidate_title_number,
        candidate.normalized_title_number,
        candidate.candidate_source,
        candidate.resolution_status,
        candidate.match_method,
        candidate.confidence,
        candidate.overlap_pct_of_site,
        candidate.nearest_distance_m,
        candidate.updated_at as title_resolution_updated_at
    from public.site_title_resolution_candidates as candidate
    where candidate.resolution_status <> 'rejected'
    order by
        candidate.site_id,
        case candidate.resolution_status
            when 'validated_title' then 1
            when 'probable_title' then 2
            when 'needs_licensed_bridge' then 3
            when 'manual_review' then 4
            else 9
        end,
        candidate.confidence desc nulls last,
        candidate.updated_at desc nulls last
),
title_resolution_counts as (
    select
        candidate.site_id,
        count(*) filter (where candidate.resolution_status <> 'rejected')::integer as title_resolution_candidate_count,
        count(*) filter (where candidate.resolution_status in ('probable_title', 'validated_title'))::integer as probable_title_candidate_count,
        count(*) filter (where candidate.resolution_status = 'needs_licensed_bridge')::integer as licensed_bridge_required_count,
        count(*) filter (where candidate.resolution_status = 'rejected')::integer as rejected_title_resolution_count,
        max(candidate.updated_at) as latest_title_resolution_at
    from public.site_title_resolution_candidates as candidate
    group by candidate.site_id
),
latest_title_workflow as (
    select distinct on (workflow.canonical_site_id)
        workflow.*
    from landintel.title_order_workflow as workflow
    order by workflow.canonical_site_id, workflow.updated_at desc nulls last, workflow.created_at desc nulls last
),
latest_title_review as (
    select distinct on (review.canonical_site_id)
        review.*
    from landintel.title_review_records as review
    order by
        review.canonical_site_id,
        review.review_date desc nulls last,
        review.updated_at desc nulls last,
        review.created_at desc nulls last
),
safe_title_candidates as (
    select *
    from landintel_reporting.v_title_candidates_operator_safe
),
title_control as (
    select *
    from landintel_reporting.v_title_control_status
)
select
    site.id as canonical_site_id,
    site.id::text as site_location_id,
    coalesce(nullif(site.site_name_primary, ''), site.site_code, site.id::text) as site_label,
    site.authority_name,
    site.area_acres as gross_area_acres,
    site.primary_ros_parcel_id,
    best_parcel.ros_parcel_id as best_ros_parcel_id,
    coalesce(parcel.ros_inspire_id, best_parcel.ros_inspire_id, best_resolution.ros_inspire_id) as best_ros_inspire_id,
    coalesce(
        public.extract_ros_cadastral_identifier(parcel.raw_attributes, parcel.ros_inspire_id),
        best_resolution.cadastral_unit_identifier
    ) as best_cadastral_unit_identifier,
    best_parcel.link_status as best_parcel_link_status,
    best_parcel.match_method as best_parcel_match_method,
    best_parcel.confidence as best_parcel_confidence,
    best_parcel.candidate_rank as best_parcel_candidate_rank,
    best_parcel.overlap_area_sqm as best_parcel_overlap_area_sqm,
    best_parcel.overlap_pct_of_site as best_parcel_overlap_pct_of_site,
    best_parcel.overlap_pct_of_parcel as best_parcel_overlap_pct_of_parcel,
    best_parcel.nearest_distance_m as best_parcel_nearest_distance_m,
    coalesce(parcel_counts.parcel_candidate_count, 0) as parcel_candidate_count,
    coalesce(parcel_counts.primary_parcel_candidate_count, 0) as primary_parcel_candidate_count,
    coalesce(parcel_counts.rejected_parcel_candidate_count, 0) as rejected_parcel_candidate_count,
    safe_title.title_number_candidate_for_manual_check as safe_title_number_candidate,
    safe_title.normalized_title_number as safe_normalized_title_number,
    safe_title.title_candidate_source as safe_title_candidate_source,
    safe_title.title_candidate_status as safe_title_candidate_status,
    safe_title.title_candidate_confidence as safe_title_candidate_confidence,
    coalesce(safe_title.safe_title_candidate_count, 0) as safe_title_candidate_count,
    best_resolution.candidate_title_number as bridge_title_number_candidate,
    best_resolution.normalized_title_number as bridge_normalized_title_number,
    best_resolution.candidate_source as bridge_candidate_source,
    best_resolution.resolution_status as bridge_resolution_status,
    best_resolution.match_method as bridge_match_method,
    best_resolution.confidence as bridge_confidence,
    coalesce(title_counts.title_resolution_candidate_count, 0) as title_resolution_candidate_count,
    coalesce(title_counts.probable_title_candidate_count, 0) as probable_title_candidate_count,
    coalesce(title_counts.licensed_bridge_required_count, 0) as licensed_bridge_required_count,
    coalesce(title_counts.rejected_title_resolution_count, 0) as rejected_title_resolution_count,
    title_workflow.title_order_status,
    title_workflow.title_review_status,
    title_workflow.title_required_flag,
    title_workflow.next_action as title_workflow_next_action,
    title_control.title_control_status,
    title_control.ownership_control_status,
    title_review.title_number as reviewed_title_number,
    title_review.normalized_title_number as reviewed_normalized_title_number,
    title_review.registered_proprietor,
    title_review.proprietor_type,
    title_review.ownership_outcome,
    title_review.review_date,
    greatest(
        site.updated_at,
        best_parcel.parcel_link_updated_at,
        parcel_counts.latest_parcel_link_at,
        best_resolution.title_resolution_updated_at,
        title_counts.latest_title_resolution_at,
        safe_title.latest_candidate_activity_at,
        title_workflow.updated_at,
        title_review.updated_at
    ) as latest_title_traceability_activity_at,
    case
        when site.geometry is null then 'no_site_geometry'
        when title_review.id is not null then 'title_review_recorded'
        when coalesce(safe_title.safe_title_candidate_count, 0) > 0 then 'safe_title_candidate_available'
        when coalesce(title_counts.licensed_bridge_required_count, 0) > 0 then 'parcel_linked_needs_licensed_title_bridge'
        when coalesce(parcel_counts.parcel_candidate_count, 0) > 0
          or site.primary_ros_parcel_id is not null then 'parcel_linked_no_safe_title_candidate'
        else 'needs_ros_parcel_linking'
    end as title_traceability_status,
    case
        when site.geometry is null then 'Add or repair site geometry before title/parcel linking.'
        when title_review.id is not null then 'Use title_review_records as the ownership/control interpretation source.'
        when coalesce(safe_title.safe_title_candidate_count, 0) > 0 then 'Review candidate title number manually; this is not ownership confirmation.'
        when coalesce(parcel_counts.parcel_candidate_count, 0) > 0
          or site.primary_ros_parcel_id is not null then 'Run resolve-title-numbers or licensed/manual title bridge for linked RoS parcel candidates.'
        else 'Run link-sites-to-ros-parcels before title spend or title review.'
    end as next_title_link_action,
    'Ownership is unconfirmed unless landintel.title_review_records supports it. RoS parcel references, SCT-like cadastral references and Companies House/FCA/control signals are not title numbers or legal ownership proof.'::text as title_traceability_caveat
from landintel.canonical_sites as site
left join best_parcel_link as best_parcel
  on best_parcel.site_id = site.id::text
left join parcel_link_counts as parcel_counts
  on parcel_counts.site_id = site.id::text
left join public.ros_cadastral_parcels as parcel
  on parcel.id = coalesce(best_parcel.ros_parcel_id, site.primary_ros_parcel_id)
left join best_title_resolution as best_resolution
  on best_resolution.site_id = site.id::text
left join title_resolution_counts as title_counts
  on title_counts.site_id = site.id::text
left join latest_title_workflow as title_workflow
  on title_workflow.canonical_site_id = site.id
left join latest_title_review as title_review
  on title_review.canonical_site_id = site.id
left join safe_title_candidates as safe_title
  on safe_title.canonical_site_id = site.id
left join title_control
  on title_control.canonical_site_id = site.id;

comment on view landintel_reporting.v_site_title_traceability_matrix is
    'Per-site title traceability matrix. It links canonical sites to RoS parcel candidates, safe title-number candidates, title orders and human title reviews without claiming ownership certainty.';

create or replace view landintel_reporting.v_site_measurement_readiness_matrix
with (security_invoker = true) as
with active_priority_layers as (
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
layer_count as (
    select count(*)::integer as active_priority_layer_count
    from active_priority_layers
),
priority_site as (
    select *
    from landintel_reporting.v_constraint_priority_sites
),
site_measurements as (
    select
        measurement.site_location_id,
        count(*)::integer as measurement_row_count,
        count(distinct measurement.constraint_layer_id)::integer as measured_layer_count,
        max(measurement.measured_at) as latest_measured_at
    from public.site_constraint_measurements as measurement
    group by measurement.site_location_id
),
site_scan_state as (
    select
        scan_state.site_location_id,
        count(*)::integer as scan_state_row_count,
        count(distinct scan_state.constraint_layer_id)::integer as scanned_layer_count,
        count(*) filter (where scan_state.has_constraint_relationship)::integer as positive_scan_state_count,
        max(scan_state.scanned_at) as latest_scanned_at
    from public.site_constraint_measurement_scan_state as scan_state
    where scan_state.scan_scope = 'canonical_site_geometry'
    group by scan_state.site_location_id
),
site_constraint_facts as (
    select
        facts.site_location_id,
        count(*)::integer as commercial_friction_fact_count,
        max(facts.created_at) as latest_commercial_friction_fact_at
    from public.site_commercial_friction_facts as facts
    group by facts.site_location_id
),
next_constraint_pair as (
    select distinct on (queue.canonical_site_id)
        queue.canonical_site_id,
        queue.site_location_id,
        queue.site_priority_band,
        queue.site_priority_rank,
        queue.constraint_priority_family,
        queue.source_family,
        queue.constraint_layer_id,
        queue.layer_key,
        queue.layer_name,
        queue.constraint_priority_rank,
        queue.queue_rank,
        queue.bounded_run_guidance
    from landintel_reporting.v_constraint_priority_measurement_queue as queue
    order by
        queue.canonical_site_id,
        queue.site_priority_rank,
        queue.constraint_priority_rank,
        queue.priority_family_queue_rank nulls last,
        queue.queue_rank
),
queue_counts as (
    select
        queue.canonical_site_id,
        count(*)::integer as unscanned_priority_pair_count,
        count(distinct queue.constraint_layer_id)::integer as unscanned_priority_layer_count,
        min(queue.queue_rank)::integer as best_queue_rank
    from landintel_reporting.v_constraint_priority_measurement_queue as queue
    group by queue.canonical_site_id
),
evidence_counts as (
    select
        evidence.canonical_site_id,
        count(*)::integer as evidence_count,
        max(evidence.created_at) as latest_evidence_at
    from landintel.evidence_references as evidence
    group by evidence.canonical_site_id
),
signal_counts as (
    select
        signal.canonical_site_id,
        count(*)::integer as signal_count,
        max(signal.created_at) as latest_signal_at
    from landintel.site_signals as signal
    group by signal.canonical_site_id
)
select
    title_matrix.canonical_site_id,
    title_matrix.site_location_id,
    title_matrix.site_label,
    title_matrix.authority_name,
    title_matrix.gross_area_acres,
    priority_site.site_priority_band,
    priority_site.site_priority_rank,
    title_matrix.title_traceability_status,
    title_matrix.safe_title_candidate_count,
    title_matrix.parcel_candidate_count,
    title_matrix.licensed_bridge_required_count,
    coalesce(layer_count.active_priority_layer_count, 0) as active_priority_constraint_layer_count,
    coalesce(measurements.measurement_row_count, 0) as constraint_measurement_row_count,
    coalesce(measurements.measured_layer_count, 0) as measured_constraint_layer_count,
    coalesce(scan_state.scan_state_row_count, 0) as constraint_scan_state_row_count,
    coalesce(scan_state.scanned_layer_count, 0) as scanned_constraint_layer_count,
    coalesce(scan_state.positive_scan_state_count, 0) as positive_constraint_scan_state_count,
    coalesce(facts.commercial_friction_fact_count, 0) as commercial_friction_fact_count,
    coalesce(queue_counts.unscanned_priority_pair_count, 0) as unscanned_priority_pair_count,
    coalesce(queue_counts.unscanned_priority_layer_count, 0) as unscanned_priority_layer_count,
    next_pair.constraint_priority_family as next_constraint_priority_family,
    next_pair.source_family as next_constraint_source_family,
    next_pair.layer_key as next_constraint_layer_key,
    next_pair.layer_name as next_constraint_layer_name,
    next_pair.queue_rank as next_constraint_queue_rank,
    coalesce(evidence_counts.evidence_count, 0) as evidence_count,
    coalesce(signal_counts.signal_count, 0) as signal_count,
    greatest(
        title_matrix.latest_title_traceability_activity_at,
        measurements.latest_measured_at,
        scan_state.latest_scanned_at,
        facts.latest_commercial_friction_fact_at,
        evidence_counts.latest_evidence_at,
        signal_counts.latest_signal_at
    ) as latest_dd_activity_at,
    case
        when title_matrix.gross_area_acres is not null and title_matrix.gross_area_acres < 4 then 'below_minimum_area'
        when title_matrix.title_traceability_status = 'no_site_geometry' then 'blocked_no_site_geometry'
        when priority_site.canonical_site_id is null then 'not_in_constraint_priority_queue'
        when coalesce(queue_counts.unscanned_priority_pair_count, 0) > 0 then 'priority_constraint_measurement_backlog'
        when coalesce(scan_state.scanned_layer_count, 0) >= coalesce(layer_count.active_priority_layer_count, 0)
          and coalesce(layer_count.active_priority_layer_count, 0) > 0 then 'priority_constraints_scanned'
        when coalesce(scan_state.scanned_layer_count, 0) > 0 then 'priority_constraints_partially_scanned'
        else 'measurement_not_started'
    end as measurement_readiness_status,
    case
        when title_matrix.gross_area_acres is not null and title_matrix.gross_area_acres < 4 then 'Do not prioritise measurement unless size rule is intentionally waived.'
        when title_matrix.title_traceability_status = 'no_site_geometry' then 'Repair site geometry before parcel linking or constraint measurement.'
        when coalesce(queue_counts.unscanned_priority_pair_count, 0) > 0 then 'Run the next bounded constraint measurement batch for the recommended source family/layer.'
        when coalesce(scan_state.scanned_layer_count, 0) > 0 then 'Use measured facts and scan-state to support DD; continue lower-priority layers only after source-family proof stays clean.'
        else 'Use title traceability and evidence/signals to decide the next bounded data step.'
    end as next_measurement_action,
    'Measurement status is a data-readiness surface only. Constraint outputs remain measured facts, not RAG scores, planning conclusions, legal certainty or engineering certainty.'::text as measurement_caveat
from landintel_reporting.v_site_title_traceability_matrix as title_matrix
cross join layer_count
left join priority_site
  on priority_site.canonical_site_id = title_matrix.canonical_site_id
left join site_measurements as measurements
  on measurements.site_location_id = title_matrix.site_location_id
left join site_scan_state as scan_state
  on scan_state.site_location_id = title_matrix.site_location_id
left join site_constraint_facts as facts
  on facts.site_location_id = title_matrix.site_location_id
left join next_constraint_pair as next_pair
  on next_pair.canonical_site_id = title_matrix.canonical_site_id
left join queue_counts
  on queue_counts.canonical_site_id = title_matrix.canonical_site_id
left join evidence_counts
  on evidence_counts.canonical_site_id = title_matrix.canonical_site_id
left join signal_counts
  on signal_counts.canonical_site_id = title_matrix.canonical_site_id;

comment on view landintel_reporting.v_site_measurement_readiness_matrix is
    'Per-site DD measurement readiness matrix. It combines title traceability, constraint scan-state, measured constraint rows, evidence and signals without running broad scans. This view does not execute measurement.';

create or replace view landintel_reporting.v_site_dd_orchestration_queue
with (security_invoker = true) as
select
    row_number() over (
        order by
            case
                when measurement.site_priority_band = 'title_spend_candidates' then 1
                when measurement.site_priority_band = 'review_queue' then 2
                when measurement.site_priority_band = 'ldn_candidate_screen' then 3
                when measurement.site_priority_band = 'prove_it_candidates' then 4
                when measurement.site_priority_band = 'wider_canonical_sites' then 5
                else 9
            end,
            case
                when measurement.title_traceability_status in ('needs_ros_parcel_linking', 'parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge') then 1
                when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 2
                when measurement.title_traceability_status = 'safe_title_candidate_available' then 3
                else 9
            end,
            measurement.next_constraint_queue_rank nulls last,
            measurement.gross_area_acres desc nulls last,
            measurement.canonical_site_id
    ) as orchestration_queue_rank,
    measurement.canonical_site_id,
    measurement.site_location_id,
    measurement.site_label,
    measurement.authority_name,
    measurement.gross_area_acres,
    measurement.site_priority_band,
    measurement.title_traceability_status,
    measurement.measurement_readiness_status,
    measurement.safe_title_candidate_count,
    measurement.parcel_candidate_count,
    measurement.licensed_bridge_required_count,
    measurement.constraint_measurement_row_count,
    measurement.constraint_scan_state_row_count,
    measurement.unscanned_priority_pair_count,
    measurement.next_constraint_priority_family,
    measurement.next_constraint_source_family,
    measurement.next_constraint_layer_key,
    measurement.next_constraint_layer_name,
    case
        when measurement.measurement_readiness_status = 'below_minimum_area' then 'hold_below_minimum_area'
        when measurement.title_traceability_status = 'no_site_geometry' then 'repair_site_geometry'
        when measurement.title_traceability_status = 'needs_ros_parcel_linking' then 'link_site_to_ros_parcel'
        when measurement.title_traceability_status in ('parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge') then 'resolve_title_candidate'
        when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 'measure_next_constraint_layer'
        when measurement.title_traceability_status = 'safe_title_candidate_available' then 'manual_title_review_or_title_spend_decision'
        else 'ready_for_operator_review'
    end as orchestration_step,
    case
        when measurement.title_traceability_status = 'needs_ros_parcel_linking' then 'link-sites-to-ros-parcels'
        when measurement.title_traceability_status in ('parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge') then 'resolve-title-numbers'
        when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog'
          and measurement.site_priority_band = 'title_spend_candidates' then 'constraint-measurement-proof-title-spend-source-family'
        when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 'measure-constraints-duckdb'
        when measurement.title_traceability_status = 'safe_title_candidate_available' then 'refresh-title-readiness'
        else 'audit-site-dd-orchestration'
    end as recommended_workflow_command,
    case
        when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog'
          and measurement.next_constraint_source_family is not null
            then 'constraint_measure_source_family=' || measurement.next_constraint_source_family
        when measurement.title_traceability_status in ('needs_ros_parcel_linking', 'parcel_linked_no_safe_title_candidate', 'parcel_linked_needs_licensed_title_bridge')
            then 'Use existing bounded title/parcel workflow inputs; do not manually mutate title truth.'
        else null
    end as recommended_workflow_input_hint,
    case
        when measurement.title_traceability_status = 'needs_ros_parcel_linking' then 'Site has no non-rejected RoS parcel link candidate yet.'
        when measurement.title_traceability_status = 'parcel_linked_needs_licensed_title_bridge' then 'RoS parcel candidate exists but title-number candidate needs licensed/manual bridge.'
        when measurement.title_traceability_status = 'parcel_linked_no_safe_title_candidate' then 'RoS parcel candidate exists but no operator-safe title-number candidate is available.'
        when measurement.measurement_readiness_status = 'priority_constraint_measurement_backlog' then 'Priority constraint scan-state is incomplete for this site.'
        when measurement.title_traceability_status = 'safe_title_candidate_available' then 'Operator-safe title candidate exists, but ownership is still unconfirmed without human review.'
        else 'Site has traceability and current priority measurement coverage suitable for operator review.'
    end as orchestration_reason,
    'Run en masse through bounded workflow batches only: parcel/title linking first, then one constraint source family or layer at a time, then audit. Do not run broad all-site/all-layer scans.'::text as bounded_orchestration_caveat
from landintel_reporting.v_site_measurement_readiness_matrix as measurement
where measurement.measurement_readiness_status <> 'below_minimum_area'
   or measurement.site_priority_band in ('title_spend_candidates', 'review_queue');

comment on view landintel_reporting.v_site_dd_orchestration_queue is
    'Operator queue for mass DD orchestration. It identifies the next safe workflow step per site without moving data, confirming ownership or measuring constraints by itself.';

create or replace view landintel_reporting.v_site_dd_orchestration_summary
with (security_invoker = true) as
select
    coalesce(site_priority_band, 'unprioritised') as site_priority_band,
    title_traceability_status,
    measurement_readiness_status,
    count(*)::bigint as site_count,
    count(*) filter (where safe_title_candidate_count > 0)::bigint as sites_with_safe_title_candidate,
    count(*) filter (where parcel_candidate_count > 0)::bigint as sites_with_ros_parcel_candidate,
    count(*) filter (where constraint_scan_state_row_count > 0)::bigint as sites_with_constraint_scan_state,
    count(*) filter (where constraint_measurement_row_count > 0)::bigint as sites_with_constraint_measurements,
    sum(unscanned_priority_pair_count)::bigint as unscanned_priority_pair_count
from landintel_reporting.v_site_measurement_readiness_matrix
group by
    coalesce(site_priority_band, 'unprioritised'),
    title_traceability_status,
    measurement_readiness_status;

comment on view landintel_reporting.v_site_dd_orchestration_summary is
    'Aggregated proof surface for site title traceability and DD measurement readiness.';

grant usage on schema landintel_reporting to authenticated;
grant select on landintel_reporting.v_site_title_traceability_matrix to authenticated;
grant select on landintel_reporting.v_site_measurement_readiness_matrix to authenticated;
grant select on landintel_reporting.v_site_dd_orchestration_queue to authenticated;
grant select on landintel_reporting.v_site_dd_orchestration_summary to authenticated;

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
        values
            (
                'landintel_reporting',
                'v_site_title_traceability_matrix',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'site title traceability matrix',
                'title_control',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                'Operator traceability surface only. It separates RoS parcel references, title candidates, title orders and human title review.',
                'Use to identify sites needing parcel linking, title bridge or manual title review; do not treat as ownership confirmation.',
                '{"migration":"077_site_title_measurement_orchestration","ownership_confirmation":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_site_measurement_readiness_matrix',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'site DD measurement readiness matrix',
                'constraints',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                'Readiness surface only. It reads constraint measurements and scan-state without executing spatial measurement.',
                'Use to decide next bounded constraint source-family/layer batch.',
                '{"migration":"077_site_title_measurement_orchestration","runs_measurement":false}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_site_dd_orchestration_queue',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'site DD orchestration queue',
                'dd_orchestration',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                'Queue is guidance only. It must be executed through bounded workflows, not broad manual scans.',
                'Use as the operating queue for parcel/title linking and layer-by-layer constraint measurement.',
                '{"migration":"077_site_title_measurement_orchestration","guidance_only":true}'::jsonb
            ),
            (
                'landintel_reporting',
                'v_site_dd_orchestration_summary',
                'view',
                'reporting_surface',
                'landintel_reporting',
                'site DD orchestration summary',
                'dd_orchestration',
                true,
                true,
                true,
                true,
                false,
                true,
                false,
                'Aggregated proof surface for title traceability and DD measurement coverage.',
                'Use after each bounded workflow run to prove movement in traceability, scan-state and measurement coverage.',
                '{"migration":"077_site_title_measurement_orchestration","proof_surface":true}'::jsonb
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

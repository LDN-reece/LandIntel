create schema if not exists landintel_reporting;

comment on schema landintel_reporting is
    'Human and machine-readable views for dashboards, audits, UI and operator review.';

create or replace view landintel_reporting.v_title_candidates_operator_safe
with (security_invoker = true) as
with title_candidate_rows as (
    select
        site.id as canonical_site_id,
        validation.title_number,
        validation.normalized_title_number,
        coalesce(validation.title_source, 'site_title_validation') as candidate_source,
        validation.validation_status as candidate_status,
        validation.confidence,
        validation.created_at,
        validation.updated_at,
        1 as source_rank
    from landintel.canonical_sites as site
    join public.site_title_validation as validation
      on validation.site_id = site.id::text
    where validation.validation_status <> 'rejected'
      and public.is_scottish_title_number_candidate(validation.normalized_title_number)

    union all

    select
        site.id as canonical_site_id,
        candidate.candidate_title_number as title_number,
        candidate.normalized_title_number,
        candidate.candidate_source,
        candidate.resolution_status as candidate_status,
        candidate.confidence,
        candidate.created_at,
        candidate.updated_at,
        2 as source_rank
    from landintel.canonical_sites as site
    join public.site_title_resolution_candidates as candidate
      on candidate.site_id = site.id::text
    where candidate.resolution_status <> 'rejected'
      and public.is_scottish_title_number_candidate(candidate.normalized_title_number)

    union all

    select
        workflow.canonical_site_id,
        workflow.title_number,
        workflow.normalized_title_number,
        'title_order_workflow' as candidate_source,
        workflow.possible_title_reference_status as candidate_status,
        workflow.title_confidence_level as confidence,
        workflow.created_at,
        workflow.updated_at,
        3 as source_rank
    from landintel.title_order_workflow as workflow
    where workflow.normalized_title_number is not null
      and public.is_scottish_title_number_candidate(workflow.normalized_title_number)
), best_title_candidate as (
    select distinct on (candidate.canonical_site_id)
        candidate.*
    from title_candidate_rows as candidate
    order by
        candidate.canonical_site_id,
        candidate.source_rank,
        candidate.confidence desc nulls last,
        candidate.updated_at desc nulls last
), title_candidate_counts as (
    select
        candidate.canonical_site_id,
        count(*)::integer as safe_title_candidate_count,
        max(candidate.updated_at) as latest_title_candidate_at
    from title_candidate_rows as candidate
    group by candidate.canonical_site_id
), parcel_reference_counts as (
    select
        site.id as canonical_site_id,
        count(*) filter (where parcel_link.link_status <> 'rejected')::integer as ros_parcel_reference_count,
        count(*) filter (where parcel_link.link_status = 'rejected')::integer as rejected_ros_parcel_reference_count,
        max(parcel_link.updated_at) as latest_ros_parcel_reference_at
    from landintel.canonical_sites as site
    join public.site_ros_parcel_link_candidates as parcel_link
      on parcel_link.site_id = site.id::text
    group by site.id
), rejected_sct_audit as (
    select
        site.id as canonical_site_id,
        count(*)::integer as rejected_sct_like_audit_count,
        max(validation.updated_at) as latest_rejected_sct_like_at
    from landintel.canonical_sites as site
    join public.site_title_validation as validation
      on validation.site_id = site.id::text
    where validation.validation_status = 'rejected'
      and coalesce(validation.normalized_title_number, validation.title_number) ~ '^SCT[0-9]+$'
    group by site.id
), rejected_resolution_sct_audit as (
    select
        site.id as canonical_site_id,
        count(*)::integer as rejected_resolution_sct_like_audit_count,
        max(candidate.updated_at) as latest_rejected_resolution_sct_like_at
    from landintel.canonical_sites as site
    join public.site_title_resolution_candidates as candidate
      on candidate.site_id = site.id::text
    where candidate.resolution_status = 'rejected'
      and coalesce(candidate.normalized_title_number, candidate.candidate_title_number, candidate.ros_inspire_id, candidate.cadastral_unit_identifier) ~ '^SCT[0-9]+$'
    group by site.id
)
select
    site.id as canonical_site_id,
    coalesce(nullif(site.site_name_primary, ''), 'Sourced land parcel') as site_label,
    site.authority_name,
    site.area_acres as gross_area_acres,
    best.title_number as title_number_candidate_for_manual_check,
    best.normalized_title_number,
    best.candidate_source as title_candidate_source,
    best.candidate_status as title_candidate_status,
    best.confidence as title_candidate_confidence,
    coalesce(title_counts.safe_title_candidate_count, 0) as safe_title_candidate_count,
    coalesce(parcel_counts.ros_parcel_reference_count, 0) as ros_parcel_reference_count,
    coalesce(parcel_counts.rejected_ros_parcel_reference_count, 0) as rejected_ros_parcel_reference_count,
    coalesce(rejected_sct.rejected_sct_like_audit_count, 0)
      + coalesce(rejected_resolution_sct.rejected_resolution_sct_like_audit_count, 0) as rejected_sct_like_audit_count,
    greatest(
        title_counts.latest_title_candidate_at,
        parcel_counts.latest_ros_parcel_reference_at,
        rejected_sct.latest_rejected_sct_like_at,
        rejected_resolution_sct.latest_rejected_resolution_sct_like_at
    ) as latest_candidate_activity_at,
    case
        when coalesce(title_counts.safe_title_candidate_count, 0) > 0 then 'title_candidate_available'
        when coalesce(parcel_counts.ros_parcel_reference_count, 0) > 0 then 'ros_parcel_reference_only'
        when coalesce(rejected_sct.rejected_sct_like_audit_count, 0)
           + coalesce(rejected_resolution_sct.rejected_resolution_sct_like_audit_count, 0) > 0 then 'audit_only_rejected_sct_reference'
        else 'no_title_candidate'
    end as operator_candidate_status,
    'Operator-safe title candidate view: RoS parcel references are not title numbers. SCT-like rejected values remain audit-only and are not exposed as title candidates.'::text as caveat
from landintel.canonical_sites as site
left join best_title_candidate as best
  on best.canonical_site_id = site.id
left join title_candidate_counts as title_counts
  on title_counts.canonical_site_id = site.id
left join parcel_reference_counts as parcel_counts
  on parcel_counts.canonical_site_id = site.id
left join rejected_sct_audit as rejected_sct
  on rejected_sct.canonical_site_id = site.id
left join rejected_resolution_sct_audit as rejected_resolution_sct
  on rejected_resolution_sct.canonical_site_id = site.id
where best.canonical_site_id is not null
   or title_counts.canonical_site_id is not null
   or parcel_counts.canonical_site_id is not null
   or rejected_sct.canonical_site_id is not null
   or rejected_resolution_sct.canonical_site_id is not null;

create or replace view landintel_reporting.v_title_control_status
with (security_invoker = true) as
with latest_title_review as (
    select distinct on (review.canonical_site_id)
        review.*
    from landintel.title_review_records as review
    order by
        review.canonical_site_id,
        review.review_date desc nulls last,
        review.updated_at desc nulls last,
        review.created_at desc nulls last
), latest_title_workflow as (
    select distinct on (workflow.canonical_site_id)
        workflow.*
    from landintel.title_order_workflow as workflow
    order by workflow.canonical_site_id, workflow.updated_at desc nulls last, workflow.created_at desc nulls last
), latest_prove_it as (
    select distinct on (assessment.canonical_site_id)
        assessment.*
    from landintel.site_prove_it_assessments as assessment
    order by
        assessment.canonical_site_id,
        assessment.assessment_version desc nulls last,
        assessment.updated_at desc nulls last,
        assessment.created_at desc nulls last
), latest_ldn_screen as (
    select distinct on (screen.canonical_site_id)
        screen.*
    from landintel.site_ldn_candidate_screen as screen
    order by screen.canonical_site_id, screen.updated_at desc nulls last, screen.created_at desc nulls last
), latest_pack as (
    select distinct on (pack.canonical_site_id)
        pack.*
    from landintel.site_urgent_address_title_pack as pack
    order by pack.canonical_site_id, pack.updated_at desc nulls last, pack.created_at desc nulls last
), control_signal_rollup as (
    select
        signal.canonical_site_id,
        count(*)::integer as control_signal_count,
        bool_or(coalesce(signal.ownership_confirmed, false)) as any_control_signal_claims_ownership_confirmed,
        array_remove(array_agg(distinct signal.signal_type order by signal.signal_type), null) as control_signal_types,
        array_remove(array_agg(distinct signal.source_key order by signal.source_key), null) as control_signal_sources,
        max(signal.updated_at) as latest_control_signal_at
    from landintel.ownership_control_signals as signal
    where signal.canonical_site_id is not null
    group by signal.canonical_site_id
), evidence_rollup as (
    select
        evidence.canonical_site_id,
        count(*) filter (where evidence.source_family = 'title_control')::integer as title_control_evidence_count,
        max(evidence.created_at) filter (where evidence.source_family = 'title_control') as latest_title_control_evidence_at
    from landintel.evidence_references as evidence
    where evidence.canonical_site_id is not null
    group by evidence.canonical_site_id
), candidates as (
    select *
    from landintel_reporting.v_title_candidates_operator_safe
), status_inputs as (
    select
        site.id as canonical_site_id,
        coalesce(nullif(site.site_name_primary, ''), 'Sourced land parcel') as site_label,
        site.authority_name,
        site.area_acres as gross_area_acres,
        title_review.id as title_review_record_id,
        title_review.title_number as reviewed_title_number,
        title_review.normalized_title_number as reviewed_normalized_title_number,
        title_review.registered_proprietor,
        title_review.proprietor_type,
        title_review.company_number,
        title_review.ownership_outcome,
        title_review.next_action as title_review_next_action,
        title_review.review_date,
        title_review.updated_at as title_review_updated_at,
        title_workflow.title_number as workflow_title_number,
        title_workflow.normalized_title_number as workflow_normalized_title_number,
        title_workflow.parcel_candidate_status,
        title_workflow.possible_title_reference_status,
        title_workflow.ownership_status_pre_title,
        title_workflow.title_required_flag,
        title_workflow.title_order_status,
        title_workflow.title_review_status,
        title_workflow.title_confidence_level,
        title_workflow.control_signal_summary,
        title_workflow.next_action as title_workflow_next_action,
        title_workflow.updated_at as title_workflow_updated_at,
        prove_it.verdict as prove_it_verdict,
        prove_it.title_spend_recommendation as prove_it_title_spend_recommendation,
        prove_it.title_spend_reason as prove_it_title_spend_reason,
        prove_it.review_next_action as prove_it_next_action,
        ldn.candidate_status as ldn_candidate_status,
        ldn.title_spend_position as ldn_title_spend_position,
        ldn.next_action as ldn_next_action,
        pack.urgency_status as urgent_title_pack_status,
        pack.title_spend_recommendation as pack_title_spend_recommendation,
        pack.next_action as pack_next_action,
        candidates.title_number_candidate_for_manual_check,
        candidates.normalized_title_number as candidate_normalized_title_number,
        candidates.title_candidate_source,
        candidates.title_candidate_confidence,
        coalesce(candidates.safe_title_candidate_count, 0) as safe_title_candidate_count,
        coalesce(candidates.ros_parcel_reference_count, 0) as ros_parcel_reference_count,
        coalesce(candidates.rejected_sct_like_audit_count, 0) as rejected_sct_like_audit_count,
        coalesce(control.control_signal_count, 0) as control_signal_count,
        coalesce(control.any_control_signal_claims_ownership_confirmed, false) as any_control_signal_claims_ownership_confirmed,
        coalesce(control.control_signal_types, '{}'::text[]) as control_signal_types,
        coalesce(control.control_signal_sources, '{}'::text[]) as control_signal_sources,
        control.latest_control_signal_at,
        coalesce(evidence.title_control_evidence_count, 0) as title_control_evidence_count,
        evidence.latest_title_control_evidence_at
    from landintel.canonical_sites as site
    left join latest_title_review as title_review
      on title_review.canonical_site_id = site.id
    left join latest_title_workflow as title_workflow
      on title_workflow.canonical_site_id = site.id
    left join latest_prove_it as prove_it
      on prove_it.canonical_site_id = site.id
    left join latest_ldn_screen as ldn
      on ldn.canonical_site_id = site.id
    left join latest_pack as pack
      on pack.canonical_site_id = site.id
    left join candidates
      on candidates.canonical_site_id = site.id
    left join control_signal_rollup as control
      on control.canonical_site_id = site.id
    left join evidence_rollup as evidence
      on evidence.canonical_site_id = site.id
)
select
    inputs.canonical_site_id,
    inputs.site_label,
    inputs.authority_name,
    inputs.gross_area_acres,
    case
        when inputs.title_review_record_id is not null
         and coalesce(inputs.ownership_outcome, '') ~* '(confirmed|known|clear|reviewed|attractive)'
         and coalesce(inputs.ownership_outcome, '') !~* '(unclear|issue|problem|blocked|complex|dispute|adverse)' then 'title_reviewed_confirmed'
        when inputs.title_review_record_id is not null then 'title_reviewed_issue'
        when coalesce(inputs.title_order_status, '') not in ('', 'not_ordered', 'not ordered', 'not_ordered_yet')
          or coalesce(inputs.title_review_status, '') not in ('', 'not_reviewed', 'not reviewed') then 'title_ordered'
        when coalesce(inputs.prove_it_title_spend_recommendation, '') in ('order_title', 'order_title_urgently')
          or coalesce(inputs.pack_title_spend_recommendation, '') in ('order_title', 'order_title_urgently')
          or coalesce(inputs.urgent_title_pack_status, '') = 'order_title_urgently'
          or coalesce(inputs.ldn_title_spend_position, '') = 'title_may_be_justified_after_dd' then 'title_order_recommended'
        when inputs.safe_title_candidate_count > 0 then 'title_candidate_available'
        when inputs.control_signal_count > 0 then 'control_hypothesis_only'
        when coalesce(inputs.title_required_flag, false) = false then 'title_not_required_yet'
        else 'ownership_unconfirmed'
    end as title_control_status,
    case
        when inputs.title_review_record_id is not null then 'human_title_review_recorded'
        when inputs.control_signal_count > 0 then 'control_hypothesis_only'
        else 'ownership_unconfirmed'
    end as ownership_control_status,
    inputs.reviewed_title_number,
    inputs.reviewed_normalized_title_number,
    inputs.registered_proprietor,
    inputs.proprietor_type,
    inputs.company_number,
    inputs.ownership_outcome,
    inputs.review_date,
    inputs.title_number_candidate_for_manual_check,
    inputs.candidate_normalized_title_number,
    inputs.title_candidate_source,
    inputs.title_candidate_confidence,
    inputs.safe_title_candidate_count,
    inputs.ros_parcel_reference_count,
    inputs.rejected_sct_like_audit_count,
    inputs.parcel_candidate_status,
    inputs.possible_title_reference_status,
    inputs.ownership_status_pre_title,
    inputs.title_required_flag,
    inputs.title_order_status,
    inputs.title_review_status,
    inputs.prove_it_title_spend_recommendation,
    inputs.ldn_title_spend_position,
    inputs.urgent_title_pack_status,
    inputs.control_signal_count,
    inputs.any_control_signal_claims_ownership_confirmed,
    inputs.control_signal_types,
    inputs.control_signal_sources,
    inputs.title_control_evidence_count,
    greatest(
        inputs.title_review_updated_at,
        inputs.title_workflow_updated_at,
        inputs.latest_control_signal_at,
        inputs.latest_title_control_evidence_at
    ) as latest_title_control_activity_at,
    coalesce(
        nullif(inputs.title_review_next_action, ''),
        nullif(inputs.pack_next_action, ''),
        nullif(inputs.prove_it_next_action, ''),
        nullif(inputs.ldn_next_action, ''),
        nullif(inputs.title_workflow_next_action, ''),
        'Review title/control evidence before spend.'
    ) as recommended_next_action,
    case
        when inputs.title_review_record_id is null then
            'Ownership remains unconfirmed because no human title review record exists. RoS parcel references are not title numbers; Companies House, FCA and control signals are hypotheses only until title review.'
        else
            'Human title review evidence exists. Use title_review_records for ownership/control interpretation and retain pre-title signals as supporting evidence only.'
    end as caveat
from status_inputs as inputs;

create or replace view landintel_reporting.v_sites_needing_title_review
with (security_invoker = true) as
select
    status.canonical_site_id,
    status.site_label,
    status.authority_name,
    status.gross_area_acres,
    status.title_control_status,
    status.ownership_control_status,
    status.title_number_candidate_for_manual_check,
    status.title_candidate_source,
    status.title_candidate_confidence,
    status.safe_title_candidate_count,
    status.ros_parcel_reference_count,
    status.rejected_sct_like_audit_count,
    status.control_signal_count,
    status.prove_it_title_spend_recommendation,
    status.ldn_title_spend_position,
    status.urgent_title_pack_status,
    status.recommended_next_action,
    status.latest_title_control_activity_at,
    case
        when status.title_control_status = 'title_order_recommended' then 'Title order is recommended by existing review evidence.'
        when status.title_control_status = 'title_candidate_available' then 'A title-number-shaped candidate is available for manual check.'
        when status.title_control_status = 'control_hypothesis_only' then 'Control or corporate signal exists, but remains hypothesis-only before title review.'
        when status.ros_parcel_reference_count > 0 then 'RoS parcel reference exists, but it is not a title number.'
        else 'Ownership remains unconfirmed and title review has not been recorded.'
    end as title_review_reason,
    'Do not treat this queue as ownership confirmation. It is a human review queue.'::text as caveat
from landintel_reporting.v_title_control_status as status
where status.reviewed_title_number is null
  and status.title_control_status in (
        'title_candidate_available',
        'title_order_recommended',
        'ownership_unconfirmed',
        'control_hypothesis_only'
  );

create or replace view landintel_reporting.v_title_spend_queue
with (security_invoker = true) as
select
    status.canonical_site_id,
    status.site_label,
    status.authority_name,
    status.gross_area_acres,
    status.title_control_status,
    status.ownership_control_status,
    status.title_number_candidate_for_manual_check,
    status.title_candidate_source,
    status.safe_title_candidate_count,
    status.ros_parcel_reference_count,
    status.rejected_sct_like_audit_count,
    status.control_signal_count,
    status.prove_it_title_spend_recommendation,
    status.ldn_title_spend_position,
    status.urgent_title_pack_status,
    status.recommended_next_action,
    status.latest_title_control_activity_at,
    case
        when status.urgent_title_pack_status = 'order_title_urgently'
          or status.prove_it_title_spend_recommendation = 'order_title_urgently' then '1_order_title_urgently'
        when status.prove_it_title_spend_recommendation = 'order_title'
          or status.title_control_status = 'title_order_recommended' then '2_order_title'
        when status.title_control_status = 'title_candidate_available'
          or status.ldn_title_spend_position = 'title_may_be_justified_after_dd' then '3_manual_review_before_title_spend'
        when status.control_signal_count > 0 then '4_control_hypothesis_review'
        else '5_not_ready_for_title_spend'
    end as title_spend_priority,
    case
        when status.urgent_title_pack_status = 'order_title_urgently'
          or status.prove_it_title_spend_recommendation = 'order_title_urgently' then 'Existing evidence says title spend may be urgent, but ownership remains unconfirmed until human title review.'
        when status.prove_it_title_spend_recommendation = 'order_title'
          or status.title_control_status = 'title_order_recommended' then 'Existing evidence recommends title order; confirm spend decision manually.'
        when status.title_control_status = 'title_candidate_available' then 'A title-number-shaped candidate exists; manual title review is needed before ownership/control interpretation.'
        when status.control_signal_count > 0 then 'Companies House, FCA or control signals are hypotheses only until title review.'
        else 'Not enough title/control evidence to justify spend yet.'
    end as title_spend_reason,
    'Title spend queue only. This view does not confirm ownership, control or legal title.'::text as caveat
from landintel_reporting.v_title_control_status as status
where status.reviewed_title_number is null
  and status.title_control_status in (
        'title_order_recommended',
        'title_candidate_available',
        'control_hypothesis_only',
        'ownership_unconfirmed'
  );

grant usage on schema landintel_reporting to authenticated;
grant select on landintel_reporting.v_title_candidates_operator_safe to authenticated;
grant select on landintel_reporting.v_title_control_status to authenticated;
grant select on landintel_reporting.v_sites_needing_title_review to authenticated;
grant select on landintel_reporting.v_title_spend_queue to authenticated;

comment on view landintel_reporting.v_title_candidates_operator_safe is
    'Operator-safe title candidate surface. Valid title-number-shaped candidates are separated from RoS parcel references; rejected SCT-like values remain audit-only.';

comment on view landintel_reporting.v_title_control_status is
    'Operator-safe title/control status view. Ownership remains unconfirmed unless landintel.title_review_records supports the position.';

comment on view landintel_reporting.v_sites_needing_title_review is
    'Sites where title review is still needed. RoS parcel references and control signals are not legal ownership proof.';

comment on view landintel_reporting.v_title_spend_queue is
    'Title spend queue for LDN operators. Companies House, FCA and control signals remain hypotheses until title review.';

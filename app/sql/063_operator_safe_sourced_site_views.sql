create schema if not exists landintel_sourced;

comment on schema landintel_sourced is
    'LandIntel Sourced Sites, the polished commercial opportunity register for LDN review.';

create or replace view landintel_sourced.v_sourced_sites
with (security_invoker = true) as
with latest_prove_it as (
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
), latest_assessment as (
    select distinct on (assessment.canonical_site_id)
        assessment.*
    from landintel.site_assessments as assessment
    order by
        assessment.canonical_site_id,
        assessment.assessment_version desc nulls last,
        assessment.updated_at desc nulls last,
        assessment.created_at desc nulls last
), latest_title_workflow as (
    select distinct on (workflow.canonical_site_id)
        workflow.*
    from landintel.title_order_workflow as workflow
    order by workflow.canonical_site_id, workflow.updated_at desc nulls last, workflow.created_at desc nulls last
), latest_title_review as (
    select distinct on (review.canonical_site_id)
        review.*
    from landintel.title_review_records as review
    order by
        review.canonical_site_id,
        review.review_date desc nulls last,
        review.updated_at desc nulls last,
        review.created_at desc nulls last
), evidence_rollup as (
    select
        evidence.canonical_site_id,
        count(*)::bigint as evidence_count
    from landintel.evidence_references as evidence
    where evidence.canonical_site_id is not null
    group by evidence.canonical_site_id
), signal_rollup as (
    select
        signal.canonical_site_id,
        array_remove(array_agg(distinct signal.signal_value_text order by signal.signal_value_text), null) as signal_values,
        max(signal.updated_at) as latest_signal_at
    from landintel.site_signals as signal
    where signal.canonical_site_id is not null
      and coalesce(signal.current_flag, true)
    group by signal.canonical_site_id
), change_rollup as (
    select
        event.canonical_site_id,
        max(event.created_at) as latest_change_event_at
    from landintel.site_change_events as event
    where event.canonical_site_id is not null
    group by event.canonical_site_id
), title_candidate_union as (
    select
        pack.canonical_site_id,
        pack.title_number,
        pack.normalized_title_number,
        pack.title_candidate_source,
        pack.title_confidence,
        1 as source_rank
    from latest_pack as pack
    where pack.normalized_title_number is not null
      and public.is_scottish_title_number_candidate(pack.normalized_title_number)

    union all

    select
        site.id as canonical_site_id,
        validation.title_number,
        validation.normalized_title_number,
        coalesce(validation.title_source, 'site_title_validation') as title_candidate_source,
        validation.confidence as title_confidence,
        2 as source_rank
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
        candidate.candidate_source as title_candidate_source,
        candidate.confidence as title_confidence,
        3 as source_rank
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
        'title_order_workflow' as title_candidate_source,
        workflow.title_confidence_level as title_confidence,
        4 as source_rank
    from latest_title_workflow as workflow
    where workflow.normalized_title_number is not null
      and public.is_scottish_title_number_candidate(workflow.normalized_title_number)
), safe_title_candidate as (
    select distinct on (candidate.canonical_site_id)
        candidate.canonical_site_id,
        candidate.title_number,
        candidate.normalized_title_number,
        candidate.title_candidate_source,
        candidate.title_confidence
    from title_candidate_union as candidate
    order by
        candidate.canonical_site_id,
        candidate.source_rank,
        candidate.title_confidence desc nulls last
), parcel_candidate_rollup as (
    select
        site.id as canonical_site_id,
        count(*)::integer as ros_parcel_candidate_count
    from landintel.canonical_sites as site
    join public.site_ros_parcel_link_candidates as parcel_link
      on parcel_link.site_id = site.id::text
    where parcel_link.link_status <> 'rejected'
    group by site.id
), control_signal_rollup as (
    select
        signal.canonical_site_id,
        array_remove(array_agg(distinct concat_ws(': ', signal.signal_label, signal.signal_value_text) order by concat_ws(': ', signal.signal_label, signal.signal_value_text)), null) as control_signals,
        bool_or(coalesce(signal.ownership_confirmed, false)) as ownership_confirmed_by_signal,
        max(signal.updated_at) as latest_control_signal_at
    from landintel.ownership_control_signals as signal
    where signal.canonical_site_id is not null
    group by signal.canonical_site_id
), site_last_touch as (
    select
        site.id as canonical_site_id,
        (
            select max(touched_at)
            from (
                values
                    (site.updated_at),
                    (prove_it.updated_at),
                    (ldn.updated_at),
                    (pack.updated_at),
                    (assessment.updated_at),
                    (title_workflow.updated_at),
                    (title_review.updated_at),
                    (change_rollup.latest_change_event_at),
                    (signal_rollup.latest_signal_at),
                    (control_signal_rollup.latest_control_signal_at)
            ) as touches(touched_at)
        ) as last_assessed_at
    from landintel.canonical_sites as site
    left join latest_prove_it as prove_it on prove_it.canonical_site_id = site.id
    left join latest_ldn_screen as ldn on ldn.canonical_site_id = site.id
    left join latest_pack as pack on pack.canonical_site_id = site.id
    left join latest_assessment as assessment on assessment.canonical_site_id = site.id
    left join latest_title_workflow as title_workflow on title_workflow.canonical_site_id = site.id
    left join latest_title_review as title_review on title_review.canonical_site_id = site.id
    left join change_rollup on change_rollup.canonical_site_id = site.id
    left join signal_rollup on signal_rollup.canonical_site_id = site.id
    left join control_signal_rollup on control_signal_rollup.canonical_site_id = site.id
)
select
    site.id as canonical_site_id,
    coalesce(
        nullif(pack.site_name, ''),
        nullif(ldn.site_name, ''),
        nullif(site.site_name_primary, ''),
        'Sourced land parcel'
    ) as site_label,
    site.authority_name,
    coalesce(
        nullif(site.metadata ->> 'settlement_name', ''),
        nullif(site.metadata ->> 'settlement', ''),
        nullif(ldn.metadata ->> 'settlement_name', ''),
        nullif(pack.metadata ->> 'settlement_name', '')
    ) as settlement_name,
    coalesce(site.area_acres, ldn.area_acres, pack.area_acres) as gross_area_acres,
    case
        when coalesce(ldn.unregistered_opportunity_signal, false) then 'unregistered_opportunity_signal'
        when coalesce(ldn.register_origin_site, false) then 'register_context_source'
        when pack.canonical_site_id is not null then 'urgent_address_title_pack'
        when prove_it.canonical_site_id is not null then 'prove_it_assessment'
        when assessment.canonical_site_id is not null then 'site_assessment'
        when nullif(site.surfaced_reason, '') is not null then site.surfaced_reason
        else 'canonical_site_spine'
    end as source_route,
    coalesce(
        ldn.candidate_status,
        prove_it.verdict,
        assessment.bucket,
        site.workflow_status,
        'not_reviewed'
    ) as current_review_status,
    prove_it.verdict as prove_it_verdict,
    ldn.candidate_status as ldn_candidate_status,
    pack.urgency_status as urgent_title_pack_status,
    case
        when title_review.id is not null then 'title_review_recorded:' || coalesce(title_review.ownership_outcome, 'ownership_outcome_not_recorded')
        when title_workflow.id is not null then 'pre_title:' || coalesce(title_workflow.ownership_status_pre_title, title_workflow.title_order_status, 'ownership_not_confirmed')
        else 'ownership_not_confirmed_title_review_required'
    end as title_control_status,
    case
        when title_review.id is not null then coalesce(title_review.ownership_outcome, 'title_review_recorded')
        else coalesce(
            prove_it.control_position,
            ldn.ownership_classification,
            title_workflow.ownership_status_pre_title,
            'ownership_not_confirmed'
        )
    end as ownership_control_position,
    coalesce(
        nullif(prove_it.claim_statement, ''),
        nullif(ldn.why_it_matters, ''),
        nullif(pack.urgency_reason, ''),
        nullif(site.surfaced_reason, ''),
        'No commercial surfacing reason has been recorded yet.'
    ) as why_site_surfaced,
    case
        when cardinality(coalesce(prove_it.top_positives, '{}'::text[])) > 0 then prove_it.top_positives
        when cardinality(coalesce(ldn.top_positives, '{}'::text[])) > 0 then ldn.top_positives
        when cardinality(coalesce(signal_rollup.signal_values, '{}'::text[])) > 0 then signal_rollup.signal_values
        else '{}'::text[]
    end as top_positive_signals,
    array_cat(
        case
            when cardinality(coalesce(prove_it.top_warnings, '{}'::text[])) > 0 then prove_it.top_warnings
            when cardinality(coalesce(ldn.top_warnings, '{}'::text[])) > 0 then ldn.top_warnings
            else '{}'::text[]
        end,
        case
            when title_review.id is null then array['Ownership is not confirmed by human title review.']::text[]
            else '{}'::text[]
        end
    ) as top_warning_signals,
    array_cat(
        case
            when cardinality(coalesce(prove_it.missing_critical_evidence, '{}'::text[])) > 0 then prove_it.missing_critical_evidence
            when cardinality(coalesce(ldn.missing_critical_evidence, '{}'::text[])) > 0 then ldn.missing_critical_evidence
            else '{}'::text[]
        end,
        case
            when title_review.id is null then array['Human title review not recorded.']::text[]
            else '{}'::text[]
        end
    ) as critical_unknowns,
    coalesce(
        nullif(pack.next_action, ''),
        nullif(prove_it.review_next_action, ''),
        nullif(ldn.next_action, ''),
        nullif(title_workflow.next_action, ''),
        'Review evidence before spending time or title money.'
    ) as recommended_next_action,
    coalesce(evidence_rollup.evidence_count, 0)::bigint as evidence_count,
    change_rollup.latest_change_event_at,
    (
        coalesce(assessment.human_review_required, false)
        or coalesce(prove_it.review_ready_flag, false)
        or coalesce(prove_it.verdict, '') in ('review', 'pursue')
        or coalesce(ldn.candidate_status, '') in (
            'true_ldn_candidate',
            'review_private_candidate',
            'review_forgotten_soul',
            'constraint_review_required'
        )
        or pack.urgency_status is not null
    ) as manual_review_required,
    site_last_touch.last_assessed_at,
    case
        when title_review.id is null then
            'Operator-safe view: ownership is unconfirmed until landintel.title_review_records contains a human title review. Title and RoS parcel candidates are workflow evidence only. Rejected or SCT-like parcel references are not exposed as title numbers.'
        else
            'A human title review record exists. Use title review output for ownership/control interpretation; pre-title candidates remain workflow evidence only.'
    end as caveat,
    (title_review.id is not null) as title_reviewed_flag,
    case
        when title_review.id is not null then title_review.title_number
        else null
    end as reviewed_title_number,
    safe_title_candidate.title_number as title_number_candidate_for_manual_check,
    safe_title_candidate.title_candidate_source,
    safe_title_candidate.title_confidence,
    coalesce(parcel_candidate_rollup.ros_parcel_candidate_count, 0) as ros_parcel_candidate_count
from landintel.canonical_sites as site
left join latest_prove_it as prove_it on prove_it.canonical_site_id = site.id
left join latest_ldn_screen as ldn on ldn.canonical_site_id = site.id
left join latest_pack as pack on pack.canonical_site_id = site.id
left join latest_assessment as assessment on assessment.canonical_site_id = site.id
left join latest_title_workflow as title_workflow on title_workflow.canonical_site_id = site.id
left join latest_title_review as title_review on title_review.canonical_site_id = site.id
left join evidence_rollup on evidence_rollup.canonical_site_id = site.id
left join signal_rollup on signal_rollup.canonical_site_id = site.id
left join change_rollup on change_rollup.canonical_site_id = site.id
left join safe_title_candidate on safe_title_candidate.canonical_site_id = site.id
left join parcel_candidate_rollup on parcel_candidate_rollup.canonical_site_id = site.id
left join control_signal_rollup on control_signal_rollup.canonical_site_id = site.id
left join site_last_touch on site_last_touch.canonical_site_id = site.id;

create or replace view landintel_sourced.v_sourced_site_briefs
with (security_invoker = true) as
select
    sourced.canonical_site_id,
    sourced.site_label,
    sourced.authority_name,
    sourced.settlement_name,
    sourced.gross_area_acres,
    sourced.source_route,
    sourced.current_review_status,
    sourced.prove_it_verdict,
    sourced.ldn_candidate_status,
    sourced.urgent_title_pack_status,
    sourced.title_control_status,
    sourced.ownership_control_position,
    sourced.why_site_surfaced,
    sourced.top_positive_signals,
    sourced.top_warning_signals,
    sourced.critical_unknowns,
    sourced.recommended_next_action,
    sourced.evidence_count,
    sourced.latest_change_event_at,
    sourced.manual_review_required,
    sourced.last_assessed_at,
    sourced.caveat,
    concat_ws(
        E'\n\n',
        'Claim: ' || sourced.why_site_surfaced,
        'Proof: ' || case
            when sourced.evidence_count > 0 then sourced.evidence_count::text || ' evidence reference(s) attached.'
            else 'No evidence references attached yet.'
        end,
        'Signals: ' || coalesce(nullif(array_to_string(sourced.top_positive_signals, '; '), ''), 'No positive signals recorded.'),
        'Warnings: ' || coalesce(nullif(array_to_string(sourced.top_warning_signals, '; '), ''), 'No warnings recorded.'),
        'Gaps: ' || coalesce(nullif(array_to_string(sourced.critical_unknowns, '; '), ''), 'No critical unknowns recorded.'),
        'Action: ' || sourced.recommended_next_action,
        'Caveat: ' || sourced.caveat
    ) as operator_brief
from landintel_sourced.v_sourced_sites as sourced;

create or replace view landintel_sourced.v_review_queue
with (security_invoker = true) as
select
    sourced.*,
    case
        when sourced.prove_it_verdict = 'pursue'
          or sourced.ldn_candidate_status = 'true_ldn_candidate'
          or sourced.urgent_title_pack_status = 'order_title_urgently' then '1_active_review'
        when sourced.prove_it_verdict = 'review'
          or sourced.ldn_candidate_status in ('review_private_candidate', 'review_forgotten_soul') then '2_director_review'
        when sourced.ldn_candidate_status = 'constraint_review_required' then '3_constraint_review'
        else '4_operator_review'
    end as review_priority_band,
    case
        when sourced.prove_it_verdict = 'pursue' then 'Prove It verdict requires active human review before any action.'
        when sourced.ldn_candidate_status = 'true_ldn_candidate' then 'LDN candidate screen has surfaced this as a target profile.'
        when sourced.urgent_title_pack_status is not null then 'Urgent address/title workflow has surfaced the site.'
        when sourced.manual_review_required then 'Manual review flag is set by the existing assessment layer.'
        else 'Review requested by sourced-site operating surface.'
    end as review_reason
from landintel_sourced.v_sourced_sites as sourced
where sourced.manual_review_required
   or sourced.prove_it_verdict in ('review', 'pursue')
   or sourced.ldn_candidate_status in (
        'true_ldn_candidate',
        'review_private_candidate',
        'review_forgotten_soul',
        'constraint_review_required'
   )
   or sourced.urgent_title_pack_status is not null;

create or replace view landintel_sourced.v_title_spend_candidates
with (security_invoker = true) as
select
    sourced.canonical_site_id,
    sourced.site_label,
    sourced.authority_name,
    sourced.settlement_name,
    sourced.gross_area_acres,
    sourced.source_route,
    sourced.current_review_status,
    sourced.prove_it_verdict,
    sourced.ldn_candidate_status,
    sourced.urgent_title_pack_status,
    sourced.title_control_status,
    sourced.ownership_control_position,
    sourced.why_site_surfaced,
    sourced.top_positive_signals,
    sourced.top_warning_signals,
    sourced.critical_unknowns,
    sourced.recommended_next_action,
    sourced.evidence_count,
    sourced.latest_change_event_at,
    sourced.manual_review_required,
    sourced.last_assessed_at,
    sourced.caveat,
    sourced.title_number_candidate_for_manual_check,
    sourced.title_candidate_source,
    sourced.title_confidence,
    sourced.ros_parcel_candidate_count,
    case
        when sourced.urgent_title_pack_status = 'order_title_urgently' then 'order_title_urgently'
        when sourced.prove_it_verdict in ('review', 'pursue')
          and sourced.recommended_next_action ilike '%%title%%' then 'title_spend_review'
        when sourced.ldn_candidate_status in ('true_ldn_candidate', 'review_private_candidate', 'review_forgotten_soul') then 'title_may_be_justified_after_dd'
        else 'manual_review_before_title_spend'
    end as title_spend_queue_position,
    'Title spend candidate only. Ownership is not confirmed until a human title review record exists.'::text as title_spend_caveat
from landintel_sourced.v_sourced_sites as sourced
where sourced.title_reviewed_flag = false
  and (
        sourced.urgent_title_pack_status = 'order_title_urgently'
     or sourced.recommended_next_action ilike '%%title%%'
     or sourced.ldn_candidate_status in ('true_ldn_candidate', 'review_private_candidate', 'review_forgotten_soul')
     or sourced.prove_it_verdict in ('review', 'pursue')
  );

create or replace view landintel_sourced.v_resurfacing_candidates
with (security_invoker = true) as
select
    sourced.canonical_site_id,
    sourced.site_label,
    sourced.authority_name,
    sourced.settlement_name,
    sourced.gross_area_acres,
    sourced.source_route,
    sourced.current_review_status,
    sourced.prove_it_verdict,
    sourced.ldn_candidate_status,
    sourced.urgent_title_pack_status,
    sourced.title_control_status,
    sourced.ownership_control_position,
    sourced.why_site_surfaced,
    sourced.top_positive_signals,
    sourced.top_warning_signals,
    sourced.critical_unknowns,
    sourced.recommended_next_action,
    sourced.evidence_count,
    sourced.latest_change_event_at,
    sourced.manual_review_required,
    sourced.last_assessed_at,
    sourced.caveat,
    case
        when sourced.latest_change_event_at is not null then 'New or changed evidence exists; site can be rechecked.'
        when sourced.prove_it_verdict = 'monitor' then 'Monitor verdict keeps the site available for future review.'
        when sourced.prove_it_verdict = 'ignore' then 'Ignore verdict is not a physical deletion; resurface if new evidence changes the position.'
        when sourced.ldn_candidate_status in ('not_enough_evidence', 'control_profile_not_ldn') then 'Current evidence is weak or control profile is not LDN-fit; new evidence may change this.'
        when sourced.ldn_candidate_status = 'build_work_started' then 'Build-start evidence should be rechecked if progress stalls or ownership changes.'
        when sourced.ldn_candidate_status = 'size_below_initial_screen' then 'Size screen can be revisited if assemblage or boundary evidence changes.'
        else 'Keep capable of resurfacing when evidence changes.'
    end as resurfacing_reason
from landintel_sourced.v_sourced_sites as sourced
where sourced.prove_it_verdict in ('ignore', 'monitor')
   or sourced.ldn_candidate_status in (
        'not_enough_evidence',
        'control_profile_not_ldn',
        'build_work_started',
        'size_below_initial_screen'
   )
   or sourced.current_review_status ilike '%%watch%%'
   or sourced.current_review_status ilike '%%reject%%'
   or sourced.latest_change_event_at is not null;

grant usage on schema landintel_sourced to authenticated;
grant select on landintel_sourced.v_sourced_sites to authenticated;
grant select on landintel_sourced.v_sourced_site_briefs to authenticated;
grant select on landintel_sourced.v_review_queue to authenticated;
grant select on landintel_sourced.v_title_spend_candidates to authenticated;
grant select on landintel_sourced.v_resurfacing_candidates to authenticated;

comment on view landintel_sourced.v_sourced_sites is
    'Operator-safe sourced land opportunity surface. It reads existing canonical, Prove It, LDN candidate, title workflow, evidence, signal and change-event objects without moving data.';

comment on view landintel_sourced.v_sourced_site_briefs is
    'Human-readable LDN site briefs generated from v_sourced_sites. Ownership remains unconfirmed unless supported by landintel.title_review_records.';

comment on view landintel_sourced.v_review_queue is
    'Manual review queue for sourced sites. This view does not auto-kill sites or make legal ownership claims.';

comment on view landintel_sourced.v_title_spend_candidates is
    'Title-spend candidate queue. Title number candidates are filtered to exclude rejected or SCT-like parcel references and are not ownership confirmation.';

comment on view landintel_sourced.v_resurfacing_candidates is
    'Sites that should remain capable of resurfacing when evidence changes, including monitor/ignore/currently weak candidates.';

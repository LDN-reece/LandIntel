drop view if exists analytics.v_site_current_strategy_recommendations;
drop view if exists analytics.v_site_current_investor_matches;
drop view if exists analytics.v_site_current_buyer_matches;
drop view if exists analytics.v_site_current_appraisal_summaries;
drop view if exists analytics.v_site_current_appraisal_runs;
drop view if exists analytics.v_site_current_reviews;

create or replace view analytics.v_site_current_reviews
with (security_invoker = true) as
select distinct on (site_id)
    review.*
from public.site_reviews as review
order by site_id, created_at desc;

create or replace view analytics.v_site_current_appraisal_runs
with (security_invoker = true) as
select distinct on (site_id)
    appraisal_run.*
from public.site_appraisal_runs as appraisal_run
where appraisal_run.status = 'completed'
order by site_id, coalesce(completed_at, created_at) desc, created_at desc;

create or replace view analytics.v_site_current_appraisal_summaries
with (security_invoker = true) as
select
    summary.*
from public.site_appraisal_summaries as summary
join analytics.v_site_current_appraisal_runs as appraisal_run
    on appraisal_run.id = summary.appraisal_run_id;

create or replace view analytics.v_site_current_buyer_matches
with (security_invoker = true) as
select
    match.id,
    match.site_id,
    match.appraisal_run_id,
    match.buyer_entity_id,
    buyer.entity_name,
    buyer.corporate_scale,
    buyer.market_focus,
    buyer.active_buying_status,
    match.match_rank,
    match.match_score,
    match.geography_score,
    match.unit_scale_score,
    match.stage_score,
    match.abnormal_tolerance_score,
    match.evidence_recency_score,
    match.route_in_quality_score,
    coalesce(match.fit_band, match.fit_rating) as fit_band,
    coalesce(match.rationale, match.match_reason) as rationale,
    match.confidence_level,
    match.provisional_flag,
    match.created_at,
    match.latest_evaluated_at
from public.site_buyer_matches as match
join analytics.v_site_current_appraisal_runs as appraisal_run
    on appraisal_run.id = match.appraisal_run_id
join public.buyer_entities as buyer
    on buyer.id = match.buyer_entity_id;

create or replace view analytics.v_site_current_investor_matches
with (security_invoker = true) as
select
    match.*,
    investor.entity_name,
    investor.investor_type,
    investor.sub_type,
    investor.typical_structure,
    investor.typical_cheque_min,
    investor.typical_cheque_max
from public.site_investor_matches as match
join analytics.v_site_current_appraisal_runs as appraisal_run
    on appraisal_run.id = match.appraisal_run_id
join public.investor_entities as investor
    on investor.id = match.investor_entity_id;

create or replace view analytics.v_site_current_strategy_recommendations
with (security_invoker = true) as
select
    recommendation.*
from public.site_strategy_recommendations as recommendation
join analytics.v_site_current_appraisal_runs as appraisal_run
    on appraisal_run.id = recommendation.appraisal_run_id;

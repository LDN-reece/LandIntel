create index if not exists idx_site_infrastructure_records_site_type
    on public.site_infrastructure_records (site_id, infrastructure_type);

create index if not exists idx_site_control_records_site_type
    on public.site_control_records (site_id, control_type);

create index if not exists idx_site_assessments_site_created
    on public.site_assessments (site_id, created_at desc);

create index if not exists idx_site_assessment_scores_assessment_score
    on public.site_assessment_scores (site_assessment_id, score_code);

create index if not exists idx_site_search_cache_bucket
    on analytics.site_search_cache (opportunity_bucket);

create index if not exists idx_site_search_cache_horizon
    on analytics.site_search_cache (monetisation_horizon);

create index if not exists idx_site_search_cache_human_review
    on analytics.site_search_cache (human_review_required);


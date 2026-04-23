alter table public.site_infrastructure_records enable row level security;
alter table public.site_control_records enable row level security;
alter table public.site_assessments enable row level security;
alter table public.site_assessment_scores enable row level security;
alter table public.site_assessment_evidence enable row level security;
alter table public.site_assessment_score_evidence enable row level security;
alter table public.site_assessment_overrides enable row level security;

revoke all on table public.site_infrastructure_records from anon, authenticated;
revoke all on table public.site_control_records from anon, authenticated;
revoke all on table public.site_assessments from anon, authenticated;
revoke all on table public.site_assessment_scores from anon, authenticated;
revoke all on table public.site_assessment_evidence from anon, authenticated;
revoke all on table public.site_assessment_score_evidence from anon, authenticated;
revoke all on table public.site_assessment_overrides from anon, authenticated;

revoke all on table analytics.v_site_current_assessments from anon, authenticated;
revoke all on table analytics.v_site_current_assessment_scores from anon, authenticated;


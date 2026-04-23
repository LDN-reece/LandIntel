drop trigger if exists site_reviews_touch_updated_at on public.site_reviews;
create trigger site_reviews_touch_updated_at
before update on public.site_reviews
for each row execute function public.touch_updated_at();

drop trigger if exists site_review_checks_touch_updated_at on public.site_review_checks;
create trigger site_review_checks_touch_updated_at
before update on public.site_review_checks
for each row execute function public.touch_updated_at();

drop trigger if exists site_appraisal_assumption_sets_touch_updated_at on public.site_appraisal_assumption_sets;
create trigger site_appraisal_assumption_sets_touch_updated_at
before update on public.site_appraisal_assumption_sets
for each row execute function public.touch_updated_at();

drop trigger if exists site_appraisal_assumptions_touch_updated_at on public.site_appraisal_assumptions;
create trigger site_appraisal_assumptions_touch_updated_at
before update on public.site_appraisal_assumptions
for each row execute function public.touch_updated_at();

drop trigger if exists site_appraisal_runs_touch_updated_at on public.site_appraisal_runs;
create trigger site_appraisal_runs_touch_updated_at
before update on public.site_appraisal_runs
for each row execute function public.touch_updated_at();

drop trigger if exists site_appraisal_summaries_touch_updated_at on public.site_appraisal_summaries;
create trigger site_appraisal_summaries_touch_updated_at
before update on public.site_appraisal_summaries
for each row execute function public.touch_updated_at();

drop trigger if exists buyer_entities_touch_updated_at on public.buyer_entities;
create trigger buyer_entities_touch_updated_at
before update on public.buyer_entities
for each row execute function public.touch_updated_at();

drop trigger if exists buyer_region_rules_touch_updated_at on public.buyer_region_rules;
create trigger buyer_region_rules_touch_updated_at
before update on public.buyer_region_rules
for each row execute function public.touch_updated_at();

drop trigger if exists buyer_residential_profiles_touch_updated_at on public.buyer_residential_profiles;
create trigger buyer_residential_profiles_touch_updated_at
before update on public.buyer_residential_profiles
for each row execute function public.touch_updated_at();

drop trigger if exists buyer_contacts_touch_updated_at on public.buyer_contacts;
create trigger buyer_contacts_touch_updated_at
before update on public.buyer_contacts
for each row execute function public.touch_updated_at();

drop trigger if exists investor_entities_touch_updated_at on public.investor_entities;
create trigger investor_entities_touch_updated_at
before update on public.investor_entities
for each row execute function public.touch_updated_at();

drop trigger if exists investor_source_systems_touch_updated_at on public.investor_source_systems;
create trigger investor_source_systems_touch_updated_at
before update on public.investor_source_systems
for each row execute function public.touch_updated_at();

drop trigger if exists site_strategy_recommendations_touch_updated_at on public.site_strategy_recommendations;
create trigger site_strategy_recommendations_touch_updated_at
before update on public.site_strategy_recommendations
for each row execute function public.touch_updated_at();

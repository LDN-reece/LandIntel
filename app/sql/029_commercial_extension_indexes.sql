create index if not exists site_reviews_site_created_idx
    on public.site_reviews (site_id, created_at desc);

create index if not exists site_reviews_verdict_idx
    on public.site_reviews (verdict);

create index if not exists site_review_checks_review_idx
    on public.site_review_checks (site_review_id);

create index if not exists site_appraisal_assumption_sets_site_created_idx
    on public.site_appraisal_assumption_sets (site_id, created_at desc);

create index if not exists site_appraisal_assumptions_set_key_idx
    on public.site_appraisal_assumptions (assumption_set_id, assumption_key, scenario_code);

create index if not exists site_appraisal_runs_site_created_idx
    on public.site_appraisal_runs (site_id, created_at desc);

create index if not exists site_appraisal_runs_status_idx
    on public.site_appraisal_runs (status);

create unique index if not exists site_appraisal_scenarios_run_scenario_uidx
    on public.site_appraisal_scenarios (appraisal_run_id, scenario_code);

create index if not exists site_appraisal_summaries_site_idx
    on public.site_appraisal_summaries (site_id, created_at desc);

create index if not exists buyer_entities_family_scale_idx
    on public.buyer_entities (buyer_family, corporate_scale);

create index if not exists buyer_entities_scotland_focus_idx
    on public.buyer_entities (scotland_focus, provisional_seed);

create index if not exists buyer_region_rules_buyer_idx
    on public.buyer_region_rules (buyer_entity_id, region_name);

create index if not exists buyer_residential_profiles_buyer_idx
    on public.buyer_residential_profiles (buyer_entity_id);

create index if not exists buyer_evidence_buyer_date_idx
    on public.buyer_evidence (buyer_entity_id, evidence_date desc);

create index if not exists buyer_contacts_buyer_quality_idx
    on public.buyer_contacts (buyer_entity_id, route_in_quality_score desc);

create index if not exists site_buyer_matches_appraisal_score_idx
    on public.site_buyer_matches (appraisal_run_id, match_score desc);

create unique index if not exists site_buyer_matches_appraisal_buyer_uidx
    on public.site_buyer_matches (appraisal_run_id, buyer_entity_id)
    where appraisal_run_id is not null
      and buyer_entity_id is not null;

create index if not exists site_buyer_match_evidence_match_idx
    on public.site_buyer_match_evidence (site_buyer_match_id);

create index if not exists investor_entities_type_idx
    on public.investor_entities (investor_type, planning_risk_appetite, provisional_seed);

create index if not exists investor_entities_structure_idx
    on public.investor_entities (typical_structure, scotland_focus, residential_focus);

create index if not exists investor_evidence_investor_date_idx
    on public.investor_evidence (investor_entity_id, evidence_date desc);

create index if not exists investor_source_systems_source_idx
    on public.investor_source_systems (source_name, geography_scope);

create index if not exists site_investor_matches_appraisal_score_idx
    on public.site_investor_matches (appraisal_run_id, match_score desc);

create index if not exists site_investor_matches_site_idx
    on public.site_investor_matches (site_id, fit_band, created_at desc);

create index if not exists site_investor_match_evidence_match_idx
    on public.site_investor_match_evidence (site_investor_match_id);

create index if not exists site_strategy_recommendations_site_idx
    on public.site_strategy_recommendations (site_id, created_at desc);

create index if not exists site_strategy_recommendations_flag_idx
    on public.site_strategy_recommendations (investor_strategy_flag, deal_route_bias);

create index if not exists source_registry_domain_role_idx
    on public.source_registry (source_domain, source_role, scope);

create index if not exists source_registry_distress_idx
    on public.source_registry (is_distress_source, source_name);

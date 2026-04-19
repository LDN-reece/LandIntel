alter table public.site_spatial_links disable row level security;
alter table public.site_title_validation disable row level security;
alter table public.constraint_layer_registry disable row level security;
alter table public.constraint_source_features disable row level security;
alter table public.site_constraint_measurements disable row level security;
alter table public.site_constraint_group_summaries disable row level security;
alter table public.site_commercial_friction_facts disable row level security;

grant usage on schema analytics to anon, authenticated;
grant select on analytics.v_constraints_tab_overview to authenticated;
grant select on analytics.v_constraints_tab_measurements to authenticated;
grant select on analytics.v_constraints_tab_group_summaries to authenticated;
grant select on analytics.v_constraints_tab_commercial_friction to authenticated;

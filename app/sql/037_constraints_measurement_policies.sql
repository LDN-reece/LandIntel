alter table public.site_spatial_links enable row level security;
alter table public.site_title_validation enable row level security;
alter table public.constraint_layer_registry enable row level security;
alter table public.constraint_source_features enable row level security;
alter table public.site_constraint_measurements enable row level security;
alter table public.site_constraint_group_summaries enable row level security;
alter table public.site_commercial_friction_facts enable row level security;

revoke all on table public.site_spatial_links from anon;
revoke all on table public.site_spatial_links from authenticated;
grant select on table public.site_spatial_links to authenticated;

drop policy if exists site_spatial_links_authenticated_select on public.site_spatial_links;
create policy site_spatial_links_authenticated_select
on public.site_spatial_links
for select
to authenticated
using (true);

revoke all on table public.site_title_validation from anon;
revoke all on table public.site_title_validation from authenticated;
grant select on table public.site_title_validation to authenticated;

drop policy if exists site_title_validation_authenticated_select on public.site_title_validation;
create policy site_title_validation_authenticated_select
on public.site_title_validation
for select
to authenticated
using (true);

revoke all on table public.constraint_layer_registry from anon;
revoke all on table public.constraint_layer_registry from authenticated;
grant select on table public.constraint_layer_registry to authenticated;

drop policy if exists constraint_layer_registry_authenticated_select on public.constraint_layer_registry;
create policy constraint_layer_registry_authenticated_select
on public.constraint_layer_registry
for select
to authenticated
using (true);

revoke all on table public.constraint_source_features from anon;
revoke all on table public.constraint_source_features from authenticated;
grant select on table public.constraint_source_features to authenticated;

drop policy if exists constraint_source_features_authenticated_select on public.constraint_source_features;
create policy constraint_source_features_authenticated_select
on public.constraint_source_features
for select
to authenticated
using (true);

revoke all on table public.site_constraint_measurements from anon;
revoke all on table public.site_constraint_measurements from authenticated;
grant select on table public.site_constraint_measurements to authenticated;

drop policy if exists site_constraint_measurements_authenticated_select on public.site_constraint_measurements;
create policy site_constraint_measurements_authenticated_select
on public.site_constraint_measurements
for select
to authenticated
using (true);

revoke all on table public.site_constraint_group_summaries from anon;
revoke all on table public.site_constraint_group_summaries from authenticated;
grant select on table public.site_constraint_group_summaries to authenticated;

drop policy if exists site_constraint_group_summaries_authenticated_select on public.site_constraint_group_summaries;
create policy site_constraint_group_summaries_authenticated_select
on public.site_constraint_group_summaries
for select
to authenticated
using (true);

revoke all on table public.site_commercial_friction_facts from anon;
revoke all on table public.site_commercial_friction_facts from authenticated;
grant select on table public.site_commercial_friction_facts to authenticated;

drop policy if exists site_commercial_friction_facts_authenticated_select on public.site_commercial_friction_facts;
create policy site_commercial_friction_facts_authenticated_select
on public.site_commercial_friction_facts
for select
to authenticated
using (true);

grant usage on schema analytics to anon, authenticated;
grant select on analytics.v_constraints_tab_overview to authenticated;
grant select on analytics.v_constraints_tab_measurements to authenticated;
grant select on analytics.v_constraints_tab_group_summaries to authenticated;
grant select on analytics.v_constraints_tab_commercial_friction to authenticated;

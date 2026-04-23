alter table public.site_reference_aliases enable row level security;
alter table public.site_geometry_versions enable row level security;
alter table public.site_reconciliation_matches enable row level security;
alter table public.site_reconciliation_review_queue enable row level security;

revoke all on table public.site_reference_aliases from anon, authenticated;
revoke all on table public.site_geometry_versions from anon, authenticated;
revoke all on table public.site_reconciliation_matches from anon, authenticated;
revoke all on table public.site_reconciliation_review_queue from anon, authenticated;
revoke all on table analytics.v_canonical_sites from anon, authenticated;
revoke all on table analytics.v_site_reference_index from anon, authenticated;

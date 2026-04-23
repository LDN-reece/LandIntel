drop trigger if exists site_reference_aliases_touch_updated_at on public.site_reference_aliases;
create trigger site_reference_aliases_touch_updated_at
before update on public.site_reference_aliases
for each row execute function public.touch_updated_at();

drop trigger if exists site_geometry_versions_touch_updated_at on public.site_geometry_versions;
create trigger site_geometry_versions_touch_updated_at
before update on public.site_geometry_versions
for each row execute function public.touch_updated_at();

drop trigger if exists site_reconciliation_matches_touch_updated_at on public.site_reconciliation_matches;
create trigger site_reconciliation_matches_touch_updated_at
before update on public.site_reconciliation_matches
for each row execute function public.touch_updated_at();

drop trigger if exists site_reconciliation_review_queue_touch_updated_at on public.site_reconciliation_review_queue;
create trigger site_reconciliation_review_queue_touch_updated_at
before update on public.site_reconciliation_review_queue
for each row execute function public.touch_updated_at();

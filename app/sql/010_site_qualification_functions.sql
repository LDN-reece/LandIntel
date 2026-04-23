drop trigger if exists trg_touch_updated_at_sites on public.sites;
create trigger trg_touch_updated_at_sites
before update on public.sites
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_locations on public.site_locations;
create trigger trg_touch_updated_at_site_locations
before update on public.site_locations
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_parcels on public.site_parcels;
create trigger trg_touch_updated_at_site_parcels
before update on public.site_parcels
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_geometry_components on public.site_geometry_components;
create trigger trg_touch_updated_at_site_geometry_components
before update on public.site_geometry_components
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_planning_records on public.planning_records;
create trigger trg_touch_updated_at_planning_records
before update on public.planning_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_planning_context_records on public.planning_context_records;
create trigger trg_touch_updated_at_planning_context_records
before update on public.planning_context_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_constraints on public.site_constraints;
create trigger trg_touch_updated_at_site_constraints
before update on public.site_constraints
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_comparable_market_records on public.comparable_market_records;
create trigger trg_touch_updated_at_comparable_market_records
before update on public.comparable_market_records
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_buyer_profiles on public.buyer_profiles;
create trigger trg_touch_updated_at_buyer_profiles
before update on public.buyer_profiles
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_site_buyer_matches on public.site_buyer_matches;
create trigger trg_touch_updated_at_site_buyer_matches
before update on public.site_buyer_matches
for each row
execute function public.touch_updated_at();

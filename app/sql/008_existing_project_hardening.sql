-- Harden legacy project objects that already exist outside this worker's schema set.
alter table if exists public.land_parcels enable row level security;
revoke all on table public.land_parcels from anon, authenticated;

alter function if exists public.set_updated_at() set search_path = pg_catalog;

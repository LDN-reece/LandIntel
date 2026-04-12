create or replace function public.calculate_area_ha(area_sqm numeric)
returns numeric
language sql
immutable
set search_path = pg_catalog
as $$
    select round(coalesce(area_sqm, 0) / 10000.0, 6);
$$;

create or replace function public.calculate_area_acres(area_sqm numeric)
returns numeric
language sql
immutable
set search_path = pg_catalog
as $$
    select round(coalesce(area_sqm, 0) / 4046.8564224, 6);
$$;

create or replace function public.classify_size_bucket(area_acres numeric)
returns text
language sql
immutable
set search_path = pg_catalog
as $$
    select case
        when coalesce(area_acres, 0) < 4 then 'bucket_1_under_4_acres'
        else 'bucket_2_4plus_acres'
    end;
$$;

create or replace function public.classify_size_bucket_label(area_acres numeric)
returns text
language sql
immutable
set search_path = pg_catalog
as $$
    select case
        when coalesce(area_acres, 0) < 4 then 'Under 4 acres'
        else '4+ acres'
    end;
$$;

create or replace function public.touch_updated_at()
returns trigger
language plpgsql
set search_path = pg_catalog
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists trg_touch_updated_at_authority_aoi on public.authority_aoi;
create trigger trg_touch_updated_at_authority_aoi
before update on public.authority_aoi
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_source_registry on public.source_registry;
create trigger trg_touch_updated_at_source_registry
before update on public.source_registry
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_ros_cadastral_parcels on public.ros_cadastral_parcels;
create trigger trg_touch_updated_at_ros_cadastral_parcels
before update on public.ros_cadastral_parcels
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_land_objects on public.land_objects;
create trigger trg_touch_updated_at_land_objects
before update on public.land_objects
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_land_object_toid_enrichment on public.land_object_toid_enrichment;
create trigger trg_touch_updated_at_land_object_toid_enrichment
before update on public.land_object_toid_enrichment
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_land_object_title_matches on public.land_object_title_matches;
create trigger trg_touch_updated_at_land_object_title_matches
before update on public.land_object_title_matches
for each row
execute function public.touch_updated_at();

drop trigger if exists trg_touch_updated_at_land_object_address_links on public.land_object_address_links;
create trigger trg_touch_updated_at_land_object_address_links
before update on public.land_object_address_links
for each row
execute function public.touch_updated_at();

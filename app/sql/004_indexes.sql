create index if not exists authority_aoi_geometry_gix
    on public.authority_aoi using gist (geometry);

create index if not exists authority_aoi_geometry_simplified_gix
    on public.authority_aoi using gist (geometry_simplified);

create index if not exists source_registry_extent_gix
    on public.source_registry using gist (geographic_extent);

create unique index if not exists source_registry_metadata_uuid_uidx
    on public.source_registry (metadata_uuid)
    where metadata_uuid is not null;

create index if not exists ingest_runs_source_started_idx
    on public.ingest_runs (source_name, started_at desc);

create index if not exists ros_cadastral_raw_geometry_gix
    on staging.ros_cadastral_parcels_raw using gist (geometry);

create index if not exists ros_cadastral_raw_run_id_idx
    on staging.ros_cadastral_parcels_raw (run_id);

create index if not exists ros_cadastral_raw_inspire_idx
    on staging.ros_cadastral_parcels_raw (ros_inspire_id);

create index if not exists ros_cadastral_clean_geometry_gix
    on staging.ros_cadastral_parcels_clean using gist (geometry);

create index if not exists ros_cadastral_clean_run_id_idx
    on staging.ros_cadastral_parcels_clean (run_id);

create index if not exists ros_cadastral_clean_inspire_idx
    on staging.ros_cadastral_parcels_clean (ros_inspire_id);

create index if not exists ros_cadastral_parcels_geometry_gix
    on public.ros_cadastral_parcels using gist (geometry);

create index if not exists ros_cadastral_parcels_centroid_gix
    on public.ros_cadastral_parcels using gist (centroid);

create index if not exists ros_cadastral_parcels_authority_idx
    on public.ros_cadastral_parcels (authority_name);

create index if not exists ros_cadastral_parcels_inspire_idx
    on public.ros_cadastral_parcels (ros_inspire_id);

create unique index if not exists ros_cadastral_parcels_inspire_authority_uidx
    on public.ros_cadastral_parcels (ros_inspire_id, authority_name)
    where ros_inspire_id is not null;

create index if not exists land_objects_geometry_gix
    on public.land_objects using gist (geometry);

create index if not exists land_objects_authority_idx
    on public.land_objects (authority_name);

create unique index if not exists land_objects_source_uidx
    on public.land_objects (source_system, source_key, authority_name);

create unique index if not exists land_object_toid_enrichment_uidx
    on public.land_object_toid_enrichment (land_object_id, toid);

create index if not exists land_object_title_matches_land_object_idx
    on public.land_object_title_matches (land_object_id);

create index if not exists land_object_title_matches_title_number_idx
    on public.land_object_title_matches (title_number);

create index if not exists land_object_address_links_land_object_idx
    on public.land_object_address_links (land_object_id);

create index if not exists land_object_address_links_uprn_idx
    on public.land_object_address_links (uprn);

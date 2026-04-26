alter table landintel.site_source_links
    drop constraint if exists site_source_links_link_method_check;

alter table landintel.site_source_links
    add constraint site_source_links_link_method_check
    check (
        link_method is null
        or link_method = any (array[
            'legacy_link',
            'continuity',
            'direct_reference',
            'trusted_alias',
            'source_reference',
            'planning_reference',
            'hla_reference',
            'dominant_spatial_overlap',
            'spatial_overlap',
            'weak_spatial_overlap',
            'source_geometry_seed',
            'new_source_geometry',
            'spatial_overlap_or_seed',
            'ros_parcel_overlap',
            'title_reviewed',
            'commercial_inference',
            'manual_link',
            'manual_review',
            'manual_override'
        ]::text[])
    ) not valid;

comment on constraint site_source_links_link_method_check on landintel.site_source_links
    is 'Phase One source link vocabulary. ELA/VDL direct publish uses spatial_overlap_or_seed where a source feature either overlaps an existing canonical site or seeds a new canonical site; legal ownership certainty remains outside this method.';

# Title Area And Access Context Surface

## Purpose

`landintel_sourced.v_site_title_area_access_context` adds the next practical DD identity layer:

- title area in acres, where a legal title-number parcel area is already held;
- RoS parcel candidate area in acres, where parcel evidence exists but legal title number is not held;
- site area in acres from canonical geometry;
- whether the site geometry is measurement-ready;
- nearest road/open-location road context already measured;
- contextual landlocked/access risk.

This is not a new truth table. It is a readable view over existing site, title, parcel and open-location context.

## Developable Geometry Rule

`developable_geometry_status` means the geometry is usable for DD measurement.

It does not mean:

- net developable area;
- planning acceptability;
- engineering viability;
- legal access;
- abnormal-cost position.

Statuses:

- `geometry_missing`
- `geometry_empty`
- `geometry_invalid`
- `below_4_acre_ldn_threshold`
- `measurement_ready_geometry`

## Access / Landlocked Rule

The view uses existing `landintel.site_open_location_spine_context` rows where the context looks road, street or highway related.

It returns:

- nearest road name;
- nearest road distance;
- counts within 400m, 800m and 1600m where already measured;
- road access context status;
- landlocked context risk.

This does not prove adopted road access, ransom-free access, legal servitude, ownership of access strips or physical buildability.

If road context is missing, the status is:

`road_context_not_measured`

That means the next action is a bounded OS Open Roads/open-location context refresh, not a commercial rejection.

## Commercial Use

Use this after the legal title/location identity view.

It tells LDN whether a site is:

- readable by legal title/location identity;
- sized properly;
- geometrically measurable;
- plausibly near road context;
- potentially field-isolated or landlocked.

It does not replace manual access review.

# Site Location Context Surface

## Purpose

`landintel_sourced.v_site_location_context` adds the first readable location DD surface on top of the legal title/location/access views.

It answers:

- where the site is identified by title, address, local area and council;
- whether the site has measurement-ready geometry;
- whether road/access context has already been measured;
- whether core location anchors have already been measured;
- which education, healthcare, transport, open-space and water context is nearest;
- whether service-anchor context exists within 1600m where already measured;
- what remains unmeasured.

This is not a new source system. It reads existing `landintel.site_open_location_spine_context` rows and the sourced-site identity/access surfaces.

## Commercial Use

The view gives LDN a fast location read before spending analyst time:

- Is the site actually locatable?
- Is the surrounding context measured?
- Is it close to service anchors that support a stronger location argument?
- Is road/access context weak or missing?
- Is the site a DD gap rather than a reject?

This matters because a site with a legal title number, clean geometry and nearby service anchors is easier to DD, easier to explain and usually easier to place in front of buyers.

## Location Families

The view classifies already-measured open-location spine rows into:

- `road_access`
- `education`
- `healthcare`
- `transport`
- `open_space`
- `water`
- `authority_boundary`
- `other_location_context`

The classification is intentionally simple and operator-safe. It does not create planning conclusions.

## Key Outputs

Important fields include:

- `location_context_status`
- `settlement_context_status`
- `education_context_status`
- `healthcare_context_status`
- `transport_context_status`
- `open_space_context_status`
- `npf4_service_anchor_context_status`
- `service_anchor_count_within_1600m`
- `nearest_service_anchor_distance_m`
- `location_context_summary`
- `location_context_caveat`

## Hard Caveats

Location context is contextual DD evidence only.

It does not prove:

- NPF4 compliance;
- adopted or legal access;
- ransom-free access;
- planning acceptability;
- buyer demand;
- net developable area.

If the view returns `location_context_not_measured`, that means the next move is a bounded open-location context refresh. It is not a commercial rejection.

## What This PR Does Not Do

This PR does not:

- ingest new data;
- run broad open-location measurement;
- create a second measurement engine;
- create a physical sourced-sites table;
- replace constraint measurement;
- score the site;
- auto-kill weak-context sites.

## Next Operational Step

After merge and migration, run:

1. `run-migrations`
2. `audit-open-location-spine-completion`

If many priority sites show `location_context_not_measured`, run a bounded open-location context refresh for priority cohorts only.

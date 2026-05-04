# Parcel Model Decision

## Purpose

This PR documents and exposes the current parcel model decision without deleting, moving or rewriting data.

It resolves the operational confusion between:

- `public.ros_cadastral_parcels`
- `public.land_objects`
- `public.land_parcels`
- parcel/title candidate bridge tables

## Decision

`public.ros_cadastral_parcels` remains the canonical RoS parcel source for LandIntel.

`public.land_objects` remains a duplicate_candidate / legacy normalised cache. It is not safe to retire now because it still has active dependencies.

`public.land_parcels` remains a legacy_candidate_retire object, but that label is not deletion approval.

Plain English: public.ros_cadastral_parcels remains the canonical RoS parcel source. Public.land_objects remains a duplicate_candidate. Public.land_parcels remains a legacy_candidate_retire object.

## Commercial Meaning

LDN needs clean parcel evidence because title/control decisions are expensive and sensitive.

The parcel model must be clear enough to answer:

- which table is the parcel source of truth;
- which objects are just caches or old models;
- what can support title spend;
- what must not be mistaken for ownership proof.

The answer is:

- parcels help identify land and title candidates;
- parcel references are not title numbers;
- title candidates are not ownership outcomes;
- human title review is still required before ownership is confirmed.

## Dependency Evidence

### `public.ros_cadastral_parcels`

Current role: canonical RoS cadastral parcel source.

Evidence:

- created in `app/sql/003_tables.sql`;
- populated by `app/src/loaders/supabase_loader.py::upsert_ros_cadastral_parcels`;
- indexed in `app/sql/004_indexes.sql`;
- used by `app/sql/047_title_resolution_bridge.sql`;
- used by `app/sql/048_site_ros_parcel_linking.sql`;
- used by `app/sql/056_urgent_site_address_title_pack.sql`;
- used by `app/sql/060_scotland_parcel_use_spine.sql`;
- used by `app/sql/061_ldn_sourced_site_briefs.sql`;
- used by `app/sql/064_title_control_operator_safety_views.sql`;
- audited by `app/src/source_expansion_runner.py::audit-title-number-control`.

Recommendation:

Keep as the canonical RoS parcel source. Use compatibility view `landintel_store.ros_cadastral_parcels` where possible.

### `public.land_objects`

Current role: duplicate_candidate / legacy normalised parcel-object cache.

Evidence:

- created in `app/sql/003_tables.sql`;
- indexed in `app/sql/004_indexes.sql`;
- populated by `app/src/loaders/supabase_loader.py::upsert_land_objects`;
- `upsert_land_objects` builds rows from the same enriched RoS parcel frame and sets:
  - `object_type = ros_cadastral_parcel`;
  - `source_system = ros_inspire`;
  - `source_key = ros_inspire_id:authority_name`;
- called by `app/src/main.py` during `ingest-ros-cadastral`;
- read by `app/sql/056_urgent_site_address_title_pack.sql` for address linkage via `public.land_object_address_links`.

Recommendation:

Do not retire now. It is a duplicate candidate, but it still supports the urgent address/title pack address-link path.

Before any retirement:

1. Replace the urgent address/title pack dependency with a `public.ros_cadastral_parcels` or `landintel_store` equivalent.
2. Prove `public.land_object_address_links` is empty, unused or safely migrated.
3. Run bounded source-key overlap checks.
4. Produce a dedicated retirement-readiness PR.

### `public.land_parcels`

Current role: legacy empty parcel stub.

Recommendation:

Keep labelled as `legacy_candidate_retire`. Do not delete without a later dependency-proof PR.

## Does `land_objects` Contain Anything Not Represented By RoS Parcels?

Does land_objects contain anything not represented by RoS parcels?

Repo evidence says no independent model is intended:

- `land_objects` is populated from the same enriched RoS parcel input;
- it stores `ros_cadastral_parcel` objects;
- the natural key is derived from `ros_inspire_id` and `authority_name`.

This PR does not run a broad exact overlap or spatial comparison. That is deliberate.

The correct later proof is a bounded source-key dependency check, not an all-parcel geometry scan.

## New Reporting Views

### `landintel_reporting.v_parcel_model_status`

Shows:

- object status;
- row count estimate;
- parcel model role;
- active read paths;
- active write paths;
- recommended canonical parcel source;
- recommended action;
- safe-to-retire flag;
- caveat.

### `landintel_reporting.v_parcel_model_lightweight_overlap_audit`

Shows:

- estimated row comparison between `public.ros_cadastral_parcels` and `public.land_objects`;
- repo evidence explaining why `land_objects` is a duplicate candidate;
- explicit limitation that no broad spatial overlap query is run;
- recommendation to keep RoS parcels canonical.

## Non-Negotiable Caveats

- No data is deleted.
- No data is moved.
- No table is renamed.
- No broad spatial join is run.
- `public.land_objects` is not retired.
- RoS parcel references are not title numbers.
- RoS parcel linkage is not ownership proof.
- title candidates are not ownership proof.
- `title_review_records` remains the human ownership confirmation layer.

Plain English: title_review_records remains the human ownership confirmation layer.

## Recommended Next Step

Phase E should proceed only after this PR is merged, migrated and verified live.

Next phase: constraint coverage scaler.

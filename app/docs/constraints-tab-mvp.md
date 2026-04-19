# Constraints Tab MVP

The Constraints tab is now designed as a measured evidence layer, not a blunt severity list.

## Commercial purpose

This matters because LandIntel should answer what is physically affecting a site, how much of the site is affected, how close the issue is, and what that means for the next commercial check.

A flat severity flag does not do that well enough. Measured overlap and distance facts do.

## Live site anchor

The live site spine for this tab is:

- `public.sites`
- `public.site_locations.geometry`

This means the tab reads from the confirmed site record and its live geometry anchor. It does not create or depend on a second canonical site system.

## New measurement architecture

The MVP adds these extension tables:

- `public.site_spatial_links`
- `public.site_title_validation`
- `public.constraint_layer_registry`
- `public.constraint_source_features`
- `public.site_constraint_measurements`
- `public.site_constraint_group_summaries`
- `public.site_commercial_friction_facts`

## What each layer does

### `public.site_spatial_links`

Stores technical spatial lineage from the live site geometry to linked parcels, titles, or other spatial records.

### `public.site_title_validation`

Stores title-validation evidence tied to the live site geometry.

### `public.constraint_layer_registry`

Defines what each constraint layer is, how it should be measured, and how it maps back to older constraint references if needed.

### `public.constraint_source_features`

Stores normalized raw constraint features ready for measurement.

### `public.site_constraint_measurements`

Stores the exact measured facts between a site geometry and a source feature:

- intersection state
- buffer state
- overlap area
- overlap percentage of the site
- nearest distance in metres

### `public.site_constraint_group_summaries`

Rolls measured facts up into grouped layer summaries for faster operator browsing.

### `public.site_commercial_friction_facts`

Stores operator-readable commercial friction statements derived from the measured facts.

## Analytics views for the tab

The analyst-facing browse surfaces are:

- `analytics.v_constraints_tab_overview`
- `analytics.v_constraints_tab_measurements`
- `analytics.v_constraints_tab_group_summaries`
- `analytics.v_constraints_tab_commercial_friction`

## Legacy rule

`public.site_constraints` remains the legacy severity-style path.

It can stay in place for backward compatibility, but it should not drive the new Constraints tab architecture.

## Deliberate exclusions

This MVP does **not** add:

- scoring
- pass/fail logic
- RAG logic

Those are intentionally excluded so the tab stays grounded in measured evidence and does not jump too early into subjective decision output.

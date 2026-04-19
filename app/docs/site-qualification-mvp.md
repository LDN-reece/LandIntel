# Site Qualification MVP

Site qualification should treat constraints as measured facts attached to the live site spine, not as blunt severity labels.

## Live site spine

Qualification for the Constraints tab should anchor on:

- `public.sites`
- `public.site_locations.geometry`

This is the live site record and live geometry anchor for qualification work.

## Recommended browse order

When qualifying a site from the Constraints tab, use this order:

1. `analytics.v_constraints_tab_overview`
2. `analytics.v_constraints_tab_group_summaries`
3. `analytics.v_constraints_tab_measurements`
4. `analytics.v_constraints_tab_commercial_friction`

## What qualification should answer

This layer is there to help answer:

- what physically overlaps the site
- what sits nearby but does not overlap
- which layer is driving the commercial friction
- what needs checked next with planning, title, or delivery context

## Legacy rule

`public.site_constraints` remains legacy severity-style context only.

It can still support backward compatibility, but new qualification logic should not be built on it.

## Deliberate exclusions

This MVP deliberately excludes:

- scoring
- pass/fail logic
- RAG logic

That keeps the qualification layer factual, explainable, and reusable for later decision logic.

# LandIntel Sourced Operator Views

## Purpose

PR 2 creates the clean human-readable LDN operating surface for sourced land opportunities.

This is a view layer only. It does not create a physical `sourced_sites` table, move data, ingest data, alter `landintel.canonical_sites`, or create a second truth model.

The operating model remains:

`canonical site spine -> source evidence -> measured context/signals -> sourced-site operating views`

## Views

### `landintel_sourced.v_sourced_sites`

The main operator-safe sourced site surface.

It reads from:

- `landintel.canonical_sites`
- `landintel.site_prove_it_assessments`
- `landintel.site_ldn_candidate_screen`
- `landintel.site_urgent_address_title_pack`
- `landintel.site_assessments`
- `landintel.evidence_references`
- `landintel.site_signals`
- `landintel.site_change_events`
- `public.site_title_validation`
- `public.site_title_resolution_candidates`
- `public.site_ros_parcel_link_candidates`
- `landintel.title_order_workflow`
- `landintel.title_review_records`
- `landintel.ownership_control_signals`

It exposes the fields LDN needs to understand what a site is, why it surfaced, what the current review position is, what evidence exists, what is unknown, and what the next action should be.

### `landintel_sourced.v_sourced_site_briefs`

A human-readable brief layer over `v_sourced_sites`.

It packages the sourced site into:

- claim
- proof count
- signals
- warnings
- gaps
- action
- caveat

This is for operator reading and dashboard display. It is not a new assessment engine.

### `landintel_sourced.v_review_queue`

The manual review queue for sites that already have enough signal to justify human attention.

It does not auto-kill sites and it does not remove weaker or watchlist sites from the system.

### `landintel_sourced.v_title_spend_candidates`

The title-spend candidate queue.

This view is deliberately cautious. It can surface a title-number-shaped candidate for manual checking, but it must not be read as ownership confirmation.

Ownership remains unconfirmed unless `landintel.title_review_records` contains a human title review for the site.

### `landintel_sourced.v_resurfacing_candidates`

The resurfacing list for sites that are currently weak, monitored, ignored, rejected by current evidence, or changed by new events.

Rejected, watchlist, monitor and currently weak sites remain capable of resurfacing. The view is designed to prevent useful land from disappearing simply because the current evidence is incomplete or not yet compelling.

## Ownership And Title Safety

Ownership remains unconfirmed unless `landintel.title_review_records` supports it.

Pre-title layers such as:

- `public.site_title_validation`
- `public.site_title_resolution_candidates`
- `public.site_ros_parcel_link_candidates`
- `landintel.site_urgent_address_title_pack`
- `landintel.title_order_workflow`
- `landintel.ownership_control_signals`

are workflow evidence only. They can support title spend decisions, but they do not prove legal ownership or control.

## SCT Parcel References

Rejected SCT-like parcel references must not be treated as title numbers.

The views only expose title-number candidates through `public.is_scottish_title_number_candidate(...)`, which excludes SCT-style RoS parcel identifiers. SCT references may remain useful as parcel references, but they are not ScotLIS title numbers and must not be presented to operators as title ownership proof.

## No Physical Data Movement

This PR creates views only.

It does not:

- create `landintel_sourced.sourced_sites`
- copy rows from `landintel.canonical_sites`
- move title, parcel, evidence or signal rows
- ingest new datasets
- run source workflows
- change scoring or prediction logic

## Commercial Impact

The value is clarity.

LDN should be able to open one sourced-site surface and see:

- what the site is
- why it surfaced
- what the current review status is
- whether it is a title-spend candidate
- whether ownership is still unconfirmed
- what evidence exists
- what is missing
- what the next action is

That helps LDN spend time and title money only where the current evidence justifies the next move.

## Caveats

These views are operating surfaces, not final DD outputs.

They preserve unknowns and caveats rather than hiding them. In particular:

- no legal ownership confirmation exists before human title review
- title-number candidates are not title review outcomes
- RoS parcel candidates are not ScotLIS title numbers
- weak, monitor or rejected-by-current-evidence sites are still allowed to resurface
- the views do not create go/no-go rules

## Future PRs

Recommended sequence:

1. Add lightweight dashboard/API exposure for `landintel_sourced` views.
2. Add contract tests against a disposable database once CI has a Postgres/PostGIS service.
3. Tighten operator columns after the first LDN review session.
4. Only after the sourced views are proven useful, improve extraction into the underlying evidence and signal layers.

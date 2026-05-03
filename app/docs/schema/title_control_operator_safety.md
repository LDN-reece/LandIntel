# Title And Control Operator Safety

## Purpose

PR 3 makes title and control outputs safe for operators and future intelligence layers.

It creates reporting views only. It does not move title data, delete rejected audit rows, alter title source truth, create ownership certainty, or treat RoS parcel references as title numbers.

## Views

### `landintel_reporting.v_title_control_status`

The main operator-safe title/control status view.

It exposes one readable status per canonical site using the existing title workflow, title candidate, title review and control signal layers.

Allowed status language includes:

- `title_not_required_yet`
- `title_candidate_available`
- `title_order_recommended`
- `title_ordered`
- `title_reviewed_confirmed`
- `title_reviewed_issue`
- `ownership_unconfirmed`
- `control_hypothesis_only`

The view only treats ownership as reviewed where `landintel.title_review_records` exists for the site.

### `landintel_reporting.v_title_candidates_operator_safe`

The safe title candidate view.

It separates:

- title-number-shaped candidates that can be checked manually
- RoS parcel references
- rejected SCT-like audit rows

RoS parcel reference is not a title number.

SCT-like references are not title numbers and rejected SCT-like values remain audit-only. They must not be surfaced as title numbers, but they should remain in the warehouse as evidence of what was rejected and why.

### `landintel_reporting.v_sites_needing_title_review`

The human review queue for sites where title review is still missing.

This view is for work allocation. It is not ownership confirmation.

### `landintel_reporting.v_title_spend_queue`

The title spend queue.

This view helps decide where title spend may be justified, but it does not confirm ownership, control, title validity or legal position.

## Non-Negotiable Rules

`title_review_records=0` means ownership remains unconfirmed.

Ownership remains unconfirmed without title review.

Companies House, FCA and control signals are hypotheses until title review. They can inform whether a site is worth a title order, but they cannot prove legal ownership or control.

RoS parcel references are spatial/cadastral references. They are not ScotLIS title numbers.

Rejected SCT-like references must remain audit-only. Do not delete them and do not surface them as title numbers.

## Safe Interpretation

Safe:

- "A title-number-shaped candidate is available for manual checking."
- "A RoS parcel reference exists, but title review is required."
- "Control signal exists, but it is hypothesis-only before title review."
- "Human title review has been recorded."

Unsafe:

- "This owner is confirmed" without `landintel.title_review_records`.
- "SCT reference is the title number."
- "Companies House proves ownership."
- "FCA or corporate data proves control."
- "Rejected candidate rows can be deleted because they are wrong."

## Why This Matters Commercially

Title spend is a commercial decision.

LDN needs to know where title spend is justified without confusing candidate evidence with legal ownership.

These views help operators see:

- where a title candidate exists
- where only a parcel reference exists
- where title order may be commercially justified
- where control signals are only hypotheses
- where human title review is still missing

The result is better capital discipline: spend title money where it moves the deal forward, not where the system has merely found a parcel reference.

## No Data Movement

This PR creates views only.

It does not:

- ingest title data
- create title review rows
- delete rejected SCT-like audit rows
- rewrite title candidates
- change `landintel.canonical_sites`
- confirm ownership
- alter title source truth destructively

## Future Work

Future PRs can add dashboard exposure and operator workflows on top of these views.

Do not automate ownership conclusions until the human title review layer is populated and governed.

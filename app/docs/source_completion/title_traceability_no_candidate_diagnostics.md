# Title Traceability No-Candidate Diagnostics

## Purpose

This layer explains why bounded title traceability runs produced no RoS parcel candidate.

It does not create title truth. It does not confirm ownership. It does not reject a site. It is a diagnostic surface so LandIntel stops repeating blind title-indexing work and fixes the real blocker.

## Views

### `landintel_reporting.v_site_title_no_candidate_diagnostics`

One row per `landintel_store.site_title_traceability_scan_state` row where `scan_status = 'no_candidate'`.

It shows, in the default audit-safe surface:

- site label, authority, area and priority band;
- whether canonical site geometry exists, is empty or is valid;
- whether same-authority RoS parcel coverage exists;
- a `diagnostic_reason`;
- a practical `recommended_action`.

The first implementation attempted nearest RoS parcel and 250m candidate-window counts inside the default reporting view. That made the orchestration audit too expensive against the RoS parcel estate. The current view deliberately avoids parcel-nearest scans by default. Nearest parcel fields are retained as nullable columns for compatibility, but detailed candidate-window failures now require a bounded per-site spot check rather than a broad reporting query.

### `landintel_reporting.v_site_title_no_candidate_diagnostic_summary`

Aggregates no-candidate sites by scan scope, priority band and diagnostic reason.

Use this before changing batch size. If the summary shows candidate-window failures, the fix is candidate logic. If it shows missing coverage, the fix is RoS coverage. If it shows geometry issues, the fix is canonical site geometry.

## Diagnostic Reasons

- `site_geometry_missing_or_empty`
- `site_geometry_invalid_repaired_for_diagnostic`
- `authority_ros_coverage_missing`
- `no_same_authority_ros_parcel_found`
- `candidate_window_or_geometry_overlap_needs_bounded_spot_check`

## Operator Rule

No-candidate diagnostics are not negative sourcing verdicts. They are indexing blockers.

Commercially, this matters because an otherwise interesting outside-register site should not be dropped just because the first RoS parcel bridge did not find a candidate. The diagnostic tells LDN whether to fix geometry, widen candidate logic, repair RoS coverage or leave the site in monitor.

Candidate-window failures still matter, but they should be investigated through a bounded per-site diagnostic rather than by making every audit scan the full RoS geometry set.

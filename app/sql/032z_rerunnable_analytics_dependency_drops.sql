-- Rerunnable migration preflight.
-- Later analytics wrapper views depend on parent views rebuilt by earlier migrations.
-- Drop the wrappers before the parent migrations run again so source jobs can
-- safely execute the full migration set more than once.

drop view if exists landintel_reporting.v_drive_source_enrichment_queue;
drop view if exists landintel_reporting.v_drive_source_duplicate_review_queue;
drop view if exists landintel_reporting.v_drive_source_dedupe_enrichment;
drop view if exists landintel_reporting.v_source_completion_matrix;
drop view if exists analytics.v_phase_one_source_estate_matrix;
drop view if exists analytics.v_live_source_coverage_freshness;
drop view if exists analytics.v_phase_one_control_policy_priority;

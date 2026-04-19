comment on table public.site_spatial_links is
    'Spatial-link extension anchored to public.sites and public.site_locations.geometry. Stores cross-object spatial lineage without creating a second site system.';
comment on column public.site_spatial_links.site_id is
    'Identifier mirrored from public.sites.id for the live site spine.';
comment on column public.site_spatial_links.site_location_id is
    'Identifier mirrored from public.site_locations.id for the geometry used as the measurement anchor.';
comment on column public.site_spatial_links.link_method is
    'How the link was established, such as intersects, nearest, manual, or derived.';

comment on table public.site_title_validation is
    'Title validation evidence for the live site spine. Used to prove whether a site geometry has documentary title support.';
comment on column public.site_title_validation.normalized_title_number is
    'Normalized title number used for consistent validation and duplicate control.';
comment on column public.site_title_validation.validation_status is
    'Current validation state for the site/title pairing.';

comment on table public.constraint_layer_registry is
    'Registry of constraint layers used by the Constraints tab measurement architecture. Defines how each layer should be measured, grouped, and interpreted.';
comment on column public.constraint_layer_registry.measurement_mode is
    'Whether the layer is measured by intersection, distance, or both.';
comment on column public.constraint_layer_registry.legacy_site_constraints_key is
    'Optional mapping key back to the legacy public.site_constraints severity-style path.';

comment on table public.constraint_source_features is
    'Normalized raw constraint features ready for spatial measurement against public.site_locations.geometry.';
comment on column public.constraint_source_features.source_feature_key is
    'Stable source-side identifier for the raw constraint feature.';
comment on column public.constraint_source_features.severity_label is
    'Source-side severity text retained for provenance only. It does not replace measured geometry facts.';

comment on table public.site_constraint_measurements is
    'Measured site-to-constraint facts anchored to the live site spine. Stores overlap and distance evidence only: no scoring, no pass/fail, and no RAG logic.';
comment on column public.site_constraint_measurements.overlap_pct_of_site is
    'Percentage of the anchored site geometry overlapped by the source constraint feature.';
comment on column public.site_constraint_measurements.nearest_distance_m is
    'Shortest measured distance in metres between the anchored site geometry and the source constraint feature.';

comment on table public.site_constraint_group_summaries is
    'Roll-up summaries of measured site-to-constraint facts by site location, constraint group, and layer.';
comment on column public.site_constraint_group_summaries.max_overlap_pct_of_site is
    'Largest measured overlap percentage for the anchored site geometry within the grouped layer.';
comment on column public.site_constraint_group_summaries.min_distance_m is
    'Closest measured distance in metres across the grouped layer.';

comment on table public.site_commercial_friction_facts is
    'Commercially relevant constraint facts derived from measured geometry. These are operator-readable statements, not scores or traffic-light outputs.';
comment on column public.site_commercial_friction_facts.fact_basis is
    'Short explanation of the measured evidence behind the friction fact.';

comment on view analytics.v_constraints_tab_overview is
    'Primary analyst-facing Constraints tab surface. One row per live site geometry anchor summarising measured coverage and commercially relevant friction facts.';
comment on column analytics.v_constraints_tab_overview.constraint_groups_measured is
    'Number of constraint groups with measured roll-up rows for the anchored site geometry.';
comment on column analytics.v_constraints_tab_overview.friction_fact_count is
    'Number of commercially relevant friction facts currently attached to the anchored site geometry.';

comment on view analytics.v_constraints_tab_measurements is
    'Detailed Constraints tab view of measured site-to-feature geometry facts. Use this when a user needs the exact overlap or distance evidence.';
comment on column analytics.v_constraints_tab_measurements.nearest_distance_m is
    'Shortest measured distance in metres from the anchored site geometry to the specific constraint feature.';

comment on view analytics.v_constraints_tab_group_summaries is
    'Constraints tab roll-up view summarising measured geometry by constraint group and layer for each live site geometry anchor.';
comment on column analytics.v_constraints_tab_group_summaries.max_overlap_pct_of_site is
    'Largest measured overlap percentage for the anchored site geometry within the grouped layer.';

comment on view analytics.v_constraints_tab_commercial_friction is
    'Constraints tab view of operator-readable commercial friction facts derived from measured geometry.';
comment on column analytics.v_constraints_tab_commercial_friction.fact_basis is
    'Measured evidence statement explaining why the commercial friction fact exists.';

do $$
begin
    if to_regclass('public.site_constraints') is not null then
        execute $comment$
            comment on table public.site_constraints is
                'Legacy severity-style constraint path retained for backward compatibility only. The current Constraints tab architecture should use public.constraint_layer_registry, public.constraint_source_features, public.site_constraint_measurements, public.site_constraint_group_summaries, and public.site_commercial_friction_facts anchored to public.sites plus public.site_locations.geometry.'
        $comment$;
    end if;
end $$;

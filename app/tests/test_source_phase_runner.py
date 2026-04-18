"""Smoke tests for stable source-phase helpers."""

from __future__ import annotations

import unittest
from unittest.mock import Mock

import geopandas as gpd
from shapely.geometry import Polygon

from src.source_phase_runner import (
    SourcePhaseRunner,
    _is_spatial_hub_illegal_property_error,
    _raise_for_spatial_hub_error_payload,
    _short_error_snippet,
)


class SourcePhaseRunnerTests(unittest.TestCase):
    def test_build_wfs_download_url_preserves_authkey_and_filter(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        runner.spatial_hub_authkey = "test-auth-key"
        authority_fields = ["local_auth", "authority_name"]
        url = SourcePhaseRunner._with_authkey(
            runner,
            "https://geo.spatialhub.scot/geoserver/sh_plnapp/wfs?service=WFS&request=GetCapabilities&authkey=test-auth-key",
            {
                "service": "WFS",
                "version": "1.0.0",
                "request": "GetFeature",
                "typeName": "sh_plnapp:pub_plnapppol",
                "outputFormat": "application/json",
                "cql_filter": SourcePhaseRunner._build_authority_filter(runner, authority_fields, "Dundee City"),
            },
        )

        self.assertIn("authkey=test-auth-key", url)
        self.assertIn("typeName=sh_plnapp%3Apub_plnapppol", url)
        self.assertIn("cql_filter=local_auth%3D%27Dundee+City%27+or+authority_name%3D%27Dundee+City%27", url)

    def test_illegal_property_name_detection(self) -> None:
        self.assertTrue(
            _is_spatial_hub_illegal_property_error(
                RuntimeError("Housing Land Supply returned a service error instead of features: Illegal property name: local_auth")
            )
        )

    def test_raise_for_spatial_hub_error_payload_surfaces_xml_errors(self) -> None:
        with self.assertRaises(RuntimeError) as context:
            _raise_for_spatial_hub_error_payload(
                "<?xml version='1.0'?><ServiceExceptionReport><ServiceException>Feature type unknown</ServiceException></ServiceExceptionReport>",
                content_type="application/xml",
                context="Planning capabilities",
            )

        self.assertIn("Feature type unknown", str(context.exception))

    def test_raise_for_spatial_hub_error_payload_allows_capabilities_xml(self) -> None:
        _raise_for_spatial_hub_error_payload(
            "<?xml version='1.0'?><wfs:WFS_Capabilities><FeatureTypeList /></wfs:WFS_Capabilities>",
            content_type="application/xml",
            context="Planning capabilities",
            allow_xml=True,
        )

    def test_short_error_snippet_strips_markup(self) -> None:
        snippet = _short_error_snippet("<html><body><h1>Error</h1><p>Access Denied</p></body></html>")
        self.assertEqual(snippet, "Error Access Denied")

    def test_find_best_site_in_frame_prefers_largest_overlap(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        site_frame = gpd.GeoDataFrame(
            [
                {"id": "site-a", "geometry": Polygon(((0, 0), (4, 0), (4, 4), (0, 4)))},
                {"id": "site-b", "geometry": Polygon(((2, 0), (8, 0), (8, 6), (2, 6)))},
            ],
            geometry="geometry",
            crs=27700,
        )

        best_site_id = SourcePhaseRunner._find_best_site_in_frame(
            runner,
            site_frame,
            Polygon(((3, 1), (7, 1), (7, 5), (3, 5))),
        )

        self.assertEqual(best_site_id, "site-b")

    def test_reconcile_canonical_sites_defers_primary_parcel_assignment(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        runner.target_authorities = ["Glasgow City"]
        runner.loader = Mock()
        runner.database = Mock()
        runner.loader.create_ingest_run.return_value = "run-id"
        runner._reset_canonical_state = Mock()
        runner._resolve_source_registry_id = Mock(side_effect=["hla-registry", "planning-registry"])
        runner._upsert_canonical_site = Mock(return_value="site-hla")
        runner._site_geometry_params = Mock(return_value={"dummy": "geometry"})
        runner._reference_alias_params = Mock(return_value={"dummy": "alias"})
        runner._source_link_params = Mock(return_value={"dummy": "link"})
        runner._evidence_params = Mock(return_value={"dummy": "evidence"})
        runner._batch_update_hla_records = Mock()
        runner._batch_update_planning_records = Mock()
        runner._batch_insert_site_geometry_versions = Mock()
        runner._batch_insert_reference_aliases = Mock()
        runner._batch_insert_source_links = Mock()
        runner._batch_insert_evidence_references = Mock()
        empty_site_frame = gpd.GeoDataFrame(
            {"id": [], "geometry": gpd.GeoSeries([], crs=27700)},
            geometry="geometry",
            crs=27700,
        )
        runner._load_canonical_site_frames = Mock(return_value={"Glasgow City": empty_site_frame})
        runner._find_best_site_in_frame = Mock(return_value="site-hla")
        runner._add_site_frame_geometry = Mock()
        runner._set_primary_parcels = Mock()

        hla_rows = gpd.GeoDataFrame(
            [
                {
                    "id": "hla-row",
                    "source_record_id": "HLA-1",
                    "authority_name": "Glasgow City",
                    "site_reference": "REF-1",
                    "site_name": "Seed Site",
                    "geometry": Polygon(((0, 0), (2, 0), (2, 2), (0, 2))),
                    "effectiveness_status": "effective",
                    "programming_horizon": "0-5",
                    "constraint_reasons": [],
                    "remaining_capacity": 20,
                }
            ],
            geometry="geometry",
            crs=27700,
        )
        planning_rows = gpd.GeoDataFrame(
            [
                {
                    "id": "planning-row",
                    "source_record_id": "PLN-1",
                    "authority_name": "Glasgow City",
                    "planning_reference": "25/00001/DC",
                    "proposal_text": "Mixed-use redevelopment",
                    "decision": "pending",
                    "geometry": Polygon(((0, 0), (2, 0), (2, 2), (0, 2))),
                }
            ],
            geometry="geometry",
            crs=27700,
        )
        runner.database.read_geodataframe.side_effect = [hla_rows, planning_rows]

        result = SourcePhaseRunner.reconcile_canonical_sites(runner)

        self.assertEqual(result, {"canonical_site_count": 1, "linked_rows": 2})
        runner._set_primary_parcels.assert_called_once_with()
        runner._batch_update_hla_records.assert_called_once()
        self.assertEqual(runner._batch_insert_site_geometry_versions.call_count, 2)
        runner._batch_update_planning_records.assert_called_once()
        runner.loader.update_ingest_run.assert_called_once()


if __name__ == "__main__":
    unittest.main()

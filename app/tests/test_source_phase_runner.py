"""Stable regression tests for the source-intelligence runner."""

from __future__ import annotations

import unittest
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
        url = SourcePhaseRunner._build_wfs_download_url(
            runner,
            "https://geo.spatialhub.scot/geoserver/sh_plnapp/wfs?service=WFS&request=GetCapabilities&authkey=test-auth-key",
            "sh_plnapp:pub_plnapppol",
            authority_name="Dundee City",
            authority_fields=["local_auth", "authority_name"],
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

    def test_consolidate_hla_rows_merges_duplicate_site_references(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)

        rows = [
            {
                "source_record_id": "EK4211",
                "authority_name": "South Lanarkshire",
                "site_reference": "EK4211",
                "site_name": None,
                "effectiveness_status": None,
                "programming_horizon": None,
                "constraint_reasons": ["roads"],
                "developer_name": None,
                "remaining_capacity": None,
                "completions": None,
                "tenure": None,
                "brownfield_indicator": None,
                "geometry_wkb": Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]).wkb_hex,
                "source_registry_id": "registry-id",
                "ingest_run_id": "ingest-id",
                "raw_payload": "{\"id\":\"pub_hls.211\",\"site_reference\":\"EK4211\"}",
            },
            {
                "source_record_id": "EK4211",
                "authority_name": "South Lanarkshire",
                "site_reference": "EK4211",
                "site_name": "East Kilbride Expansion",
                "effectiveness_status": "effective",
                "programming_horizon": "0-2 years",
                "constraint_reasons": ["drainage"],
                "developer_name": "Example Homes",
                "remaining_capacity": 120,
                "completions": 5,
                "tenure": "mixed",
                "brownfield_indicator": True,
                "geometry_wkb": Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]).wkb_hex,
                "source_registry_id": "registry-id",
                "ingest_run_id": "ingest-id",
                "raw_payload": "{\"id\":\"pub_hls.999\",\"site_reference\":\"EK4211\"}",
            },
        ]

        merged_rows = SourcePhaseRunner._consolidate_hla_rows(runner, rows)

        self.assertEqual(len(merged_rows), 1)
        merged = merged_rows[0]
        self.assertEqual(merged["source_record_id"], "EK4211")
        self.assertEqual(merged["site_name"], "East Kilbride Expansion")
        self.assertEqual(merged["effectiveness_status"], "effective")
        self.assertEqual(merged["programming_horizon"], "0-2 years")
        self.assertEqual(merged["developer_name"], "Example Homes")
        self.assertEqual(merged["remaining_capacity"], 120)
        self.assertEqual(merged["completions"], 5)
        self.assertEqual(merged["tenure"], "mixed")
        self.assertTrue(merged["brownfield_indicator"])
        self.assertEqual(sorted(merged["constraint_reasons"]), ["drainage", "roads"])
        self.assertIn("\"source_row_count\": 2", merged["raw_payload"])
        self.assertIsNotNone(merged["geometry_wkb"])

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


if __name__ == "__main__":
    unittest.main()

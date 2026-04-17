"""Smoke tests for stable source-phase helpers."""

from __future__ import annotations

import unittest

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

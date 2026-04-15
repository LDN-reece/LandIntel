"""Regression tests for the source-intelligence runner."""

from __future__ import annotations

import unittest

from src.source_phase_runner import (
    SourcePhaseRunner,
    SpatialHubSourceConfig,
    _raise_for_spatial_hub_error_payload,
    _short_error_snippet,
)


PLANNING_RESOURCE_HTML = """
<iframe src="https://maps.spatialhub.scot/ckan_preview_map/?layer=pub_plnapppol" title="Preview_map"></iframe>
<a onclick='EntryPoint.base_handler(
    "https://geo.spatialhub.scot/geoserver/",
    "plnapp",
    "Planning Applications: Official - Scotland",
    "geojson",
    "False",
    "True",
    "")'>
</a>
<tr class="toggle-more">
  <th scope="row">alternative name</th>
  <td>plnapppol</td>
</tr>
"""


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _FakeClient:
    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping

    def get(self, url: str) -> _FakeResponse:
        return _FakeResponse(self.mapping[url])


class SourcePhaseRunnerTests(unittest.TestCase):
    def test_resolve_spatial_hub_resource_handle_extracts_workspace_and_layer(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        runner.manifest = {"spatial_hub": {"package_show_base_url": "https://data.spatialhub.scot/api/3/action/package_show?id="}}
        runner._spatial_hub_handle_cache = {}
        runner.spatial_hub_authkey = "test-auth-key"
        runner.client = _FakeClient(
            {
                "https://data.spatialhub.scot/dataset/planning_applications_official-is/resource/resource-123": PLANNING_RESOURCE_HTML,
            }
        )

        config = SpatialHubSourceConfig(
            source_name="Planning Applications: Official - Scotland",
            publisher="Improvement Service",
            dataset_id="planning_applications_official-is",
            metadata_uuid="spatialhub:planning_applications_official-is",
            field_mappings={},
            authority_field_candidates=["local_auth"],
            resource_name_contains="Polygons",
        )

        payload = {"result": {"title": config.source_name}}
        resource = {"id": "resource-123"}

        handle = SourcePhaseRunner._resolve_spatial_hub_resource_handle(runner, config, payload, resource)

        self.assertEqual(handle.workspace_name, "sh_plnapp")
        self.assertEqual(handle.preview_layer_name, "pub_plnapppol")
        self.assertEqual(handle.alternative_name, "plnapppol")
        self.assertIn("authkey=test-auth-key", handle.capabilities_url)

    def test_select_feature_type_prefers_preview_layer_name(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        config = SpatialHubSourceConfig(
            source_name="Planning Applications: Official - Scotland",
            publisher="Improvement Service",
            dataset_id="planning_applications_official-is",
            metadata_uuid="spatialhub:planning_applications_official-is",
            field_mappings={},
            authority_field_candidates=["local_auth"],
            resource_name_contains="Polygons",
        )

        selected = SourcePhaseRunner._select_spatial_hub_feature_type_name(
            runner,
            ["sh_plnapp:pub_plnapppnt", "sh_plnapp:pub_plnapppol"],
            config,
            "pub_plnapppol",
            "plnapppol",
        )

        self.assertEqual(selected, "sh_plnapp:pub_plnapppol")

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

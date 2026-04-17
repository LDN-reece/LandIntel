"""Regression tests for the source-intelligence runner."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Polygon

from src.source_phase_runner import (
    SourcePhaseRunner,
    SpatialHubSourceConfig,
    SpatialHubResourceHandle,
    _is_spatial_hub_illegal_property_error,
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

DESCRIBE_FEATURE_TYPE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <xsd:complexType name="pub_plnapppolType">
    <xsd:complexContent>
      <xsd:extension base="gml:AbstractFeatureType" xmlns:gml="http://www.opengis.net/gml">
        <xsd:sequence>
          <xsd:element maxOccurs="1" minOccurs="0" name="the_geom" type="gml:MultiSurfacePropertyType"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="local_auth" type="xsd:string"/>
          <xsd:element maxOccurs="1" minOccurs="0" name="reference" type="xsd:string"/>
        </xsd:sequence>
      </xsd:extension>
    </xsd:complexContent>
  </xsd:complexType>
</xsd:schema>
"""


class _FakeResponse:
    def __init__(self, text: str, *, content_type: str = "text/html") -> None:
        self.text = text
        self.headers = {"content-type": content_type}
        self.content = text.encode("utf-8")

    def raise_for_status(self) -> None:
        return None


class _FakeClient:
    def __init__(self, mapping: dict[str, str | tuple[str, str]]) -> None:
        self.mapping = mapping

    def get(self, url: str) -> _FakeResponse:
        payload = self.mapping[url]
        if isinstance(payload, tuple):
            text, content_type = payload
            return _FakeResponse(text, content_type=content_type)
        return _FakeResponse(payload)


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

    def test_fetch_spatial_hub_property_names_reads_describe_feature_type(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        runner._spatial_hub_property_name_cache = {}
        runner.spatial_hub_authkey = "test-auth-key"
        runner.client = _FakeClient(
            {
                "https://geo.spatialhub.scot/geoserver/sh_plnapp/wfs?service=WFS&version=1.0.0&request=DescribeFeatureType&typeName=sh_plnapp%3Apub_plnapppol&authkey=test-auth-key": (
                    DESCRIBE_FEATURE_TYPE_XML,
                    "application/xml",
                )
            }
        )
        handle = SpatialHubResourceHandle(
            source_name="Planning Applications: Official - Scotland",
            resource_id="resource-123",
            resource_page_url="https://data.spatialhub.scot/dataset/planning_applications_official-is/resource/resource-123",
            geoserver_root="https://geo.spatialhub.scot/geoserver/",
            workspace_name="sh_plnapp",
            preview_layer_name="pub_plnapppol",
            alternative_name="plnapppol",
            capabilities_url="https://geo.spatialhub.scot/geoserver/sh_plnapp/wfs?service=WFS&request=GetCapabilities&authkey=test-auth-key",
        )

        names = SourcePhaseRunner._fetch_spatial_hub_property_names(runner, handle, "sh_plnapp:pub_plnapppol")

        self.assertIn("local_auth", names)
        self.assertIn("reference", names)

    def test_select_spatial_hub_authority_fields_uses_only_available_fields(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)

        fields = SourcePhaseRunner._select_spatial_hub_authority_fields(
            runner,
            ["the_geom", "local_auth", "reference"],
            ["local_auth", "local_authority", "authority_name"],
        )

        self.assertEqual(fields, ["local_auth"])

    def test_illegal_property_name_detection(self) -> None:
        self.assertTrue(
            _is_spatial_hub_illegal_property_error(
                RuntimeError("Housing Land Supply returned a service error instead of features: Illegal property name: local_auth")
            )
        )

    def test_fetch_spatial_hub_frame_retries_without_server_filter(self) -> None:
        runner = SourcePhaseRunner.__new__(SourcePhaseRunner)
        runner.logger = type("Logger", (), {"warning": staticmethod(lambda *args, **kwargs: None)})()
        runner._spatial_hub_unfiltered_frame_cache = {}
        runner._get_spatial_hub_package_payload = lambda config: {"result": {"title": config.source_name}}
        runner._select_spatial_hub_resource = lambda payload, config: {"id": "resource-123"}
        runner._resolve_spatial_hub_resource_handle = lambda config, payload, resource: SpatialHubResourceHandle(
            source_name=config.source_name,
            resource_id="resource-123",
            resource_page_url="https://data.spatialhub.scot/dataset/housing_land_supply-is/resource/resource-123",
            geoserver_root="https://geo.spatialhub.scot/geoserver/",
            workspace_name="sh_hls",
            preview_layer_name="pub_hls",
            alternative_name="pub_hls",
            capabilities_url="https://geo.spatialhub.scot/geoserver/sh_hls/wfs?service=WFS&request=GetCapabilities&authkey=test-auth-key",
        )
        runner._fetch_spatial_hub_feature_type_names = lambda handle: ["sh_hls:pub_hls"]
        runner._select_spatial_hub_feature_type_name = (
            lambda feature_type_names, config, preview_layer_name, alternative_name: "sh_hls:pub_hls"
        )
        runner._fetch_spatial_hub_property_names = lambda handle, layer_name: ["local_auth", "site_name"]
        runner.authority_aoi = gpd.GeoDataFrame(columns=["authority_name", "geometry"], geometry="geometry", crs=27700)

        config = SpatialHubSourceConfig(
            source_name="Housing Land Supply - Scotland",
            publisher="Improvement Service",
            dataset_id="housing_land_supply-is",
            metadata_uuid="spatialhub:housing_land_supply-is",
            field_mappings={},
            authority_field_candidates=["local_auth", "local_authority", "authority_name"],
        )

        filtered_calls: list[tuple[str | None, list[str]]] = []

        def fake_download(capabilities_url, layer_name, *, authority_name, authority_fields, context):
            filtered_calls.append((authority_name, list(authority_fields)))
            if authority_name is not None:
                raise RuntimeError(
                    "Housing Land Supply - Scotland GetFeature for Glasgow City returned a service error instead of features: Illegal property name: local_auth"
                )
            return gpd.GeoDataFrame(
                {"local_authority": ["Glasgow City"], "site_name": ["Test Site"]},
                geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
                crs=27700,
            )

        runner._download_spatial_hub_frame = fake_download

        with patch.object(SourcePhaseRunner, "_standardise_frame", autospec=True) as standardise_mock:
            standardise_mock.side_effect = lambda self, frame, authority_name, authority_fields: frame
            frame = SourcePhaseRunner._fetch_spatial_hub_frame(runner, config, "Glasgow City")

        self.assertEqual(len(filtered_calls), 2)
        self.assertEqual(filtered_calls[0], ("Glasgow City", ["local_auth"]))
        self.assertEqual(filtered_calls[1], (None, []))
        _, _, authority_name, authority_fields = standardise_mock.call_args.args
        self.assertEqual(authority_name, "Glasgow City")
        self.assertEqual(authority_fields, ["local_authority"])
        self.assertEqual(len(frame), 1)

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

"""Tests for the operational parcel retention filter."""

from __future__ import annotations

import unittest

import geopandas as gpd

from src.processors.filter_operational_candidates import filter_operational_candidates


class FilterOperationalCandidatesTest(unittest.TestCase):
    def test_filters_below_threshold_rows(self) -> None:
        gdf = gpd.GeoDataFrame(
            [
                {"area_acres": 1.5},
                {"area_acres": 3.99},
                {"area_acres": 4.0},
                {"area_acres": 12.25},
            ],
            geometry=[None, None, None, None],
            crs="EPSG:27700",
        )

        retained, summary = filter_operational_candidates(gdf, minimum_area_acres=4.0)

        self.assertEqual(len(retained), 2)
        self.assertEqual(summary["input_rows"], 4)
        self.assertEqual(summary["retained_rows"], 2)
        self.assertEqual(summary["filtered_out_rows"], 2)
        self.assertEqual(summary["retained_area_acres"], 16.25)
        self.assertEqual(summary["filtered_out_area_acres"], 5.49)

    def test_empty_frame_stays_empty(self) -> None:
        gdf = gpd.GeoDataFrame({"area_acres": []}, geometry=[], crs="EPSG:27700")

        retained, summary = filter_operational_candidates(gdf, minimum_area_acres=4.0)

        self.assertTrue(retained.empty)
        self.assertEqual(summary["retained_rows"], 0)
        self.assertEqual(summary["filtered_out_rows"], 0)


if __name__ == "__main__":
    unittest.main()

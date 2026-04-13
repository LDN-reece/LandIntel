"""Tests for the operational parcel filter."""

from __future__ import annotations

import unittest

import geopandas as gpd
from shapely.geometry import Polygon

from src.processors.filter_operational_candidates import filter_operational_candidates


class FilterOperationalCandidatesTests(unittest.TestCase):
    def test_filters_rows_below_threshold(self) -> None:
        frame = gpd.GeoDataFrame(
            {
                "ros_inspire_id": ["a", "b", "c"],
                "area_acres": [1.5, 4.0, 9.25],
                "geometry": [
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                    Polygon([(0, 0), (3, 0), (3, 3), (0, 3)]),
                ],
            },
            geometry="geometry",
            crs="EPSG:27700",
        )

        filtered, summary = filter_operational_candidates(frame, minimum_area_acres=4.0)

        self.assertEqual(filtered["ros_inspire_id"].tolist(), ["b", "c"])
        self.assertEqual(summary["input_rows"], 3)
        self.assertEqual(summary["retained_rows"], 2)
        self.assertEqual(summary["filtered_out_rows"], 1)
        self.assertAlmostEqual(float(summary["filtered_out_area_acres"]), 1.5)


if __name__ == "__main__":
    unittest.main()

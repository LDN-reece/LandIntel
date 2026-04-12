"""Regression tests for acreage bucketing."""

from __future__ import annotations

import unittest

import geopandas as gpd

from src.processors.classify_size import classify_size_buckets


class ClassifySizeBucketsTest(unittest.TestCase):
    """Verify the two core acreage differentiators."""

    def test_uses_four_acre_threshold(self) -> None:
        frame = gpd.GeoDataFrame(
            {
                "area_acres": [3.99, 4.0, 4.01],
                "geometry": [None, None, None],
            }
        )

        classified = classify_size_buckets(frame)

        self.assertEqual(
            classified["size_bucket"].tolist(),
            [
                "bucket_1_under_4_acres",
                "bucket_2_4plus_acres",
                "bucket_2_4plus_acres",
            ],
        )
        self.assertEqual(
            classified["size_bucket_label"].tolist(),
            [
                "Under 4 acres",
                "4+ acres",
                "4+ acres",
            ],
        )


if __name__ == "__main__":
    unittest.main()

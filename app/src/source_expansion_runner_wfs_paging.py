"""Paged WFS entrypoint for Phase One source expansion.

SpatialHub can return service exceptions when large national layers are pulled in
one request. This wrapper preserves the existing source-expansion implementation
but replaces WFS reads with bounded pages so larger sources such as VDL can be
loaded through GitHub Actions without weakening the data model.
"""

from __future__ import annotations

from typing import Any
import traceback

import geopandas as gpd

from config.settings import Settings, get_settings
from src.logging_config import configure_logging
from src.source_expansion_runner import SourceExpansionRunner, build_parser


class PagedWfsSourceExpansionRunner(SourceExpansionRunner):
    """Source expansion runner with bounded GeoServer WFS paging."""

    def __init__(self, settings: Settings, logger: Any) -> None:
        super().__init__(settings, logger)
        self.logger = logger.getChild("source_expansion_paged_wfs")

    def _fetch_wfs_source_frames(self, source: dict[str, Any]) -> list[gpd.GeoDataFrame]:
        endpoint_url = str(source["endpoint_url"])
        type_names = self._wfs_feature_types(source)
        if not type_names:
            raise RuntimeError(f"No WFS feature types found for {source['source_key']}.")

        frames: list[gpd.GeoDataFrame] = []
        layer_errors: list[str] = []
        page_size = max(1, self.page_size)

        for type_name in type_names:
            fetched = 0
            offset = 0
            while True:
                batch_limit = page_size
                if self.max_features > 0:
                    remaining = self.max_features - fetched
                    if remaining <= 0:
                        break
                    batch_limit = min(batch_limit, remaining)

                params = {
                    "service": "WFS",
                    "version": "1.0.0",
                    "request": "GetFeature",
                    "typeName": type_name,
                    "outputFormat": "application/json",
                    "srsName": "EPSG:27700",
                    "maxFeatures": str(batch_limit),
                }
                if offset > 0:
                    # GeoServer supports startIndex as a vendor parameter for paged WFS reads.
                    params["startIndex"] = str(offset)
                params.update(self._auth_params(source))

                try:
                    response = self.client.get(endpoint_url, params=params)
                    response.raise_for_status()
                    payload = self._json_payload(response, f"WFS GetFeature {source['source_key']} {type_name}")
                    frame = self._feature_collection_to_gdf(payload, source, type_name)
                except Exception as exc:
                    layer_errors.append(f"{type_name} offset {offset}: {exc}")
                    break

                if frame.empty:
                    break

                frames.append(frame)
                batch_count = len(frame)
                fetched += batch_count
                if batch_count < batch_limit:
                    break
                offset += batch_count

        if not frames and layer_errors:
            raise RuntimeError(
                f"No usable WFS features returned for {source['source_key']}. "
                + " | ".join(layer_errors[:5])
            )
        return frames


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    logger = configure_logging(settings)
    runner = PagedWfsSourceExpansionRunner(settings, logger)
    try:
        runner.run_command(args.command)
        logger.info("source_expansion_command_completed", extra={"command": args.command})
        return 0
    except Exception:
        logger.exception("source_expansion_command_failed", extra={"command": args.command, "traceback": traceback.format_exc()})
        return 1
    finally:
        runner.close()


if __name__ == "__main__":
    raise SystemExit(main())

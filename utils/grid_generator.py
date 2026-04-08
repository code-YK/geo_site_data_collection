"""
utils/grid_generator.py
Creates H3 resolution-7 hexagonal grid (~5.16 sq km/cell) covering all of India.
Outputs a GeoDataFrame with cell centroids + H3 IDs.

Dependencies:
    pip install h3 geopandas shapely pandas pyproj
"""

import json
import logging
from pathlib import Path

import geopandas as gpd
import h3
import pandas as pd
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union

from config.settings import H3_RESOLUTION, INDIA_BBOX, OUTPUT_CRS

logger = logging.getLogger(__name__)


def get_india_boundary(shapefile_path: str | None = None) -> gpd.GeoDataFrame:
    """
    Load India's administrative boundary.
    Falls back to the bounding-box rectangle if no shapefile is provided.

    Recommended shapefile:
        naturalearth_lowres  (built into geopandas — good enough for grid masking)
        OR download from:
        https://www.gadm.org/download_country.html  (choose India, GeoJSON level 0)
    """
    if shapefile_path and Path(shapefile_path).exists():
        gdf = gpd.read_file(shapefile_path)
        logger.info(f"Loaded boundary from {shapefile_path}")
        return gdf.to_crs(OUTPUT_CRS)

    # Built-in naturalearth fallback
    try:
        world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
        india = world[world["name"] == "India"].copy()
        logger.warning("Using low-resolution naturalearth boundary. For production, supply GADM shapefile.")
        return india.to_crs(OUTPUT_CRS)
    except Exception as e:
        logger.warning(f"naturalearth fallback failed ({e}). Using bounding box.")
        from shapely.geometry import box
        bbox_poly = box(
            INDIA_BBOX["min_lon"], INDIA_BBOX["min_lat"],
            INDIA_BBOX["max_lon"], INDIA_BBOX["max_lat"],
        )
        return gpd.GeoDataFrame({"geometry": [bbox_poly]}, crs=OUTPUT_CRS)


def generate_h3_grid(
    boundary_gdf: gpd.GeoDataFrame,
    resolution: int = H3_RESOLUTION,
    batch_size: int = 50_000,
) -> gpd.GeoDataFrame:
    """
    Fill India's boundary polygon with H3 hexagons at the given resolution.
    Returns a GeoDataFrame with columns: [id, h3_index, latitude, longitude, geometry]

    H3 resolution 7 stats:
        avg area  : 5.161 sq km
        avg edge  : 1.406 km
        total cells in India: ~650,000
    """
    india_poly = unary_union(boundary_gdf.geometry)
    geojson_poly = mapping(india_poly)

    logger.info(f"Filling H3 resolution {resolution} cells into India boundary ...")
    h3_cells = h3.polyfill_geojson(geojson_poly, resolution)
    logger.info(f"Total H3 cells generated: {len(h3_cells):,}")

    records = []
    for i, cell in enumerate(h3_cells):
        lat, lon = h3.h3_to_geo(cell)
        records.append({
            "id":        i + 1,
            "h3_index":  cell,
            "latitude":  round(lat, 6),
            "longitude": round(lon, 6),
        })

    df = pd.DataFrame(records)

    # Build hexagon geometries
    def h3_to_polygon(cell):
        coords = h3.h3_to_geo_boundary(cell, geo_json=True)
        return Polygon(coords)

    df["geometry"] = df["h3_index"].apply(h3_to_polygon)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=OUTPUT_CRS)

    logger.info(f"Grid GeoDataFrame created: {len(gdf):,} rows")
    return gdf


def save_grid(gdf: gpd.GeoDataFrame, out_dir: str = "data/interim") -> Path:
    """Save grid to parquet + GeoJSON for downstream collectors."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    parquet_path = out_path / "india_h3_grid.parquet"
    geojson_path = out_path / "india_h3_grid.geojson"

    gdf.to_parquet(parquet_path, index=False)
    gdf.to_file(geojson_path, driver="GeoJSON")

    logger.info(f"Grid saved → {parquet_path}")
    logger.info(f"Grid saved → {geojson_path}")
    return parquet_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    boundary = get_india_boundary()          # use GADM shapefile in production
    grid     = generate_h3_grid(boundary)
    save_grid(grid)
    print(grid.head())
    print(f"\nGrid stats:\n{grid.describe()}")

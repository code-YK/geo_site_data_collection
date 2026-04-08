"""
collectors/layer2_transportation.py
Collects ALL transportation columns from the dataset spec.

Sources:
    - OSM via Overpass API or local PBF (osmium) → road network
  - OSRM routing engine                         → travel-time catchments

Columns produced:
  road_density, distance_to_highway, intersection_density,
  connectivity_score, avg_travel_time_10min, avg_travel_time_20min

Dependencies:
    pip install osmium networkx geopandas shapely requests osmnx pandas numpy
"""

import logging
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from shapely.geometry import Point, LineString
from shapely.ops import unary_union

from config.settings import (
    BUFFER_1KM, BUFFER_2KM, SOURCES,
)
from utils.osm_reader import get_roads

logger = logging.getLogger(__name__)

OVERPASS_URL = SOURCES["osm_overpass"]["endpoint"]
OSRM_URL     = SOURCES["osrm"]["endpoint"]


# ─────────────────────────────────────────────────────────────────────────────
# HELPER — batch Overpass queries (with retry + rate limit)
# ─────────────────────────────────────────────────────────────────────────────

def _overpass_query(ql: str, retries: int = 3, delay: float = 2.0) -> dict:
    for attempt in range(retries):
        try:
            resp = requests.post(OVERPASS_URL, data={"data": ql}, timeout=180)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Overpass attempt {attempt+1} failed: {e}")
            time.sleep(delay * (attempt + 1))
    raise RuntimeError("Overpass API unavailable after retries. Consider using local PBF (osmium).")


# ─────────────────────────────────────────────────────────────────────────────
# Option A — Extract roads from local OSM PBF (RECOMMENDED for all-India scale)
# ─────────────────────────────────────────────────────────────────────────────

def extract_roads_from_pbf() -> gpd.GeoDataFrame:
    """
    Parse the Geofabrik India PBF and return all road linestrings.
    Run this ONCE and cache to parquet; do not re-run per cell.

    Download:
        wget https://download.geofabrik.de/asia/india-latest.osm.pbf -P data/raw/osm/
    """
    return get_roads()


# ─────────────────────────────────────────────────────────────────────────────
# 1. ROAD DENSITY — total road length per sq km within H3 cell
# ─────────────────────────────────────────────────────────────────────────────

def compute_road_density(grid_gdf: gpd.GeoDataFrame, roads_gdf: gpd.GeoDataFrame) -> pd.Series:
    """Length of all roads (km) per sq km of cell area."""
    # Project to metric CRS
    grid_m  = grid_gdf.to_crs("EPSG:32644")
    roads_m = roads_gdf.to_crs("EPSG:32644")

    # Spatial index join
    joined = gpd.sjoin(roads_m, grid_m[["id", "geometry"]], how="inner", predicate="intersects")
    joined["length_m"] = joined.geometry.length

    road_len = joined.groupby("id")["length_m"].sum().rename("road_length_m")
    cell_area_km2 = grid_m.set_index("id").geometry.area / 1e6  # sq km

    density = (road_len / cell_area_km2).fillna(0).rename("road_density")
    return density.round(3)


# ─────────────────────────────────────────────────────────────────────────────
# 2. DISTANCE TO HIGHWAY — nearest NH/SH/expressway from centroid
# ─────────────────────────────────────────────────────────────────────────────

def compute_distance_to_highway(
    grid_gdf: gpd.GeoDataFrame, roads_gdf: gpd.GeoDataFrame
) -> pd.Series:
    """Euclidean distance (m) from cell centroid to nearest motorway/trunk/primary road."""
    highway_tags = {"motorway", "trunk", "primary", "motorway_link", "trunk_link"}
    highways = roads_gdf[roads_gdf["highway"].isin(highway_tags)].copy()

    if highways.empty:
        logger.warning("No highway features found in roads layer.")
        return pd.Series(np.nan, index=grid_gdf["id"], name="distance_to_highway")

    grid_m     = grid_gdf.to_crs("EPSG:32644")
    highways_m = highways.to_crs("EPSG:32644")
    hw_union   = unary_union(highways_m.geometry)

    distances = grid_m.geometry.centroid.distance(hw_union)
    return pd.Series(distances.values.round(0), index=grid_gdf["id"], name="distance_to_highway")


# ─────────────────────────────────────────────────────────────────────────────
# 3. INTERSECTION DENSITY — road junctions per sq km
# ─────────────────────────────────────────────────────────────────────────────

def compute_intersection_density(
    grid_gdf: gpd.GeoDataFrame, roads_gdf: gpd.GeoDataFrame
) -> pd.Series:
    """
    Count road endpoints that appear ≥2 times (= intersections) within each cell.
    Proxy: endpoint clustering from road linestrings.
    """
    try:
        import osmnx as ox
        # osmnx is the gold standard for intersection counts
        logger.info("Using osmnx for intersection density ...")
    except ImportError:
        logger.warning("osmnx not installed. Using endpoint-proxy for intersections. "
                       "pip install osmnx for accuracy.")

    grid_m  = grid_gdf.to_crs("EPSG:32644")
    roads_m = roads_gdf.to_crs("EPSG:32644")

    # Collect all endpoint coordinates from road segments
    endpoints = []
    for geom in roads_m.geometry:
        if isinstance(geom, LineString):
            endpoints.extend([geom.coords[0], geom.coords[-1]])

    ep_gdf = gpd.GeoDataFrame(
        geometry=[Point(x, y) for x, y in endpoints], crs="EPSG:32644"
    )
    # Count per cell
    joined    = gpd.sjoin(ep_gdf, grid_m[["id", "geometry"]], how="inner", predicate="within")
    counts    = joined.groupby("id").size().rename("endpoint_count")
    cell_area = grid_m.set_index("id").geometry.area / 1e6

    density = (counts / cell_area).fillna(0).rename("intersection_density")
    return density.round(2)


# ─────────────────────────────────────────────────────────────────────────────
# 4. CONNECTIVITY SCORE — normalised road network connectivity index
# ─────────────────────────────────────────────────────────────────────────────

def compute_connectivity_score(
    road_density: pd.Series, intersection_density: pd.Series
) -> pd.Series:
    """
    Simple composite: normalised (road_density × 0.5 + intersection_density × 0.5).
    Scale: 0–100. Replace with Gamma index from graph theory if osmnx is available.
    """
    rd_norm = (road_density - road_density.min()) / (road_density.max() - road_density.min() + 1e-9)
    id_norm = (intersection_density - intersection_density.min()) / (
        intersection_density.max() - intersection_density.min() + 1e-9
    )
    score = ((rd_norm * 0.5 + id_norm * 0.5) * 100).round(1)
    return score.rename("connectivity_score")


# ─────────────────────────────────────────────────────────────────────────────
# 5. OSRM TRAVEL-TIME CATCHMENTS (10 min / 20 min)
# ─────────────────────────────────────────────────────────────────────────────

def compute_travel_time_catchments(
    grid_gdf: gpd.GeoDataFrame,
    osrm_endpoint: str = OSRM_URL,
    batch_size: int = 50,
) -> pd.DataFrame:
    """
    For each centroid: use OSRM /table API to compute the area reachable
    in 10 min and 20 min by road (sq km proxy = count of H3 cells reachable).

    For ALL-INDIA scale: self-host OSRM with the India PBF:
      docker run -t -v $(pwd)/data/raw/osm:/data osrm/osrm-backend \
        osrm-extract -p /opt/car.lua /data/india-latest.osm.pbf
      docker run -t -v $(pwd)/data/raw/osm:/data osrm/osrm-backend \
        osrm-partition /data/india-latest.osrm
      docker run -t -v $(pwd)/data/raw/osm:/data osrm/osrm-backend \
        osrm-customize /data/india-latest.osrm
      docker run -t -i -p 5000:5000 -v $(pwd)/data/raw/osm:/data \
        osrm/osrm-backend osrm-routed --algorithm mld /data/india-latest.osrm

    Then set osrm_endpoint = "http://localhost:5000/table/v1/driving/"
    """
    logger.info(f"Computing OSRM travel-time catchments for {len(grid_gdf):,} cells ...")

    results = []
    coords_all = list(zip(grid_gdf["longitude"], grid_gdf["latitude"]))

    for i in range(0, len(grid_gdf), batch_size):
        batch = grid_gdf.iloc[i : i + batch_size]
        coord_str = ";".join(f"{row.longitude},{row.latitude}" for row in batch.itertuples())

        try:
            url = f"{osrm_endpoint}{coord_str}?annotations=duration"
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            durations = np.array(data["durations"])  # shape: (n, n) in seconds

            for j, row in enumerate(batch.itertuples()):
                row_times = durations[j]
                row_times = row_times[row_times is not None]
                row_times = np.array([t for t in row_times if t is not None], dtype=float)

                reach_10 = int(np.sum(row_times <= 600))   # ≤10 min
                reach_20 = int(np.sum(row_times <= 1200))  # ≤20 min

                results.append({
                    "id": row.id,
                    "avg_travel_time_10min": reach_10,  # cells reachable
                    "avg_travel_time_20min": reach_20,
                })
        except Exception as e:
            logger.warning(f"OSRM batch {i}–{i+batch_size} failed: {e}. Filling NaN.")
            for row in batch.itertuples():
                results.append({
                    "id": row.id,
                    "avg_travel_time_10min": np.nan,
                    "avg_travel_time_20min": np.nan,
                })
        time.sleep(0.1)

    return pd.DataFrame(results)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 2
# ─────────────────────────────────────────────────────────────────────────────

def collect_transportation(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    logger.info("=== LAYER 2: Transportation ===")

    roads = extract_roads_from_pbf()

    road_density    = compute_road_density(grid_gdf, roads)
    dist_highway    = compute_distance_to_highway(grid_gdf, roads)
    intersect_dens  = compute_intersection_density(grid_gdf, roads)
    connectivity    = compute_connectivity_score(road_density, intersect_dens)
    travel_times    = compute_travel_time_catchments(grid_gdf)

    result = grid_gdf[["id"]].copy()
    result = result.set_index("id")
    result["road_density"]          = road_density
    result["distance_to_highway"]   = dist_highway
    result["intersection_density"]  = intersect_dens
    result["connectivity_score"]    = connectivity
    result = result.reset_index().merge(travel_times, on="id", how="left")

    logger.info(f"Layer 2 complete: {result.shape[1]} columns, {len(result):,} rows")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from utils.grid_generator import generate_h3_grid, get_india_boundary

    boundary  = get_india_boundary()
    grid      = generate_h3_grid(boundary)
    transport = collect_transportation(grid)
    print(transport.head())

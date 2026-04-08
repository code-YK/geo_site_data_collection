"""
collectors/layer6_infrastructure.py
Collects ALL infrastructure columns from the dataset spec.

Source: OpenStreetMap (Geofabrik India PBF via osmium)

Columns produced:
  distance_to_power_substation, power_line_density, electricity_access_score,
  distance_to_water_source, water_body_proximity, water_availability_score,
  distance_to_bus_stop, distance_to_railway_station, public_transport_score

Dependencies:
    pip install osmium geopandas pandas numpy shapely
"""

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point
from shapely.ops import unary_union

from config.settings import BUFFER_1KM, SOURCES
from utils.osm_reader import get_features

logger = logging.getLogger(__name__)

OSM_PBF = Path(SOURCES["osm_geofabrik"]["local_path"])


# ─────────────────────────────────────────────────────────────────────────────
# Generic OSM feature loader (with caching)
# ─────────────────────────────────────────────────────────────────────────────

def _load_osm_features(feature_type: str, custom_filter: dict) -> gpd.GeoDataFrame:
    """
    Extract OSM features by type+filter from the India PBF.
    feature_type: descriptive name used for cache filename.
    """
    preserve_way_geometry = feature_type in {"power_lines"}
    features = get_features(
        tag_filters=custom_filter,
        cache_name=feature_type,
        include_ways=True,
        preserve_way_geometry=preserve_way_geometry,
    )
    if features is None or features.empty:
        logger.warning(f"No {feature_type} features found in PBF.")
        return gpd.GeoDataFrame()
    return features.to_crs("EPSG:4326")


# ─────────────────────────────────────────────────────────────────────────────
# Generic distance-to-nearest calculator
# ─────────────────────────────────────────────────────────────────────────────

def _distance_to_nearest(
    grid_gdf: gpd.GeoDataFrame,
    features_gdf: gpd.GeoDataFrame,
    col_name: str,
) -> pd.Series:
    """Euclidean distance (m) from each cell centroid to the nearest feature geometry."""
    if features_gdf is None or features_gdf.empty:
        logger.warning(f"No features for {col_name}. Returning NaN.")
        return pd.Series(np.nan, index=grid_gdf["id"], name=col_name)

    grid_m    = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    feats_m   = features_gdf.to_crs("EPSG:32644")
    feat_union = unary_union(feats_m.geometry.centroid
                             if "Point" not in str(feats_m.geometry.geom_type.unique())
                             else feats_m.geometry)

    distances = grid_m.geometry.centroid.distance(feat_union).round(0)
    return pd.Series(distances.values, index=grid_gdf["id"], name=col_name)


# ─────────────────────────────────────────────────────────────────────────────
# 1. POWER INFRASTRUCTURE
# ─────────────────────────────────────────────────────────────────────────────

def compute_power_features(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    distance_to_power_substation : metres to nearest substation/transformer
    power_line_density            : total power line length per sq km
    electricity_access_score      : normalised composite (0–100)
    """
    substations = _load_osm_features(
        "power_substations",
        {"power": ["substation", "transformer", "generator"]}
    )
    power_lines = _load_osm_features(
        "power_lines",
        {"power": ["line", "minor_line", "cable"]}
    )

    dist_substation = _distance_to_nearest(grid_gdf, substations, "distance_to_power_substation")

    # Power line density (km / sq km)
    if not power_lines.empty:
        grid_m  = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
        lines_m = power_lines.to_crs("EPSG:32644")
        joined  = gpd.sjoin(lines_m, grid_m[["id", "geometry"]], how="inner", predicate="intersects")
        joined["length_km"] = joined.geometry.length / 1000
        line_density = joined.groupby("id")["length_km"].sum()
        cell_area_km2 = grid_m.set_index("id").geometry.area / 1e6
        power_density = (line_density / cell_area_km2).fillna(0).round(3)
    else:
        power_density = pd.Series(0, index=grid_gdf["id"])
    power_density.name = "power_line_density"

    # Electricity access score: high density + close substation = high score
    dist_norm = 1 - (dist_substation / dist_substation.max().clip(1)).clip(0, 1)
    dens_norm = (power_density / power_density.max().clip(1e-6)).clip(0, 1)
    elec_score = ((dist_norm * 0.6 + dens_norm * 0.4) * 100).round(1)
    elec_score.name = "electricity_access_score"

    result = grid_gdf[["id"]].copy()
    for s in [dist_substation, power_density, elec_score]:
        result = result.merge(s.reset_index(name=s.name), on="id", how="left")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 2. WATER INFRASTRUCTURE
# ─────────────────────────────────────────────────────────────────────────────

def compute_water_features(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    distance_to_water_source : metres to nearest water body / treatment plant
    water_body_proximity     : binary 1/0 — water body within 2km
    water_availability_score : normalised composite (0–100)
    """
    water_bodies = _load_osm_features(
        "water_bodies",
        {"natural": ["water", "wetland"], "waterway": ["river", "canal", "stream"],
         "amenity": ["water_point", "drinking_water"]}
    )

    dist_water = _distance_to_nearest(grid_gdf, water_bodies, "distance_to_water_source")

    water_proximity = (dist_water <= 2000).astype(int).rename("water_body_proximity")

    dist_norm  = 1 - (dist_water / dist_water.max().clip(1)).clip(0, 1)
    water_score = (dist_norm * 100).round(1).rename("water_availability_score")

    result = grid_gdf[["id"]].copy()
    for s in [dist_water, water_proximity, water_score]:
        result = result.merge(s.reset_index(name=s.name), on="id", how="left")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 3. PUBLIC TRANSPORT
# ─────────────────────────────────────────────────────────────────────────────

def compute_transport_features(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    distance_to_bus_stop        : metres to nearest bus stop / stand
    distance_to_railway_station : metres to nearest railway station
    public_transport_score      : normalised composite (0–100)
    """
    bus_stops = _load_osm_features(
        "bus_stops",
        {"highway": ["bus_stop"], "amenity": ["bus_station"],
         "public_transport": ["stop_position", "platform"]}
    )

    railway_stations = _load_osm_features(
        "railway_stations",
        {"railway": ["station", "halt", "tram_stop"],
         "amenity": ["train_station"]}
    )

    dist_bus      = _distance_to_nearest(grid_gdf, bus_stops,          "distance_to_bus_stop")
    dist_rail     = _distance_to_nearest(grid_gdf, railway_stations,   "distance_to_railway_station")

    # Public transport score: closer to both = higher
    bus_norm  = 1 - (dist_bus  / dist_bus.max().clip(1)).clip(0, 1)
    rail_norm = 1 - (dist_rail / dist_rail.max().clip(1)).clip(0, 1)
    pt_score  = ((bus_norm * 0.6 + rail_norm * 0.4) * 100).round(1).rename("public_transport_score")

    result = grid_gdf[["id"]].copy()
    for s in [dist_bus, dist_rail, pt_score]:
        result = result.merge(s.reset_index(name=s.name), on="id", how="left")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 6
# ─────────────────────────────────────────────────────────────────────────────

def collect_infrastructure(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    logger.info("=== LAYER 6: Infrastructure ===")

    power     = compute_power_features(grid_gdf)
    water     = compute_water_features(grid_gdf)
    transport = compute_transport_features(grid_gdf)

    result = (
        grid_gdf[["id"]]
        .merge(power,     on="id", how="left")
        .merge(water,     on="id", how="left")
        .merge(transport, on="id", how="left")
    )

    logger.info(f"Layer 6 complete: {result.shape[1]} columns, {len(result):,} rows")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from utils.grid_generator import generate_h3_grid, get_india_boundary

    boundary = get_india_boundary()
    grid     = generate_h3_grid(boundary)
    infra_df = collect_infrastructure(grid)
    print(infra_df.head())

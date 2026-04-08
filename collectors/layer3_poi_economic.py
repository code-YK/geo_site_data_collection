"""
collectors/layer3_poi_economic.py
Collects ALL POI / Economic Activity columns from the dataset spec.

Source: OpenStreetMap (Geofabrik India PBF via osmium, or Overpass API)

Columns produced:
  poi_count_500m, poi_count_1km, poi_count_2km,
  competitor_count, complementary_business_count,
  restaurant_count, shop_count, hospital_count, school_count, bank_count,
  poi_diversity_score, footfall_proxy_score

Usage:
  Set BUSINESS_TYPE in your run config to filter competitor / complementary POIs.
  Example: BUSINESS_TYPE = "pharmacy"
           COMPLEMENTARY = {"hospital", "clinic", "doctor"}

Dependencies:
    pip install osmium geopandas pandas numpy shapely
"""

import logging

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

from config.settings import BUFFER_500M, BUFFER_1KM, BUFFER_2KM
from utils.osm_reader import get_pois

logger = logging.getLogger(__name__)

# ── Business type configuration ───────────────────────────────────────────────
# Override these at runtime to tailor competitor/complementary logic
BUSINESS_TYPE  = "restaurant"          # the business being evaluated
COMPETITOR_AMENITIES   = {"restaurant", "cafe", "fast_food", "food_court"}
COMPLEMENTARY_AMENITIES = {"supermarket", "grocery", "cinema", "bar", "pub", "hotel"}


# ─────────────────────────────────────────────────────────────────────────────
# Load POIs from OSM PBF (run once, cache to parquet)
# ─────────────────────────────────────────────────────────────────────────────

# Comprehensive OSM tags that map to each category
POI_TAG_MAP = {
    "restaurant": {"amenity": ["restaurant", "cafe", "fast_food", "food_court",
                               "bar", "pub", "biergarten"]},
    "shop":       {"shop": True},   # all shop=* tags
    "hospital":   {"amenity": ["hospital", "clinic", "doctors", "health_post",
                               "nursing_home", "dentist"]},
    "school":     {"amenity": ["school", "college", "university", "kindergarten",
                               "language_school", "library"]},
    "bank":       {"amenity": ["bank", "atm", "bureau_de_change",
                               "money_transfer", "microfinance"]},
}


def load_pois_from_pbf() -> gpd.GeoDataFrame:
    """
    Extract point-of-interest nodes and building centroids from India OSM PBF.
    Returns a GeoDataFrame with columns: [osm_id, amenity, shop, geometry, category]
    Caches result to data/interim/india_pois.parquet.
    """
    pois = get_pois()

    if pois is None or pois.empty:
        raise ValueError("No POIs extracted from PBF. Check the file is valid.")

    # Keep point geometries only (building polygons → centroids)
    pois = pois.copy()
    pois.loc[pois.geometry.geom_type != "Point", "geometry"] = (
        pois.loc[pois.geometry.geom_type != "Point", "geometry"].centroid
    )
    pois = pois[pois.geometry.geom_type == "Point"].to_crs("EPSG:4326")

    # Assign category label
    def _categorise(row):
        amenity = str(row.get("amenity", "") or "")
        shop    = str(row.get("shop", "")    or "")
        for cat, tags in POI_TAG_MAP.items():
            if "amenity" in tags and amenity in tags["amenity"]:
                return cat
            if "shop" in tags and tags["shop"] is True and shop not in ("", "None"):
                return "shop"
        return "other"

    pois["category"] = pois.apply(_categorise, axis=1)

    keep = ["osmid", "amenity", "shop", "name", "category", "geometry"]
    pois = pois[[c for c in keep if c in pois.columns]]

    return pois


# ─────────────────────────────────────────────────────────────────────────────
# Core spatial count function
# ─────────────────────────────────────────────────────────────────────────────

def count_pois_in_buffers(
    grid_gdf: gpd.GeoDataFrame,
    pois_gdf: gpd.GeoDataFrame,
    radii: dict[str, int],
    category_filter: set | None = None,
) -> pd.DataFrame:
    """
    For each grid cell centroid, count POIs within each radius (metres).
    radii: {"col_name": radius_m}
    category_filter: if set, count only POIs whose 'category' is in the set.
    """
    grid_m = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    pois_m = pois_gdf.to_crs("EPSG:32644")

    if category_filter:
        pois_m = pois_m[pois_m["category"].isin(category_filter)]

    results = {"id": grid_gdf["id"].values}

    for col_name, radius in radii.items():
        buffers = grid_m.copy()
        buffers["geometry"] = grid_m.geometry.centroid.buffer(radius)
        joined  = gpd.sjoin(pois_m[["geometry"]], buffers[["id", "geometry"]],
                            how="inner", predicate="within")
        counts  = joined.groupby("id").size()
        results[col_name] = grid_gdf["id"].map(counts).fillna(0).astype(int).values

    return pd.DataFrame(results)


# ─────────────────────────────────────────────────────────────────────────────
# Diversity score — Shannon entropy of POI categories within 1km
# ─────────────────────────────────────────────────────────────────────────────

def compute_poi_diversity(grid_gdf: gpd.GeoDataFrame, pois_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Shannon entropy of POI category distribution within 1km.
    Higher value = more diverse economic activity.
    """
    grid_m = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    pois_m = pois_gdf[["category", "geometry"]].to_crs("EPSG:32644")

    buffers = grid_m.copy()
    buffers["geometry"] = grid_m.geometry.centroid.buffer(BUFFER_1KM)
    buffers = buffers.to_crs("EPSG:4326")
    pois_m  = pois_m.to_crs("EPSG:4326")

    joined = gpd.sjoin(pois_m, buffers[["id", "geometry"]], how="inner", predicate="within")

    def shannon(group):
        counts = group["category"].value_counts()
        probs  = counts / counts.sum()
        return float(-np.sum(probs * np.log2(probs + 1e-9)))

    diversity = joined.groupby("id").apply(shannon).rename("poi_diversity_score")
    return grid_gdf["id"].map(diversity).fillna(0).round(3)


# ─────────────────────────────────────────────────────────────────────────────
# Footfall proxy — weighted sum of high-traffic POIs within 500m
# ─────────────────────────────────────────────────────────────────────────────

FOOTFALL_WEIGHTS = {
    "hospital":   5,
    "school":     4,
    "shop":       3,
    "restaurant": 2,
    "bank":       2,
    "other":      1,
}

def compute_footfall_proxy(grid_gdf: gpd.GeoDataFrame, pois_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Weighted count of POIs within 500m, weighted by expected footfall per category.
    Normalised 0–100.
    """
    grid_m = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    pois_m = pois_gdf[["category", "geometry"]].to_crs("EPSG:32644")

    buffers = grid_m.copy()
    buffers["geometry"] = grid_m.geometry.centroid.buffer(BUFFER_500M)
    buffers = buffers.to_crs("EPSG:4326")
    pois_joined = gpd.sjoin(pois_m.to_crs("EPSG:4326"),
                            buffers[["id", "geometry"]], how="inner", predicate="within")

    pois_joined["weight"] = pois_joined["category"].map(FOOTFALL_WEIGHTS).fillna(1)
    weighted = pois_joined.groupby("id")["weight"].sum().rename("footfall_raw")

    score = grid_gdf["id"].map(weighted).fillna(0)
    max_s = score.max()
    if max_s > 0:
        score = (score / max_s * 100).round(1)
    return score.rename("footfall_proxy_score")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 3
# ─────────────────────────────────────────────────────────────────────────────

def collect_poi_economic(
    grid_gdf: gpd.GeoDataFrame,
    competitor_amenities: set   = COMPETITOR_AMENITIES,
    complementary_amenities: set = COMPLEMENTARY_AMENITIES,
) -> pd.DataFrame:
    logger.info("=== LAYER 3: POI / Economic Activity ===")

    pois = load_pois_from_pbf()

    # 1. General POI counts at 3 radii
    general = count_pois_in_buffers(
        grid_gdf, pois,
        radii={
            "poi_count_500m": BUFFER_500M,
            "poi_count_1km":  BUFFER_1KM,
            "poi_count_2km":  BUFFER_2KM,
        }
    )

    # 2. Competitor count (same business type) within 2km
    competitor = count_pois_in_buffers(
        grid_gdf, pois,
        radii={"competitor_count": BUFFER_2KM},
        category_filter=competitor_amenities,
    )

    # 3. Complementary business count within 2km
    complementary = count_pois_in_buffers(
        grid_gdf, pois,
        radii={"complementary_business_count": BUFFER_2KM},
        category_filter=complementary_amenities,
    )

    # 4. Individual category counts (500m)
    restaurants = count_pois_in_buffers(
        grid_gdf, pois, radii={"restaurant_count": BUFFER_500M},
        category_filter={"restaurant"}
    )
    shops = count_pois_in_buffers(
        grid_gdf, pois, radii={"shop_count": BUFFER_500M},
        category_filter={"shop"}
    )
    hospitals = count_pois_in_buffers(
        grid_gdf, pois, radii={"hospital_count": BUFFER_1KM},
        category_filter={"hospital"}
    )
    schools = count_pois_in_buffers(
        grid_gdf, pois, radii={"school_count": BUFFER_1KM},
        category_filter={"school"}
    )
    banks = count_pois_in_buffers(
        grid_gdf, pois, radii={"bank_count": BUFFER_500M},
        category_filter={"bank"}
    )

    # 5. Diversity + footfall scores
    diversity = compute_poi_diversity(grid_gdf, pois).to_frame()
    diversity["id"] = grid_gdf["id"].values
    footfall  = compute_footfall_proxy(grid_gdf, pois).to_frame()
    footfall["id"] = grid_gdf["id"].values

    # Merge all
    dfs = [general, competitor, complementary,
           restaurants, shops, hospitals, schools, banks,
           diversity, footfall]

    result = grid_gdf[["id"]].copy()
    for df in dfs:
        result = result.merge(df, on="id", how="left")

    logger.info(f"Layer 3 complete: {result.shape[1]} columns, {len(result):,} rows")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from utils.grid_generator import generate_h3_grid, get_india_boundary

    boundary = get_india_boundary()
    grid     = generate_h3_grid(boundary)
    poi_df   = collect_poi_economic(grid)
    print(poi_df.head())
    print(poi_df.dtypes)

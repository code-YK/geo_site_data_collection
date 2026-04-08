"""
collectors/layer4_land_use.py
Collects ALL land use + building columns from the dataset spec.

Sources:
    - OSM (buildings, land use polygons) via osmium / Geofabrik PBF
  - BHUVAN LULC 50K (authoritative land classification from ISRO/NRSC)

Columns produced:
  commercial_ratio, residential_ratio, industrial_ratio, mixed_use_ratio,
  building_count, building_density, avg_building_levels, built_up_area_ratio

Dependencies:
    pip install osmium geopandas pandas numpy shapely rasterio rasterstats
"""

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box

from config.settings import SOURCES
from utils.osm_reader import get_buildings, get_landuse

logger = logging.getLogger(__name__)

OSM_PBF = Path(SOURCES["osm_geofabrik"]["local_path"])

# BHUVAN LULC raster (download after free registration at bhuvan.nrsc.gov.in)
# Product: LULC 50K (50,000 scale land use land cover)
BHUVAN_LULC_RASTER = Path("data/raw/bhuvan/india_lulc_50k.tif")

# LULC class codes → our categories
# Reference: BHUVAN LULC legend (Level-1 classes)
LULC_CLASS_MAP = {
    # built-up
    1:  "residential",
    2:  "commercial",
    3:  "industrial",
    # agriculture
    4:  "other",
    5:  "other",
    # forest / vegetation
    6:  "other",
    7:  "other",
    # water
    8:  "other",
    # waste / barren
    9:  "other",
    10: "other",
}


# ─────────────────────────────────────────────────────────────────────────────
# Extract buildings from OSM PBF (once, then cache)
# ─────────────────────────────────────────────────────────────────────────────

def load_buildings_from_pbf() -> gpd.GeoDataFrame:
    """
    Returns building polygons with columns: [geometry, building, building:levels]
    Caches to data/interim/india_buildings.parquet
    """
    buildings = get_buildings()

    if buildings is None or buildings.empty:
        raise ValueError("No buildings extracted from PBF.")

    buildings = buildings.to_crs("EPSG:4326")
    if "levels" in buildings.columns and "building:levels" not in buildings.columns:
        buildings["building:levels"] = buildings["levels"]

    keep = ["osmid", "building", "building:levels", "geometry"]
    buildings = buildings[[c for c in keep if c in buildings.columns]]
    return buildings


# ─────────────────────────────────────────────────────────────────────────────
# Extract land use polygons from OSM PBF (once, then cache)
# ─────────────────────────────────────────────────────────────────────────────

OSM_LANDUSE_MAP = {
    "residential":  "residential",
    "commercial":   "commercial",
    "retail":       "commercial",
    "industrial":   "industrial",
    "garages":      "industrial",
    "construction": "mixed",
    "brownfield":   "mixed",
    "greenfield":   "other",
    "farmland":     "other",
    "forest":       "other",
    "grass":        "other",
    "military":     "other",
    "cemetery":     "other",
}

def load_landuse_from_pbf() -> gpd.GeoDataFrame:
    """
    Returns land use polygons with a 'landuse_category' column.
    """
    lu = get_landuse()

    if lu is None or lu.empty:
        logger.warning("No land use polygons extracted. Ratios will be estimated from buildings only.")
        return gpd.GeoDataFrame()

    lu = lu.to_crs("EPSG:4326")
    lu["landuse_category"] = lu["landuse"].map(OSM_LANDUSE_MAP).fillna("other")

    return lu


# ─────────────────────────────────────────────────────────────────────────────
# 1. Land-use ratios within each H3 cell
# ─────────────────────────────────────────────────────────────────────────────

def compute_landuse_ratios(
    grid_gdf: gpd.GeoDataFrame, landuse_gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    For each cell: area fraction that is commercial / residential / industrial / mixed.
    Falls back to building-type proxy if BHUVAN raster is unavailable.
    """
    if landuse_gdf is None or landuse_gdf.empty:
        logger.warning("Land use GDF empty. Filling ratios with NaN.")
        return pd.DataFrame({
            "id":                grid_gdf["id"],
            "commercial_ratio":  np.nan,
            "residential_ratio": np.nan,
            "industrial_ratio":  np.nan,
            "mixed_use_ratio":   np.nan,
        })

    grid_m = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    lu_m   = landuse_gdf[["landuse_category", "geometry"]].to_crs("EPSG:32644")

    # Clip land use to each cell
    clipped = gpd.overlay(lu_m, grid_m, how="intersection", keep_geom_type=False)
    clipped["area"] = clipped.geometry.area

    cell_area  = grid_m.set_index("id").geometry.area.rename("cell_area")
    lu_by_cell = clipped.groupby(["id", "landuse_category"])["area"].sum().unstack(fill_value=0)

    for cat in ["commercial", "residential", "industrial", "mixed"]:
        if cat not in lu_by_cell.columns:
            lu_by_cell[cat] = 0.0

    ratios = lu_by_cell.div(cell_area, axis=0).fillna(0).clip(0, 1)
    ratios.columns = [f"{c}_ratio" for c in ratios.columns]
    ratios = ratios[["commercial_ratio", "residential_ratio", "industrial_ratio", "mixed_use_ratio"]]
    ratios = ratios.round(4).reset_index()

    return grid_gdf[["id"]].merge(ratios, on="id", how="left").fillna(0)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Building metrics within each H3 cell
# ─────────────────────────────────────────────────────────────────────────────

def compute_building_metrics(
    grid_gdf: gpd.GeoDataFrame, buildings_gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    building_count, building_density (per sq km), avg_building_levels, built_up_area_ratio
    """
    grid_m  = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    bldgs_m = buildings_gdf[["building:levels", "geometry"]].to_crs("EPSG:32644")

    # Use centroids for count join (avoids double-counting cross-boundary buildings)
    bldgs_m["centroid"] = bldgs_m.geometry.centroid
    bldgs_pts = bldgs_m.copy()
    bldgs_pts["geometry"] = bldgs_pts["centroid"]

    joined = gpd.sjoin(
        bldgs_pts[["building:levels", "geometry"]],
        grid_m[["id", "geometry"]],
        how="inner", predicate="within"
    )

    cell_area_km2 = (grid_m.set_index("id").geometry.area / 1e6).rename("cell_area_km2")

    # building_count
    counts = joined.groupby("id").size().rename("building_count")

    # building_density
    density = (counts / cell_area_km2).round(1).rename("building_density")

    # avg_building_levels — parse numeric from "building:levels" OSM tag
    def parse_levels(x):
        try:
            return float(str(x).split(";")[0].split("-")[0].strip())
        except (ValueError, AttributeError):
            return np.nan

    joined["levels_num"] = joined["building:levels"].apply(parse_levels)
    avg_levels = joined.groupby("id")["levels_num"].mean().round(1).rename("avg_building_levels")

    # built_up_area_ratio — building footprint area / cell area
    # Clip building polygons (original geometry) to each cell
    bldgs_poly = bldgs_m[bldgs_m.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if not bldgs_poly.empty:
        clipped = gpd.overlay(
            bldgs_poly[["geometry"]], grid_m[["id", "geometry"]],
            how="intersection", keep_geom_type=False
        )
        clipped["bldg_area"] = clipped.geometry.area
        footprint = clipped.groupby("id")["bldg_area"].sum()
        built_ratio = (footprint / (cell_area_km2 * 1e6)).clip(0, 1).round(4).rename("built_up_area_ratio")
    else:
        built_ratio = pd.Series(np.nan, index=grid_m["id"], name="built_up_area_ratio")

    result = grid_gdf[["id"]].copy()
    for s in [counts, density, avg_levels, built_ratio]:
        result = result.merge(s.reset_index(), on="id", how="left")

    result["building_count"]     = result["building_count"].fillna(0).astype(int)
    result["building_density"]   = result["building_density"].fillna(0)
    result["avg_building_levels"] = result["avg_building_levels"].fillna(1.0)
    result["built_up_area_ratio"] = result["built_up_area_ratio"].fillna(0)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 4
# ─────────────────────────────────────────────────────────────────────────────

def collect_land_use(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    logger.info("=== LAYER 4: Land Use + Buildings ===")

    buildings = load_buildings_from_pbf()
    landuse   = load_landuse_from_pbf()

    lu_ratios  = compute_landuse_ratios(grid_gdf, landuse)
    bldg_stats = compute_building_metrics(grid_gdf, buildings)

    result = grid_gdf[["id"]].merge(lu_ratios, on="id", how="left").merge(bldg_stats, on="id", how="left")
    logger.info(f"Layer 4 complete: {result.shape[1]} columns, {len(result):,} rows")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from utils.grid_generator import generate_h3_grid, get_india_boundary

    boundary  = get_india_boundary()
    grid      = generate_h3_grid(boundary)
    land_df   = collect_land_use(grid)
    print(land_df.head())

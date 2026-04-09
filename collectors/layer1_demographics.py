"""
collectors/layer1_demographics.py
Collects ALL demographic columns from the dataset spec.

Sources:
  - WorldPop 1km raster   → population counts (1km / 5km rings, density)
  - Census of India 2011  → sex ratio, age groups, literacy, household count
  - Income proxy          → nighttime lights (VIIRS) as income-level surrogate

Required data downloads (run once):
  1. WorldPop raster — NO LOGIN, paste directly in browser:
     https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km.tif
     Save as: data/raw/worldpop/ind_pd_2020_1km.tif

  2. Census district data — NO LOGIN:
     https://censusindia.gov.in/census.website/en/data/tables
     Download "Primary Census Abstract Data Tables — District Level (Excel)"
     Save as: data/raw/census/primary_census_abstract_2011.xlsx

  3. VIIRS nighttime lights (income proxy) — FREE REGISTRATION (instant):
     https://eogdata.mines.edu/products/vnl/
     OR no-login alternative (NASA Black Marble):
     https://blackmarble.gsfc.nasa.gov/
     Save as: data/raw/viirs/india_viirs_2022.tif

Dependencies:
    pip install rasterio rasterstats pandas geopandas numpy openpyxl
"""

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.crs import CRS
from rasterstats import zonal_stats
from shapely.geometry import Point

from config.settings import BUFFER_1KM, BUFFER_5KM

logger = logging.getLogger(__name__)

# ── File paths ────────────────────────────────────────────────────────────────
WORLDPOP_RASTER  = Path("data/raw/worldpop/ind_pd_2020_1km.tif")
CENSUS_XLSX      = Path("data/raw/census/primary_census_abstract_2011.xlsx")
VIIRS_RASTER     = Path("data/raw/viirs/india_viirs_2022.tif")


# ─────────────────────────────────────────────────────────────────────────────
# 1. WORLDPOP — population counts within buffer rings
# ─────────────────────────────────────────────────────────────────────────────

def extract_worldpop(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    For each grid centroid compute:
      population_1km  : sum of WorldPop cells within 1 km
      population_5km  : sum within 5 km
      population_density: pop_1km / (π × 1²)  [people/sq km]
    """
    if not WORLDPOP_RASTER.exists():
        raise FileNotFoundError(
            f"WorldPop raster not found at {WORLDPOP_RASTER}.\n"
            "Download (no login needed) — paste in browser:\n"
            "https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km.tif\n"
            "Save as: data/raw/worldpop/ind_pd_2020_1km.tif"
        )

    # Project to metric CRS for accurate buffer distances
    grid_proj = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")  # UTM Zone 44N (central India)

    results = []
    for radius, col in [(BUFFER_1KM, "population_1km"), (BUFFER_5KM, "population_5km")]:
        buffers = grid_proj.copy()
        buffers["geometry"] = grid_proj.geometry.buffer(radius)
        buffers = buffers.to_crs("EPSG:4326")

        stats = zonal_stats(
            buffers,
            str(WORLDPOP_RASTER),
            stats=["sum"],
            nodata=-99999,
            all_touched=True,
        )
        df = pd.DataFrame({"id": grid_gdf["id"], col: [s["sum"] or 0 for s in stats]})
        results.append(df.set_index("id"))

    combined = pd.concat(results, axis=1).reset_index()
    combined["population_density"] = (
        combined["population_1km"] / (np.pi * 1**2)
    ).round(1)

    logger.info(f"WorldPop extraction done: {len(combined):,} cells")
    return combined


# ─────────────────────────────────────────────────────────────────────────────
# 2. CENSUS OF INDIA — age/sex/literacy attributes
# ─────────────────────────────────────────────────────────────────────────────

def load_census_district(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Join Census 2011 district-level statistics to each grid cell via spatial join.

    Census columns used:
      TOT_P  → total population
      TOT_M  → male population
      TOT_F  → female population
      AGE_0_14 (derived)   → child_population
      AGE_15_64 (derived)  → working_age_population
      AGE_65_PLUS (derived) → elderly_population
      TOT_WORK_P → working_age proxy
      LITERACY_RATE
      NO_HH  → household_count

    NOTE: The Census abstract doesn't have single-year age bands at district level.
    Use the Census Age Data tables (C-13) for finer age breakdown.
    Download C-13: https://censusindia.gov.in/nada/index.php/catalog/42
    """
    if not CENSUS_XLSX.exists():
        logger.warning(
            f"Census file not found at {CENSUS_XLSX}. "
            "Demographic ratios will be NaN. "
            "Download Primary Census Abstract from censusindia.gov.in"
        )
        cols = [
            "male_population", "female_population", "sex_ratio",
            "child_population", "working_age_population", "elderly_population",
            "child_ratio", "working_age_ratio", "dependency_ratio",
            "household_count", "literacy_rate",
        ]
        return pd.DataFrame({"id": grid_gdf["id"], **{c: np.nan for c in cols}})

    logger.info("Loading Census 2011 district data ...")
    census = pd.read_excel(CENSUS_XLSX, skiprows=5)  # header row varies by download

    # Standardise column names (Census spreadsheet uses mixed naming)
    col_map = {
        "TOT_P":    "total_pop",
        "TOT_M":    "male_population",
        "TOT_F":    "female_population",
        "NO_HH":    "household_count",
        "TOT_WORK_P": "working_age_proxy",
        "LITERATES_PERSONS": "literates",
        "State":    "state",
        "District": "district",
    }
    census.rename(columns=col_map, inplace=True)

    # Derived fields
    census["sex_ratio"]   = (census["female_population"] / census["male_population"] * 1000).round(0)
    census["literacy_rate"] = (census["literates"] / census["total_pop"] * 100).round(2)

    # Age groups — load from C-13 age table if available, else proxy from total
    # Here we use national age distribution proxy (Census 2011):
    #   0-14: 28.5%,  15-64: 64.9%,  65+: 6.6%
    census["child_population"]      = (census["total_pop"] * 0.285).round(0)
    census["working_age_population"] = (census["total_pop"] * 0.649).round(0)
    census["elderly_population"]     = (census["total_pop"] * 0.066).round(0)

    census["child_ratio"]       = 0.285
    census["working_age_ratio"] = 0.649
    census["dependency_ratio"]  = ((census["child_population"] + census["elderly_population"])
                                   / census["working_age_population"]).round(3)

    # Load district boundaries (GADM level 2)
    district_shp = Path("data/raw/boundaries/gadm41_IND_2.shp")
    if not district_shp.exists():
        logger.warning(
            "District shapefile not found. Download from https://gadm.org/download_country.html "
            "(India, level 2). Skipping spatial join — census values will be NaN."
        )
        cols_keep = [
            "male_population", "female_population", "sex_ratio",
            "child_population", "working_age_population", "elderly_population",
            "child_ratio", "working_age_ratio", "dependency_ratio",
            "household_count", "literacy_rate",
        ]
        return pd.DataFrame({"id": grid_gdf["id"], **{c: np.nan for c in cols_keep}})

    districts = gpd.read_file(district_shp).to_crs("EPSG:4326")
    districts["district_join"] = districts["NAME_2"].str.upper().str.strip()
    census["district_join"]    = census["district"].str.upper().str.strip()

    districts = districts.merge(
        census[["district_join", "state", "male_population", "female_population",
                "sex_ratio", "child_population", "working_age_population",
                "elderly_population", "child_ratio", "working_age_ratio",
                "dependency_ratio", "household_count", "literacy_rate"]],
        on="district_join", how="left",
    )

    # Spatial join: each H3 centroid → district polygon
    centroids = grid_gdf[["id", "latitude", "longitude"]].copy()
    centroids["geometry"] = gpd.points_from_xy(centroids["longitude"], centroids["latitude"])
    centroids_gdf = gpd.GeoDataFrame(centroids, crs="EPSG:4326")

    joined = gpd.sjoin(centroids_gdf, districts, how="left", predicate="within")

    keep_cols = ["id", "male_population", "female_population", "sex_ratio",
                 "child_population", "working_age_population", "elderly_population",
                 "child_ratio", "working_age_ratio", "dependency_ratio",
                 "household_count", "literacy_rate"]
    result = joined[keep_cols].drop_duplicates("id")

    logger.info(f"Census join done: {len(result):,} cells")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 3. VIIRS NIGHTTIME LIGHTS — income proxy
# ─────────────────────────────────────────────────────────────────────────────

def extract_income_proxy(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Extract mean VIIRS radiance within each H3 cell as income_level proxy.
    Higher nighttime light intensity correlates with higher economic activity.

    VIIRS Annual VNL V2 download:
    https://eogdata.mines.edu/products/vnl/#annual_v2
    → Select India bounding box or download global and clip.
    """
    if not VIIRS_RASTER.exists():
        logger.warning(
            f"VIIRS raster not found at {VIIRS_RASTER}. income_level will be NaN.\n"
            "Download from https://eogdata.mines.edu/products/vnl/"
        )
        return pd.DataFrame({"id": grid_gdf["id"], "income_level": np.nan})

    grid_proj = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    grid_wgs  = grid_proj.to_crs("EPSG:4326")

    stats = zonal_stats(
        grid_wgs,
        str(VIIRS_RASTER),
        stats=["mean"],
        nodata=0,
    )
    income = pd.DataFrame({
        "id":           grid_gdf["id"],
        "income_level": [round(s["mean"] or 0, 4) for s in stats],
    })
    logger.info("VIIRS income proxy extraction done.")
    return income


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 1
# ─────────────────────────────────────────────────────────────────────────────

def collect_demographics(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Runs all three sub-collectors and merges into a single DataFrame
    with all Layer 1 columns from the spec.
    """
    logger.info("=== LAYER 1: Demographics ===")

    wp     = extract_worldpop(grid_gdf)
    census = load_census_district(grid_gdf)
    viirs  = extract_income_proxy(grid_gdf)

    result = (
        grid_gdf[["id"]]
        .merge(wp,     on="id", how="left")
        .merge(census, on="id", how="left")
        .merge(viirs,  on="id", how="left")
    )

    logger.info(f"Layer 1 complete: {result.shape[1]} columns, {len(result):,} rows")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from utils.grid_generator import generate_h3_grid, get_india_boundary

    boundary = get_india_boundary()
    grid     = generate_h3_grid(boundary)
    demo_df  = collect_demographics(grid)
    print(demo_df.head())
    print(demo_df.dtypes)
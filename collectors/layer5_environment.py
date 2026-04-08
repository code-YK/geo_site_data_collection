"""
collectors/layer5_environment.py
Collects ALL environment / risk columns from the dataset spec.

Sources:
  - OpenAQ v3 API         → aqi, pm25, pm10 (latest station readings + interpolation)
  - NDMA / BHUVAN raster  → flood_risk_score, earthquake_risk_score
  - OSM / BHUVAN          → green_space_ratio
  - NASA POWER API        → temperature (climatological average)

Columns produced:
  aqi, pm25, pm10, flood_risk_score, earthquake_risk_score,
  green_space_ratio, temperature

Dependencies:
    pip install requests geopandas pandas numpy rasterio rasterstats scipy
"""

import logging
import os
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from scipy.interpolate import griddata

from config.settings import BUFFER_1KM, BUFFER_2KM, SOURCES
from utils.osm_reader import get_landuse

logger = logging.getLogger(__name__)

OPENAQ_KEY  = os.getenv(SOURCES["openaq"]["api_key_env"], "")
OPENAQ_BASE = SOURCES["openaq"]["endpoint"]
NASA_POWER  = SOURCES["nasa_power"]["endpoint"]

# Raster paths (download separately — see docstring)
FLOOD_RASTER      = Path("data/raw/risk/india_flood_hazard.tif")
EARTHQUAKE_RASTER = Path("data/raw/risk/india_seismic_zone.tif")
# Green space raster from BHUVAN (vegetation / parks layer)
BHUVAN_VEG_RASTER = Path("data/raw/bhuvan/india_vegetation.tif")


# ─────────────────────────────────────────────────────────────────────────────
# 1. AIR QUALITY — OpenAQ v3 + IDW interpolation to grid
# ─────────────────────────────────────────────────────────────────────────────

def fetch_openaq_stations(country: str = "IN") -> pd.DataFrame:
    """
    Fetch all OpenAQ stations in India with latest PM2.5, PM10 readings.
    Free API key: https://openaq.org/#/register
    Rate limit: 60 req/min (free tier).

    Returns DataFrame: [station_id, lat, lon, pm25, pm10, aqi_estimate]
    """
    cache = Path("data/interim/openaq_stations_india.parquet")
    if cache.exists():
        age_hours = (time.time() - cache.stat().st_mtime) / 3600
        if age_hours < 24:
            logger.info(f"Using cached OpenAQ data ({age_hours:.1f}h old)")
            return pd.read_parquet(cache)

    headers = {"X-API-Key": OPENAQ_KEY} if OPENAQ_KEY else {}
    stations_url = f"{OPENAQ_BASE}locations?country={country}&limit=1000&page=1"

    logger.info("Fetching OpenAQ station list for India ...")
    try:
        resp = requests.get(stations_url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"OpenAQ station fetch failed: {e}. AQI columns will be NaN.")
        return pd.DataFrame(columns=["station_id", "lat", "lon", "pm25", "pm10", "aqi_estimate"])

    records = []
    for loc in data.get("results", []):
        coord = loc.get("coordinates", {})
        if not coord:
            continue
        entry = {
            "station_id": loc["id"],
            "lat": coord.get("latitude"),
            "lon": coord.get("longitude"),
            "pm25": None, "pm10": None,
        }
        for param in loc.get("parameters", []):
            if param.get("parameter") == "pm25":
                entry["pm25"] = param.get("lastValue")
            elif param.get("parameter") == "pm10":
                entry["pm10"] = param.get("lastValue")
        records.append(entry)

    df = pd.DataFrame(records).dropna(subset=["lat", "lon"])
    df["pm25"] = pd.to_numeric(df["pm25"], errors="coerce")
    df["pm10"] = pd.to_numeric(df["pm10"], errors="coerce")

    # AQI estimate (India NAQI formula — simplified linear for PM2.5)
    # Full formula: https://cpcb.nic.in/displaypdf.php?id=bmFxaS1yZXBvcnQucGRm
    def pm25_to_aqi(pm):
        if pd.isna(pm):
            return np.nan
        breakpoints = [
            (0, 30, 0, 50), (30, 60, 51, 100), (60, 90, 101, 200),
            (90, 120, 201, 300), (120, 250, 301, 400), (250, 500, 401, 500),
        ]
        for bpl, bph, al, ah in breakpoints:
            if bpl <= pm <= bph:
                return round(((ah - al) / (bph - bpl)) * (pm - bpl) + al)
        return 500

    df["aqi_estimate"] = df["pm25"].apply(pm25_to_aqi)

    cache.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache)
    logger.info(f"OpenAQ: {len(df)} stations cached.")
    return df


def interpolate_aq_to_grid(
    grid_gdf: gpd.GeoDataFrame, station_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Inverse-distance weighted (IDW) interpolation of station AQ values to grid centroids.
    """
    if station_df.empty or station_df[["pm25", "pm10"]].isna().all().all():
        return pd.DataFrame({
            "id": grid_gdf["id"], "pm25": np.nan,
            "pm10": np.nan, "aqi": np.nan
        })

    stations = station_df.dropna(subset=["lat", "lon"]).copy()
    grid_pts  = np.column_stack([grid_gdf["latitude"].values, grid_gdf["longitude"].values])
    stn_pts   = np.column_stack([stations["lat"].values, stations["lon"].values])

    result = grid_gdf[["id"]].copy()
    for col, out_col in [("pm25", "pm25"), ("pm10", "pm10"), ("aqi_estimate", "aqi")]:
        vals = stations[col].fillna(stations[col].median())
        interpolated = griddata(stn_pts, vals.values, grid_pts, method="linear", fill_value=np.nan)
        # Fallback: nearest for cells outside convex hull
        nearest = griddata(stn_pts, vals.values, grid_pts, method="nearest")
        mask = np.isnan(interpolated)
        interpolated[mask] = nearest[mask]
        result[out_col] = interpolated.round(2)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 2. FLOOD RISK — from NDMA raster or BHUVAN flood hazard atlas
# ─────────────────────────────────────────────────────────────────────────────

def compute_flood_risk(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Extract mean flood hazard score within each H3 cell from raster.
    Returns 0–1 score (0 = safe, 1 = high risk).

    Download:
        Flood Hazard Atlas of India (NDMA):
        https://ndma.gov.in/Resources/ndma-pdf/maps/Flood_Hazard_Atlas.pdf
        For raster: BHUVAN WMS layer "Flood_Hazard"
        https://bhuvan-vec2.nrsc.gov.in/bhuvan/wms?SERVICE=WMS&VERSION=1.1.1&
            REQUEST=GetMap&LAYERS=india_flood_hazard&BBOX=...

    Seismic zone raster:
        NDMA Seismic Zonation Map → digitise as raster or download from:
        https://bhukosh.gsi.gov.in  (GSI Bhukosh portal — seismic zonation layer)
    """
    from rasterstats import zonal_stats

    if not FLOOD_RASTER.exists():
        logger.warning(
            f"Flood raster not found at {FLOOD_RASTER}. flood_risk_score will be NaN.\n"
            "Download BHUVAN flood hazard layer or NDMA atlas raster."
        )
        return pd.Series(np.nan, index=grid_gdf["id"], name="flood_risk_score")

    stats = zonal_stats(grid_gdf, str(FLOOD_RASTER), stats=["mean"], nodata=0)
    scores = np.array([s["mean"] or 0 for s in stats])
    max_s  = scores.max()
    if max_s > 0:
        scores = scores / max_s
    return pd.Series(scores.round(3), index=grid_gdf["id"], name="flood_risk_score")


def compute_earthquake_risk(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Extract seismic zone value within each H3 cell.
    India zones: II (low), III (moderate), IV (high), V (very high).
    Normalised to 0–1.

    Download raster from GSI Bhukosh: https://bhukosh.gsi.gov.in
    """
    from rasterstats import zonal_stats

    if not EARTHQUAKE_RASTER.exists():
        logger.warning(
            f"Earthquake raster not found at {EARTHQUAKE_RASTER}. earthquake_risk_score = NaN.\n"
            "Download seismic zone raster from GSI Bhukosh (bhukosh.gsi.gov.in)."
        )
        return pd.Series(np.nan, index=grid_gdf["id"], name="earthquake_risk_score")

    # Zone values 2–5 → normalise to 0–1
    stats  = zonal_stats(grid_gdf, str(EARTHQUAKE_RASTER), stats=["mean"], nodata=0)
    zones  = np.array([s["mean"] or 2 for s in stats])
    scores = (zones - 2) / 3  # Zone II=0.0, III=0.33, IV=0.67, V=1.0
    return pd.Series(scores.clip(0, 1).round(3), index=grid_gdf["id"], name="earthquake_risk_score")


# ─────────────────────────────────────────────────────────────────────────────
# 3. GREEN SPACE RATIO — parks + vegetation within H3 cell
# ─────────────────────────────────────────────────────────────────────────────

def compute_green_space_ratio(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Fraction of H3 cell covered by parks / vegetation / forest.
    Uses OSM leisure=park / landuse=forest layers or BHUVAN vegetation raster.
    """
    try:
        lu = get_landuse()
        if lu is None or lu.empty:
            green = gpd.GeoDataFrame()
        else:
            wanted = {"park", "nature_reserve", "garden", "forest", "grass", "meadow", "orchard"}
            green = lu[lu["landuse"].isin(wanted)].copy() if "landuse" in lu.columns else gpd.GeoDataFrame()
    except Exception as e:
        logger.warning(f"Green space extraction failed: {e}")
        green = gpd.GeoDataFrame()

    if green.empty:
        logger.warning("No green space data. green_space_ratio = 0.")
        return pd.Series(0.0, index=grid_gdf["id"], name="green_space_ratio")

    grid_m  = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    green_m = green.to_crs("EPSG:32644")
    cell_area = grid_m.set_index("id").geometry.area

    clipped = gpd.overlay(green_m[["geometry"]], grid_m[["id", "geometry"]],
                          how="intersection", keep_geom_type=False)
    clipped["green_area"] = clipped.geometry.area
    green_per_cell = clipped.groupby("id")["green_area"].sum()

    ratio = (green_per_cell / cell_area).clip(0, 1).fillna(0).round(4)
    return grid_gdf["id"].map(ratio).fillna(0).rename("green_space_ratio")


# ─────────────────────────────────────────────────────────────────────────────
# 4. TEMPERATURE — NASA POWER climatological average (2m air temperature)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_nasa_temperature(grid_gdf: gpd.GeoDataFrame, batch_size: int = 200) -> pd.Series:
    """
    Fetches 20-year climatological mean 2m temperature (°C) from NASA POWER API.
    Batched by unique 0.5° grid tiles (NASA POWER is 0.5° resolution).

    NASA POWER API docs: https://power.larc.nasa.gov/docs/services/api/
    """
    cache = Path("data/interim/nasa_temperature_india.parquet")
    if cache.exists():
        logger.info("Using cached NASA POWER temperature data")
        return pd.read_parquet(cache).set_index("id")["temperature"]

    logger.info("Fetching NASA POWER temperature (batching by 0.5° tiles) ...")

    # Round to 0.5° resolution (POWER native) to deduplicate API calls
    grid_gdf = grid_gdf.copy()
    grid_gdf["lat_tile"] = (grid_gdf["latitude"]  / 0.5).round(0) * 0.5
    grid_gdf["lon_tile"] = (grid_gdf["longitude"] / 0.5).round(0) * 0.5

    tile_map = {}
    unique_tiles = grid_gdf[["lat_tile", "lon_tile"]].drop_duplicates()
    logger.info(f"Unique 0.5° tiles: {len(unique_tiles)}")

    for _, tile in unique_tiles.iterrows():
        lat, lon = tile["lat_tile"], tile["lon_tile"]
        key = (round(lat, 1), round(lon, 1))
        try:
            url = (
                f"{NASA_POWER}?parameters=T2M&community=RE"
                f"&longitude={lon}&latitude={lat}&format=JSON"
            )
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            annual_mean = data["properties"]["parameter"]["T2M"].get("ANN", np.nan)
            tile_map[key] = float(annual_mean) if annual_mean != -999 else np.nan
        except Exception as e:
            logger.debug(f"NASA POWER tile ({lat},{lon}) failed: {e}")
            tile_map[key] = np.nan
        time.sleep(0.05)

    grid_gdf["temp_key"] = list(
        zip(grid_gdf["lat_tile"].round(1), grid_gdf["lon_tile"].round(1))
    )
    grid_gdf["temperature"] = grid_gdf["temp_key"].map(tile_map)

    result = grid_gdf[["id", "temperature"]].copy()
    result.to_parquet(cache)
    return result.set_index("id")["temperature"]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 5
# ─────────────────────────────────────────────────────────────────────────────

def collect_environment(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    logger.info("=== LAYER 5: Environment / Risk ===")

    stations = fetch_openaq_stations()
    aq_df    = interpolate_aq_to_grid(grid_gdf, stations)

    flood_risk   = compute_flood_risk(grid_gdf).reset_index(name="flood_risk_score")
    quake_risk   = compute_earthquake_risk(grid_gdf).reset_index(name="earthquake_risk_score")
    green_ratio  = compute_green_space_ratio(grid_gdf).reset_index(name="green_space_ratio")
    temperature  = fetch_nasa_temperature(grid_gdf).reset_index(name="temperature")

    result = (
        grid_gdf[["id"]]
        .merge(aq_df,       on="id", how="left")
        .merge(flood_risk,  on="id", how="left")
        .merge(quake_risk,  on="id", how="left")
        .merge(green_ratio, on="id", how="left")
        .merge(temperature, on="id", how="left")
    )

    logger.info(f"Layer 5 complete: {result.shape[1]} columns, {len(result):,} rows")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from utils.grid_generator import generate_h3_grid, get_india_boundary

    boundary = get_india_boundary()
    grid     = generate_h3_grid(boundary)
    env_df   = collect_environment(grid)
    print(env_df.head())

"""
collectors/layer5_environment.py
Collects ALL environment / risk columns from the dataset spec.

Sources:
  - OpenAQ v3 API    → aqi, pm25, pm10
  - GDACS API        → flood_risk_score, earthquake_risk_score  ← NO LOGIN, NO FILE DOWNLOAD
  - OSM (via cache)  → green_space_ratio
  - NASA POWER API   → temperature

GDACS (Global Disaster Alert and Coordination System) is run by the UN +
European Commission. Free REST API, no key, no registration.
Install: pip install gdacs-api

Columns produced:
  aqi, pm25, pm10, flood_risk_score, earthquake_risk_score,
  green_space_ratio, temperature
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
from shapely.geometry import Point

from config.settings import BUFFER_1KM, SOURCES

logger = logging.getLogger(__name__)

OPENAQ_KEY  = os.getenv(SOURCES["openaq"]["api_key_env"], "")
OPENAQ_BASE = SOURCES["openaq"]["endpoint"]
NASA_POWER  = SOURCES["nasa_power"]["endpoint"]
GDACS_CFG   = SOURCES["gdacs"]

# India bounding box for GDACS queries
INDIA_BBOX = SOURCES.get("gdacs", {}).get("india_bbox", "6.5,68.1,37.6,97.4")


# ─────────────────────────────────────────────────────────────────────────────
# 1. AIR QUALITY — OpenAQ v3 + IDW interpolation to grid
# ─────────────────────────────────────────────────────────────────────────────

def fetch_openaq_stations(country: str = "IN") -> pd.DataFrame:
    """
    Fetch all OpenAQ stations in India with latest PM2.5, PM10 readings.
    Free API key: https://openaq.org/register  (instant after email confirm)
    Rate limit: 60 req/min (free tier).
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
    """IDW interpolation of station AQ values to grid centroids."""
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
        nearest      = griddata(stn_pts, vals.values, grid_pts, method="nearest")
        mask = np.isnan(interpolated)
        interpolated[mask] = nearest[mask]
        result[out_col] = interpolated.round(2)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 2. DISASTER RISK — GDACS API (flood + earthquake)
#    No login. No file download. Free UN/EC API.
#    pip install gdacs-api
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_gdacs_events(event_type: str, page_size: int = 100) -> pd.DataFrame:
    """
    Fetch all historical GDACS events of a given type within India's bounding box.
    Paginates automatically (API returns max 100 per page).

    event_type: "FL" = flood, "EQ" = earthquake, "TC" = cyclone,
                "WF" = wildfire, "DR" = drought
    Returns DataFrame with columns: [eventid, lat, lon, alertlevel, severity, fromdate]
    """
    cache = Path(f"data/interim/gdacs_{event_type.lower()}_india.parquet")
    if cache.exists():
        age_days = (time.time() - cache.stat().st_mtime) / 86400
        if age_days < 7:
            logger.info(f"Using cached GDACS {event_type} events ({age_days:.1f} days old)")
            return pd.read_parquet(cache)

    base_url = GDACS_CFG["endpoint"]
    min_lat, min_lon, max_lat, max_lon = INDIA_BBOX.split(",")

    all_records = []
    page = 1

    logger.info(f"Fetching GDACS {event_type} events for India (paginating) ...")
    while True:
        params = {
            "eventtype":  event_type,
            "fromdate":   "2000-01-01",
            "todate":     "2025-12-31",
            "bbox":       f"{min_lon},{min_lat},{max_lon},{max_lat}",
            "pagesize":   page_size,
            "pagenumber": page,
        }
        try:
            resp = requests.get(base_url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"GDACS {event_type} page {page} failed: {e}")
            break

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            props = feat.get("properties", {})
            geom  = feat.get("geometry", {})
            coords = geom.get("coordinates", [None, None])
            all_records.append({
                "eventid":    props.get("eventid"),
                "event_type": event_type,
                "lon":        coords[0],
                "lat":        coords[1],
                "alertlevel": props.get("alertlevel", "Green"),
                "severity":   props.get("severitydata", {}).get("severity", 0),
                "fromdate":   props.get("fromdate"),
            })

        logger.info(f"  GDACS {event_type}: page {page}, {len(features)} events")
        if len(features) < page_size:
            break
        page += 1
        time.sleep(0.3)

    df = pd.DataFrame(all_records)
    if df.empty:
        logger.warning(f"No GDACS {event_type} events found for India bbox.")
        return df

    df["severity"] = pd.to_numeric(df["severity"], errors="coerce").fillna(0)
    df["alert_weight"] = df["alertlevel"].map({"Green": 1, "Orange": 3, "Red": 5}).fillna(1)

    cache.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache)
    logger.info(f"GDACS {event_type}: {len(df)} total events cached → {cache}")
    return df


def _gdacs_events_to_grid_score(
    grid_gdf: gpd.GeoDataFrame,
    events_df: pd.DataFrame,
    radius_m: float = 50_000,
    col_name: str = "risk_score",
) -> pd.Series:
    """
    For each H3 cell: count weighted GDACS events within radius_m metres.
    Weight = alert_weight (Red=5, Orange=3, Green=1) × severity.
    Normalise result to 0–1.

    radius_m = 50km default — captures district-level disaster footprint.
    """
    if events_df.empty or events_df[["lat", "lon"]].isna().all().all():
        logger.warning(f"No events data for {col_name}. Returning 0.")
        return pd.Series(0.0, index=grid_gdf["id"], name=col_name)

    events_clean = events_df.dropna(subset=["lat", "lon"]).copy()
    events_gdf = gpd.GeoDataFrame(
        events_clean,
        geometry=gpd.points_from_xy(events_clean["lon"], events_clean["lat"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:32644")

    grid_m = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    buffers = grid_m.copy()
    buffers["geometry"] = grid_m.geometry.centroid.buffer(radius_m)
    buffers = buffers.to_crs("EPSG:4326")
    events_gdf = events_gdf.to_crs("EPSG:4326")

    joined = gpd.sjoin(
        events_gdf[["alert_weight", "severity", "geometry"]],
        buffers[["id", "geometry"]],
        how="inner", predicate="within",
    )
    joined["weighted"] = joined["alert_weight"] * joined["severity"].clip(lower=1)
    agg = joined.groupby("id")["weighted"].sum()

    score = grid_gdf["id"].map(agg).fillna(0)
    max_s = score.max()
    if max_s > 0:
        score = score / max_s
    return score.round(4).rename(col_name)


def compute_flood_risk(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Flood risk score (0–1) from GDACS historical flood events.
    Source: GDACS FL events within 50km of each H3 cell, weighted by alert level.
    No login. No file download. Fully API-driven.
    """
    logger.info("Computing flood risk from GDACS FL events ...")
    events = _fetch_gdacs_events("FL")
    return _gdacs_events_to_grid_score(grid_gdf, events, col_name="flood_risk_score")


def compute_earthquake_risk(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Earthquake risk score (0–1) from GDACS historical earthquake events.
    Source: GDACS EQ events within 50km of each H3 cell, weighted by alert level.
    No login. No file download. Fully API-driven.
    """
    logger.info("Computing earthquake risk from GDACS EQ events ...")
    events = _fetch_gdacs_events("EQ")
    return _gdacs_events_to_grid_score(grid_gdf, events, col_name="earthquake_risk_score")


# ─────────────────────────────────────────────────────────────────────────────
# 3. GREEN SPACE RATIO — OSM parks + vegetation within H3 cell
# ─────────────────────────────────────────────────────────────────────────────

def compute_green_space_ratio(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Fraction of H3 cell covered by parks / vegetation / forest.
    Uses OSM leisure=park / landuse=forest via osm_reader (already cached from Layer 4).
    """
    from utils.osm_reader import get_features

    try:
        green = get_features(
            tag_filters={
                "leisure": ["park", "nature_reserve", "garden", "recreation_ground"],
                "landuse": ["forest", "grass", "meadow", "orchard", "greenfield"],
                "natural": ["wood", "scrub", "heath"],
            },
            cache_name="greenspace",
            include_ways=True,
        )
    except Exception as e:
        logger.warning(f"Green space extraction failed: {e}")
        return pd.Series(0.0, index=grid_gdf["id"], name="green_space_ratio")

    if green.empty:
        return pd.Series(0.0, index=grid_gdf["id"], name="green_space_ratio")

    grid_m  = grid_gdf[["id", "geometry"]].to_crs("EPSG:32644")
    green_m = green.to_crs("EPSG:32644")
    buffers = grid_m.copy()
    buffers["geometry"] = grid_m.geometry.centroid.buffer(BUFFER_1KM)
    buffers = buffers.to_crs("EPSG:4326")

    joined = gpd.sjoin(
        green_m[["geometry"]].to_crs("EPSG:4326"),
        buffers[["id", "geometry"]], how="inner", predicate="within"
    )
    counts = joined.groupby("id").size()
    ratio  = (grid_gdf["id"].map(counts).fillna(0) / 20).clip(0, 0.5).round(4)
    return ratio.rename("green_space_ratio")


# ─────────────────────────────────────────────────────────────────────────────
# 4. TEMPERATURE — NASA POWER climatological average (2m air temp)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_nasa_temperature(grid_gdf: gpd.GeoDataFrame) -> pd.Series:
    """
    Fetches climatological mean 2m temperature (°C) from NASA POWER API.
    Batched by 0.5° grid tiles to minimise API calls.
    No login required. Docs: https://power.larc.nasa.gov/docs/services/api/
    """
    cache = Path("data/interim/nasa_temperature_india.parquet")
    if cache.exists():
        logger.info("Using cached NASA POWER temperature data")
        return pd.read_parquet(cache).set_index("id")["temperature"]

    logger.info("Fetching NASA POWER temperature (batching by 0.5° tiles) ...")

    grid_gdf = grid_gdf.copy()
    grid_gdf["lat_tile"] = (grid_gdf["latitude"]  / 0.5).round(0) * 0.5
    grid_gdf["lon_tile"] = (grid_gdf["longitude"] / 0.5).round(0) * 0.5

    tile_map = {}
    unique_tiles = grid_gdf[["lat_tile", "lon_tile"]].drop_duplicates()
    logger.info(f"Unique 0.5° tiles to fetch: {len(unique_tiles)}")

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

    grid_gdf["temp_key"]    = list(zip(grid_gdf["lat_tile"].round(1), grid_gdf["lon_tile"].round(1)))
    grid_gdf["temperature"] = grid_gdf["temp_key"].map(tile_map)

    result = grid_gdf[["id", "temperature"]].copy()
    result.to_parquet(cache)
    return result.set_index("id")["temperature"]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — assemble Layer 5
# ─────────────────────────────────────────────────────────────────────────────

def collect_environment(grid_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    logger.info("=== LAYER 5: Environment / Risk ===")

    stations    = fetch_openaq_stations()
    aq_df       = interpolate_aq_to_grid(grid_gdf, stations)
    flood_risk  = compute_flood_risk(grid_gdf).reset_index(name="flood_risk_score")
    quake_risk  = compute_earthquake_risk(grid_gdf).reset_index(name="earthquake_risk_score")
    green_ratio = compute_green_space_ratio(grid_gdf).reset_index(name="green_space_ratio")
    temperature = fetch_nasa_temperature(grid_gdf).reset_index(name="temperature")

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
    print(env_df[["flood_risk_score", "earthquake_risk_score", "aqi"]].describe())
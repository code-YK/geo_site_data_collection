"""
run_pipeline.py
═══════════════════════════════════════════════════════════════
India Site Feasibility Scoring Pipeline — Main Orchestrator
Each row = one H3 resolution-7 cell (~5 sq km) across all India
═══════════════════════════════════════════════════════════════

Usage:
    # Full pipeline
    python run_pipeline.py

    # Skip already-completed layers (checkpoint resume)
    python run_pipeline.py --resume

    # Run a specific layer only
    python run_pipeline.py --layers 1 3 5

    # Custom business type (affects competitor / complementary logic)
    python run_pipeline.py --business pharmacy

    # Output formats
    python run_pipeline.py --output parquet csv geojson

    # Dry run (grid only, no data collection)
    python run_pipeline.py --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd

from config.settings import OUTPUT_DIR, PARQUET_FILE, CSV_FILE, GEOJSON_FILE
from utils.grid_generator import generate_h3_grid, get_india_boundary, save_grid
from collectors.layer1_demographics  import collect_demographics
from collectors.layer2_transportation import collect_transportation
from collectors.layer3_poi_economic  import collect_poi_economic
from collectors.layer4_land_use      import collect_land_use
from collectors.layer5_environment   import collect_environment
from collectors.layer6_infrastructure import collect_infrastructure
from processors.scoring_engine       import compute_all_scores

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger("pipeline")

CHECKPOINT_DIR = Path("data/checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint helpers
# ─────────────────────────────────────────────────────────────────────────────

def _checkpoint_path(layer: int) -> Path:
    return CHECKPOINT_DIR / f"layer{layer}.parquet"


def _save_checkpoint(df: pd.DataFrame, layer: int):
    path = _checkpoint_path(layer)
    df.to_parquet(path, index=False)
    logger.info(f"Checkpoint saved → {path}")


def _load_checkpoint(layer: int) -> pd.DataFrame | None:
    path = _checkpoint_path(layer)
    if path.exists():
        logger.info(f"Resuming from checkpoint: {path}")
        return pd.read_parquet(path)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Layer runner with timing + checkpoint
# ─────────────────────────────────────────────────────────────────────────────

def run_layer(
    layer_num: int,
    layer_fn,
    grid_gdf: gpd.GeoDataFrame,
    resume: bool,
    **kwargs,
) -> pd.DataFrame:
    if resume:
        cached = _load_checkpoint(layer_num)
        if cached is not None:
            return cached

    t0 = time.time()
    result = layer_fn(grid_gdf, **kwargs)
    elapsed = time.time() - t0
    logger.info(f"Layer {layer_num} completed in {elapsed/60:.1f} min")

    _save_checkpoint(result, layer_num)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Output writers
# ─────────────────────────────────────────────────────────────────────────────

def write_outputs(
    scored_df: pd.DataFrame,
    grid_gdf: gpd.GeoDataFrame,
    formats: list[str],
):
    out = Path(OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    if "parquet" in formats:
        p = out / PARQUET_FILE
        scored_df.to_parquet(p, index=False)
        logger.info(f"Output → {p}  ({p.stat().st_size/1e6:.1f} MB)")

    if "csv" in formats:
        p = out / CSV_FILE
        scored_df.to_csv(p, index=False)
        logger.info(f"Output → {p}")

    if "geojson" in formats:
        # Merge scores back with H3 geometries
        gdf = grid_gdf[["id", "geometry"]].merge(scored_df, on="id", how="inner")
        # Write only essential columns to keep file size manageable
        minimal_cols = [
            "id", "h3_index", "latitude", "longitude", "state", "district",
            "site_readiness_score",
            "demand_score", "accessibility_score", "competition_score",
            "suitability_score", "risk_score", "infrastructure_score",
            "geometry",
        ]
        cols_present = [c for c in minimal_cols if c in gdf.columns]
        p = out / GEOJSON_FILE
        gdf[cols_present].to_file(p, driver="GeoJSON")
        logger.info(f"Output → {p}  ({p.stat().st_size/1e6:.1f} MB)")


# ─────────────────────────────────────────────────────────────────────────────
# Column order validator — ensures final dataset matches spec exactly
# ─────────────────────────────────────────────────────────────────────────────

SPEC_COLUMNS = [
    # Layer 0
    "id", "latitude", "longitude", "state", "district", "area_name", "grid_id",
    # Layer 1
    "population_1km", "population_5km", "population_density",
    "male_population", "female_population", "sex_ratio",
    "child_population", "working_age_population", "elderly_population",
    "child_ratio", "working_age_ratio", "dependency_ratio",
    "household_count", "literacy_rate", "income_level",
    # Layer 2
    "road_density", "distance_to_highway", "intersection_density",
    "connectivity_score", "avg_travel_time_10min", "avg_travel_time_20min",
    # Layer 3
    "poi_count_500m", "poi_count_1km", "poi_count_2km",
    "competitor_count", "complementary_business_count",
    "restaurant_count", "shop_count", "hospital_count",
    "school_count", "bank_count",
    "poi_diversity_score", "footfall_proxy_score",
    # Layer 4
    "commercial_ratio", "residential_ratio", "industrial_ratio", "mixed_use_ratio",
    "building_count", "building_density", "avg_building_levels", "built_up_area_ratio",
    # Layer 5
    "aqi", "pm25", "pm10",
    "flood_risk_score", "earthquake_risk_score",
    "green_space_ratio", "temperature",
    # Layer 6
    "distance_to_power_substation", "power_line_density", "electricity_access_score",
    "distance_to_water_source", "water_body_proximity", "water_availability_score",
    "distance_to_bus_stop", "distance_to_railway_station", "public_transport_score",
    # Layer 7
    "demand_score", "accessibility_score", "competition_score",
    "suitability_score", "risk_score", "infrastructure_score",
    # Layer 8
    "site_readiness_score",
]

def validate_and_reorder(df: pd.DataFrame) -> pd.DataFrame:
    present  = [c for c in SPEC_COLUMNS if c in df.columns]
    missing  = [c for c in SPEC_COLUMNS if c not in df.columns]
    extra    = [c for c in df.columns if c not in SPEC_COLUMNS]

    if missing:
        logger.warning(f"Missing columns ({len(missing)}): {missing}")
    if extra:
        logger.info(f"Extra columns not in spec (keeping): {extra}")

    return df[present + extra]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="India Site Feasibility Pipeline")
    parser.add_argument("--resume",   action="store_true", help="Resume from layer checkpoints")
    parser.add_argument("--dry-run",  action="store_true", help="Generate grid only, no data collection")
    parser.add_argument("--layers",   nargs="+", type=int,  default=[1,2,3,4,5,6],
                        help="Which layers to collect (default: all)")
    parser.add_argument("--business", type=str, default="restaurant",
                        help="Business type for competitor logic (default: restaurant)")
    parser.add_argument("--output",   nargs="+", default=["parquet", "csv"],
                        choices=["parquet", "csv", "geojson"],
                        help="Output formats")
    parser.add_argument("--boundary", type=str, default=None,
                        help="Path to India boundary shapefile (GADM recommended)")
    args = parser.parse_args()

    t_total = time.time()
    logger.info("=" * 60)
    logger.info("INDIA SITE FEASIBILITY PIPELINE — START")
    logger.info(f"Layers: {args.layers} | Business: {args.business} | Resume: {args.resume}")
    logger.info("=" * 60)

    # ── 0. Grid generation ────────────────────────────────────────────────────
    logger.info("Step 0: Generating H3 grid for India ...")
    boundary = get_india_boundary(args.boundary)
    grid_gdf = generate_h3_grid(boundary)
    save_grid(grid_gdf)
    logger.info(f"Grid: {len(grid_gdf):,} cells (~5 sq km each)")

    if args.dry_run:
        logger.info("Dry run complete. Grid saved.")
        return

    # ── Layer collection ──────────────────────────────────────────────────────
    layer_frames = {"base": grid_gdf[["id", "latitude", "longitude"]].copy()}

    # Reverse geocode state/district (uses GADM boundaries)
    layer_frames["base"] = _add_admin_boundaries(grid_gdf, layer_frames["base"])

    if 1 in args.layers:
        layer_frames[1] = run_layer(1, collect_demographics, grid_gdf, args.resume)

    if 2 in args.layers:
        layer_frames[2] = run_layer(2, collect_transportation, grid_gdf, args.resume)

    if 3 in args.layers:
        from collectors.layer3_poi_economic import (
            COMPETITOR_AMENITIES, COMPLEMENTARY_AMENITIES
        )
        layer_frames[3] = run_layer(
            3, collect_poi_economic, grid_gdf, args.resume,
            competitor_amenities=COMPETITOR_AMENITIES,
            complementary_amenities=COMPLEMENTARY_AMENITIES,
        )

    if 4 in args.layers:
        layer_frames[4] = run_layer(4, collect_land_use, grid_gdf, args.resume)

    if 5 in args.layers:
        layer_frames[5] = run_layer(5, collect_environment, grid_gdf, args.resume)

    if 6 in args.layers:
        layer_frames[6] = run_layer(6, collect_infrastructure, grid_gdf, args.resume)

    # ── Merge all layers ──────────────────────────────────────────────────────
    logger.info("Merging all layers ...")
    full_df = layer_frames["base"].copy()
    for key in sorted(k for k in layer_frames if k != "base"):
        full_df = full_df.merge(layer_frames[key], on="id", how="left",
                                suffixes=("", f"_l{key}"))

    # H3 index as grid_id
    if "h3_index" in grid_gdf.columns:
        full_df = full_df.merge(grid_gdf[["id", "h3_index"]], on="id", how="left")
        full_df.rename(columns={"h3_index": "grid_id"}, inplace=True)

    # ── Scoring ───────────────────────────────────────────────────────────────
    logger.info("Computing site readiness scores ...")
    full_df = compute_all_scores(full_df)

    # ── Validate column order ─────────────────────────────────────────────────
    full_df = validate_and_reorder(full_df)

    # ── Write outputs ─────────────────────────────────────────────────────────
    write_outputs(full_df, grid_gdf, args.output)

    elapsed = (time.time() - t_total) / 60
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.1f} min")
    logger.info(f"Total cells: {len(full_df):,}")
    logger.info(f"Total columns: {len(full_df.columns)}")
    logger.info(
        f"site_readiness_score — "
        f"min: {full_df['site_readiness_score'].min():.1f}  "
        f"median: {full_df['site_readiness_score'].median():.1f}  "
        f"max: {full_df['site_readiness_score'].max():.1f}"
    )
    logger.info("=" * 60)


def _add_admin_boundaries(grid_gdf: gpd.GeoDataFrame, base_df: pd.DataFrame) -> pd.DataFrame:
    """Reverse-geocode state and district names using GADM level-2 shapefile."""
    district_shp = Path("data/raw/boundaries/gadm41_IND_2.shp")
    if not district_shp.exists():
        logger.warning(
            "GADM boundary file not found. state/district will be NaN.\n"
            "Download from https://gadm.org/download_country.html (India, level 2, shapefile)."
        )
        base_df["state"]    = None
        base_df["district"] = None
        base_df["area_name"] = None
        return base_df

    logger.info("Reverse geocoding state/district ...")
    districts = gpd.read_file(district_shp).to_crs("EPSG:4326")[["NAME_1", "NAME_2", "geometry"]]

    centroids = gpd.GeoDataFrame(
        grid_gdf[["id"]],
        geometry=gpd.points_from_xy(grid_gdf["longitude"], grid_gdf["latitude"]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(centroids, districts, how="left", predicate="within")
    joined = joined[["id", "NAME_1", "NAME_2"]].rename(
        columns={"NAME_1": "state", "NAME_2": "district"}
    ).drop_duplicates("id")

    base_df = base_df.merge(joined, on="id", how="left")
    base_df["area_name"] = None  # populated from OSM place names if needed
    return base_df


if __name__ == "__main__":
    main()

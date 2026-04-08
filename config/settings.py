"""
Pipeline Configuration — India Site Feasibility Scoring
Each grid cell = 5 sq km (~2.24 km side length using H3 resolution 7)
"""

# ─── Grid Settings ────────────────────────────────────────────────────────────
H3_RESOLUTION = 7          # ~5.16 sq km per cell, closest to 5 sq km
INDIA_BBOX = {
    "min_lat": 6.5,
    "max_lat": 37.6,
    "min_lon": 68.1,
    "max_lon": 97.4,
}

# ─── Buffer Radii (metres) ────────────────────────────────────────────────────
BUFFER_500M  = 500
BUFFER_1KM   = 1_000
BUFFER_2KM   = 2_000
BUFFER_5KM   = 5_000

# ─── Data Sources ─────────────────────────────────────────────────────────────
SOURCES = {
    # Census of India 2011 (latest public release) + Office of RGI
    "census": {
        "base_url": "https://censusindia.gov.in/nada/index.php/catalog",
        "district_csv": "data/raw/census/district_census_2011.csv",
        "notes": "Download Primary Census Abstract from censusindia.gov.in → Data Products → Primary Census Abstract"
    },
    # WorldPop 100m grid (UN-adjusted, 2020)
    "worldpop": {
        "base_url": "https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/BSGM/IND/",
        "filename": "ind_ppp_2020_UNadj_constrained.tif",
        "resolution_m": 100,
    },
    # OpenStreetMap via Overpass API (free, live data)
    "osm_overpass": {
        "endpoint": "https://overpass-api.de/api/interpreter",
        "rate_limit_s": 2,          # polite delay between requests
        "timeout_s": 180,
    },
    # OSM bulk data (Geofabrik India extract — updated daily)
    "osm_geofabrik": {
        "url": "https://download.geofabrik.de/asia/india-latest.osm.pbf",
        "local_path": "data/raw/osm/india-latest.osm.pbf",
    },
    # OSRM routing (public demo server — for production use self-hosted)
    "osrm": {
        "endpoint": "http://router.project-osrm.org/table/v1/driving/",
        "note": "Self-host with India OSM extract for scale. Instructions in README.",
    },
    # OpenAQ v3 (real-time + historical AQI)
    "openaq": {
        "endpoint": "https://api.openaq.org/v3/",
        "parameters": ["pm25", "pm10", "o3", "no2"],
        "api_key_env": "OPENAQ_API_KEY",   # free key at openaq.org
    },
    # Bhoonidhi / ISRO BHUVAN (flood/earthquake risk, DEM, land use)
    "bhuvan": {
        "url": "https://bhuvan.nrsc.gov.in/bhuvan_links.php",
        "wms_endpoint": "https://bhuvan-vec2.nrsc.gov.in/bhuvan/wms",
        "note": "Free registration required. Provides LULC 50K, flood hazard atlas.",
    },
    # National Disaster Management Authority (NDMA) flood/earthquake
    "ndma": {
        "earthquake_hazard": "https://ndma.gov.in/Resources/ndma-pdf/maps/Seismic_Zone.pdf",
        "flood_atlas": "https://ndma.gov.in/Resources/ndma-pdf/maps/Flood_Hazard_Atlas.pdf",
        "note": "Digitize zone maps or use BHUVAN raster layers for grid overlay.",
    },
    # NASA POWER (temperature, climate)
    "nasa_power": {
        "endpoint": "https://power.larc.nasa.gov/api/temporal/climatology/point",
        "parameter": "T2M",          # 2-metre temperature (°C)
        "community": "RE",
    },
}

# ─── Scoring Weights (tweak per business type) ────────────────────────────────
SCORE_WEIGHTS = {
    "demand_score":         0.25,   # demographics → market size
    "accessibility_score":  0.20,   # transport → customers can reach
    "competition_score":    0.15,   # POI → competitor pressure (inverted)
    "suitability_score":    0.20,   # land use → zoning fit
    "risk_score":           0.10,   # environment → operational risk (inverted)
    "infrastructure_score": 0.10,   # utilities → operational feasibility
}

# ─── Output ───────────────────────────────────────────────────────────────────
OUTPUT_DIR   = "data/output/"
OUTPUT_CRS   = "EPSG:4326"
PARQUET_FILE = "india_site_scores.parquet"
CSV_FILE     = "india_site_scores.csv"
GEOJSON_FILE = "india_site_scores.geojson"

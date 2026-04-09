"""
Pipeline Configuration — India Site Feasibility Scoring
Each grid cell = 5 sq km (~2.24 km side length using H3 resolution 7)
"""

# ─── Grid Settings ────────────────────────────────────────────────────────────
H3_RESOLUTION = 7          # ~5.16 sq km — closest H3 resolution to 4 sq km
                           # H3 res 8 = 0.74 sq km (too fine); res 7 = 5.16 sq km (best match)
                           # ~650,000 cells for all India
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
    # WorldPop 1km grid (UN-adjusted, 2020) — direct download, no login
    # Full download URL: https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km.tif
    "worldpop": {
        "base_url": "https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/",
        "filename": "ind_pd_2020_1km.tif",
        "resolution_m": 1000,
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
    # GDACS — Global Disaster Alert & Coordination System (UN + European Commission)
    # Replaces BHUVAN and GSI Bhukosh — no login, no file download, free REST API
    # Covers: floods (FL), earthquakes (EQ), cyclones (TC), wildfires (WF), droughts (DR)
    "gdacs": {
        "endpoint": "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH",
        "event_types": ["FL", "EQ"],   # flood + earthquake for risk scoring
        "india_bbox":  "6.5,68.1,37.6,97.4",  # minlat,minlon,maxlat,maxlon
        "note": "No API key needed. pip install gdacs-api",
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
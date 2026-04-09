# SETTUP Guide

This document contains end-to-end setup instructions for running the India Site Feasibility Scoring Pipeline from a fresh machine.

## 1. Clone the Repository

Replace the repository URL with your actual repo.

### HTTPS

```bash
git clone https://github.com/code-YK/geo_site_data_collection.git
cd geo_site_data_collection
```

### SSH

```bash
git clone git@github.com:code-YK/geo_site_data_collection.git
# SETTUP Guide — India Site Feasibility Scoring Pipeline

Complete setup instructions for a fresh machine.
All download links verified as of April 2026.

---

## 1. Recommended Python Version

Use **Python 3.11 or 3.12** on Windows.
Python 3.13 has limited wheel support for geospatial libraries and will cause build errors.

Check your version:
```
python --version
```

If needed, download Python 3.11 from:
```
https://www.python.org/downloads/release/python-3119/
```

---

## 2. Clone and Set Up Environment

```
git clone https://github.com/code-YK/geo_site_data_collection.git
cd geo_site_data_collection
```

### Windows
```
py -3.11 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Linux / macOS
```
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 3. Create Data Directories

### Windows PowerShell
```
New-Item -ItemType Directory -Force `
  data/raw/osm, data/raw/worldpop, data/raw/census, `
  data/raw/boundaries, data/raw/viirs, data/raw/risk, `
  data/interim, data/checkpoints, data/output | Out-Null
```

### Linux / macOS
```
mkdir -p data/raw/osm data/raw/worldpop data/raw/census \
  data/raw/boundaries data/raw/viirs data/raw/risk \
  data/interim data/checkpoints data/output
```

---

## 4. Download Required Datasets

### STATUS KEY
- NO LOGIN — paste link directly in browser, downloads immediately
- FREE API — no account, call from code
- FREE REGISTRATION — email + password, access is instant after confirming email

---

### A. OSM India Extract — NO LOGIN REQUIRED

**What it provides:** Roads, POIs, buildings, land use, power lines, water bodies, bus stops, railway stations. Used by Layers 2, 3, 4, 6.

**Direct download link (paste in browser):**
```
https://download.geofabrik.de/asia/india-latest.osm.pbf
```

**Save as:** `data/raw/osm/india-latest.osm.pbf`

**File size:** ~1.6 GB. Updated daily by Geofabrik.

---

### B. WorldPop Population Raster — NO LOGIN REQUIRED

**What it provides:** 100m resolution gridded population counts for India (2020). Used by Layer 1 for population_1km, population_5km, population_density.

**Direct download link (paste in browser):**
```
https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km.tif
```

**Save as:** `data/raw/worldpop/ind_pd_2020_1km.tif`

**File size:** ~25 MB (1km resolution). If you want the full 100m version (~200MB):
```
https://data.worldpop.org/GIS/Population/Global_2000_2020/2020/IND/ind_ppp_2020.tif
```

Both are hosted by WorldPop / University of Southampton. Open CC BY 4.0 licence.

---

### C. Census of India 2011 — NO LOGIN REQUIRED

**What it provides:** District-level demographics — households, literacy, working population. Used by Layer 1.

**Step 1 — Go to this page:**
```
https://censusindia.gov.in/census.website/en/data/tables
```

**Step 2 — Download this specific file:**
Look for the section titled:
> "Primary Census Abstract Data Tables (India & States/UTs - District Level) (Excel Format)"

Click the Excel download link directly on that page. No account required.

**Save as:** `data/raw/census/primary_census_abstract_2011.xlsx`

**Alternative direct catalog link:**
```
https://censusindia.gov.in/nada/index.php/catalog/6191
```

---

### D. GADM India Boundaries — NO LOGIN REQUIRED

**What it provides:** State and district polygon boundaries for reverse geocoding each H3 cell to a state/district name. Used in the base layer.

**Direct download link (paste in browser):**
```
https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_IND_shp.zip
```

**Save as:** `data/raw/boundaries/gadm41_IND_shp.zip`

**Then extract:**

Windows PowerShell:
```
Expand-Archive data/raw/boundaries/gadm41_IND_shp.zip data/raw/boundaries/
```

Linux / macOS:
```
unzip data/raw/boundaries/gadm41_IND_shp.zip -d data/raw/boundaries/
```

After extraction you will see files like `gadm41_IND_0.shp`, `gadm41_IND_1.shp`, `gadm41_IND_2.shp`. The pipeline uses level 2 (district boundaries).

---

### E. GDACS Disaster Risk Data — FREE API, NO LOGIN, NO DOWNLOAD NEEDED

**What it provides:** Historical flood, earthquake, cyclone, wildfire and drought events for India with coordinates and severity scores. Used in Layer 5 to compute `flood_risk_score` and `earthquake_risk_score`. This replaces BHUVAN and GSI Bhukosh which require manual registration and have unreliable portals.

**Source:** GDACS — Global Disaster Alert and Coordination System (UN + European Commission)

**No file to download.** The pipeline queries the GDACS API directly at runtime:
```
https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH
```

**Install the Python client:**
```
pip install gdacs-api
```

**How the pipeline uses it:** At Layer 5 runtime, it fetches all historical flood and earthquake events within India's bounding box, builds a heatmap of disaster frequency per H3 cell, and normalises it to a 0–1 risk score. No manual file management needed.

**API documentation:** `https://www.gdacs.org/gdacsapi/swagger/index.html`

---

### F. VIIRS Nighttime Lights (Income Proxy) — FREE REGISTRATION

**What it provides:** Satellite-measured nighttime light intensity as a proxy for income level. Used by Layer 1 for `income_level`.

**Status:** Requires a free account. Registration is instant (email + password, no approval wait).

**Register here:**
```
https://eogdata.mines.edu/products/vnl/
```

Click "Download V2.2" and it will prompt you to create a free account. Confirm your email, then download the annual composite GeoTIFF for your region.

**Alternative with no login at all — NASA Black Marble:**
```
https://blackmarble.gsfc.nasa.gov/
```
Download the VNP46A4 annual product for Asia. Same underlying VIIRS data hosted by NASA, completely open access.

**Save as:** `data/raw/viirs/india_viirs_2022.tif`

**Note:** If you skip this file, `income_level` will be NaN and the pipeline will redistribute weights. The final score still works.

---

### G. OpenAQ API Key (Air Quality) — FREE REGISTRATION, INSTANT

**What it provides:** Real station-measured PM2.5, PM10 and AQI data for India. Used by Layer 5.

**Register (free, instant):**
```
https://openaq.org/register
```

After confirming email, go to your account → API Keys → Generate key.

**Add to your project:**
Create a file called `.env` in the project root:
```
OPENAQ_API_KEY=your_key_here
```

**Note:** If you skip this, AQ columns will be NaN. The pipeline still runs.

---

## 5. Verify Your data/raw Directory

After completing all downloads, your structure should look like:

```
data/
└── raw/
    ├── osm/
    │   └── india-latest.osm.pbf          (~1.6 GB)
    ├── worldpop/
    │   └── ind_pd_2020_1km.tif           (~25 MB)
    ├── census/
    │   └── primary_census_abstract_2011.xlsx
    ├── boundaries/
    │   ├── gadm41_IND_0.shp
    │   ├── gadm41_IND_1.shp
    │   └── gadm41_IND_2.shp  ← this is the one the pipeline uses
    ├── viirs/
    │   └── india_viirs_2022.tif          (optional)
    └── risk/
        (empty — GDACS is fetched live via API, no file needed here)
```

---

## 6. Optional: Self-host OSRM for Travel-Time Calculations

The public OSRM demo server rate-limits at scale. For full India travel-time coverage, run OSRM locally using Docker.

**Requires:** Docker Desktop installed (`https://www.docker.com/products/docker-desktop/`)

```
cd data/raw/osm

docker run -t -v %cd%:/data osrm/osrm-backend osrm-extract -p /opt/car.lua /data/india-latest.osm.pbf
docker run -t -v %cd%:/data osrm/osrm-backend osrm-partition /data/india-latest.osrm
docker run -t -v %cd%:/data osrm/osrm-backend osrm-customize /data/india-latest.osrm
docker run -t -i -p 5000:5000 -v %cd%:/data osrm/osrm-backend osrm-routed --algorithm mld /data/india-latest.osrm
```

Then update `config/settings.py`:
```python
"osrm": {
    "endpoint": "http://localhost:5000/table/v1/driving/"
}
```

**Note:** OSM extract + OSRM preprocessing requires ~20 GB disk and ~4 hours. Skip for now — the pipeline falls back to NaN for travel-time columns if OSRM is unavailable.

---

## 7. Run the Pipeline

### Validate setup (grid only, no data collection):
```
python run_pipeline.py --dry-run
```

Expected output: "Grid: ~650,000 cells" — if you see this, your environment is working.

### Run specific layers first (recommended for first run):
```
python run_pipeline.py --layers 3 6 --resume --output parquet
```
Layer 3 (POI) and Layer 6 (Infrastructure) use only OSM — fastest to verify.

### Full pipeline:
```
python run_pipeline.py --resume --output parquet csv
```

### Full pipeline with GeoJSON output:
```
python run_pipeline.py --resume --output parquet csv geojson
```

---

## 8. Resume After Interruption

The pipeline saves a checkpoint after each layer to `data/checkpoints/`.
Always use `--resume` to avoid re-running completed layers:

```
python run_pipeline.py --resume --output parquet csv
```

---

## 9. Estimated Run Times (First Run)

| Step | Approx Time | Notes |
|---|---|---|
| Grid generation | 5 min | H3 fill, runs once |
| Layer 1 — Demographics | 40 min | WorldPop raster + Census join |
| Layer 2 — Transportation | 20 min | OSM parse + road metrics |
| Layer 3 — POI | 15 min | OSM parse + spatial counts |
| Layer 4 — Land Use | 25 min | OSM parse + overlay |
| Layer 5 — Environment | 30 min | GDACS API + OpenAQ API |
| Layer 6 — Infrastructure | 20 min | OSM parse + distance calcs |
| Scoring | 5 min | Pure pandas |
| **Total** | **~3 hrs** | Subsequent runs are faster due to caching |

OSM parsing (layers 2–4, 6) runs once and caches to `data/interim/`.
Re-running the pipeline after the first time takes ~30 minutes total.

---

## 10. Output Files

| File | Size | Description |
|---|---|---|
| `data/output/india_site_scores.parquet` | ~500 MB | Full dataset, fast columnar format |
| `data/output/india_site_scores.csv` | ~1.2 GB | Human-readable, opens in Excel |
| `data/output/india_site_scores.geojson` | ~200 MB | Score columns + H3 geometry for QGIS/mapping |

---

## 11. Common Issues

**osmium not installing:**
```
pip install osmium --pre
```
If that fails, check you are on Python 3.11 or 3.12. Python 3.13 may not have a wheel yet.

**rasterio / fiona failing on Windows:**
Install prebuilt wheels from Christoph Gohlke's repository:
```
https://github.com/cgohlke/geospatial-wheels/releases
```
Download the `.whl` files matching your Python version (cp311 = Python 3.11) and install:
```
pip install GDAL-*.whl Fiona-*.whl rasterio-*.whl
```

**OSM file not found error:**
Make sure the file is at exactly: `data/raw/osm/india-latest.osm.pbf`
The filename must match exactly — no spaces, no version suffix.

**GDACS returns no events:**
The GDACS API returns max 100 events per page. The pipeline paginates automatically.
If you see empty results, check your internet connection — no API key is needed.

**Census Excel column names differ:**
Different downloads of the Census PCA may have slightly different column headers.
Open the file in Excel first and check the actual column names, then update the `col_map` dict in `collectors/layer1_demographics.py`.

---

## 12. Dataset Sources Summary

| Dataset | URL | Login | Licence |
|---|---|---|---|
| OSM India PBF | https://download.geofabrik.de/asia/india-latest.osm.pbf | None | ODbL |
| WorldPop 2020 | https://data.worldpop.org/GIS/Population_Density/Global_2000_2020_1km/2020/IND/ind_pd_2020_1km.tif | None | CC BY 4.0 |
| Census 2011 PCA | https://censusindia.gov.in/census.website/en/data/tables | None | Open Govt Data |
| GADM Boundaries | https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_IND_shp.zip | None | Free academic use |
| GDACS Disaster API | https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH | None | Free / UN open data |
| VIIRS Lights | https://eogdata.mines.edu/products/vnl/ | Free account | CC BY 4.0 |
| NASA Black Marble | https://blackmarble.gsfc.nasa.gov/ | None | Public domain |
| OpenAQ (AQI) | https://openaq.org/register | Free account | CC BY 4.0 |
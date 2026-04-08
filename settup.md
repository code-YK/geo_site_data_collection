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
cd geo_site_data_collection
```

## 2. Create Virtual Environment

Python 3.11 or 3.12 is recommended on Windows.

### Windows

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Linux and macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Create Data Directories

### Windows PowerShell

```powershell
New-Item -ItemType Directory -Force data/raw/osm, data/raw/worldpop, data/raw/census, data/raw/boundaries, data/raw/viirs, data/raw/risk, data/raw/bhuvan, data/interim, data/checkpoints, data/output | Out-Null
```

### Linux and macOS

```bash
mkdir -p data/raw/osm data/raw/worldpop data/raw/census data/raw/boundaries data/raw/viirs data/raw/risk data/raw/bhuvan data/interim data/checkpoints data/output
```

## 4. Download Required Datasets

All datasets below are real public sources used by the pipeline.

### A. OSM India Extract (Required)

Purpose: Roads, POIs, buildings, land use, power, water, transport infrastructure.

Source:
https://download.geofabrik.de/asia/india-latest.osm.pbf

Save as:
data/raw/osm/india-latest.osm.pbf

### B. WorldPop Population Raster (Required)

Purpose: Population and density features.

Source folder:
https://data.worldpop.org/GIS/Population/Global_2000_2020_Constrained/2020/BSGM/IND/

File:
ind_ppp_2020_UNadj_constrained.tif

Save as:
data/raw/worldpop/ind_ppp_2020_UNadj_constrained.tif

### C. Census of India Files (Required)

Purpose: Demographic enrichments (households, literacy, age bands).

Source:
https://censusindia.gov.in/nada/index.php/catalog/42

Download:
1. Primary Census Abstract (Excel)
2. C-13 Single Year Age Data

Save under:
data/raw/census/

Recommended filenames:
1. data/raw/census/primary_census_abstract_2011.xlsx
2. data/raw/census/c13_single_year_age_2011.xlsx

### D. GADM India Boundaries (Required)

Purpose: State and district reverse geocoding.

Source zip:
https://geodata.ucdavis.edu/gadm/gadm4.1/shp/gadm41_IND_shp.zip

Save and extract into:
data/raw/boundaries/

After extraction, shapefiles should remain in that folder.

### E. VIIRS Nighttime Lights (Recommended)

Purpose: Income proxy feature.

Source:
https://eogdata.mines.edu/products/vnl/#annual_v2

Save as:
data/raw/viirs/india_viirs_2022.tif

### F. Flood Risk Raster (Recommended)

Purpose: flood_risk_score.

Source:
BHUVAN portal and services
https://bhuvan.nrsc.gov.in
https://bhuvan-vec2.nrsc.gov.in/bhuvan/wms

Suggested output filename:
data/raw/risk/india_flood_hazard.tif

### G. Earthquake Risk Raster (Recommended)

Purpose: earthquake_risk_score.

Source:
GSI Bhukosh
https://bhukosh.gsi.gov.in

Suggested output filename:
data/raw/risk/india_seismic_zone.tif

### H. OpenAQ API Key (Optional but Recommended)

Purpose: AQI, PM2.5, PM10 enrichment.

Register:
https://openaq.org/#/register

Create .env in repo root:

```bash
OPENAQ_API_KEY=your_key_here
```

## 5. Optional: Self-host OSRM for Scale

For all-India travel-time calculations, a local OSRM server is recommended.

1. Install Docker.
2. Use india-latest.osm.pbf from data/raw/osm.
3. Build and run OSRM backend.
4. Update config/settings.py osrm endpoint to local host.

Example local endpoint:
http://localhost:5000/table/v1/driving/

## 6. Validate Installation

Run a dry test:

```bash
python run_pipeline.py --dry-run
```

If successful, run selected layers:

```bash
python run_pipeline.py --layers 2 3 6 --resume
```

Then run full pipeline:

```bash
python run_pipeline.py --resume --output parquet csv
```

## 7. Common Issues and Fixes

1. Missing OSM file
- Ensure data/raw/osm/india-latest.osm.pbf exists.

2. Raster not found warnings
- These layers degrade gracefully, but you should place files in data/raw/risk and data/raw/viirs for full scoring quality.

3. OpenAQ key missing
- AQ columns may be empty. Add OPENAQ_API_KEY in .env.

4. Slow runtime
- First run is heavy. Subsequent runs are faster due to cached parquet files in data/interim.

## 8. Expected Output Files

1. data/output/india_site_scores.parquet
2. data/output/india_site_scores.csv
3. data/output/india_site_scores.geojson

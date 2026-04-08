# India Site Feasibility Scoring Pipeline

One H3 cell (~5 sq km) per row, ~650,000 rows across India, 70+ data columns, and a final site_readiness_score (0 to 100).

This project now uses osmium (pyosmium) for OSM PBF parsing instead of pyrosm, to avoid Windows and Python build issues.

## Quick Start

1. Clone the repository.
2. Create and activate a virtual environment.
3. Install dependencies.
4. Download required datasets.
5. Run the pipeline.

Detailed setup instructions are in settup.md.

## Clone Commands

Replace the URL below with your repository URL.

### HTTPS

```bash
git clone https://github.com/your-org/geo_site_data_collection.git
cd geo_site_data_collection
```

### SSH

```bash
git clone git@github.com:your-org/geo_site_data_collection.git
cd geo_site_data_collection
```

## Environment Setup

### Windows (recommended)

```bash
py -3.11 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Linux or macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Run Examples

### Dry run (grid only)

```bash
python run_pipeline.py --dry-run
```

### Full run

```bash
python run_pipeline.py --output parquet csv
```

### Resume from checkpoints

```bash
python run_pipeline.py --resume --output parquet csv
```

### Selected layers only

```bash
python run_pipeline.py --layers 1 2 3 --resume
```

## Project Structure

```text
geo_site_data_collection/
├── run_pipeline.py
├── requirements.txt
├── README.md
├── settup.md
├── config/
│   └── settings.py
├── collectors/
│   ├── layer1_demographics.py
│   ├── layer2_transportation.py
│   ├── layer3_poi_economic.py
│   ├── layer4_land_use.py
│   ├── layer5_environment.py
│   └── layer6_infrastructure.py
├── processors/
│   └── scoring_engine.py
├── utils/
│   ├── grid_generator.py
│   └── osm_reader.py
└── data/
    ├── raw/
    ├── interim/
    ├── checkpoints/
    └── output/
```

## Datasets and Source Notes

Required datasets and step-by-step download instructions are documented in settup.md.

Primary sources used:

1. OpenStreetMap India PBF (Geofabrik)
2. WorldPop population raster
3. Census of India (PCA and C-13)
4. GADM India boundaries
5. VIIRS nighttime lights
6. Flood and seismic risk rasters (BHUVAN and GSI)
7. OpenAQ API
8. NASA POWER API

## Outputs

1. data/output/india_site_scores.parquet
2. data/output/india_site_scores.csv
3. data/output/india_site_scores.geojson

## Notes

1. First run is heavy due to OSM parsing and raster processing.
2. OSM-derived parquet caches are reused in subsequent runs.
3. For large-scale routing, use a self-hosted OSRM instance.

"""
utils/osm_reader.py
Replaces pyrosm with osmium (pyosmium) for OSM PBF parsing.

pyosmium has prebuilt Windows wheels on PyPI and works on Python 3.10–3.13.
Install: pip install osmium

Each function parses the India PBF ONCE and caches the result to parquet.
All subsequent calls load from cache — re-parsing takes ~15–30 min per feature type.

Usage:
    from utils.osm_reader import get_roads, get_pois, get_buildings, get_landuse, get_features
"""

import logging
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, Polygon, MultiPolygon
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

OSM_PBF      = Path("data/raw/osm/india-latest.osm.pbf")
INTERIM_DIR  = Path("data/interim")
INTERIM_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Internal osmium handler classes
# ─────────────────────────────────────────────────────────────────────────────

def _check_osmium():
    try:
        import osmium
        return osmium
    except ImportError:
        raise ImportError(
            "osmium not installed. Run: pip install osmium\n"
            "Prebuilt wheels available for Windows Python 3.10–3.13."
        )


def _check_pbf():
    if not OSM_PBF.exists():
        raise FileNotFoundError(
            f"OSM PBF not found at {OSM_PBF}\n"
            "Download: wget https://download.geofabrik.de/asia/india-latest.osm.pbf "
            "-P data/raw/osm/\n"
            "Windows: use a download manager or browser to get the ~1.3 GB file."
        )


# ─────────────────────────────────────────────────────────────────────────────
# ROADS
# ─────────────────────────────────────────────────────────────────────────────

def get_roads(force_rebuild: bool = False) -> gpd.GeoDataFrame:
    """
    Extract all road linestrings from India PBF.
    Columns: osmid, highway, name, geometry

    Cached to: data/interim/india_roads.parquet
    """
    cache = INTERIM_DIR / "india_roads.parquet"
    if cache.exists() and not force_rebuild:
        logger.info(f"Loading cached roads from {cache}")
        return gpd.read_parquet(cache)

    _check_pbf()
    osmium = _check_osmium()

    class RoadHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.records = []
            self.wkb = osmium.geom.WKBFactory()

        def way(self, w):
            tags = dict(w.tags)
            if "highway" not in tags:
                return
            try:
                wkb = self.wkb.create_linestring(w)
                import shapely.wkb
                geom = shapely.wkb.loads(wkb, hex=True)
                self.records.append({
                    "osmid":    w.id,
                    "highway":  tags.get("highway"),
                    "name":     tags.get("name"),
                    "geometry": geom,
                })
            except Exception:
                pass

    logger.info("Parsing roads from OSM PBF (~15 min for India) ...")
    handler = RoadHandler()
    handler.apply_file(str(OSM_PBF), locations=True, idx="flex_mem")

    gdf = gpd.GeoDataFrame(handler.records, crs="EPSG:4326")
    gdf.to_parquet(cache)
    logger.info(f"Roads cached: {len(gdf):,} features → {cache}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# POIs (nodes + way centroids with amenity/shop/leisure tags)
# ─────────────────────────────────────────────────────────────────────────────

POI_KEYS = {"amenity", "shop", "leisure", "tourism", "healthcare",
            "office", "public_transport", "railway", "aeroway"}

def get_pois(force_rebuild: bool = False) -> gpd.GeoDataFrame:
    """
    Extract POI nodes + way centroids from India PBF.
    Columns: osmid, amenity, shop, leisure, name, category, geometry

    Cached to: data/interim/india_pois.parquet
    """
    cache = INTERIM_DIR / "india_pois.parquet"
    if cache.exists() and not force_rebuild:
        logger.info(f"Loading cached POIs from {cache}")
        return gpd.read_parquet(cache)

    _check_pbf()
    osmium = _check_osmium()

    class POIHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.records = []
            self.wkb = osmium.geom.WKBFactory()

        def _process(self, obj, geom_fn):
            tags = dict(obj.tags)
            if not any(k in tags for k in POI_KEYS):
                return
            try:
                import shapely.wkb
                geom = shapely.wkb.loads(geom_fn(), hex=True)
                if geom.geom_type != "Point":
                    geom = geom.centroid
                self.records.append({
                    "osmid":   obj.id,
                    "amenity": tags.get("amenity"),
                    "shop":    tags.get("shop"),
                    "leisure": tags.get("leisure"),
                    "name":    tags.get("name"),
                    "geometry": geom,
                })
            except Exception:
                pass

        def node(self, n):
            self._process(n, lambda: self.wkb.create_point(n))

        def way(self, w):
            self._process(w, lambda: self.wkb.create_linestring(w))

    logger.info("Parsing POIs from OSM PBF (~10 min for India) ...")
    handler = POIHandler()
    handler.apply_file(str(OSM_PBF), locations=True, idx="flex_mem")

    gdf = gpd.GeoDataFrame(handler.records, crs="EPSG:4326")

    # Assign simplified category
    def _cat(row):
        amenity = str(row.get("amenity") or "")
        shop    = str(row.get("shop")    or "")
        if amenity in {"restaurant","cafe","fast_food","food_court","bar","pub"}:
            return "restaurant"
        if amenity in {"hospital","clinic","doctors","health_post","dentist","nursing_home"}:
            return "hospital"
        if amenity in {"school","college","university","kindergarten","language_school"}:
            return "school"
        if amenity in {"bank","atm","bureau_de_change","money_transfer"}:
            return "bank"
        if shop not in {"", "None", "none"} and shop:
            return "shop"
        return "other"

    gdf["category"] = gdf.apply(_cat, axis=1)
    gdf.to_parquet(cache)
    logger.info(f"POIs cached: {len(gdf):,} features → {cache}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# BUILDINGS
# ─────────────────────────────────────────────────────────────────────────────

def get_buildings(force_rebuild: bool = False) -> gpd.GeoDataFrame:
    """
    Extract building polygons from India PBF.
    Columns: osmid, building, levels, geometry

    Cached to: data/interim/india_buildings.parquet
    """
    cache = INTERIM_DIR / "india_buildings.parquet"
    if cache.exists() and not force_rebuild:
        logger.info(f"Loading cached buildings from {cache}")
        return gpd.read_parquet(cache)

    _check_pbf()
    osmium = _check_osmium()

    class BuildingHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.records = []
            self.wkb = osmium.geom.WKBFactory()

        def way(self, w):
            tags = dict(w.tags)
            if "building" not in tags:
                return
            try:
                import shapely.wkb
                geom = shapely.wkb.loads(self.wkb.create_polygon(w), hex=True)
                self.records.append({
                    "osmid":    w.id,
                    "building": tags.get("building"),
                    "levels":   tags.get("building:levels"),
                    "geometry": geom,
                })
            except Exception:
                pass

    logger.info("Parsing buildings from OSM PBF (~20 min for India) ...")
    handler = BuildingHandler()
    handler.apply_file(str(OSM_PBF), locations=True, idx="flex_mem")

    gdf = gpd.GeoDataFrame(handler.records, crs="EPSG:4326")
    gdf.to_parquet(cache)
    logger.info(f"Buildings cached: {len(gdf):,} features → {cache}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# LAND USE polygons
# ─────────────────────────────────────────────────────────────────────────────

LANDUSE_MAP = {
    "residential": "residential", "apartments": "residential",
    "commercial":  "commercial",  "retail": "commercial", "office": "commercial",
    "industrial":  "industrial",  "garages": "industrial", "port": "industrial",
    "construction":"mixed",       "brownfield": "mixed",
    "greenfield":  "other",  "farmland": "other",  "forest": "other",
    "grass": "other",  "cemetery": "other",  "military": "other",
}

def get_landuse(force_rebuild: bool = False) -> gpd.GeoDataFrame:
    """
    Extract land use polygons from India PBF.
    Columns: osmid, landuse, landuse_category, geometry

    Cached to: data/interim/india_landuse.parquet
    """
    cache = INTERIM_DIR / "india_landuse.parquet"
    if cache.exists() and not force_rebuild:
        logger.info(f"Loading cached land use from {cache}")
        return gpd.read_parquet(cache)

    _check_pbf()
    osmium = _check_osmium()

    class LandUseHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.records = []
            self.wkb = osmium.geom.WKBFactory()

        def way(self, w):
            tags = dict(w.tags)
            lu = tags.get("landuse") or tags.get("leisure")
            if not lu:
                return
            try:
                import shapely.wkb
                geom = shapely.wkb.loads(self.wkb.create_polygon(w), hex=True)
                self.records.append({
                    "osmid":             w.id,
                    "landuse":           lu,
                    "landuse_category":  LANDUSE_MAP.get(lu, "other"),
                    "geometry":          geom,
                })
            except Exception:
                pass

    logger.info("Parsing land use from OSM PBF (~10 min for India) ...")
    handler = LandUseHandler()
    handler.apply_file(str(OSM_PBF), locations=True, idx="flex_mem")

    gdf = gpd.GeoDataFrame(handler.records, crs="EPSG:4326")
    gdf.to_parquet(cache)
    logger.info(f"Land use cached: {len(gdf):,} features → {cache}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# Generic feature extractor (power, water, transport nodes)
# ─────────────────────────────────────────────────────────────────────────────

def get_features(
    tag_filters: dict,
    cache_name: str,
    force_rebuild: bool = False,
    include_ways: bool = True,
    preserve_way_geometry: bool = False,
) -> gpd.GeoDataFrame:
    """
    Generic extractor for any OSM tag combination.

    tag_filters: {tag_key: [value1, value2, ...] or True}
      e.g. {"power": ["substation", "transformer"]}
      e.g. {"highway": ["bus_stop"], "amenity": ["bus_station"]}

    Returns GeoDataFrame with point geometries by default (way centroids for areas).
    Set preserve_way_geometry=True to keep way geometries as linestrings/polygons.
    """
    cache = INTERIM_DIR / f"india_{cache_name}.parquet"
    if cache.exists() and not force_rebuild:
        logger.info(f"Loading cached {cache_name} from {cache}")
        return gpd.read_parquet(cache)

    _check_pbf()
    osmium = _check_osmium()

    def _matches(tags):
        for key, values in tag_filters.items():
            if key in tags:
                if values is True:
                    return True
                if tags[key] in values:
                    return True
        return False

    class FeatureHandler(osmium.SimpleHandler):
        def __init__(self):
            super().__init__()
            self.records = []
            self.wkb = osmium.geom.WKBFactory()

        def _append(self, obj_id, tags, geom, centroid_if_needed: bool = True):
            import shapely.wkb
            try:
                g = shapely.wkb.loads(geom, hex=True)
                if centroid_if_needed and g.geom_type != "Point":
                    g = g.centroid
                row = {"osmid": obj_id, "geometry": g}
                row.update(tags)
                self.records.append(row)
            except Exception:
                pass

        def node(self, n):
            tags = dict(n.tags)
            if _matches(tags):
                self._append(n.id, tags, self.wkb.create_point(n))

        def way(self, w):
            if not include_ways:
                return
            tags = dict(w.tags)
            if _matches(tags):
                try:
                    self._append(
                        w.id,
                        tags,
                        self.wkb.create_linestring(w),
                        centroid_if_needed=not preserve_way_geometry,
                    )
                except Exception:
                    pass

    logger.info(f"Parsing {cache_name} from OSM PBF ...")
    handler = FeatureHandler()
    handler.apply_file(str(OSM_PBF), locations=True, idx="flex_mem")

    if not handler.records:
        logger.warning(f"No {cache_name} features found in PBF.")
        return gpd.GeoDataFrame(columns=["osmid", "geometry"], geometry="geometry", crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(handler.records, crs="EPSG:4326")
    gdf.to_parquet(cache)
    logger.info(f"{cache_name} cached: {len(gdf):,} features → {cache}")
    return gdf

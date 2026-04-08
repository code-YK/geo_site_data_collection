"""
processors/scoring_engine.py
Computes all Layer 7 derived scores + Layer 8 final site_readiness_score.

Scoring philosophy:
  - Each sub-score is min-max normalised to 0–100
  - risk_score and competition_score are inverted (lower = better)
  - Final score = weighted average per config/settings.py SCORE_WEIGHTS
  - All scores are computed on the FULL India grid, so normalization
    is relative to the national distribution.

Columns produced (Layer 7):
  demand_score, accessibility_score, competition_score,
  suitability_score, risk_score, infrastructure_score

Columns produced (Layer 8):
  site_readiness_score (0–100)
"""

import logging

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from config.settings import SCORE_WEIGHTS

logger = logging.getLogger(__name__)


def _minmax(series: pd.Series, invert: bool = False) -> pd.Series:
    """Normalise a Series to 0–100. invert=True flips direction (for risk/competition)."""
    s = series.copy().astype(float)
    finite = s[np.isfinite(s)]
    if finite.empty or finite.max() == finite.min():
        return pd.Series(50.0, index=s.index, name=s.name)

    scaled = (s - finite.min()) / (finite.max() - finite.min()) * 100
    if invert:
        scaled = 100 - scaled
    return scaled.clip(0, 100).round(2)


# ─────────────────────────────────────────────────────────────────────────────
# 7a. DEMAND SCORE — from demographics
# ─────────────────────────────────────────────────────────────────────────────

def compute_demand_score(df: pd.DataFrame) -> pd.Series:
    """
    Composite of:
      - population_density (40%)
      - working_age_ratio  (25%)
      - income_level       (25%)
      - literacy_rate      (10%)
    """
    components = []

    if "population_density" in df.columns:
        components.append((_minmax(df["population_density"]), 0.40))
    if "working_age_ratio" in df.columns:
        components.append((_minmax(df["working_age_ratio"]), 0.25))
    if "income_level" in df.columns:
        components.append((_minmax(df["income_level"].fillna(df["income_level"].median())), 0.25))
    if "literacy_rate" in df.columns:
        components.append((_minmax(df["literacy_rate"].fillna(df["literacy_rate"].median())), 0.10))

    if not components:
        logger.warning("No demographic columns found for demand_score.")
        return pd.Series(np.nan, index=df.index, name="demand_score")

    total_weight = sum(w for _, w in components)
    score = sum(s * w for s, w in components) / total_weight
    return score.round(2).rename("demand_score")


# ─────────────────────────────────────────────────────────────────────────────
# 7b. ACCESSIBILITY SCORE — from transportation
# ─────────────────────────────────────────────────────────────────────────────

def compute_accessibility_score(df: pd.DataFrame) -> pd.Series:
    """
    Composite of:
      - road_density          (25%)
      - distance_to_highway   (25%, inverted)
      - connectivity_score    (25%)
      - avg_travel_time_20min (25%)
    """
    components = []

    if "road_density" in df.columns:
        components.append((_minmax(df["road_density"]), 0.25))
    if "distance_to_highway" in df.columns:
        components.append((_minmax(df["distance_to_highway"], invert=True), 0.25))
    if "connectivity_score" in df.columns:
        components.append((_minmax(df["connectivity_score"]), 0.25))
    if "avg_travel_time_20min" in df.columns:
        components.append((_minmax(df["avg_travel_time_20min"]), 0.25))

    if not components:
        return pd.Series(np.nan, index=df.index, name="accessibility_score")

    total_weight = sum(w for _, w in components)
    score = sum(s * w for s, w in components) / total_weight
    return score.round(2).rename("accessibility_score")


# ─────────────────────────────────────────────────────────────────────────────
# 7c. COMPETITION SCORE — from POIs (inverted = fewer competitors = better)
# ─────────────────────────────────────────────────────────────────────────────

def compute_competition_score(df: pd.DataFrame) -> pd.Series:
    """
    Composite (all inverted — higher raw = more competition = lower score):
      - competitor_count          (60%)
      - poi_count_2km             (20%)
      - complementary_business_count (20%, NOT inverted — more complementary = better)
    """
    components = []

    if "competitor_count" in df.columns:
        components.append((_minmax(df["competitor_count"], invert=True), 0.60))
    if "poi_count_2km" in df.columns:
        components.append((_minmax(df["poi_count_2km"]), 0.20))
    if "complementary_business_count" in df.columns:
        components.append((_minmax(df["complementary_business_count"]), 0.20))

    if not components:
        return pd.Series(np.nan, index=df.index, name="competition_score")

    total_weight = sum(w for _, w in components)
    score = sum(s * w for s, w in components) / total_weight
    return score.round(2).rename("competition_score")


# ─────────────────────────────────────────────────────────────────────────────
# 7d. SUITABILITY SCORE — from land use
# ─────────────────────────────────────────────────────────────────────────────

def compute_suitability_score(df: pd.DataFrame) -> pd.Series:
    """
    Composite of:
      - commercial_ratio  (40%)
      - building_density  (30%)
      - mixed_use_ratio   (20%)
      - built_up_area_ratio (10%)
    """
    components = []

    if "commercial_ratio" in df.columns:
        components.append((_minmax(df["commercial_ratio"]), 0.40))
    if "building_density" in df.columns:
        components.append((_minmax(df["building_density"]), 0.30))
    if "mixed_use_ratio" in df.columns:
        components.append((_minmax(df["mixed_use_ratio"]), 0.20))
    if "built_up_area_ratio" in df.columns:
        components.append((_minmax(df["built_up_area_ratio"]), 0.10))

    if not components:
        return pd.Series(np.nan, index=df.index, name="suitability_score")

    total_weight = sum(w for _, w in components)
    score = sum(s * w for s, w in components) / total_weight
    return score.round(2).rename("suitability_score")


# ─────────────────────────────────────────────────────────────────────────────
# 7e. RISK SCORE — from environment (inverted = lower risk = higher score)
# ─────────────────────────────────────────────────────────────────────────────

def compute_risk_score(df: pd.DataFrame) -> pd.Series:
    """
    Composite (all inverted — higher raw risk = lower score):
      - aqi                  (30%)
      - pm25                 (20%)
      - flood_risk_score     (30%)
      - earthquake_risk_score(20%)
    """
    components = []

    if "aqi" in df.columns:
        components.append((_minmax(df["aqi"].fillna(100), invert=True), 0.30))
    if "pm25" in df.columns:
        components.append((_minmax(df["pm25"].fillna(df["pm25"].median()), invert=True), 0.20))
    if "flood_risk_score" in df.columns:
        components.append((_minmax(df["flood_risk_score"].fillna(0), invert=True), 0.30))
    if "earthquake_risk_score" in df.columns:
        components.append((_minmax(df["earthquake_risk_score"].fillna(0), invert=True), 0.20))

    if not components:
        return pd.Series(np.nan, index=df.index, name="risk_score")

    total_weight = sum(w for _, w in components)
    score = sum(s * w for s, w in components) / total_weight
    return score.round(2).rename("risk_score")


# ─────────────────────────────────────────────────────────────────────────────
# 7f. INFRASTRUCTURE SCORE — from utilities
# ─────────────────────────────────────────────────────────────────────────────

def compute_infrastructure_score(df: pd.DataFrame) -> pd.Series:
    """
    Composite of:
      - electricity_access_score  (35%)
      - water_availability_score  (35%)
      - public_transport_score    (30%)
    """
    components = []

    if "electricity_access_score" in df.columns:
        components.append((_minmax(df["electricity_access_score"]), 0.35))
    if "water_availability_score" in df.columns:
        components.append((_minmax(df["water_availability_score"]), 0.35))
    if "public_transport_score" in df.columns:
        components.append((_minmax(df["public_transport_score"]), 0.30))

    if not components:
        return pd.Series(np.nan, index=df.index, name="infrastructure_score")

    total_weight = sum(w for _, w in components)
    score = sum(s * w for s, w in components) / total_weight
    return score.round(2).rename("infrastructure_score")


# ─────────────────────────────────────────────────────────────────────────────
# Layer 8 — FINAL SITE READINESS SCORE
# ─────────────────────────────────────────────────────────────────────────────

def compute_site_readiness(df: pd.DataFrame) -> pd.Series:
    """
    Weighted combination of the 6 derived scores → site_readiness_score (0–100).
    Weights are defined in config/settings.py SCORE_WEIGHTS.
    Missing sub-scores are filled with the national median (no data ≠ bad site).
    """
    score_cols = list(SCORE_WEIGHTS.keys())
    present    = [c for c in score_cols if c in df.columns]

    if not present:
        raise ValueError("No derived score columns found. Run compute_all_scores first.")

    total_weight = sum(SCORE_WEIGHTS[c] for c in present)
    site_score   = pd.Series(0.0, index=df.index)

    for col in present:
        vals = df[col].fillna(df[col].median())
        site_score += vals * (SCORE_WEIGHTS[col] / total_weight)

    return site_score.clip(0, 100).round(2).rename("site_readiness_score")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — compute all scores on assembled dataset
# ─────────────────────────────────────────────────────────────────────────────

def compute_all_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input  : assembled DataFrame with all 6 raw layers merged.
    Output : same DataFrame with Layer 7 + Layer 8 columns appended.
    """
    logger.info("=== SCORING ENGINE: Computing derived scores ===")

    df = df.copy()
    df["demand_score"]         = compute_demand_score(df)
    df["accessibility_score"]  = compute_accessibility_score(df)
    df["competition_score"]    = compute_competition_score(df)
    df["suitability_score"]    = compute_suitability_score(df)
    df["risk_score"]           = compute_risk_score(df)
    df["infrastructure_score"] = compute_infrastructure_score(df)
    df["site_readiness_score"] = compute_site_readiness(df)

    logger.info(
        f"Scoring complete. "
        f"site_readiness_score stats:\n{df['site_readiness_score'].describe()}"
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Quick smoke test with random data
    np.random.seed(42)
    n = 1000
    mock = pd.DataFrame({
        "id":                    range(n),
        "population_density":    np.random.exponential(500, n),
        "working_age_ratio":     np.random.uniform(0.5, 0.75, n),
        "income_level":          np.random.uniform(0, 100, n),
        "literacy_rate":         np.random.uniform(40, 95, n),
        "road_density":          np.random.exponential(2, n),
        "distance_to_highway":   np.random.exponential(5000, n),
        "connectivity_score":    np.random.uniform(0, 100, n),
        "avg_travel_time_20min": np.random.randint(5, 200, n),
        "competitor_count":      np.random.poisson(3, n),
        "poi_count_2km":         np.random.poisson(20, n),
        "complementary_business_count": np.random.poisson(5, n),
        "commercial_ratio":      np.random.uniform(0, 0.4, n),
        "building_density":      np.random.exponential(50, n),
        "mixed_use_ratio":       np.random.uniform(0, 0.2, n),
        "built_up_area_ratio":   np.random.uniform(0, 0.6, n),
        "aqi":                   np.random.uniform(30, 300, n),
        "pm25":                  np.random.uniform(10, 150, n),
        "flood_risk_score":      np.random.uniform(0, 1, n),
        "earthquake_risk_score": np.random.uniform(0, 1, n),
        "electricity_access_score": np.random.uniform(0, 100, n),
        "water_availability_score": np.random.uniform(0, 100, n),
        "public_transport_score":   np.random.uniform(0, 100, n),
    })

    scored = compute_all_scores(mock)
    print(scored[["id", "demand_score", "accessibility_score", "risk_score",
                  "site_readiness_score"]].head(10))

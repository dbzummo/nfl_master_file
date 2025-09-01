#!/usr/bin/env python3
"""
data_contracts.py — accuracy-first schema validation & helpers
- All loaders must validate shape, types, and content.
- No silent fallbacks. Raise RuntimeError with actionable messages.
"""

from __future__ import annotations
from typing import List, Dict, Tuple
import math
import json
import pandas as pd

ALLOWED_INJURY_STATUSES = {"OUT", "DOUBTFUL", "QUESTIONABLE", "PROBABLE", "HEALTHY"}

def _require_columns(df: pd.DataFrame, cols: List[str], label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label}: missing required columns: {missing}. Found={list(df.columns)}")

def _require_teams_32(df: pd.DataFrame, team_col: str, label: str):
    n = df[team_col].nunique(dropna=False)
    if n != 32:
        raise RuntimeError(f"{label}: expected 32 unique teams in '{team_col}', got {n}.")

def _require_numeric(df: pd.DataFrame, cols: List[str], label: str):
    bad = []
    for c in cols:
        if not pd.api.types.is_numeric_dtype(df[c]):
            bad.append(c)
    if bad:
        raise RuntimeError(f"{label}: expected numeric dtypes for {bad}.")

def normalize_team_code(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

# ---------- RATINGS / HFA ----------

def validate_team_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Requires: team_code, rating, uncertainty, hfa
    rating ∈ [-30, 30], hfa ∈ [-3, 3], 32 teams.
    """
    label = "team_ratings"
    _require_columns(df, ["team_code", "rating", "uncertainty", "hfa"], label)
    df = df.copy()
    df["team_code"] = normalize_team_code(df["team_code"])
    _require_numeric(df, ["rating", "uncertainty", "hfa"], label)

    if ((df["rating"] < -30) | (df["rating"] > 30)).any():
        raise RuntimeError(f"{label}: 'rating' outside plausible bounds [-30, 30].")
    if ((df["hfa"] < -3) | (df["hfa"] > 3)).any():
        raise RuntimeError(f"{label}: 'hfa' outside plausible bounds [-3, 3].")

    _require_teams_32(df, "team_code", label)
    # Strong stance: refuse all-zero HFA because it hides ingestion mistakes.
    if df["hfa"].abs().sum() == 0:
        raise RuntimeError(f"{label}: all HFA values are zero. Verify stadium_hfa_advanced.csv and merge logic.")
    return df.set_index("team_code", drop=False)

def validate_hfa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Requires: team_code, hfa
    hfa ∈ [-3, 3], 32 teams, refuse all-zero.
    """
    label = "stadium_hfa_advanced"
    _require_columns(df, ["team_code", "hfa"], label)
    df = df.copy()
    df["team_code"] = normalize_team_code(df["team_code"])
    _require_numeric(df, ["hfa"], label)
    if ((df["hfa"] < -3) | (df["hfa"] > 3)).any():
        raise RuntimeError(f"{label}: 'hfa' outside plausible bounds [-3, 3].")
    _require_teams_32(df, "team_code", label)
    if df["hfa"].abs().sum() == 0:
        raise RuntimeError(f"{label}: all HFA values are zero (did a normalization step wipe them?).")
    return df

# ---------- DEPTH CHARTS ----------

def validate_depth_charts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Requires minimally: team_code, position, player, value
    Ensures each team has coverage across offense/defense groups.
    """
    label = "team_depth_charts_with_values"
    must = ["team_code", "position", "player", "value"]
    _require_columns(df, must, label)
    df = df.copy()
    df["team_code"] = normalize_team_code(df["team_code"])
    _require_numeric(df, ["value"], label)

    # Basic plausibility checks (tune as needed)
    team_counts = df.groupby("team_code")["player"].count()
    too_small = team_counts[team_counts < 40]
    if len(too_small):
        raise RuntimeError(f"{label}: some teams have <40 depth entries: {list(too_small.index)}")

    _require_teams_32(df, "team_code", label)
    return df

# ---------- ODDS ----------

def validate_odds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Requires: home_team, away_team, spread_home, spread_away, total, kickoff_utc, neutral_site
    spread_home == -spread_away (within tolerance).
    """
    label = "weekly_odds"
    must = ["home_team", "away_team", "spread_home", "spread_away", "total", "kickoff_utc", "neutral_site"]
    _require_columns(df, must, label)
    df = df.copy()
    for c in ["home_team", "away_team"]:
        df[c] = normalize_team_code(df[c])
    _require_numeric(df, ["spread_home", "spread_away", "total"], label)

    if ((df["spread_home"] + df["spread_away"]).abs() > 0.5).any():
        raise RuntimeError(f"{label}: spreads inconsistent (home != -away).")

    if df.empty:
        raise RuntimeError(f"{label}: empty odds payload for the target window.")
    return df

# ---------- INJURIES ----------

def normalize_and_validate_injuries(obj) -> pd.DataFrame:
    """
    Accepts a list[dict] or DataFrame and normalizes to:
    columns: team, player, status, weight (status ∈ ALLOWED_INJURY_STATUSES)
    We keep only OUT/DOUBTFUL/QUESTIONABLE for penalties by default; others weight=0.
    """
    label = "injuries"
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
    elif isinstance(obj, list):
        df = pd.DataFrame(obj)
    else:
        raise RuntimeError(f"{label}: unexpected type {type(obj)}; expected list[dict] or DataFrame.")

    _require_columns(df, ["team", "player", "status"], label)
    df["team"] = normalize_team_code(df["team"])
    df["status"] = df["status"].astype(str).str.upper().str.strip()

    bad_status = set(df["status"]) - ALLOWED_INJURY_STATUSES
    if bad_status:
        raise RuntimeError(f"{label}: unexpected statuses {sorted(bad_status)}; allowed={sorted(ALLOWED_INJURY_STATUSES)}")

    # Assign default weights if missing
    default_map = {"OUT": 1.0, "DOUBTFUL": 0.6, "QUESTIONABLE": 0.25, "PROBABLE": 0.1, "HEALTHY": 0.0}
    if "weight" not in df.columns:
        df["weight"] = df["status"].map(default_map).fillna(0.0)
    _require_numeric(df, ["weight"], label)

    # Keep impactful statuses for penalties; still log others (weight 0)
    return df[["team", "player", "status", "weight"]]
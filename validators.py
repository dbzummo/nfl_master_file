from __future__ import annotations

from typing import Iterable, List, Dict, Optional
import pandas as pd

# --- Team alias normalization -------------------------------------------------

TEAM_ALIASES = {
    # Washington
    "WAS": "WSH", "WFT": "WSH", "COMMANDERS": "WSH",
    # Los Angeles teams
    "STL": "LAR", "SD": "LAC",
    # Vegas
    "OAK": "LV", "RAIDERS": "LV",
    # Misc historical/common
    "JAX": "JAX", "JAC": "JAX",
    "NWE": "NE", "N.E.": "NE",
    "GNB": "GB",
    "SFO": "SF",
    "NOR": "NO",
    "KAN": "KC",
    "TAM": "TB",
    "N.Y. JETS": "NYJ", "NY JETS": "NYJ", "NEW YORK JETS": "NYJ",
    "N.Y. GIANTS": "NYG", "NY GIANTS": "NYG", "NEW YORK GIANTS": "NYG",
}

def _normalize_team_code(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    return TEAM_ALIASES.get(s, s)

def apply_aliases(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    """
    Return a copy of df with all team code columns normalized to our canonical codes.
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].astype(str).map(_normalize_team_code)
    return out


# --- Value validators ---------------------------------------------------------

def _require_columns(df: pd.DataFrame, name: str, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")

def validate_ratings(ratings: pd.DataFrame, strict: bool = True) -> None:
    """
    Checks:
      - required columns present
      - no nulls in team_code, rating, uncertainty, hfa
      - hfa within a sane range (-3..3)
      - unique team_code
    """
    name = "ratings"
    _require_columns(ratings, name, ["team_code","rating","uncertainty","hfa"])
    r = ratings.copy()
    r["team_code"] = r["team_code"].astype(str).str.strip().str.upper()

    # Null checks
    bad_nulls = r[r[["team_code","rating","uncertainty","hfa"]].isna().any(axis=1)]
    if not bad_nulls.empty and strict:
        raise ValueError(f"{name}: nulls found in key columns (first rows):\n{bad_nulls.head()}")

    # HFA range
    if ((r["hfa"] < -3.0) | (r["hfa"] > 3.0)).any() and strict:
        raise ValueError(f"{name}: 'hfa' outside [-3, 3] range detected.")

    # Unique team codes
    dups = r["team_code"][r["team_code"].duplicated(keep=False)].unique().tolist()
    if dups and strict:
        raise ValueError(f"{name}: duplicate team_code entries: {dups}")

def validate_odds(odds: pd.DataFrame, ratings: pd.DataFrame, strict: bool = True) -> None:
    """
    Checks:
      - required columns present
      - teams exist in ratings team universe
      - line/total within sane windows (spread in [-30, 30], total in [25, 80])
      - kickoff_utc parseable datetimes
      - no duplicate (home, away, kickoff_utc) rows
    """
    name = "odds"
    _require_columns(odds, name, ["home_team","away_team","spread_home","spread_away","total","kickoff_utc","neutral_site"])
    teams = set(ratings["team_code"].astype(str).str.upper())

    o = odds.copy()
    o["home_team"] = o["home_team"].astype(str).str.upper()
    o["away_team"] = o["away_team"].astype(str).str.upper()
    o["_k"] = pd.to_datetime(o["kickoff_utc"], errors="coerce", utc=True)

    # Team universe check
    missing_home = sorted(set(o["home_team"]) - teams)
    missing_away = sorted(set(o["away_team"]) - teams)
    if strict and (missing_home or missing_away):
        raise ValueError(f"{name}: teams not in ratings -> home:{missing_home} away:{missing_away}")

    # Sanity windows
    bad_spread = (~o["spread_home"].between(-30, 30)) | (~o["spread_away"].between(-30, 30))
    bad_total  = (~o["total"].between(25, 80))
    if strict and (bad_spread.any() or bad_total.any()):
        raise ValueError(f"{name}: out-of-range spreads/totals detected.")

    # Time parse
    if strict and o["_k"].isna().any():
        raise ValueError(f"{name}: unparseable kickoff_utc values present.")

    # Duplicate games
    dups = o.duplicated(subset=["home_team","away_team","kickoff_utc"], keep=False)
    if strict and dups.any():
        raise ValueError(f"{name}: duplicate (home,away,kickoff_utc) rows detected.")

def validate_depth(depth: pd.DataFrame, strict: bool = True) -> None:
    """
    Checks:
      - required columns present
      - team_code not null/empty
      - value numeric
    """
    name = "depth_charts"
    _require_columns(depth, name, ["team_code","position","player","value"])
    d = depth.copy()
    d["team_code"] = d["team_code"].astype(str).str.upper().str.strip()
    if strict and ((d["team_code"] == "") | (d["team_code"].isna())).any():
        raise ValueError(f"{name}: blank team_code rows present.")

    # value numeric check
    try:
        pd.to_numeric(d["value"])
    except Exception:
        if strict:
            raise ValueError(f"{name}: 'value' column must be numeric-coercible.")

def validate_injuries(inj: pd.DataFrame, strict: bool = False) -> None:
    """
    Checks (lenient by default):
      - columns exist if df is non-empty
      - team_code / player not null/empty
      - status (if present) in allowed set (lenient: warn only)
    """
    if inj is None or not isinstance(inj, pd.DataFrame) or inj.empty:
        return

    name = "injuries"
    _require_columns(inj, name, ["team_code","player"])
    i = inj.copy()
    i["team_code"] = i["team_code"].astype(str).str.upper().str.strip()
    i["player"] = i["player"].astype(str).str.strip()

    if strict and ((i["team_code"] == "") | i["team_code"].isna() | (i["player"] == "") | i["player"].isna()).any():
        raise ValueError(f"{name}: blank team_code/player rows present.")

    if "status" in i.columns:
        allowed = {"IR","PUP","NFI","SUSPENDED","OUT","DOUBTFUL","QUESTIONABLE","PROBABLE","NA",""}
        bad = sorted(set(i["status"].astype(str).str.upper()) - allowed)
        if bad and strict:
            raise ValueError(f"{name}: unexpected status values {bad}")

__all__ = [
    "apply_aliases",
    "validate_odds",
    "validate_ratings",
    "validate_depth",
    "validate_injuries",
]

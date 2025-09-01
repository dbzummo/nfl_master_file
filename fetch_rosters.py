#!/usr/bin/env python3
"""
fetch_rosters.py
- Unified roster providers with team code mapping (multi-candidate retries)
- Primary: SportsDataIO (needs SPORTSDATAIO_API_KEY in env)
- Secondary: local nflverse CSV mirrors (optional) under ./data/
"""

import os
import json
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Dict

import pandas as pd

# ---------- TEAM CODE MAPS (multi-candidate) ----------
# For each internal code, try these provider codes in order until one yields players.
SPORTSDATAIO_CANDIDATES: Dict[str, List[str]] = {
    # Known tricky ones:
    "WSH": ["WAS", "WSH"],
    "JAX": ["JAX", "JAC"],  # <-- key fix: try JAX first, then JAC
    "LAR": ["LAR", "LA"],
    "LAC": ["LAC", "SD"],   # historical fallback, just in case provider quirks
    "ARI": ["ARI", "ARZ"],  # some feeds use ARZ
    "NO":  ["NO", "NOR"],
    "NE":  ["NE", "NWE"],
    "GB":  ["GB", "GNB"],
    "SF":  ["SF", "SFO"],
    "TB":  ["TB", "TAM"],
    "KC":  ["KC", "KAN"],
    "LV":  ["LV", "OAK"],   # legacy fallback
    # Most others are stable:
    "PHI": ["PHI"], "DAL": ["DAL"], "NYG": ["NYG"], "ATL": ["ATL"], "CAR": ["CAR"],
    "CIN": ["CIN"], "CLE": ["CLE"], "MIA": ["MIA"], "IND": ["IND"], "PIT": ["PIT"],
    "NYJ": ["NYJ"], "TEN": ["TEN"], "DEN": ["DEN"], "SEA": ["SEA"], "DET": ["DET"],
    "HOU": ["HOU"], "BAL": ["BAL"], "BUF": ["BUF"], "MIN": ["MIN"], "CHI": ["CHI"]
}

def _candidates_for_sportsdataio(team: str) -> List[str]:
    return SPORTSDATAIO_CANDIDATES.get(team, [team])

# ---------- Helpers ----------
def _http_get_json(url: str, headers: Dict[str, str] = None, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    return json.loads(data.decode("utf-8"))

def _clean_df(df: pd.DataFrame, team: str, source: str) -> pd.DataFrame:
    # Normalize columns we use downstream
    cols = {
        "Team": "team", "team": "team",
        "Player": "player", "player": "player",
        "Position": "position", "position": "position",
        "Depth": "depth", "depth": "depth",
        "Status": "status", "status": "status",
        "Name": "player"  # common from APIs
    }
    rename = {k: v for k, v in cols.items() if k in df.columns}
    df = df.rename(columns=rename)

    # Ensure required columns exist
    for c in ["team", "player", "position"]:
        if c not in df.columns:
            df[c] = None
    if "depth" not in df.columns:
        df["depth"] = None
    if "status" not in df.columns:
        df["status"] = None

    df["team"] = team
    df["source"] = source

    # Basic cleanup
    df["player"] = df["player"].astype(str).str.strip()
    df["position"] = df["position"].astype(str).str.strip().str.upper()

    return df[["team", "player", "position", "depth", "status", "source"]]

# ---------- Providers ----------
def get_roster_sportsdataio(team: str) -> pd.DataFrame:
    """
    Fetch roster from SportsDataIO.
    Expects SPORTSDATAIO_API_KEY in env.
    Tries multiple candidate codes until a non-empty roster is returned.
    """
    api_key = os.getenv("SPORTSDATAIO_API_KEY")
    if not api_key:
        print("⚠️  SPORTS provider skipped: SPORTSDATAIO_API_KEY missing")
        return pd.DataFrame(columns=["team","player","position","depth","status","source"])

    base = "https://api.sportsdata.io/v3/nfl/scores/json/Players"
    tried = []
    for code in _candidates_for_sportsdataio(team):
        url = f"{base}/{urllib.parse.quote(code)}?key={urllib.parse.quote(api_key)}"
        try:
            data = _http_get_json(url)
            if isinstance(data, list) and len(data) > 0:
                rows = []
                for p in data:
                    rows.append({
                        "team": team,
                        "player": (p.get("Name") or f"{p.get('FirstName','')} {p.get('LastName','')}".strip()),
                        "position": (p.get("Position") or "").upper(),
                        "depth": p.get("DepthChartOrder"),
                        "status": p.get("Status"),
                        "source": "sportsdataio",
                    })
                df = pd.DataFrame(rows)
                df = _clean_df(df, team, "sportsdataio")
                print(f"SPORTS {team} ← {code}: {len(df)} players")
                return df
            else:
                tried.append(code)
                print(f"⚠️  SPORTS {team} ← {code}: empty payload")
        except Exception as e:
            tried.append(code)
            print(f"⚠️  SPORTS fetch failed for {team} ({code}): {e}")

    print(f"⚠️  SPORTS {team}: all codes tried with no success: {tried}")
    return pd.DataFrame(columns=["team","player","position","depth","status","source"])

def get_roster_nflverse(team: str) -> pd.DataFrame:
    """
    Secondary/local CSV mirror for audit comparisons.
    If present, file path: ./data/nflverse_rosters_{TEAM}.csv
    """
    p = Path("data") / f"nflverse_rosters_{team}.csv"
    if not p.exists():
        return pd.DataFrame(columns=["team","player","position","depth","status","source"])
    try:
        df = pd.read_csv(p)
        df = _clean_df(df, team, "nflverse_local")
        print(f"NFVERSE {team}: {len(df)} players from {p}")
        return df
    except Exception as e:
        print(f"⚠️  NFVERSE read failed for {team}: {e}")
        return pd.DataFrame(columns=["team","player","position","depth","status","source"])

def get_roster_for_audit(team: str) -> Dict[str, pd.DataFrame]:
    """Return both providers for the audit layer."""
    return {
        "primary": get_roster_sportsdataio(team),
        "secondary": get_roster_nflverse(team),
    }
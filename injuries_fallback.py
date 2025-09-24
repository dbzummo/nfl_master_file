#!/usr/bin/env python3
"""
injuries_fallbacks.py — conservative injuries fallback derived from roster status
When live feeds are empty/plan-locked, we at least mark IR/PUP/NFI/Suspended players as unavailable.

Output schema (DataFrame):
  ['team_code','player','status','position']
"""

from __future__ import annotations
from typing import Iterable, List, Dict
import pandas as pd

# import your existing roster helpers
from fetch_rosters import get_roster_sportsdataio  # iff you have nflverse too, you can blend later

# Map common roster statuses -> simple injury-like status
STATUS_MAP = {
    "INJURED RESERVE": "IR",
    "IR": "IR",
    "PUP": "PUP",
    "PUP-R": "PUP",
    "NFI": "NFI",
    "NON-FOOTBALL INJURY": "NFI",
    "SUSPENDED": "Suspended",
    "SUS": "Suspended",
    "OUT": "Out",  # sometimes present in roster feeds, often not
}

def _normalize_status(s: str) -> str:
    if not s:
        return ""
    u = str(s).strip().upper()
    for key, out in STATUS_MAP.items():
        if key in u:
            return out
    return ""

def derive_injuries_from_rosters(teams: Iterable[str]) -> pd.DataFrame:
    rows: List[Dict] = []
    for t in teams:
        try:
            r = get_roster_sportsdataio(t)  # columns: team, player, position, status, ...
        except Exception:
            r = pd.DataFrame(columns=["team","player","position","status"])

        if r.empty:
            continue

        # normalize columns
        r_cols = {c.lower().strip(): c for c in r.columns}
        team_col = r_cols.get("team", None)
        ply_col  = r_cols.get("player", None)
        pos_col  = r_cols.get("position", None)
        stat_col = r_cols.get("status", None)

        if not all([team_col, ply_col]):
            continue

        # derive simplified status
        r["_status_simplified"] = r[stat_col].astype(str).map(_normalize_status) if stat_col else ""

        # keep only definite long-term unavailabilities
        keep = r[r["_status_simplified"].isin(["IR","PUP","NFI","Suspended"])]
        for _, x in keep.iterrows():
            rows.append({
                "team_code": str(x[team_col]).strip().upper(),
                "player":    str(x[ply_col]).strip(),
                "position":  (str(x[pos_col]).strip().upper() if pos_col else ""),
                "status":    str(x["_status_simplified"]),
            })

    if not rows:
        return pd.DataFrame(columns=["team_code","player","status","position"])

    df = pd.DataFrame(rows).drop_duplicates()
    # final polish
    for c in ["team_code","player","status","position"]:
        df[c] = df[c].astype(str).str.strip()
    df = df[(df["team_code"]!="") & (df["player"]!="")]
    return df.reset_index(drop=True)

#!/usr/bin/env python3
from __future__ import annotations
from typing import Iterable, List, Dict
import pandas as pd
from fetch_rosters import get_roster_sportsdataio

STATUS_MAP = {
    "INJURED RESERVE": "IR",
    "IR": "IR",
    "PUP": "PUP",
    "PUP-R": "PUP",
    "NFI": "NFI",
    "NON-FOOTBALL INJURY": "NFI",
    "SUSPENDED": "Suspended",
    "SUS": "Suspended",
    "OUT": "Out",
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
            r = get_roster_sportsdataio(t)
        except Exception:
            r = pd.DataFrame(columns=["team","player","position","status"])

        if r.empty:
            continue

        cols = {c.lower().strip(): c for c in r.columns}
        team_col = cols.get("team")
        ply_col  = cols.get("player")
        pos_col  = cols.get("position")
        stat_col = cols.get("status")
        if not (team_col and ply_col):
            continue

        if stat_col:
            r["_status_simplified"] = r[stat_col].astype(str).map(_normalize_status)
        else:
            r["_status_simplified"] = ""

        keep = r[r["_status_simplified"].isin(["IR","PUP","NFI","Suspended"])]
        for _, x in keep.iterrows():
            rows.append({
                "team_code": str(x[team_col]).strip().upper(),
                "player":    str(x[ply_col]).strip(),
                "position":  (str(x[pos_col]).strip().upper() if pos_col else ""),
                "status":    str(x["_status_simplified"]).strip(),
            })

    if not rows:
        return pd.DataFrame(columns=["team_code","player","status","position"])

    df = pd.DataFrame(rows).drop_duplicates()
    for c in ["team_code","player","status","position"]:
        df[c] = df[c].astype(str).str.strip()
    df = df[(df["team_code"] != "") & (df["player"] != "")]
    return df.reset_index(drop=True)

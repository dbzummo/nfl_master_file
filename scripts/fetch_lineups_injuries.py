#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch v2 MSF injuries + game lineups for the current msf_week window
and write normalized CSVs with permanent guardrails.

Outputs:
  out/msf_details/injuries_week.csv
    team,player_id,player,position,status,description,last_updated,date
  out/msf_details/lineups_week.csv
    msf_game_id,week,startTime_utc,date,team,lineup_type,unit,slot,
    is_starter,player_id,player,position,jersey
"""

from __future__ import annotations
import os, time, json, sys, csv, argparse
from pathlib import Path
from typing import Any, Dict, List
import requests
import pandas as pd

BASE = "https://api.mysportsfeeds.com/v2.1/pull/nfl"
OUT_DIR = Path("out/msf_details")
MSF_WEEK = Path("out/msf_week.csv")

def _auth():
    key = os.getenv("MSF_KEY", "")
    pw  = os.getenv("MSF_PASS", "")
    return (key, pw)

def _get(url: str, params: Dict[str,Any]|None=None, retries=3, backoff=0.7):
    auth = _auth()
    for i in range(retries):
        try:
            r = requests.get(url, params=params or {}, auth=auth, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff*(i+1))
                continue
            return r
        except requests.RequestException:
            time.sleep(backoff*(i+1))
    return None

def _read_week() -> pd.DataFrame:
    cols = ["date","away_team","home_team","msf_game_id","week","startTime_utc"]
    if MSF_WEEK.exists():
        df = pd.read_csv(MSF_WEEK)
    else:
        df = pd.DataFrame(columns=cols)
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df["msf_game_id"] = pd.to_numeric(df["msf_game_id"], errors="coerce")
    df["date"] = df["date"].astype(str).str[:10]
    df["away_team"] = df["away_team"].fillna("").astype(str).str.upper()
    df["home_team"] = df["home_team"].fillna("").astype(str).str.upper()
    return df

def _teams_from_week(wk: pd.DataFrame) -> List[str]:
    return sorted(set([t for t in (wk["away_team"].tolist()+wk["home_team"].tolist()) if t]))

def _gids_from_week(wk: pd.DataFrame) -> List[int]:
    return sorted(set(wk["msf_game_id"].dropna().astype(int).tolist()))

# ---------------- Injuries ----------------

def fetch_injuries(teams: List[str]) -> pd.DataFrame:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    url = f"{BASE}/injuries.json"
    r = _get(url, params={"force":"false"})
    inj = r.json() if (r and r.status_code==200) else {}

    # cache raw
    try:
        (OUT_DIR/"injuries_raw_latest.json").write_text(json.dumps(inj))
    except Exception:
        pass

    rows = []
    last_updated = inj.get("lastUpdatedOn")
    teams_lc = {t.lower() for t in teams}
    for p in (inj.get("players") or []):
        team = (((p.get("currentTeam") or {}).get("abbreviation")) or "").upper()
        if not team or team.lower() not in teams_lc:
            continue
        cur = p.get("currentInjury") or {}
        if not cur:
            continue
        rows.append({
            "team": team,
            "player_id": p.get("id"),
            "player": " ".join(filter(None,[p.get("firstName"), p.get("lastName")])),
            "position": p.get("primaryPosition"),
            "status": cur.get("status") or cur.get("designation") or cur.get("shortName"),
            "description": cur.get("desc") or cur.get("description") or cur.get("notes"),
            "last_updated": last_updated,
            "date": (str(last_updated)[:10] if last_updated else pd.NA),
        })
    df = pd.DataFrame(rows, columns=[
        "team","player_id","player","position","status","description","last_updated","date"
    ])
    if not df.empty:
        df["team"] = df["team"].astype(str).str.upper()
        df["date"] = df["date"].astype(str).str[:10]
    outp = OUT_DIR/"injuries_week.csv"
    df.to_csv(outp, index=False)
    print(f"[injuries][ok] wrote {outp} rows={len(df)} teams={teams}")
    return df

# ---------------- Lineups ----------------

def _parse_lineup_json(path: Path) -> List[Dict[str,Any]]:
    try:
        d = json.loads(path.read_text())
    except Exception:
        return []
    g = d.get("game") or {}
    gid = g.get("id")
    week = g.get("week")
    start = g.get("startTime")
    rows: List[Dict[str,Any]] = []
    for tl in (d.get("teamLineups") or []):
        team = ((tl.get("team") or {}).get("abbreviation") or "").upper()
        for ltype in ("actual",):
            sec = tl.get(ltype) or {}
            for lp in (sec.get("lineupPositions") or []):
                pos = lp.get("position")
                pl  = lp.get("player") or {}
                rows.append({
                    "msf_game_id": gid,
                    "week": week,
                    "startTime_utc": start,
                    "date": (str(start)[:10] if start else None),
                    "team": team,
                    "lineup_type": ltype,
                    "unit": (pos or "").split("-")[0].lower(),
                    "slot": pos,
                    "is_starter": str(pos or "").endswith("-1"),
                    "player_id": pl.get("id"),
                    "player": " ".join(filter(None,[pl.get("firstName"), pl.get("lastName")])),
                    "position": pl.get("position") or pl.get("primaryPosition"),
                    "jersey": pl.get("jerseyNumber"),
                })
    return rows

def fetch_lineups(gids: List[int], season: str) -> pd.DataFrame:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str,Any]] = []
    for gid in gids:
        url = f"{BASE}/{season}/games/{gid}/lineup.json"
        r = _get(url, params={"lineuptype":"actual"})
        if not r or r.status_code != 200:
            print(f"[lineups][WARN] {gid} HTTP {getattr(r,'status_code', 'NA')}")
            continue
        path = OUT_DIR / f"lineup_{gid}_actual.json"
        try:
            path.write_bytes(r.content)
        except Exception:
            pass
        rows.extend(_parse_lineup_json(path))

    cols = ["msf_game_id","week","startTime_utc","date","team","lineup_type",
            "unit","slot","is_starter","player_id","player","position","jersey"]
    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df["team"] = df["team"].astype(str).str.upper()
        df["position"] = df["position"].fillna("").astype(str).str.upper()
        df["is_starter"] = df["is_starter"].astype(str).str.lower().isin(
            ["true","1","t","yes","y"]).astype(int)
        df["msf_game_id"] = pd.to_numeric(df["msf_game_id"], errors="coerce").astype("Int64")
        df["date"] = df["date"].astype(str).str[:10]
    outp = OUT_DIR/"lineups_week.csv"
    df.to_csv(outp, index=False)
    print(f"[lineups][ok] wrote {outp} rows={len(df)}")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2025-regular")
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    wk = _read_week()
    teams = _teams_from_week(wk)
    gids  = _gids_from_week(wk)

    fetch_injuries(teams)
    fetch_lineups(gids, args.season)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
fetch_injuries.py — resilient SportsDataIO fetcher with multi-endpoint fallback.

Normalized output columns: ['team_code','player','status','position']

Env overrides:
  SPORTSDATAIO_API_KEY=...     (required for live fetch)
  INJURIES_SEASON=2025         (optional)
  INJURIES_WEEK=1              (optional; use integer)
  INJURIES_ENDPOINT=week|season|basic|byteam  (optional; forces one endpoint)
"""

from __future__ import annotations
from typing import Optional, List, Dict, Any, Tuple
import os, datetime as dt
import pandas as pd

try:
    import requests
except Exception:
    requests = None

NFL_TEAMS = [
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB",
    "HOU","IND","JAX","KC","LAC","LAR","LV","MIA","MIN","NE","NO","NYG","NYJ",
    "PHI","PIT","SEA","SF","TB","TEN","WAS","WSH"
]
NFL_TEAMS = sorted(set(t.replace("WAS","WSH") for t in NFL_TEAMS))

def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def _infer_season_week() -> Tuple[int, Optional[int]]:
    d = _now_utc()
    season = d.year if d.month >= 6 else d.year - 1
    # Leave week None unless provided; many endpoints work season-only.
    w_env = os.getenv("INJURIES_WEEK")
    week = int(w_env) if (w_env and w_env.isdigit()) else None
    s_env = os.getenv("INJURIES_SEASON")
    if s_env and s_env.isdigit():
        season = int(s_env)
    return season, week

def _normalize_rows(items: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for it in items or []:
        team = str(it.get("Team") or it.get("TeamKey") or it.get("TeamAbbr") or "").strip().upper()
        player = str(it.get("Name") or it.get("PlayerName") or it.get("ShortName") or "").strip()
        pos = str(it.get("Position") or "").strip()
        status = str(it.get("InjuryStatus") or it.get("Status") or "").strip()
        if team and player:
            rows.append({"team_code": team, "player": player, "status": status, "position": pos})
    if not rows:
        return pd.DataFrame(columns=["team_code","player","status","position"])
    df = pd.DataFrame(rows).drop_duplicates()
    for c in ["team_code","player","status","position"]:
        df[c] = df[c].astype(str).str.strip()
    return df.reset_index(drop=True)

def _get(url: str, headers: Dict[str, str], timeout: int = 15) -> Tuple[int, Any]:
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        code = r.status_code
        try:
            js = r.json()
        except Exception:
            js = None
        return code, js
    except Exception as e:
        print(f"⚠️ Request failed: {e} [{url}]")
        return -1, None

def _try_week(season: int, week: int, headers: Dict[str,str]) -> pd.DataFrame:
    url = f"https://api.sportsdata.io/v3/nfl/injuries/json/InjuriesByWeek/{season}/{week}"
    code, js = _get(url, headers)
    if code == 200 and isinstance(js, list):
        df = _normalize_rows(js)
        print(f"ℹ️ InjuriesByWeek: {len(df)} rows (season={season}, week={week})")
        return df
    print(f"⚠️ InjuriesByWeek returned {code}")
    return pd.DataFrame(columns=["team_code","player","status","position"])

def _try_season(season: int, headers: Dict[str,str]) -> pd.DataFrame:
    url = f"https://api.sportsdata.io/v3/nfl/injuries/json/Injuries/{season}"
    code, js = _get(url, headers)
    if code == 200 and isinstance(js, list):
        df = _normalize_rows(js)
        print(f"ℹ️ Injuries (season): {len(df)} rows (season={season})")
        return df
    print(f"⚠️ Injuries (season) returned {code}")
    return pd.DataFrame(columns=["team_code","player","status","position"])

def _try_basic(season: int, headers: Dict[str,str]) -> pd.DataFrame:
    url = f"https://api.sportsdata.io/v3/nfl/injuries/json/InjuriesBasic/{season}"
    code, js = _get(url, headers)
    if code == 200 and isinstance(js, list):
        df = _normalize_rows(js)
        print(f"ℹ️ InjuriesBasic: {len(df)} rows (season={season})")
        return df
    print(f"⚠️ InjuriesBasic returned {code}")
    return pd.DataFrame(columns=["team_code","player","status","position"])

def _try_byteam(season: int, headers: Dict[str,str]) -> pd.DataFrame:
    # Some plans only expose team-level endpoints; aggregate them.
    base = "https://api.sportsdata.io/v3/nfl/injuries/json/InjuriesByTeam"
    frames = []
    for t in NFL_TEAMS:
        url = f"{base}/{season}/{t}"
        code, js = _get(url, headers)
        if code == 200 and isinstance(js, list):
            frames.append(_normalize_rows(js))
        else:
            # Don’t spam logs; show a single-line summary when done
            pass
    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates()
        print(f"ℹ️ InjuriesByTeam aggregated: {len(df)} rows across {len(frames)} teams (season={season})")
        return df
    print("⚠️ InjuriesByTeam returned no data for all teams (likely plan-locked).")
    return pd.DataFrame(columns=["team_code","player","status","position"])

def fetch_injured_players(season: Optional[int] = None, week: Optional[int] = None) -> pd.DataFrame:
    if requests is None:
        print("⚠️ 'requests' is not installed; returning empty injuries.")
        return pd.DataFrame(columns=["team_code","player","status","position"])

    api_key = os.getenv("SPORTSDATAIO_API_KEY")
    if not api_key:
        print("⚠️ SPORTSDATAIO_API_KEY not set; returning empty injuries.")
        return pd.DataFrame(columns=["team_code","player","status","position"])

    season_inferred, week_inferred = _infer_season_week()
    season = season if season is not None else season_inferred
    week = week if week is not None else week_inferred

    headers = {"Ocp-Apim-Subscription-Key": api_key}
    forced = (os.getenv("INJURIES_ENDPOINT") or "").lower().strip()

    tried = []
    out = pd.DataFrame(columns=["team_code","player","status","position"])

    # Forced endpoint path (if user wants to constrain)
    if forced in {"week","season","basic","byteam"}:
        if forced == "week":
            if week is None:
                print("⚠️ INJURIES_ENDPOINT=week but week is None; skipping.")
            else:
                tried.append("InjuriesByWeek")
                out = _try_week(season, week, headers)
        elif forced == "season":
            tried.append("Injuries")
            out = _try_season(season, headers)
        elif forced == "basic":
            tried.append("InjuriesBasic")
            out = _try_basic(season, headers)
        elif forced == "byteam":
            tried.append("InjuriesByTeam")
            out = _try_byteam(season, headers)
    else:
        # Smart fallback chain
        if week is not None:
            tried.append("InjuriesByWeek")
            out = _try_week(season, week, headers)
        if out.empty:
            tried.append("Injuries")
            out = _try_season(season, headers)
        if out.empty:
            tried.append("InjuriesBasic")
            out = _try_basic(season, headers)
        if out.empty:
            tried.append("InjuriesByTeam")
            out = _try_byteam(season, headers)

    if out.empty:
        print("⚠️ Injuries feed still empty after trying endpoints:", ", ".join(tried))
        print("   → Likely plan restriction or out-of-window season/week. Set INJURIES_ENDPOINT=basic or byteam,")
        print("     or provide INJURIES_SEASON / INJURIES_WEEK to force a known-good window.")
    else:
        print(f"ℹ️ Injuries normalized: {len(out)} rows from {', '.join(tried)}")

    return out

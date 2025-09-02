
from __future__ import annotations
import os, json, time
from typing import Any, Dict, List, Optional
import pandas as pd
import requests

_COLS = ["home_team","away_team","vegas_line","vegas_total","kickoff_utc","neutral_site"]

def _as_df(obj: Optional[Any]) -> pd.DataFrame:
    """
    Normalize any provider result to our standard DataFrame, never None.
    Expected keys if obj is iterable of dicts:
      home_team, away_team, vegas_line, vegas_total, kickoff_utc, neutral_site
    """
    try:
        df = pd.DataFrame(list(obj) if obj is not None else [])
    except Exception:
        df = pd.DataFrame([])
    # ensure columns
    for c in _COLS:
        if c not in df.columns:
            df[c] = None
    # order and copy
    return df[_COLS].copy()

# ------------------------ OddsAPI (primary) ------------------------

def _oddsapi_fetch() -> pd.DataFrame:
    """
    Direct call to The Odds API v4.
    Env keys tried: ODDS_API_KEY, THEODDS_API_KEY
    Regions/markets kept minimal for performance.
    """
    api_key = os.getenv("ODDS_API_KEY") or os.getenv("THEODDS_API_KEY")
    if not api_key:
        return _as_df(None)

    url = (
        "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/"
        "?regions=us&markets=spreads,totals&oddsFormat=american&dateFormat=iso"
        f"&apiKey={api_key}"
    )

    t0 = time.time()
    try:
        r = requests.get(url, timeout=25)
    except Exception as e:
        _write_oddsapi_error(f"request exception: {e!r}")
        return _as_df(None)

    if not r or r.status_code != 200:
        excerpt = ""
        try:
            excerpt = r.text if r is not None else ""
        except Exception:
            pass
        _write_oddsapi_error(f"HTTP {getattr(r,'status_code',None)}: {excerpt[:400]}")
        return _as_df(None)

    try:
        data = r.json()
    except Exception as e:
        _write_oddsapi_error(f"json error: {e!r}")
        return _as_df(None)

    # optional debug dump (helps later diagnostics)
    try:
        with open("out_oddsapi_raw.json","w",encoding="utf-8") as f:
            json.dump({"fetched_at": time.time(), "sample": (data[:3] if isinstance(data, list) else data)}, f, indent=2)
    except Exception:
        pass

    if not isinstance(data, list) or not data:
        return _as_df(None)

    def _consensus_spread(markets: List[Dict[str,Any]], home_team: str) -> Optional[float]:
        # Find "spreads" market across books and median the home spread if present
        values = []
        for bk in markets or []:
            for m in (bk.get("markets") or []):
                if (m.get("key") or "").lower() != "spreads":
                    continue
                for o in (m.get("outcomes") or []):
                    t = o.get("name")
                    pt = o.get("point")
                    if t == home_team and isinstance(pt, (int,float)):
                        values.append(float(pt))
        if not values:
            return None
        values.sort()
        mid = len(values)//2
        return (values[mid] if len(values)%2==1 else (values[mid-1]+values[mid])/2.0)

    def _consensus_total(markets: List[Dict[str,Any]]) -> Optional[float]:
        # Find "totals" market and median the line (Over/Under usually share same point)
        values = []
        for bk in markets or []:
            for m in (bk.get("markets") or []):
                if (m.get("key") or "").lower() != "totals":
                    continue
                for o in (m.get("outcomes") or []):
                    pt = o.get("point")
                    if isinstance(pt, (int,float)):
                        values.append(float(pt))
        if not values:
            return None
        values.sort()
        mid = len(values)//2
        return (values[mid] if len(values)%2==1 else (values[mid-1]+values[mid])/2.0)

    rows = []
    for g in data:
        home = g.get("home_team")
        away = g.get("away_team")
        kickoff = g.get("commence_time")  # ISO string
        books = g.get("bookmakers") or []
        sp = _consensus_spread(books, home) if home else None
        tot = _consensus_total(books)

        # We define vegas_line as spread for home (negative if home is favorite)
        # The Odds API spreads are usually in home-team terms; our median should reflect that directly.
        rows.append({
            "home_team": home,
            "away_team": away,
            "vegas_line": sp,
            "vegas_total": tot,
            "kickoff_utc": kickoff,
            "neutral_site": False
        })

    df = _as_df(rows)
    # drop rows with no teams
    df = df.dropna(subset=["home_team","away_team"]).reset_index(drop=True)
    return df

def _write_oddsapi_error(msg: str) -> None:
    try:
        with open("out_oddsapi_error.txt","a",encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}\n")
    except Exception:
        pass

# ------------------------ SportsDataIO (fallback) ------------------------

def _sio_fetch() -> pd.DataFrame:
    """
    Uses an external helper module if available: sportsdataio_provider.fetch_game_odds_by_week(api_key, season, week)
    Env:
      SPORTSDATAIO_API_KEY, SIO_SEASON, SIO_WEEK
    Returns empty if missing or quota/plan blocks.
    """
    api_key = os.getenv("SPORTSDATAIO_API_KEY")
    season  = os.getenv("SIO_SEASON")
    week    = os.getenv("SIO_WEEK")
    if not (api_key and season and week):
        return _as_df(None)
    try:
        from sportsdataio_provider import fetch_game_odds_by_week
    except Exception:
        return _as_df(None)

    try:
        rows: List[Dict[str,Any]] = fetch_game_odds_by_week(api_key, season, week)
        return _as_df(rows)
    except Exception:
        return _as_df(None)

# ------------------------ public entry ------------------------

def get_consensus_nfl_odds() -> pd.DataFrame:
    """
    Deterministic router:
      1) Try OddsAPI (primary).
      2) If empty, try SIO (only if env provided).
      3) Always return a DataFrame with _COLS (possibly empty).
    """
    df = _oddsapi_fetch()
    if isinstance(df, pd.DataFrame) and len(df) > 0:
        return df

    df2 = _sio_fetch()
    if isinstance(df2, pd.DataFrame) and len(df2) > 0:
        return df2

    return _as_df(None)

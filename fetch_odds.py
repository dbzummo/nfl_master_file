# fetch_odds.py
import os
import sys
import json
import time
import math
import datetime as dt
from typing import List, Dict, Any, Optional, Tuple

import requests
import pandas as pd

THE_ODDS_BASE = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"

# -------------------------------------------------------------------
# ENV / KEY HANDLING
# -------------------------------------------------------------------
def _get_odds_key() -> Optional[str]:
    """
    Accept either THE_ODDS_API_KEY or ODDS_API_KEY to avoid env mismatches.
    """
    return (os.getenv("THE_ODDS_API_KEY") or os.getenv("ODDS_API_KEY") or "").strip() or None

# -------------------------------------------------------------------
# DATES
# -------------------------------------------------------------------
def _to_utc_window(week_start_utc: str, week_end_utc: str) -> Tuple[dt.datetime, dt.datetime]:
    def _parse(s: str) -> dt.datetime:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    return _parse(week_start_utc), _parse(week_end_utc)

def _default_week_window_now() -> Tuple[str, str]:
    """
    If no window provided, return a conservative default:
    from 2025-09-04T00:00:00Z to 2025-09-09T12:00:00Z (Week 1).
    Adjust later weeks in your controller if needed.
    """
    start = dt.datetime(2025, 9, 4, 0, 0, 0, tzinfo=dt.timezone.utc)
    end   = dt.datetime(2025, 9, 9, 12, 0, 0, tzinfo=dt.timezone.utc)
    return start.isoformat().replace("+00:00","Z"), end.isoformat().replace("+00:00","Z")

# -------------------------------------------------------------------
# TEAM NORMALIZATION
# -------------------------------------------------------------------
_FALLBACK_TEAM_MAP = {
    # Common book/team name variants → short codes used by your project
    "Philadelphia Eagles": "PHI", "Eagles": "PHI", "Phi Eagles": "PHI",
    "Dallas Cowboys": "DAL", "Cowboys": "DAL",
    "Washington Commanders": "WSH", "Commanders": "WSH", "Washington": "WSH",
    "New York Giants": "NYG", "Giants": "NYG",
    "Kansas City Chiefs": "KC", "Chiefs": "KC",
    "Los Angeles Chargers": "LAC", "LA Chargers": "LAC", "Chargers": "LAC",
    "Tampa Bay Buccaneers": "TB", "Buccaneers": "TB", "Bucs": "TB",
    "Atlanta Falcons": "ATL", "Falcons": "ATL",
    "Cincinnati Bengals": "CIN", "Bengals": "CIN",
    "Cleveland Browns": "CLE", "Browns": "CLE",
    "Miami Dolphins": "MIA", "Dolphins": "MIA",
    "Indianapolis Colts": "IND", "Colts": "IND",
    "Carolina Panthers": "CAR", "Panthers": "CAR",
    "Jacksonville Jaguars": "JAX", "Jaguars": "JAX", "Jags": "JAX",
    "Arizona Cardinals": "ARI", "Cardinals": "ARI",
    "New Orleans Saints": "NO", "Saints": "NO", "N.O. Saints": "NO",
    "Pittsburgh Steelers": "PIT", "Steelers": "PIT",
    "New York Jets": "NYJ", "Jets": "NYJ",
    "Tennessee Titans": "TEN", "Titans": "TEN",
    "Denver Broncos": "DEN", "Broncos": "DEN",
    "San Francisco 49ers": "SF", "49ers": "SF", "Niners": "SF",
    "Seattle Seahawks": "SEA", "Seahawks": "SEA",
    "Detroit Lions": "DET", "Lions": "DET",
    "Green Bay Packers": "GB", "Packers": "GB",
    "Houston Texans": "HOU", "Texans": "HOU",
    "Los Angeles Rams": "LAR", "Rams": "LAR",
    "Baltimore Ravens": "BAL", "Ravens": "BAL",
    "Buffalo Bills": "BUF", "Bills": "BUF",
    "Minnesota Vikings": "MIN", "Vikings": "MIN",
    "Chicago Bears": "CHI", "Bears": "CHI",
    "Las Vegas Raiders": "LV", "Raiders": "LV",
    "New England Patriots": "NE", "Patriots": "NE",
}

def _load_team_map() -> Dict[str,str]:
    path = "teams_lookup.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                custom = json.load(f)
            # Merge with fallback so custom overrides but we still have broad coverage
            merged = {**_FALLBACK_TEAM_MAP, **custom}
            return merged
        except Exception:
            pass
    return _FALLBACK_TEAM_MAP

TEAM_MAP = _load_team_map()

def _to_code(name: str) -> Optional[str]:
    if not name:
        return None
    # exact
    if name in TEAM_MAP:
        return TEAM_MAP[name]
    # try stripped and title-cased variants
    key = name.strip()
    return TEAM_MAP.get(key, None)

# -------------------------------------------------------------------
# API CALL
# -------------------------------------------------------------------
def _call_the_odds_api(week_start_utc: str, week_end_utc: str) -> List[Dict[str, Any]]:
    key = _get_odds_key()
    if not key:
        raise RuntimeError("Missing THE_ODDS_API_KEY in .env (or ODDS_API_KEY).")

    params = {
        "regions": "us,eu",           # cover your Bwin/EU plus US books for consensus
        "markets": "spreads,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "apiKey": key
    }
    # TheOddsAPI returns future events by default; we’ll filter by commence_time window
    r = requests.get(THE_ODDS_BASE, params=params, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"TheOddsAPI HTTP {r.status_code}: {r.text[:300]}")
    data = r.json()
    # Filter by kickoff window
    ws, we = _to_utc_window(week_start_utc, week_end_utc)
    def _in_window(iso: str) -> bool:
        try:
            t = dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            return ws <= t <= we
        except Exception:
            return False
    return [g for g in data if _in_window(g.get("commence_time",""))]

# -------------------------------------------------------------------
# CONSENSUS BUILDERS
# -------------------------------------------------------------------
def _median(vals: List[float]) -> Optional[float]:
    v = [x for x in vals if x is not None and not math.isnan(x)]
    if not v:
        return None
    return float(pd.Series(v).median())

def _extract_book_lines(book: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Return (spread_home, total_points, bookmaker_key) for a single bookmaker listing.
    We’ll interpret spreads market and totals market.
    """
    spread_home = None
    total_points = None
    bkey = book.get("key")
    for market in book.get("markets", []):
        if market.get("key") == "spreads":
            for outcome in market.get("outcomes", []):
                # outcomes look like { "name": "Philadelphia Eagles", "price": -110, "point": -7.5 }
                name = outcome.get("name")
                point = outcome.get("point")
                # Home team spread is indicated by matching team name later; we will reconcile by consensus across both outcomes.
                # Here, just collect both points; the home adjustment is done at game-level.
                # For consensus, we’ll keep the signed point from the team listed as "home" later.
                # Return just the numeric; assignment to home happens after team map.
                # We'll just return point; at game-level, we’ll assign sign for home based on which outcome matches home.
                # For simplicity, we’ll let game-level code handle spread direction.
                # Here, we can’t decide home/away yet.
                pass
        elif market.get("key") == "totals":
            # outcomes like { "name": "Over", "point": 47.5 } and "Under"
            for outcome in market.get("outcomes", []):
                if outcome.get("name","").lower() == "over":
                    total_points = outcome.get("point")
    # We did not extract spread here; game-level will compute medians using per-team outcomes.
    return (spread_home, total_points, bkey or None)

def _consensus_from_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build a single consensus row for one event.
    """
    home_name = event.get("home_team") or ""
    away_name = event.get("away_team") or ""
    commence_iso = event.get("commence_time","")

    home = _to_code(home_name)
    away = _to_code(away_name)

    # If either team didn’t map, drop it (safer than guessing)
    if not home or not away:
        return None

    # Collect per-book spreads relative to the listed home/away
    spread_home_samples = []
    totals_samples = []

    for book in event.get("bookmakers", []):
        # Totals
        for m in book.get("markets", []):
            if m.get("key") == "totals":
                over = next((o for o in m.get("outcomes", []) if o.get("name","").lower()=="over"), None)
                if over and over.get("point") is not None:
                    totals_samples.append(float(over["point"]))
            elif m.get("key") == "spreads":
                # We want the home spread (signed from home team POV)
                # outcomes: two teams with "point" (e.g., PHI -7.5, DAL +7.5)
                home_out = next((o for o in m.get("outcomes", []) if _to_code(o.get("name","")) == home), None)
                if home_out and home_out.get("point") is not None:
                    spread_home_samples.append(float(home_out["point"]))

    spread_home = _median(spread_home_samples)
    total_pts   = _median(totals_samples)

    # Neutral-site note: KC vs LAC (São Paulo) — keep KC as designated home from event
    neutral = False
    try:
        t = dt.datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
        if t.year == 2025 and t.month == 9 and t.day == 6 or t.day == 5:
            # If TheOddsAPI lists it on 5/6 Sep and teams are KC/LAC, tag neutral for downstream use if needed.
            if set([home, away]) == set(["KC","LAC"]):
                neutral = True
    except Exception:
        pass

    return {
        "home_team": home,
        "away_team": away,
        "spread_home": spread_home,                 # e.g., -7.5 if home is favorite by 7.5
        "spread_away": -spread_home if spread_home is not None else None,
        "total": total_pts,
        "kickoff_utc": commence_iso,
        "neutral_site": neutral
    }

# -------------------------------------------------------------------
# PUBLIC API
# -------------------------------------------------------------------
def fetch_odds_for_week(week_start_utc: str, week_end_utc: str) -> pd.DataFrame:
    """
    Return a DataFrame with columns:
      home_team, away_team, spread_home, spread_away, total, kickoff_utc, neutral_site
    """
    data = _call_the_odds_api(week_start_utc, week_end_utc)
    rows = []
    for ev in data:
        row = _consensus_from_event(ev)
        if row:
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=[
            "home_team","away_team","spread_home","spread_away","total","kickoff_utc","neutral_site"
        ])
    df = pd.DataFrame(rows)
    # Drop exact dupes if any, keep first
    df = df.drop_duplicates(subset=["home_team","away_team","kickoff_utc"], keep="first")
    # Sort by kickoff
    with pd.option_context('mode.use_inf_as_na', True):
        df["kickoff_utc_parsed"] = pd.to_datetime(df["kickoff_utc"], errors="coerce", utc=True)
    df = df.sort_values("kickoff_utc_parsed").drop(columns=["kickoff_utc_parsed"])
    return df

def get_consensus_nfl_odds() -> pd.DataFrame:
    """
    Convenience wrapper for Week 1 default window.
    Your run controller can replace this window per week.
    """
    ws, we = _default_week_window_now()
    return fetch_odds_for_week(ws, we)

# -------------------------------------------------------------------
# CLI SMOKE TEST
# -------------------------------------------------------------------
if __name__ == "__main__":
    try:
        ws, we = _default_week_window_now()
        print(f"Fetching odds window: {ws} → {we}")
        df = fetch_odds_for_week(ws, we)
        print(df.to_string(index=False))
        if df.empty:
            print("\n⚠️  No odds found in window. Check API key/plan or adjust the window.")
    except Exception as e:
        print(f"❌ fetch_odds failed: {type(e).__name__}: {e}")
        sys.exit(1)
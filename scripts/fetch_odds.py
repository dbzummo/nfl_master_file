#!/usr/bin/env python3
import csv, json, os, pathlib, sys, time
from datetime import datetime
from typing import List, Dict, Optional

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "out" / "odds_week.csv"

# Optional team name -> abbr mapping (if file exists)
TEAMS_LK_PATHS = [
    ROOT / "teams_lookup.json",
    ROOT / "config" / "teams_lookup.json",
]
TEAMS_LK = {}
for p in TEAMS_LK_PATHS:
    if p.exists():
        try:
            TEAMS_LK = json.loads(p.read_text())
            break
        except Exception:
            pass

def to_abbr(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    s = str(name).strip()
    if not s:
        return s
    # normalize lookup keys (best effort)
    key = s
    # direct map, else passthrough
    return TEAMS_LK.get(key, s if len(s) <= 4 else s)

def ml_to_prob(ml: Optional[float]) -> Optional[float]:
    if ml is None or ml == "":
        return None
    try:
        ml = float(ml)
    except Exception:
        return None
    # American odds -> implied prob (vigged)
    if ml > 0:
        return 100.0 / (ml + 100.0)
    else:
        return abs(ml) / (abs(ml) + 100.0)

def write_rows(rows: List[Dict], path: Optional[pathlib.Path] = None) -> None:
    path = path or OUT
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["home_abbr","away_abbr","commence_time","book_count","ml_home","ml_away","market_p_home"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({
                "home_abbr": r.get("home_abbr"),
                "away_abbr": r.get("away_abbr"),
                "commence_time": r.get("commence_time"),
                "book_count": r.get("book_count", 0),
                "ml_home": r.get("ml_home"),
                "ml_away": r.get("ml_away"),
                "market_p_home": r.get("market_p_home"),
            })

# ---- MSF (weekly) ----
def fetch_msf_weekly_odds(season: str, week: str) -> List[Dict]:
    key = os.getenv("MSF_API_KEY")
    if not key:
        print("[WARN] MSF_API_KEY not set; skipping MSF.", flush=True)
        return []
    url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{season}/week/{week}/odds_gamelines.json?force=true"
    print(f"[INFO] Fetching MSF Weekly odds: {season} week {week}", flush=True)
    try:
        r = requests.get(url, auth=(key, "MYSPORTSFEEDS"), timeout=30)
        if r.status_code != 200:
            print(f"[INFO] MSF weekly returned HTTP {r.status_code}.", flush=True)
            return []
        data = r.json()
    except Exception as e:
        print(f"[WARN] MSF weekly fetch failed: {e}", flush=True)
        return []

    games = data.get("games") or []
    rows: List[Dict] = []

    # MSF's odds blocks vary in casing; check several keys
    def odds_block(g):
        return g.get("oddsgamelines") or g.get("oddsGameLines") or g.get("oddsLines") or g.get("odds") or []

    for g in games:
        sch = g.get("schedule") or {}
        home = sch.get("homeTeam", {}).get("abbreviation") or sch.get("homeTeam", {}).get("abbrev") or sch.get("homeTeam", {}).get("name")
        away = sch.get("awayTeam", {}).get("abbreviation") or sch.get("awayTeam", {}).get("abbrev") or sch.get("awayTeam", {}).get("name")
        start = sch.get("startTime") or sch.get("startTimeUTC") or sch.get("startTimeET")

        lines = odds_block(g)
        book_count = len(lines) if isinstance(lines, list) else 0

        # Try to synthesize a consensus ML from first line that has moneyline info
        ml_home, ml_away = None, None
        if isinstance(lines, list):
            for ln in lines:
                # moneyline often lives at ln["moneyline"] or ln["prices"]
                moneyline = ln.get("moneyline") or ln.get("prices") or {}
                # sometimes nested under "home"/"away" with "moneyline"
                if not moneyline and "home" in ln and "away" in ln:
                    moneyline = {
                        "home": ln["home"].get("moneyline"),
                        "away": ln["away"].get("moneyline"),
                    }
                if isinstance(moneyline, dict):
                    ml_home = moneyline.get("home")
                    ml_away = moneyline.get("away")
                if ml_home is not None or ml_away is not None:
                    break

        # compute p_home
        p_home = ml_to_prob(ml_home)
        if p_home is None and ml_away is not None:
            inv = ml_to_prob(ml_away)
            p_home = 1 - inv if inv is not None else None

        rows.append({
            "home_abbr": to_abbr(home),
            "away_abbr": to_abbr(away),
            "commence_time": start,
            "book_count": book_count,
            "ml_home": ml_home,
            "ml_away": ml_away,
            "market_p_home": p_home,
        })

    # Drop rows with no teams or no start time
    rows = [r for r in rows if r["home_abbr"] and r["away_abbr"] and r["commence_time"]]
    print(f"[INFO] MSF weekly usable rows: {len(rows)}", flush=True)
    return rows

# ---- OddsAPI (week window) ----
def week_window_utc(season: str, week: str) -> Optional[tuple[str, str]]:
    """
    Minimal mapping for 2025-regular. Extend as needed.
    """
    # Week windows derived from MSF games: distinct UTC dates
    windows = {
        "1": ("2025-09-04", "2025-09-09"),
        "2": ("2025-09-11", "2025-09-16"),
        "3": ("2025-09-19", "2025-09-23"),
        "4": ("2025-09-25", "2025-09-29"),
        "5": ("2025-10-02", "2025-10-06"),
        # add more as needed
    }
    return windows.get(str(week))

def fetch_odds_api_for_week(season: str, week: str) -> List[Dict]:
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("[WARN] ODDS_API_KEY not set; skipping OddsAPI.", flush=True)
        return []
    win = week_window_utc(season, week)
    if not win:
        print(f"[WARN] No week window mapping for {season} w{week}; skipping OddsAPI week-scoped fetch.", flush=True)
        return []

    start, end = win
    base = os.getenv("ODDS_API_BASE", "https://api.the-odds-api.com/v4/sports")
    sport = os.getenv("ODDS_API_SPORT", "americanfootball_nfl")
    regions = os.getenv("ODDS_API_REGIONS", "us")
    markets = os.getenv("ODDS_API_MARKETS", "h2h")
    fmt = os.getenv("ODDS_API_ODDS_FORMAT", "american")

    url = (
        f"{base}/{sport}/odds"
        f"?regions={regions}&markets={markets}&oddsFormat={fmt}&apiKey={api_key}"
        f"&commenceTimeFrom={start}T00:00:00Z&commenceTimeTo={end}T23:59:59Z"
    )
    print(f"[INFO] Fetching OddsAPI: {sport} {start}→{end}", flush=True)
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"[WARN] OddsAPI HTTP {r.status_code}: {r.text[:200]}", flush=True)
            return []
        data = r.json()
    except Exception as e:
        print(f"[WARN] OddsAPI fetch failed: {e}", flush=True)
        return []

    rows: List[Dict] = []
    for g in data:
        home = g.get("home_team")
        away = g.get("away_team")
        ct = g.get("commence_time")
        books = g.get("bookmakers") or []
        book_count = len(books)
        ml_home, ml_away = None, None
        for b in books:
            for mk in b.get("markets", []):
                if mk.get("key") == "h2h":
                    for o in mk.get("outcomes", []):
                        nm = o.get("name")
                        price = o.get("price")
                        if nm == home:
                            ml_home = price
                        elif nm == away:
                            ml_away = price
        p_home = ml_to_prob(ml_home)
        if p_home is None and ml_away is not None:
            inv = ml_to_prob(ml_away)
            p_home = 1 - inv if inv is not None else None
        rows.append({
            "home_abbr": to_abbr(home),
            "away_abbr": to_abbr(away),
            "commence_time": ct,
            "book_count": book_count,
            "ml_home": ml_home,
            "ml_away": ml_away,
            "market_p_home": p_home,
        })
    print(f"[INFO] OddsAPI usable rows: {len(rows)}", flush=True)
    return rows

def main():
    season = os.getenv("SEASON")
    week = os.getenv("WEEK")
    date = os.getenv("DATE")  # not used here; week-based
    if not (season and week):
        print("[FATAL] Provide WEEK and SEASON (e.g., WEEK=3 SEASON=2025-regular).", file=sys.stderr)
        sys.exit(1)

    # 1) Try MSF weekly; 2) fallback to OddsAPI week window
    msf_rows = fetch_msf_weekly_odds(season, week)
    oddsapi_rows = []
    if not msf_rows:
        oddsapi_rows = fetch_odds_api_for_week(season, week)

    rows = msf_rows if msf_rows else oddsapi_rows
    if not rows:
        print("[WARN] No odds rows available from any source.", flush=True)

    # 2) Provenance: write week-scoped files
    weekdir = ROOT / "out" / "odds" / f"{season}_w{week}"
    weekdir.mkdir(parents=True, exist_ok=True)
    if msf_rows:
        write_rows(msf_rows, path=weekdir / "odds_msf.csv")
    if oddsapi_rows:
        write_rows(oddsapi_rows, path=weekdir / "odds_oddsapi.csv")
    write_rows(rows, path=weekdir / "odds_combined.csv")

    # 3) Legacy flat output for downstream joins
    write_rows(rows, path=OUT)
    print(f"[OK] Wrote legacy odds -> {OUT} (rows={len(rows)})", flush=True)

if __name__ == "__main__":
    main()

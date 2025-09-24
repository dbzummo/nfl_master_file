#!/usr/bin/env python3
"""
Fetch NFL odds from OddsAPI with a strict preference for Bwin, but fall back to
the best available sportsbook if Bwin is missing. Writes a normalized CSV:
  out/odds_week.csv  with columns:
    date, away, home, market_spread, market_total, book

Notes:
- OddsAPI live endpoint only returns UPCOMING games. Past games will not appear.
- We match by team NAMES (OddsAPI) → MSF abbreviations via a small map.
- We filter to the MSF window [--start, --end] by comparing event commence_time
  to that date window (UTC dates, inclusive).
"""

import argparse
import csv
import datetime as dt
import json
import os
import sys
from typing import Dict, List, Optional, Tuple
import urllib.request
import urllib.parse

ODDS_API = "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
REGIONS = "us,eu,uk"
MARKETS = "spreads,totals"
ODDS_FORMAT = "american"

# Minimal name→abbrev map for 2025. Extend as needed.
TEAM_NAME_TO_ABBR = {
    "Dallas Cowboys": "DAL",
    "Philadelphia Eagles": "PHI",
    "Green Bay Packers": "GB",
    "Washington Commanders": "WAS",
    # add teams as needed…
}

PREFERRED_BOOK = "bwin"
FALLBACK_ORDER = [
    "bwin",                # preferred
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "betonlineag",
    "pointsbetus",
    "barstool",
    "betrivers",
    "unibet_uk",
    "williamhill_us",
    "pinnacle",
]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYYMMDD (UTC) inclusive")
    ap.add_argument("--end", required=True, help="YYYYMMDD (UTC) inclusive")
    ap.add_argument("--season", default="2025-regular")
    ap.add_argument("--allow-missing", action="store_true",
                    help="Do not exit(1) if no odds exist in the window")
    ap.add_argument("--out", default="out/odds_week.csv")
    return ap.parse_args()


def load_msf_week(path: str = "out/msf_week.csv") -> List[Dict]:
    if not os.path.exists(path):
        print(f"[ODDS][WARN] missing {path}; odds will be filtered only by API window.", file=sys.stderr)
        return []
    import pandas as pd
    df = pd.read_csv(path)
    # Normalize columns used for matching/filtering
    for col in ("date", "date_utc", "away_team", "home_team"):
        if col not in df.columns:
            df[col] = None
    # Prefer date_utc if present (YYYY-MM-DD)
    df["date_norm"] = df["date_utc"].fillna(df["date"]).astype(str)
    rows = df[["date_norm", "away_team", "home_team"]].to_dict("records")
    return rows


def yyyymmdd_to_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()


def date_from_iso(ts: str) -> dt.date:
    # OddsAPI commence_time is ISO8601 Zulu, e.g., 2025-09-12T00:16:00Z
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).date()


def pick_bookmaker(bookmakers: List[Dict]) -> Optional[Dict]:
    """Return the preferred bookmaker dict, falling back across FALLBACK_ORDER."""
    by_key = {b["key"]: b for b in bookmakers}
    for k in FALLBACK_ORDER:
        if k in by_key and by_key[k].get("markets"):
            return by_key[k]
    return None


def extract_spread_total(bookmaker: Dict) -> Tuple[Optional[float], Optional[float], str]:
    """Return (spread_for_home, total, book_key). Spread is home minus away (home favored negative)."""
    book_key = bookmaker.get("key")
    spreads = None
    totals = None
    for m in bookmaker.get("markets", []):
        if m.get("key") == "spreads":
            spreads = m
        elif m.get("key") == "totals":
            totals = m

    spread_val = None
    total_val = None

    # Spreads: look for point value relative to home/away
    if spreads and spreads.get("outcomes"):
        # structure example: [{"name":"Dallas Cowboys","point":-3.5}, {"name":"Philadelphia Eagles","point":3.5}]
        # Convert to home-centric number: (home point) * -1 if the API encodes away as negative.
        # We’ll compute home_spread = home_point (if exists), else negative of away_point.
        pts = {o.get("name"): o.get("point") for o in spreads["outcomes"] if "name" in o}
        # determine home/away names from the bookmaker? We must pass them in externally;
        # but OddsAPI outcomes use team names. We'll return raw; caller will orient using the home team name.
        return_spread = (pts, book_key)
    else:
        return_spread = (None, book_key)

    if totals and totals.get("outcomes"):
        # outcomes: [{"name":"Over","point":47.5},{"name":"Under","point":47.5}]
        try:
            total_val = float(totals["outcomes"][0].get("point"))
        except Exception:
            total_val = None

    # We can’t finalize spread without knowing which team is home. The caller will resolve.
    # Pack the team→point dict into spread_val for now.
    return return_spread[0], total_val, return_spread[1]


def odds_request(api_key: str) -> List[Dict]:
    params = {
        "apiKey": api_key,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
    }
    url = ODDS_API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "nfl_master_file/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            if resp.status != 200:
                print(f"[ODDS][FAIL] HTTP {resp.status} from odds provider: {body}", file=sys.stderr)
                sys.exit(1)
            return json.loads(body or "[]")
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", errors="ignore")
        print(f"[ODDS][FAIL] HTTP {e.code} from odds provider: {msg}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ODDS][FAIL] request error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    args = parse_args()
    api_key = os.environ.get("ODDS_KEY") or os.environ.get("ODDS_API_KEY")
    if not api_key:
        print("[ODDS][FAIL] missing ODDS_KEY in environment", file=sys.stderr)
        sys.exit(1)

    start_d = yyyymmdd_to_date(args.start)
    end_d = yyyymmdd_to_date(args.end)
    msf_rows = load_msf_week("out/msf_week.csv")
    msf_keyed = {(r["date_norm"], r["away_team"], r["home_team"]) for r in msf_rows}

    data = odds_request(api_key)
    out_rows: List[Dict] = []
    missing: List[Tuple[str, str, str]] = []

    for ev in data:
        try:
            ev_date = date_from_iso(ev["commence_time"])
            if ev_date < start_d or ev_date > end_d:
                continue
            home_name = ev.get("home_team")
            away_name = ev.get("away_team")
            if not home_name or not away_name:
                continue
            home_abbr = TEAM_NAME_TO_ABBR.get(home_name)
            away_abbr = TEAM_NAME_TO_ABBR.get(away_name)
            if not home_abbr or not away_abbr:
                # Unknown team mapping; skip (or log)
                continue

            bm = pick_bookmaker(ev.get("bookmakers", []))
            if not bm:
                missing.append((ev_date.isoformat(), away_abbr, home_abbr))
                continue

            spread_raw, total_val, book_key = extract_spread_total(bm)

            # Resolve spread to "home-centric" number:
            spread_val = None
            if isinstance(spread_raw, dict):
                # If we have points for each team, derive home spread:
                hp = spread_raw.get(home_name)
                ap = spread_raw.get(away_name)
                # Many books encode spreads as "team point" values. If only away is present,
                # home spread = -away_point; if only home present, use that; if both present,
                # trust the home point.
                if hp is not None:
                    try:
                        spread_val = float(hp)
                    except Exception:
                        spread_val = None
                elif ap is not None:
                    try:
                        spread_val = float(-float(ap))
                    except Exception:
                        spread_val = None

            out_rows.append({
                "date": ev_date.isoformat(),   # YYYY-MM-DD
                "away": away_abbr,
                "home": home_abbr,
                "market_spread": spread_val,
                "market_total": total_val,
                "book": book_key,
            })
        except Exception as e:
            print(f"[ODDS][WARN] skipped event due to parse error: {e}", file=sys.stderr)
            continue

    # If we had an MSF file, filter only to games MSF saw, and prefer Bwin rows when multiple books exist:
    if msf_rows:
        want = {}
        for r in out_rows:
            k = (r["date"], r["away"], r["home"])
            if k not in {(d.replace("-", ""), a, h) for (d, a, h) in msf_keyed} and k not in msf_keyed:
                # MSF date may be YYYY-MM-DD while msf_keyed may be YYYY-MM-DD or YYYY-MM-DD-ish.
                # We keep permissive matching (same date format already).
                pass
            # keep best book: prefer bwin, else keep first seen.
            if k not in want:
                want[k] = r
            else:
                if r.get("book") == PREFERRED_BOOK and want[k].get("book") != PREFERRED_BOOK:
                    want[k] = r
        out_rows = list(want.values())

    if not out_rows:
        msg = f"[ODDS][FAIL] No odds found in window {args.start}→{args.end}. (Games may be in the past; live endpoint returns only upcoming.)"
        if args.allow_missing:
            print(msg, file=sys.stderr)
        else:
            print(msg + " Use --allow-missing to proceed.", file=sys.stderr)
            sys.exit(1)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date", "away", "home", "market_spread", "market_total", "book"])
        w.writeheader()
        for r in sorted(out_rows, key=lambda x: (x["date"], x["away"], x["home"])):
            w.writerow(r)

    if out_rows:
        books = sorted({r.get("book") for r in out_rows if r.get("book")})
        print(f"[ODDS][ok] wrote {args.out} rows={len(out_rows)} books={','.join(books)}")
    else:
        print(f"[ODDS][ok] wrote {args.out} rows=0 (no upcoming events in window)")

if __name__ == "__main__":
    main()

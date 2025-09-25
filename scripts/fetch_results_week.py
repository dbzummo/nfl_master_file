#!/usr/bin/env python3
"""
Fetch results for a given week by calling MSF games endpoint for the dates in out/week_predictions.csv.

This script expects out/week_predictions.csv (or week_predictions.csv in CWD) with columns:
  date (YYYYMMDD), away_team, home_team

It prefers MSF_SEASON from env (e.g., 2025-regular). If not set, it uses a safe default.
"""
import os, sys, requests, pathlib, json, time
import pandas as pd
from datetime import datetime, timedelta

MSF_API_KEY = os.environ.get("MSF_API_KEY")
if not MSF_API_KEY:
    print("[FATAL] MSF_API_KEY not set. Export MSF_API_KEY before running.", file=sys.stderr)
    sys.exit(2)

MSF_SEASON = os.environ.get("MSF_SEASON", "2025-regular")

INPUT = pathlib.Path("out/week_predictions.csv")
OUT = pathlib.Path("out/week_results.csv")

def read_predictions():
    if not INPUT.exists():
        print(f"[FATAL] missing {INPUT}", file=sys.stderr)
        sys.exit(2)
    df = pd.read_csv(INPUT, dtype=str)
    required = {"date","away_team","home_team"}
    if not required.issubset(set(df.columns)):
        print(f"[FATAL] {INPUT} must have date, away_team, home_team columns", file=sys.stderr)
        sys.exit(2)
    # normalize date column: ensure YYYYMMDD strings
    df["date"] = df["date"].astype(str).str.strip().str.replace("-","")
    # validate format
    try:
        _ = pd.to_datetime(df["date"], format="%Y%m%d")
    except Exception:
        print("[FATAL] date column must be YYYYMMDD strings.", file=sys.stderr)
        sys.exit(2)
    return df

def _http_get(session, url, params):
    resp = session.get(url, auth=(MSF_API_KEY, "MYSPORTSFEEDS"), params=params, timeout=30)
    if resp.status_code != 200:
        snippet = (resp.text or "")[:400]
        raise RuntimeError(f"HTTP {resp.status_code} url={url} params={params} body={snippet!r}")
    return resp.json()

def collect_by_date(d0, d1):
    session = requests.Session()
    results_rows = []
    cur = datetime.strptime(d0, "%Y%m%d")
    endd = datetime.strptime(d1, "%Y%m%d")
    while cur <= endd:
        dstr = cur.strftime("%Y%m%d")
        url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{MSF_SEASON}/date/{dstr}/games.json"
        try:
            payload = _http_get(session, url, params={"force":"false"})
        except RuntimeError as e:
            print(f"[WARN] fetch for {dstr} failed: {e}", file=sys.stderr)
            cur += timedelta(days=1)
            time.sleep(0.2)
            continue
        # payload parsing: adapt to your MSF shape; defensive extraction
        games = payload.get("games") or payload.get("gamesByDate") or []
        for g in games:
            try:
                # MSF game object likely has 'homeTeam' and 'awayTeam' nested structures and 'schedule' for score
                home = g.get("homeTeam") if isinstance(g, dict) else None
                away = g.get("awayTeam") if isinstance(g, dict) else None
                home_abbr = (home or {}).get("abbreviation") or g.get("home") or None
                away_abbr = (away or {}).get("abbreviation") or g.get("away") or None
                home_score = None
                away_score = None
                if "score" in g:
                    s = g.get("score") or {}
                    home_score = s.get("home")
                    away_score = s.get("away")
                # fallback keys
                if home_score is None and "homeScore" in g:
                    home_score = g.get("homeScore")
                if away_score is None and "awayScore" in g:
                    away_score = g.get("awayScore")
                if home_abbr and away_abbr:
                    results_rows.append({
                        "date": dstr,
                        "home_team": home_abbr,
                        "away_team": away_abbr,
                        "home_score": home_score,
                        "away_score": away_score,
                    })
            except Exception:
                continue
        cur += timedelta(days=1)
        time.sleep(0.2)
    return pd.DataFrame(results_rows)

def main():
    preds = read_predictions()
    dmin = preds["date"].min()
    dmax = preds["date"].max()
    print(f"[INFO] fetching results for dates {dmin}..{dmax} season={MSF_SEASON}")
    results_df = collect_by_date(dmin, dmax)
    if results_df.empty:
        print("[FATAL] No results rows fetched. Check MSF_API_KEY and MSF_SEASON.", file=sys.stderr)
        sys.exit(2)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} (rows={len(results_df)})")

if __name__ == "__main__":
    main()

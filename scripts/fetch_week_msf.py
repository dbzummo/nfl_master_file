#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch MSF Weekly or Daily games (v2.1) using correct v2 auth:
  username = <YOUR GUID API KEY>
  password = "MYSPORTSFEEDS"     (literal per MSF v2 spec)

We **only** call CORE endpoints:
  - Weekly: https://api.mysportsfeeds.com/v2.1/pull/nfl/{season}/week/{week}/games.json
  - Daily:  https://api.mysportsfeeds.com/v2.1/pull/nfl/{season}/date/{yyyymmdd}/games.json

We write: out/msf_details/msf_week.csv with columns:
  date, away_team, home_team, week, msf_game_id

Guardrails:
  - On HTTP 403 → exit non-zero and DO NOT write msf_week.csv
  - On empty/malformed JSON → exit non-zero and DO NOT write msf_week.csv
  - On no games → exit non-zero and DO NOT write msf_week.csv

Examples:
  python3 scripts/fetch_week_msf.py --season 2025-regular --week 2
  python3 scripts/fetch_week_msf.py --season 2025-regular --start 20250911 --end 20250915
"""

import argparse
import os
import sys
import pathlib
import datetime as dt
import requests
import pandas as pd

UA = "MSFClient/1.0 (+https://yourdomain.example)"

def must_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        sys.exit(f"[msf][ERR] missing env {name}")
    return val

def daterange(start_yyyymmdd: str, end_yyyymmdd: str):
    s = dt.datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    e = dt.datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    d = s
    while d <= e:
        yield d.strftime("%Y%m%d")
        d += dt.timedelta(days=1)

def fetch_json(url: str, key: str) -> dict:
    headers = {"Accept": "application/json", "User-Agent": UA}
    auth = (key, "MYSPORTSFEEDS")
    print(f"[msf] GET {url}")
    r = requests.get(url, headers=headers, auth=auth, timeout=25)
    if r.status_code == 403:
        sys.exit("[msf][ERR] 403 from MSF (key OK but feed not enabled / auth mode mismatch). "
                 "We are using v2 weekly/daily endpoints with v2 auth.")
    if not r.ok:
        sys.exit(f"[msf][ERR] HTTP {r.status_code} from MSF at {url}")
    try:
        return r.json()
    except Exception as e:
        sys.exit(f"[msf][ERR] response not JSON: {e}")

def normalize_games(data: dict, week_number: int | None) -> pd.DataFrame:
    games = data.get("games") or []
    rows = []
    for g in games:
        sched = g.get("schedule") or {}
        start = (sched.get("startTime") or "")[:10]  # YYYY-MM-DD
        home  = ((sched.get("homeTeam") or {}).get("abbreviation") or "").upper()
        away  = ((sched.get("awayTeam") or {}).get("abbreviation") or "").upper()
        gid   = sched.get("id", "")
        if not (start and home and away):
            continue
        rows.append({
            "date": start,
            "away_team": away,
            "home_team": home,
            "week": week_number if week_number is not None else (sched.get("week") or ""),
            "msf_game_id": gid
        })
    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", required=True, help="e.g., 2025-regular")
    ap.add_argument("--week", type=int, help="preferred path (CORE)")
    ap.add_argument("--start", help="YYYYMMDD (daily fallback if week not provided)")
    ap.add_argument("--end", help="YYYYMMDD (daily fallback if week not provided)")
    args = ap.parse_args()

    key = must_env("MSF_KEY")      # GUID
    must_env("MSF_PASS")           # ensure it's set; we still send literal "MYSPORTSFEEDS"

    out_dir = pathlib.Path("out/msf_details")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "msf_week.csv"

    all_rows = []

    if args.week is not None:
        # Weekly (preferred)
        url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{args.season}/week/{args.week}/games.json"
        data = fetch_json(url, key)
        df = normalize_games(data, args.week)
        if df.empty:
            sys.exit("[msf][ERR] weekly fetch returned no games; not writing msf_week.csv")
        all_rows.append(df)
    else:
        # Daily fallback (must have start/end)
        if not (args.start and args.end):
            sys.exit("[msf][ERR] provide --week OR (--start AND --end)")
        for ymd in daterange(args.start, args.end):
            url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{args.season}/date/{ymd}/games.json"
            data = fetch_json(url, key)
            df = normalize_games(data, None)
            if not df.empty:
                all_rows.append(df)

        if not all_rows:
            sys.exit("[msf][ERR] daily fetch returned no games in window; not writing msf_week.csv")

    out = pd.concat(all_rows, ignore_index=True).drop_duplicates(
        subset=["date", "away_team", "home_team"]
    )
    # If week is blank from daily pulls, try to infer most-common week
    if "week" in out.columns and (out["week"] == "").any():
        try:
            wk = out["week"].mode().iloc[0]
            out["week"] = out["week"].replace("", wk)
        except Exception:
            pass

    # Final validation before write
    need = {"date", "away_team", "home_team", "week", "msf_game_id"}
    if not need.issubset(out.columns) or out.empty:
        sys.exit("[msf][ERR] normalized games missing required columns or empty; not writing file.")

    out = out.sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)
    out.to_csv(out_file, index=False)
    print(f"[ok] wrote {out_file} rows={len(out)}")
    try:
        print(out.head(16).to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()
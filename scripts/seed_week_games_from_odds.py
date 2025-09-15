#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Seed out/ingest/week_games.csv from The Odds API events that were already fetched
by scripts/fetch_odds.py (it writes out/odds/_debug/_events_seen.csv).

- Defaults to reading events from: out/odds/_debug/_events_seen.csv
- Uses team name → abbreviation mapping from: data/odds_team_map.csv
- Writes: out/ingest/week_games.csv with columns:
    date, away_team, home_team, week, msf_game_id

Example:
  python3 scripts/seed_week_games_from_odds.py \
    --week 2 --start 20250911 --end 20250915 --season 2025-regular
"""

import argparse
import sys
import pathlib
import pandas as pd

DEF_EVENTS_CSV = "out/odds/_debug/_events_seen.csv"
DEF_TEAM_MAP   = "data/odds_team_map.csv"

def load_team_map(team_map_path: str) -> dict:
    p = pathlib.Path(team_map_path)
    if not p.exists():
        sys.exit(f"[seed][ERR] team map not found: {team_map_path}")
    # Allow both with/without header. If header missing, pass names.
    try:
        df = pd.read_csv(p)
        if not {"team","api"}.issubset(df.columns):
            df = pd.read_csv(p, names=["team","api"])
    except Exception:
        df = pd.read_csv(p, names=["team","api"])
    df["team"] = df["team"].astype(str).str.strip().str.upper()
    df["api"]  = df["api"].astype(str).str.strip()
    return {row.api: row.team for _, row in df.iterrows()}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--week", required=True, type=int, help="NFL week number")
    ap.add_argument("--start", help="YYYYMMDD (unused here; kept for CLI symmetry)")
    ap.add_argument("--end", help="YYYYMMDD (unused here; kept for CLI symmetry)")
    ap.add_argument("--season", help="e.g., 2025-regular (unused here; kept for CLI symmetry)")
    ap.add_argument("--events", default=DEF_EVENTS_CSV,
                    help=f"CSV of events (default: {DEF_EVENTS_CSV})")
    ap.add_argument("--team-map", default=DEF_TEAM_MAP,
                    help=f"CSV mapping odds API names → team abbreviations (default: {DEF_TEAM_MAP})")
    args = ap.parse_args()

    events_csv = args.events
    team_map_path = args.team_map

    # Load events
    try:
        events = pd.read_csv(events_csv)
    except Exception as e:
        sys.exit(f"[seed][ERR] failed to read events from {events_csv}: {e}")
    if events.empty:
        sys.exit(f"[seed][ERR] events file is empty: {events_csv}")

    # Required columns from Odds API diagnostics export
    req_cols = {"home_team", "away_team", "commence_time"}
    missing = req_cols - set(events.columns)
    if missing:
        sys.exit(f"[seed][ERR] events CSV missing columns: {sorted(missing)}")

    # Load map: Odds API team name -> OUR ABBR (e.g., 'Philadelphia Eagles' -> 'PHI')
    abbr_by_api = load_team_map(team_map_path)

    rows = []
    for _, r in events.iterrows():
        home_api = str(r["home_team"]).strip()
        away_api = str(r["away_team"]).strip()
        home = abbr_by_api.get(home_api)
        away = abbr_by_api.get(away_api)
        if not home or not away:
            print(f"[seed][WARN] missing map: {away_api} @ {home_api}")
            continue
        date = str(r["commence_time"])[:10]  # YYYY-MM-DD
        rows.append({
            "date": date,
            "away_team": away,
            "home_team": home,
            "week": args.week,
            "msf_game_id": ""  # unknown (we aren't calling MSF here)
        })

    out = pd.DataFrame(rows)
    if out.empty:
        sys.exit("[seed][ERR] no rows produced (likely mapping gaps). Check data/odds_team_map.csv")

    out = out.drop_duplicates(subset=["date", "away_team", "home_team"])
    out = out.sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)

    out_dir = pathlib.Path("out/ingest")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "week_games.csv"
    out.to_csv(out_file, index=False)

    print(f"[seed] wrote {out_file} rows={len(out)} (week={args.week})")
    try:
        print(out.to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()
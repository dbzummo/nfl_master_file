#!/usr/bin/env python3
"""
fetch_msf_week.py

Fetch NFL weekly games from MySportsFeeds v2.1 using correct auth:
  username = MSF_API_KEY
# MSF_PASSWORD_PLACEHOLDER (demo only, not a secret)

Usage:
  python scripts/fetch_msf_week.py <WEEK_NUMBER>

Writes:
  out/msf/week_games.json   (raw)
  out/msf/week_games.csv    (flat table)
"""

import os, sys, json
from pathlib import Path
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("[FATAL] need WEEK arg", file=sys.stderr)
        sys.exit(1)
    week = int(sys.argv[1])

    api_key = os.getenv("MSF_API_KEY")
    if not api_key:
        print("[FATAL] MSF_API_KEY not set (put it in _secrets/.env)", file=sys.stderr)
        sys.exit(1)

    base = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/2025-regular/week/{week}/games.json"
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(api_key, "MYSPORTSFEEDS")

    r = sess.get(base, timeout=30)
    r.raise_for_status()
    data = r.json()

    out_dir = Path("out/msf"); out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "week_games.json").write_text(json.dumps(data, indent=2))

    games = data.get("games", [])
    rows = []
    for g in games:
        gi = g.get("schedule", {})
        ht = gi.get("homeTeam", {}) or {}
        at = gi.get("awayTeam", {}) or {}
        rows.append({
            "msf_game_id": gi.get("id"),
            "game_start": gi.get("startTime"),
            "home_abbr": (ht.get("abbreviation") or "").upper(),
            "away_abbr": (at.get("abbreviation") or "").upper(),
            "venue": gi.get("venue", {}).get("name"),
            "status": gi.get("playedStatus"),
        })

    pd.DataFrame(rows).to_csv(out_dir / "week_games.csv", index=False)
    print(f"[OK] Fetched {len(rows)} games -> out/msf/week_games.csv")

if __name__ == "__main__":
    main()

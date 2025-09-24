#!/usr/bin/env python3
"""
update_injuries_week.py
Single-pass MSF v2.1 injuries fetch with correct auth + retry/backoff + paging.
Writes out/injuries/injuries_feed.csv once; never re-runs or overwrites with smaller pages.
"""

import os, sys, time, csv, requests
from requests.auth import HTTPBasicAuth
from pathlib import Path

MSF_API_KEY = os.getenv("MSF_API_KEY")
if not MSF_API_KEY:
    print("[FATAL] MSF_API_KEY not found in environment. Put it in _secrets/.env and source it.", file=sys.stderr)
    sys.exit(1)

BASE_URL   = "https://api.mysportsfeeds.com/v2.1/pull/nfl/injuries.json"
OUT_DIR    = Path("out/injuries"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE   = OUT_DIR / "injuries_feed.csv"
LIMIT      = 500
FORCE      = "false"
MAX_RETRY  = 5
BACKOFF    = 2.0
PAGE_SLEEP = 1.0

def fetch_page(session, offset):
    url = f"{BASE_URL}?offset={offset}&limit={LIMIT}&force={FORCE}"
    for attempt in range(1, MAX_RETRY+1):
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 429:
                wait = BACKOFF ** attempt
                print(f"[429] offset={offset} attempt={attempt}/{MAX_RETRY}; sleep {wait:.1f}s")
                time.sleep(wait); continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            wait = BACKOFF ** attempt
            print(f"[ERR] offset={offset} attempt={attempt}/{MAX_RETRY}: {e}; retry in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed at offset {offset} after {MAX_RETRY} retries")

def normalize(data):
    players = data.get("players", [])
    rows = []
    for entry in players:
        p = entry.get("player", {}) or {}
        t = entry.get("team", {}) or {}
        inj = (entry.get("injuries") or [{}])[0]
        rows.append({
            "player_id": p.get("id"),
            "first_name": p.get("firstName"),
            "last_name": p.get("lastName"),
            "position": p.get("primaryPosition"),
            "team_abbr": t.get("abbreviation"),
            "injury_desc": inj.get("desc"),
            "injury_status": inj.get("status"),
            "injury_start_date": inj.get("startDate"),
        })
    return rows

def main():
    print("[INFO] Starting injury fetch...")
    session = requests.Session()
    session.auth = HTTPBasicAuth(MSF_API_KEY, "MYSPORTSFEEDS")

    all_rows = []
    offset = 0
    total = None
    while True:
        data = fetch_page(session, offset)
        rows = normalize(data)
        if not rows:
            print(f"[DONE] No rows at offset={offset}; stopping.")
            break
        all_rows.extend(rows)

        paging = data.get("paging", {})
        total = paging.get("totalItems", total)
        if total is not None and offset + LIMIT >= total:
            break

        offset += LIMIT
        time.sleep(PAGE_SLEEP)

    # write exactly once
    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "player_id","first_name","last_name","position","team_abbr",
            "injury_desc","injury_status","injury_start_date"
        ])
        w.writeheader(); w.writerows(all_rows)
    print(f"[SUCCESS] Wrote {len(all_rows)} rows -> {OUT_FILE}")

if __name__ == "__main__":
    main()

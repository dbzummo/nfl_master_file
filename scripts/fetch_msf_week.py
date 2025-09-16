#!/usr/bin/env python3
import os, sys, csv, json, datetime, pathlib, requests

def arg(name, default=None):
    if name in os.environ and os.environ[name]:
        return os.environ[name]
    try:
        i = sys.argv.index(f"--{name.lower()}") + 1
        return sys.argv[i]
    except (ValueError, IndexError):
        return default

def main():
    start  = arg("START")
    end    = arg("END")
    season = arg("SEASON", "2025-regular")
    if not (start and end):
        print("[FATAL] need --start and --end (YYYYMMDD) or env START/END", file=sys.stderr)
        sys.exit(2)

    key = os.environ.get("MSF_KEY")
    pw  = os.environ.get("MSF_PASS", "MYSPORTSFEEDS")
    if not key:
        print("[FATAL] MSF_KEY is required in env", file=sys.stderr)
        sys.exit(3)

    d0 = datetime.datetime.strptime(start, "%Y%m%d")
    d1 = datetime.datetime.strptime(end,   "%Y%m%d")

    out_dir = pathlib.Path("out/msf_details")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "msf_week.csv"

    rows = []
    for i in range((d1 - d0).days + 1):
        d = (d0 + datetime.timedelta(days=i)).strftime("%Y%m%d")
        url = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{season}/date/{d}/games.json"
        r = requests.get(url, auth=(key, pw), timeout=30)
        r.raise_for_status()
        for g in r.json().get("games", []):
            s   = g.get("schedule", {}) or {}
            gid = str(s.get("id") or s.get("msfGameId") or "")
            home = (s.get("homeTeam") or {}).get("abbreviation") or ""
            away = (s.get("awayTeam") or {}).get("abbreviation") or ""
            wk   = s.get("week", {})
            week = (wk.get("week") if isinstance(wk, dict) else wk) or ""
            # prefer UTC-ish date if present
            date = s.get("startTimeUTC") or s.get("startTime") or s.get("startTimeLocal") or ""
            rows.append({"date": date[:10], "week": week, "away_team": away, "home_team": home, "game_id": gid})

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","week","away_team","home_team","game_id"])
        w.writeheader(); w.writerows(rows)

    print(f"[OK] wrote {out} rows={len(rows)}")

if __name__ == "__main__":
    main()

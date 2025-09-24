#!/usr/bin/env python3
"""
Fetch NFL game results from MySportsFeeds v2.1 and write history/season_<YEAR>_from_site.csv.

- Auth: Basic auth (username=<API_KEY>, passphrase "MYSPORTSFEEDS")
- Endpoint(s):
    https://api.mysportsfeeds.com/v2.1/pull/nfl/{season-path}/week/{week}/games.json
  where season-path is like "2025-regular" and week is 1..18
- We collect games, keep those with final scores, normalize team names via teams_lookup.json if present,
  and write columns: home_team, away_team, date, home_score, away_score, season, week, game_id, game_status
"""

import os, sys, json, argparse, time
from typing import Dict, Any, List
import requests
import pandas as pd

BASE = "https://api.mysportsfeeds.com/v2.1/pull/nfl"

def read_teams_lookup() -> Dict[str,str]:
    try:
        with open("teams_lookup.json","r",encoding="utf-8") as f:
            m=json.load(f)
        return {str(k).strip().upper(): str(v).strip().upper() for k,v in m.items()}
    except Exception:
        return {}

def norm_team(s: str, m: Dict[str,str]) -> str:
    t = str(s).strip().upper()
    return m.get(t, t)

def msf_get(url: str, api_key: str, retries: int = 3, sleep_s: float = 1.0) -> Dict[str,Any]:
    last = None
    for i in range(retries):
        r = requests.get(url, auth=(api_key, "MYSPORTSFEEDS"), timeout=20)
        last = r
        if r.status_code == 200:
            return r.json()
        # 403 means auth OK but not entitled: give a clean hint immediately
        if r.status_code == 403:
            msg = (
                f"403 Access Restricted for {url}. Your key is valid, but your plan likely does not include: \n"
                f" - league = NFL, or\n - season = {url.split('/pull/nfl/')[1].split('/')[0]}, or\n - endpoint scope (games by week).\n"
                f"Body: {r.text[:300]}"
            )
            raise SystemExit(msg)
        # Otherwise retry a couple times
        time.sleep(sleep_s)
    raise SystemExit(f"MSF error {last.status_code}: {last.text[:200]} @ {url}")

def parse_games(payload: Dict[str,Any]) -> List[Dict[str,Any]]:
    # Expected top-level key "games"
    games = payload.get("games") or []
    out = []
    for g in games:
        sched = g.get("schedule") or {}
        score = g.get("score") or {}
        home = (sched.get("homeTeam") or {}).get("abbreviation") or (sched.get("homeTeam") or {}).get("name")
        away = (sched.get("awayTeam") or {}).get("abbreviation") or (sched.get("awayTeam") or {}).get("name")
        # Date/time often under "startTime" or "startTimeUTC"
        dt = sched.get("startTimeUTC") or sched.get("startTime") or sched.get("startTimeLocal") or sched.get("startTimeISO")
        status = (sched.get("playedStatus") or sched.get("status") or "").lower()
        week = sched.get("week")
        gid  = sched.get("id") or g.get("id")

        home_pts = score.get("homeScoreTotal") or score.get("homeScore") or score.get("homePoints")
        away_pts = score.get("awayScoreTotal") or score.get("awayScore") or score.get("awayPoints")

        out.append({
            "home_raw": home, "away_raw": away, "date_raw": dt, "status_raw": status,
            "week": week, "game_id": gid, "home_score": home_pts, "away_score": away_pts
        })
    return out

def keep_final(row: Dict[str,Any]) -> bool:
    s = str(row.get("status_raw") or "").lower()
    # MSF uses playedStatus like "COMPLETED" / "COMPLETED_PENDING_REVIEW" etc. Normalize liberally.
    return ("final" in s) or ("complete" in s) or ("completed" in s)

def nfl_season_year(dser: pd.Series) -> pd.Series:
    dt = pd.to_datetime(dser, errors="coerce").dt.tz_localize(None)
    m = dt.dt.month
    y = dt.dt.year
    return (y.where(m >= 8, y - 1)).astype("Int64")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True, help="Season year, e.g., 2025")
    ap.add_argument("--weeks", default="1-18", help="Weeks to fetch, e.g., 1-3 or 1,2,5")
    ap.add_argument("--out", required=True, help="Output CSV path, e.g., history/season_2025_from_site.csv")
    ap.add_argument("--api_key", default=None, help="MySportsFeeds API KEY; or set MSF_API_KEY env var")
    ap.add_argument("--sleep", type=float, default=0.4, help="Delay between calls")
    args = ap.parse_args()

    api_key = args.api_key or os.environ.get("MSF_API_KEY")
    if not api_key:
        sys.exit("No MySportsFeeds key. Use --api_key or export MSF_API_KEY.")

    # parse weeks
    wk_set = set()
    s = args.weeks.strip()
    if "-" in s:
        a,b = s.split("-",1)
        wk_set = set(range(int(a), int(b)+1))
    else:
        wk_set = set(int(x) for x in s.split(",") if x.strip())

    season_path = f"{args.season}-regular"
    team_map = read_teams_lookup()

    rows: List[Dict[str,Any]] = []
    for w in sorted(wk_set):
        url = f"{BASE}/{season_path}/week/{w}/games.json"
        data = msf_get(url, api_key)
        rows.extend(parse_games(data))
        time.sleep(args.sleep)

    if not rows:
        sys.exit("No games returned from MSF.")

    df = pd.DataFrame(rows)
    # date normalization
    df["date"] = pd.to_datetime(df["date_raw"], errors="coerce").dt.tz_localize(None).dt.date
    # status + final filter
    df["status_raw"] = df["status_raw"].astype(str)
    finals = df[df.apply(keep_final, axis=1)].copy()
    if finals.empty:
        sys.exit("No FINAL/COMPLETED games in the requested weeks.")

    # scores
    finals["home_score"] = pd.to_numeric(finals["home_score"], errors="coerce").astype("Int64")
    finals["away_score"] = pd.to_numeric(finals["away_score"], errors="coerce").astype("Int64")

    # teams (apply lookup -> uppercase)
    finals["home_team"] = finals["home_raw"].astype(str).str.strip().str.upper().map(lambda s: team_map.get(s, s))
    finals["away_team"] = finals["away_raw"].astype(str).str.strip().str.upper().map(lambda s: team_map.get(s, s))

    # sanity: season derivation must equal requested
    finals["_season"] = nfl_season_year(finals["date"])
    finals = finals[finals["_season"] == args.season].copy()

    out = finals[["home_team","away_team","date","home_score","away_score","week","game_id","status_raw"]].rename(
        columns={"status_raw":"game_status"}
    ).copy()
    out["season"] = args.season

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out} rows={len(out)} (weeks {min(wk_set)}..{max(wk_set)})")
    # quick per-week summary
    print(out.groupby("week", dropna=False).size().rename("games_by_week").to_string())
if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_enrich_msf.py
Pulls weekly enrichment from MySportsFeeds (STATS + DETAILED) and emits:
  - out/msf_enrich_week.csv  (per-team per-game summary with light features)
  - out/msf_raw/*.json       (cached raw responses for inspection)

Inputs:
  --start YYYYMMDD
  --end   YYYYMMDD
  --season like "2025-regular"

Environment:
  MSF_KEY, MSF_PASS

Safe to re-run. Missing feeds or fields won't crash the pipeline.
"""

import argparse
import base64
import json
import os
import sys
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_DIR = os.path.join(ROOT, "out")
RAW_DIR = os.path.join(OUT_DIR, "msf_raw")
ENRICH_CSV = os.path.join(OUT_DIR, "msf_enrich_week.csv")

def _abort(msg, code=1):
    print(f"[fail] {msg}", file=sys.stderr)
    sys.exit(code)

def _ok(msg):
    print(f"[ok] {msg}")

def _msf_get(path: str, season: str, query: str) -> dict | None:
    """
    Minimal HTTP GET to MSF v2.1. Returns parsed JSON or None on error.
    """
    base = f"https://api.mysportsfeeds.com/v2.1/pull/nfl/{season}/{path}.json"
    url = f"{base}?{query}" if query else base

    key = os.environ.get("MSF_KEY", "").strip()
    pw  = os.environ.get("MSF_PASS", "").strip()
    if not key or not pw:
        _abort("MSF_KEY/MSF_PASS not set")

    token = base64.b64encode(f"{key}:{pw}".encode("utf-8")).decode("ascii")
    req = Request(url, headers={"Authorization": f"Basic {token}"})
    try:
        with urlopen(req, timeout=60) as r:
            data = json.loads(r.read().decode("utf-8"))
            return data
    except HTTPError as e:
        print(f"[warn] {path} HTTP {e.code}", file=sys.stderr)
    except URLError as e:
        print(f"[warn] {path} URL error {e}", file=sys.stderr)
    except Exception as e:
        print(f"[warn] {path} unexpected error {e}", file=sys.stderr)
    return None

def _save_raw(obj: dict | None, fname: str):
    if obj is None:
        return
    os.makedirs(RAW_DIR, exist_ok=True)
    fp = os.path.join(RAW_DIR, fname)
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    # no noisy output here

def _get_stat(d: dict, *keys, default=None):
    """Nested .get with safety."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def _safe_div(a, b):
    try:
        if b in (0, None):
            return None
        return a / b
    except Exception:
        return None

def _normalize_team(abbrev: str | None) -> str | None:
    if not abbrev:
        return None
    return abbrev.strip().upper()

def _rows_from_teamlogs(teamlogs: dict) -> list[dict]:
    """
    Convert Weekly Team Gamelogs to per-team-per-game rows with light features.
    We’re intentionally conservative with field names to survive schema quirks.
    """
    rows = []
    items = (teamlogs or {}).get("gamelogs", [])
    for i in items:
        # schedule / game identification
        sch  = i.get("game", {}) or i.get("schedule", {})
        dt   = _get_stat(sch, "startTime") or _get_stat(sch, "startTimeUTC") or _get_stat(sch, "startTimeLocal")
        # MSF dates are ISO; keep YYYY-MM-DD
        date = None
        if dt:
            try:
                # parse first 10 chars if ISO
                date = dt[:10]
            except Exception:
                pass
        game_id = _get_stat(sch, "id") or _get_stat(i, "game", "id")

        team   = _normalize_team(_get_stat(i, "team", "abbreviation"))
        is_home = _get_stat(i, "isHome") or ( _get_stat(sch, "homeTeam", "abbreviation") == team )

        # scoring / final
        home_pts = _get_stat(sch, "homeScore") or _get_stat(i, "stats", "points", "homeScore")
        away_pts = _get_stat(sch, "awayScore") or _get_stat(i, "stats", "points", "awayScore")

        # aggregates
        pass_att = (
            _get_stat(i, "stats", "passing", "attempts")
            or _get_stat(i, "teamStats", "passing", "attempts")
            or 0
        )
        rush_att = (
            _get_stat(i, "stats", "rushing", "attempts")
            or _get_stat(i, "teamStats", "rushing", "attempts")
            or 0
        )
        plays = None
        try:
            plays = (pass_att or 0) + (rush_att or 0)
        except Exception:
            plays = None

        pass_cmp = (
            _get_stat(i, "stats", "passing", "completions")
            or _get_stat(i, "teamStats", "passing", "completions")
        )
        pass_pct = _safe_div(pass_cmp, pass_att)  # completion rate proxy

        yards = (
            _get_stat(i, "stats", "yards", "total")
            or _get_stat(i, "teamStats", "yards", "total")
            or _get_stat(i, "stats", "totalYards")
            or _get_stat(i, "teamStats", "totalYards")
        )
        ypp = _safe_div(yards, plays) if plays else None

        sacks = (
            _get_stat(i, "stats", "passing", "sacks")
            or _get_stat(i, "teamStats", "passing", "sacks")
        )
        sack_rate = _safe_div(sacks, pass_att) if pass_att else None

        turnovers = (
            _get_stat(i, "stats", "turnovers", "total")
            or _get_stat(i, "teamStats", "turnovers", "total")
            or _get_stat(i, "stats", "fumbles", "lost")
        )

        rows.append({
            "date": date,
            "msf_game_id": game_id,
            "team": team,
            "side": "HOME" if is_home else "AWAY",
            "final_home": home_pts,
            "final_away": away_pts,
            "plays": plays,
            "pass_att": pass_att,
            "rush_att": rush_att,
            "pass_pct": pass_pct,
            "yards": yards,
            "ypp": ypp,
            "sacks": sacks,
            "sack_rate": sack_rate,
            "turnovers": turnovers,
        })
    return rows

def _injury_badges(inj_json: dict) -> dict:
    """
    Map TEAM -> short injury flag string for the window.
    Conservative: if feed missing, return {}.
    """
    res = {}
    if not inj_json:
        return res
    items = inj_json.get("players", []) or inj_json.get("injuries", [])
    for p in items:
        team = _normalize_team(_get_stat(p, "team", "abbreviation"))
        status = (_get_stat(p, "injury", "status") or "").upper()
        if not team:
            continue
        # Minimal rollup: flag if any OUT/DOUBTFUL/QUESTIONABLE entries exist.
        flag = None
        if "OUT" in status:
            flag = "OUT"
        elif "DOUBTFUL" in status:
            flag = "D"
        elif "QUESTIONABLE" in status:
            flag = "Q"
        if flag:
            res[team] = flag if team not in res else res[team]  # keep first seen
    return res

def main():
    ap = argparse.ArgumentParser(description="Fetch weekly enrichment from MySportsFeeds")
    ap.add_argument("--start", default="20250904", help="YYYYMMDD inclusive")
    ap.add_argument("--end",   default="20250909", help="YYYYMMDD inclusive")
    ap.add_argument("--season", default="2025-regular")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

    # Weekly Team Gamelogs
    teamlogs = _msf_get("weekly_team_gamelogs", args.season, f"date={args.start}-{args.end}") \
               or _msf_get("weeklyTeamGamelogs", args.season, f"date={args.start}-{args.end}")  # alt casing
    _save_raw(teamlogs, f"weekly_team_gamelogs_{args.start}_{args.end}.json")

    # Weekly Player Gamelogs (not required for now, but cached for future)
    playerlogs = _msf_get("weekly_player_gamelogs", args.season, f"date={args.start}-{args.end}") \
                 or _msf_get("weeklyPlayerGamelogs", args.season, f"date={args.start}-{args.end}")
    _save_raw(playerlogs, f"weekly_player_gamelogs_{args.start}_{args.end}.json")

    # Player injuries
    injuries = _msf_get("player_injuries", args.season, f"date={args.start}-{args.end}") \
               or _msf_get("playerInjuries", args.season, f"date={args.start}-{args.end}")
    _save_raw(injuries, f"player_injuries_{args.start}_{args.end}.json")

    # Build per-team rows
    rows = _rows_from_teamlogs(teamlogs) if teamlogs else []
    inj_flags = _injury_badges(injuries)

    # Attach injury flag to team rows
    for r in rows:
        r["inj_flag"] = inj_flags.get(r.get("team"))

    # Output CSV
    import csv
    fieldnames = [
        "date","msf_game_id","team","side","final_home","final_away",
        "plays","pass_att","rush_att","pass_pct","yards","ypp","sacks","sack_rate","turnovers",
        "inj_flag"
    ]
    with open(ENRICH_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    _ok(f"wrote {ENRICH_CSV} rows={len(rows)}")

if __name__ == "__main__":
    main()
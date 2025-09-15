#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Consolidate v2 MSF details you already cached into tidy CSVs the model expects.

Inputs (already on disk from your earlier fetch loop):
  out/msf_details/boxscore_*.json
  out/msf_details/pbp_*.json

Outputs:
  out/msf_details/boxscores_week.csv  (one row per game)
  out/msf_details/pbp_week.csv        (one row per play; best-effort fields)

This script is schema-tolerant: it won’t error if fields are missing; it writes what it can.
"""

import os, glob, json, csv
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "out", "msf_details")
os.makedirs(OUT_DIR, exist_ok=True)

BOX_GLOB = os.path.join(OUT_DIR, "boxscore_*.json")
PBP_GLOB = os.path.join(OUT_DIR, "pbp_*.json")

BOX_OUT = os.path.join(OUT_DIR, "boxscores_week.csv")
PBP_OUT = os.path.join(OUT_DIR, "pbp_week.csv")


def _first(x, *keys):
    """nested safe-get: _first(d, 'a','b') returns d.get('a',{}).get('b')…"""
    cur = x
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def parse_boxscore(path):
    """
    v2 boxscore files look like:
      {"lastUpdatedOn":"...","game":{...,"homeTeam":{...},"awayTeam":{...},"score":{...}}}
    We extract the weekly report keys we use elsewhere.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    g = data.get("game", {}) or {}
    score = g.get("score", {}) or {}

    # schedule-ish fields live under the same 'game' object in v2 details
    start_utc = g.get("startTime", "")
    end_utc = g.get("endedTime", "")
    date_utc = (start_utc or "")[:10]
    date_local = date_utc  # renderer uses this; OK to keep UTC date (consistent with prior)
    msf_game_id = g.get("id")
    week = g.get("week")
    venue = _first(g, "venue", "name") or ""
    away_abbr = _first(g, "awayTeam", "abbreviation") or ""
    home_abbr = _first(g, "homeTeam", "abbreviation") or ""

    # Status: if endedTime exists, treat as COMPLETED; otherwise try to infer
    status = "COMPLETED" if end_utc else (g.get("playedStatus") or "").upper() or ""

    row = {
        "date": date_local,
        "date_utc": date_utc,
        "away_team": away_abbr,
        "home_team": home_abbr,
        "status": status,
        "final_away": score.get("awayScoreTotal"),
        "final_home": score.get("homeScoreTotal"),
        "msf_game_id": msf_game_id,
        "week": week,
        "venue": venue,
        "startTime_utc": start_utc,
        "endedTime_utc": end_utc,
    }
    return row


def parse_pbp(path):
    """
    v2 play-by-play:
      We’ve seen both:
        {"lastUpdatedOn":"...","game":{...},"plays":[ {...}, {...} ]}
      and other mild variations.
      We write one row per play, capturing useful, generic fields if present.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    g = data.get("game", {}) or {}
    msf_game_id = g.get("id")
    home_abbr = _first(g, "homeTeam", "abbreviation") or ""
    away_abbr = _first(g, "awayTeam", "abbreviation") or ""
    start_utc = g.get("startTime", "")
    date_utc = (start_utc or "")[:10]

    plays = data.get("plays")
    if plays is None:
        # sometimes nested under game
        plays = g.get("plays")
    plays = plays or []

    rows = []
    for p in plays:
        # Common, schema-tolerant fields
        quarter = str(p.get("quarter", "")) if p.get("quarter") is not None else ""
        clock = p.get("time") or p.get("clock") or ""
        desc = p.get("description") or p.get("details") or ""
        play_type = (p.get("type") or p.get("playType") or "").lower()

        # Teams involved (best effort)
        offense = _first(p, "team", "abbreviation") or p.get("teamAbbreviation") or ""
        # some schemas expose participants / possession
        possession = p.get("possessionTeam") or ""
        defense = ""
        if offense:
            # if offense equals home, defense is away, etc. Best-effort fallback
            if offense == home_abbr:
                defense = away_abbr
            elif offense == away_abbr:
                defense = home_abbr

        yards = p.get("yards") or p.get("netYards") or None
        success = p.get("success")  # may not exist; leave as-is

        # Score context if present
        home_pts = p.get("homeScore") or None
        away_pts = p.get("awayScore") or None

        rows.append({
            "msf_game_id": msf_game_id,
            "date_utc": date_utc,
            "quarter": quarter,
            "clock": clock,
            "offense": offense or possession,
            "defense": defense,
            "play_type": play_type,
            "yards": yards,
            "success": success,
            "home_score": home_pts,
            "away_score": away_pts,
            "description": desc,
        })
    return rows


def write_csv(path, fieldnames, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # keep only known fields; missing become empty
            w.writerow({k: r.get(k, "") for k in fieldnames})


def main():
    # ---- BOX ----
    box_rows = []
    for fp in sorted(glob.glob(BOX_GLOB)):
        try:
            row = parse_boxscore(fp)
            if row.get("msf_game_id"):
                box_rows.append(row)
        except Exception as e:
            print(f"[box][WARN] {os.path.basename(fp)} parse error: {e}")

    box_fields = [
        "date","date_utc","away_team","home_team","status",
        "final_away","final_home","msf_game_id","week","venue",
        "startTime_utc","endedTime_utc"
    ]
    write_csv(BOX_OUT, box_fields, box_rows)
    print(f"[ok] wrote {BOX_OUT} rows={len(box_rows)}")

    # ---- PBP ----
    pbp_rows = []
    for fp in sorted(glob.glob(PBP_GLOB)):
        try:
            pbp_rows.extend(parse_pbp(fp))
        except Exception as e:
            print(f"[pbp][WARN] {os.path.basename(fp)} parse error: {e}")

    pbp_fields = [
        "msf_game_id","date_utc","quarter","clock",
        "offense","defense","play_type","yards","success",
        "home_score","away_score","description"
    ]
    write_csv(PBP_OUT, pbp_fields, pbp_rows)
    print(f"[ok] wrote {PBP_OUT} rows={len(pbp_rows)}")

    # Friendly hint if no plays were emitted
    if len(pbp_rows) == 0:
        print("[hint] PBP files parsed to zero rows. Double-check that your v2 PBP JSONs exist under out/msf_details and include 'plays'.")

if __name__ == "__main__":
    main()
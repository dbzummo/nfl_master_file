#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
compute_coach_features.py

Purpose
-------
- Join your curated coaching tables (Data/Coaches/YYYY.csv) to the teams
  appearing in out/msf_week.csv for the active window/season.
- Compute HC/OC/DC tenure (consecutive seasons with the same team up to year).
- Compute continuity_index vs. previous season (fraction of HC/OC/DC unchanged).
- Detect OC play-caller flag if "(play-caller)" appears in the OC cell.
- Write:
    out/coach_features_week.csv
    out/week_features_plus_coach.csv (only if out/ingest/week_features.csv exists; original untouched)

Inputs
------
- out/msf_week.csv  (produced by your MSF fetch)
- Data/Coaches/{2019..2025}.csv (Team,Head Coach,Offensive Coordinator,Defensive Coordinator)

Outputs
-------
- out/coach_features_week.csv
- (optional) out/week_features_plus_coach.csv

Usage
-----
python3 scripts/compute_coach_features.py --season 2025-regular
# or if msf_week is already created and you just want to compute against its year:
python3 scripts/compute_coach_features.py
"""

import argparse
import os
import re
import sys
from typing import Dict, Tuple, List

import pandas as pd


# ---------- Helpers ----------

TEAM_NAME_TO_ABBR = {
    # NFC East
    "Dallas Cowboys": "DAL",
    "New York Giants": "NYG",
    "Philadelphia Eagles": "PHI",
    "Washington Redskins": "WAS",
    "Washington Football Team": "WAS",
    "Washington Commanders": "WAS",

    # NFC North
    "Chicago Bears": "CHI",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Minnesota Vikings": "MIN",

    # NFC South
    "Atlanta Falcons": "ATL",
    "Carolina Panthers": "CAR",
    "New Orleans Saints": "NO",
    "Tampa Bay Buccaneers": "TB",

    # NFC West
    "Arizona Cardinals": "ARI",
    "Los Angeles Rams": "LAR",
    "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA",

    # AFC East
    "Buffalo Bills": "BUF",
    "Miami Dolphins": "MIA",
    "New England Patriots": "NE",
    "New York Jets": "NYJ",

    # AFC North
    "Baltimore Ravens": "BAL",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Pittsburgh Steelers": "PIT",

    # AFC South
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Tennessee Titans": "TEN",

    # AFC West
    "Denver Broncos": "DEN",
    "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV",
    "Oakland Raiders": "OAK",  # historical naming guard
    "Los Angeles Chargers": "LAC",
}

ABBR_CANON = {
    # normalize a few known variants to our abbreviations
    "WSH": "WAS", "WFT": "WAS",
    "LA": "LAR",  # if a data source uses LA (Rams) ambiguously
    "SD": "LAC",  # historical Chargers
    "OAK": "LV",  # we will standardize to LV for 2020+
    "NO": "NO",   # already fine
}

def canon_team_abbr(abbr: str, season_year: int) -> str:
    if not abbr:
        return abbr
    ab = abbr.strip().upper()
    ab = ABBR_CANON.get(ab, ab)
    # Raiders: OAK -> LV from 2020 onward
    if ab == "OAK" and season_year >= 2020:
        return "LV"
    return ab

def parse_season_year_from_msf(msf_week_path: str) -> int:
    df = pd.read_csv(msf_week_path)
    # use date or startTime_utc; take the year portion
    # prefer "date" because that's what we render with
    if "date" in df.columns and pd.notna(df["date"].iloc[0]):
        return int(str(df["date"].iloc[0])[:4])
    if "startTime_utc" in df.columns and pd.notna(df["startTime_utc"].iloc[0]):
        return int(str(df["startTime_utc"].iloc[0])[:4])
    raise ValueError("Could not infer season year from out/msf_week.csv")

def strip_playcaller(text: str) -> Tuple[str, bool]:
    """Return (clean_name, is_playcaller)."""
    if not isinstance(text, str):
        return "", False
    t = text.strip()
    is_pc = "(play-caller)" in t.lower() or "(play caller)" in t.lower() or "(playcaller)" in t.lower()
    # remove the parenthetical note
    clean = re.sub(r"\s*\(.*?play[-\s]?caller.*?\)\s*", "", t, flags=re.IGNORECASE).strip()
    return clean, is_pc

def load_coach_table(year: int) -> pd.DataFrame:
    path = os.path.join("Data", "Coaches", f"{year}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing coaching file: {path}")
    df = pd.read_csv(path).fillna("")
    # Expected columns: Team,Head Coach,Offensive Coordinator,Defensive Coordinator
    for col in ["Team", "Head Coach", "Offensive Coordinator", "Defensive Coordinator"]:
        if col not in df.columns:
            raise ValueError(f"{path} missing column: {col}")
    # add abbr + clean/flags
    df["team_abbr"] = df["Team"].map(TEAM_NAME_TO_ABBR).fillna("")
    if (df["team_abbr"] == "").any():
        missing = df.loc[df["team_abbr"] == "", "Team"].unique().tolist()
        raise ValueError(f"{path}: could not map these team names to abbreviations: {missing}")

    df["hc_name"] = df["Head Coach"].astype(str).str.strip()
    df["oc_name_raw"] = df["Offensive Coordinator"].astype(str)
    df["dc_name"] = df["Defensive Coordinator"].astype(str).str.strip()

    oc_pairs = df["oc_name_raw"].apply(strip_playcaller)
    df["oc_name"] = [p[0] for p in oc_pairs]
    df["oc_playcaller_flag"] = [p[1] for p in oc_pairs]
    df = df.drop(columns=["oc_name_raw"])
    df["season_year"] = year
    return df[["season_year","team_abbr","hc_name","oc_name","dc_name","oc_playcaller_flag"]]

def compute_tenure(coach_stack: Dict[int, pd.DataFrame], team: str, role: str, year: int) -> int:
    """
    Count consecutive seasons (including `year`) with same 'role' coach name for `team`,
    scanning backward until change.
    """
    assert role in {"hc_name","oc_name","dc_name"}
    this_df = coach_stack.get(year)
    if this_df is None:
        return 0
    try:
        current = this_df.loc[this_df["team_abbr"] == team, role].iloc[0]
    except IndexError:
        return 0
    if not current:
        return 0

    tenure = 0
    yr = year
    while True:
        df_y = coach_stack.get(yr)
        if df_y is None:
            break
        sub = df_y[df_y["team_abbr"] == team]
        if sub.empty:
            break
        name_y = sub[role].iloc[0]
        if not name_y or str(name_y).strip() != str(current).strip():
            break
        tenure += 1
        yr -= 1
    return tenure

def continuity_index(coach_stack: Dict[int, pd.DataFrame], team: str, year: int) -> float:
    """Fraction of (HC,OC,DC) unchanged vs prior season."""
    cur = coach_stack.get(year)
    prv = coach_stack.get(year - 1)
    if cur is None or prv is None:
        return 0.0
    try:
        c = cur[cur["team_abbr"] == team].iloc[0]
        p = prv[prv["team_abbr"] == team].iloc[0]
    except IndexError:
        return 0.0
    same = 0
    for role in ["hc_name","oc_name","dc_name"]:
        if str(c[role]).strip() and str(c[role]).strip() == str(p[role]).strip():
            same += 1
    return round(same / 3.0, 3)


# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="", help="e.g., 2025-regular (optional; year inferred from out/msf_week.csv)")
    ap.add_argument("--msf-week", default="out/msf_week.csv", help="Path to msf_week.csv")
    args = ap.parse_args()

    if not os.path.exists(args.msf_week):
        sys.exit(f"[coach] missing {args.msf_week} — run fetch_week_msf first.")

    season_year = parse_season_year_from_msf(args.msf_week)
    # Build a small stack around the current year to compute tenure/continuity
    years_needed = list(range(2019, season_year + 1))  # since your data starts at 2019
    coach_stack: Dict[int, pd.DataFrame] = {}
    for y in years_needed:
        try:
            coach_stack[y] = load_coach_table(y)
        except FileNotFoundError:
            # Skip silently; tenure/continuity may be reduced
            continue

    # Read the teams in-window
    games = pd.read_csv(args.msf_week)
    # We’ll emit one row per TEAM appearing in msf_week (home + away unique)
    teams = pd.unique(pd.concat([games["home_team"], games["away_team"]], ignore_index=True)).tolist()
    teams = [canon_team_abbr(t, season_year) for t in teams if isinstance(t, str)]

    # Build feature rows
    rows: List[Dict] = []
    cur = coach_stack.get(season_year)
    if cur is None:
        sys.exit(f"[coach][FAIL] No coaching table for {season_year} in Data/Coaches/{season_year}.csv")

    for t in sorted(set(teams)):
        sub = cur[cur["team_abbr"] == t]
        if sub.empty:
            # Try to locate via name-mapping failure — unlikely since we use abbrs in msf_week
            rows.append({
                "season_year": season_year, "team": t,
                "hc_name": "", "oc_name": "", "dc_name": "",
                "oc_playcaller": False,
                "hc_tenure": 0, "oc_tenure": 0, "dc_tenure": 0,
                "continuity_index": 0.0,
            })
            continue

        rec = sub.iloc[0]
        hc = str(rec["hc_name"]).strip()
        oc = str(rec["oc_name"]).strip()
        dc = str(rec["dc_name"]).strip()
        oc_pc = bool(rec["oc_playcaller_flag"])

        hc_ten = compute_tenure(coach_stack, t, "hc_name", season_year)
        oc_ten = compute_tenure(coach_stack, t, "oc_name", season_year)
        dc_ten = compute_tenure(coach_stack, t, "dc_name", season_year)
        cont = continuity_index(coach_stack, t, season_year)

        rows.append({
            "season_year": season_year,
            "team": t,
            "hc_name": hc,
            "oc_name": oc,
            "dc_name": dc,
            "oc_playcaller": oc_pc,
            "hc_tenure": hc_ten,
            "oc_tenure": oc_ten,
            "dc_tenure": dc_ten,
            "continuity_index": cont,
        })

    out_dir = "out"
    os.makedirs(out_dir, exist_ok=True)
    coach_out = os.path.join(out_dir, "coach_features_week.csv")
    df_coach = pd.DataFrame(rows)
    df_coach.to_csv(coach_out, index=False)
    print(f"[coach][ok] wrote {coach_out} rows={len(df_coach)} season_year={season_year}")

    # Optional: produce a merged view with week_features.csv if it exists.
    wf_path = os.path.join("out", "ingest", "week_features.csv")
    if os.path.exists(wf_path):
        try:
            wf = pd.read_csv(wf_path)
            # try common join key names
            join_col = None
            for c in ["team", "team_abbr", "abbr", "home_team", "away_team"]:
                if c in wf.columns:
                    join_col = c
                    break
            if join_col is None:
                # Fallback: if wf is game-side rows with explicit team col missing, create one?
                # We keep it simple — emit an unmerged copy and warn.
                merged_out = os.path.join("out", "week_features_plus_coach.csv")
                df_coach.to_csv(merged_out, index=False)
                print(f"[coach][warn] Could not find a team key in {wf_path}; wrote {merged_out} with coach-only features.")
            else:
                wf["__join_team"] = wf[join_col].astype(str).str.upper().map(lambda x: canon_team_abbr(x, season_year))
                df_coach["__join_team"] = df_coach["team"]
                merged = wf.merge(df_coach.drop(columns=["team","season_year"]),
                                  how="left", on="__join_team")
                merged = merged.drop(columns=["__join_team"])
                merged_out = os.path.join("out", "week_features_plus_coach.csv")
                merged.to_csv(merged_out, index=False)
                print(f"[coach][ok] wrote {merged_out} rows={len(merged)} (non-destructive; original week_features.csv unchanged)")
        except Exception as e:
            print(f"[coach][warn] merge with week_features.csv skipped due to error: {e}")
    else:
        print("[coach][info] out/ingest/week_features.csv not found — skipped merged output.")

if __name__ == "__main__":
    main()
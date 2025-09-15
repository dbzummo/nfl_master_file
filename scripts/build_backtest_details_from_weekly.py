#!/usr/bin/env python3
# Build out/backtest_details.csv by merging weekly predictions (already fused) with history outcomes.
# Predictions CSV MUST have: home_team, away_team, date, home_win_prob.
# Join order: date+teams → teams+nearest_date(≤1d) → week-bounded (same season) → teams-only (same season).
# Cross-season matches are DISALLOWED.

import os, sys, json, argparse
from typing import Optional, List
import pandas as pd

OUT_DIR = "out"
OUT_FILE = os.path.join(OUT_DIR, "backtest_details.csv")

HOME_ALIASES = ["home_team","home","Home","home_name","home_abbr","team_home","homeTeam","home_code"]
AWAY_ALIASES = ["away_team","away","Away","away_name","away_abbr","team_away","awayTeam","away_code"]

# Accept many history date headers, including UTC-ish ones
DATE_ALIASES_HIST = ["date","Date","game_date","GameDate","kickoff","start_time","kickoff_utc","GameDateUTC"]

SCORE_HOME = ["home_score","HomeScore","home_pts","HomePts"]
SCORE_AWAY = ["away_score","AwayScore","away_pts","AwayPts"]
LABEL_ALIASES = ["home_win","HomeWin","is_home_win","label","result"]

def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return None

def read_teams_lookup() -> dict:
    m = read_json("teams_lookup.json") or {}
    return {str(k).strip().upper(): str(v).strip().upper() for k,v in m.items()}

def norm_team(s, table):
    if s is None: return None
    t = str(s).strip().upper()
    return table.get(t, t)

def pick_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    return next((c for c in aliases if c in df.columns), None)

def coerce_date_series(df: pd.DataFrame, col: str) -> None:
    df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None).dt.date

def find_date_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    for c in aliases:
        if c in df.columns:
            try:
                coerce_date_series(df, c)
                if df[c].notna().any(): return c
            except Exception:
                pass
    return None

def ensure_label(dfh: pd.DataFrame) -> str:
    lbl = pick_col(dfh, LABEL_ALIASES)
    if lbl: return lbl
    h = pick_col(dfh, SCORE_HOME); a = pick_col(dfh, SCORE_AWAY)
    if not h or not a:
        sys.exit("History has neither a label column nor scores to derive it.")
    dfh["home_win"] = (pd.to_numeric(dfh[h], errors="coerce") > pd.to_numeric(dfh[a], errors="coerce")).astype("Int64")
    return "home_win"

def nfl_season_year(d: pd.Series) -> pd.Series:
    # NFL: Aug–Dec -> same year; Jan–Feb -> previous season year
    dt = pd.to_datetime(d, errors="coerce")
    m = dt.dt.month
    y = dt.dt.year
    return (y.where(m >= 8, y - 1)).astype("Int64")

def nearest_date_join(preds: pd.DataFrame, dfh: pd.DataFrame, hist_date_col: str, max_days: int = 1) -> pd.DataFrame:
    # Avoid name collisions by renaming history date to _hist_date before the join
    h = dfh[["_home_norm","_away_norm",hist_date_col]].rename(columns={hist_date_col: "_hist_date"})
    merged = preds.merge(h, on=["_home_norm","_away_norm"], how="inner")
    if merged.empty:
        return merged
    merged["abs_diff_days"] = (pd.to_datetime(merged["date"]) - pd.to_datetime(merged["_hist_date"])).abs().dt.days
    merged = merged[merged["abs_diff_days"] <= max_days]
    if merged.empty:
        return merged
    merged["_k"] = merged["_home_norm"] + "|" + merged["_away_norm"] + "|" + merged["date"].astype(str)
    merged = merged.sort_values(["_k","abs_diff_days"]).groupby("_k", as_index=False).first()
    # bring all history columns back via exact key
    final = merged.merge(dfh, left_on=["_home_norm","_away_norm","_hist_date"], right_on=["_home_norm","_away_norm",hist_date_col], how="left")
    return final

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True, help="Predictions CSV with columns: home_team, away_team, date, home_win_prob")
    ap.add_argument("--hist", required=True, help="History CSV for outcomes")
    ap.add_argument("--strategy", default="date_then_fallback", choices=["strict","date_then_fallback","teams_only"])
    ap.add_argument("--strict_season", action="store_true", help="Abort instead of falling back if seasons don't overlap")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    team_map = read_teams_lookup()

    # Load predictions (fused)
    dfp = pd.read_csv(args.pred)
    for col in ["home_team","away_team","date","home_win_prob"]:
        if col not in dfp.columns:
            sys.exit(f"Predictions missing required column: {col}")
    dfp["_home_norm"] = dfp["home_team"].map(lambda s: norm_team(s, team_map))
    dfp["_away_norm"] = dfp["away_team"].map(lambda s: norm_team(s, team_map))
    coerce_date_series(dfp, "date")
    dfp["_season"] = nfl_season_year(dfp["date"])
    pred_seasons = sorted(dfp["_season"].dropna().unique().tolist())
    if not pred_seasons:
        sys.exit("Predictions contain no valid dates to deduce season.")

    # Load history
    dfh = pd.read_csv(args.hist)
    home_h = pick_col(dfh, HOME_ALIASES); away_h = pick_col(dfh, AWAY_ALIASES)
    if not home_h or not away_h:
        sys.exit(f"History file missing home/away columns: {args.hist}")
    dfh["_home_norm"] = dfh[home_h].map(lambda s: norm_team(s, team_map))
    dfh["_away_norm"] = dfh[away_h].map(lambda s: norm_team(s, team_map))
    hist_date = find_date_col(dfh, DATE_ALIASES_HIST)
    if not hist_date:
        sys.exit(f"History '{args.hist}' has no usable date column (looked for {DATE_ALIASES_HIST}).")
    dfh["_season"] = nfl_season_year(dfh[hist_date])

    # Season guard (never cross seasons)
    dfh_season = dfh[dfh["_season"].isin(pred_seasons)].copy()
    if dfh_season.empty:
        msg = f"No history rows for prediction season(s) {pred_seasons} in '{args.hist}'. " \
              f"Ingest the correct season results before evaluating."
        sys.exit(msg)

    label_col = ensure_label(dfh_season)
    merged = None; merge_name = None

    # 1) Strict date+teams
    if args.strategy in ("strict","date_then_fallback"):
        m = dfp.merge(
            dfh_season,
            left_on=["date","_home_norm","_away_norm"],
            right_on=[hist_date,"_home_norm","_away_norm"],
            how="inner",
            suffixes=("_pred","_hist")
        )
        if not m.empty:
            merged = m; merge_name = "date+teams"

    # 2) Teams + nearest date (≤1 day)
    if merged is None and args.strategy in ("strict","date_then_fallback"):
        m2 = nearest_date_join(dfp, dfh_season, hist_date, max_days=1)
        if not m2.empty:
            merged = m2; merge_name = "teams+nearest_date_<=1day"

    # 3) Week-bounded teams-only (same season) via week_info.json
    if merged is None and args.strategy in ("date_then_fallback",):
        wi = read_json("week_info.json")
        if wi and ("week_start" in wi and "week_end" in wi):
            ws = pd.to_datetime(wi["week_start"]).date()
            we = pd.to_datetime(wi["week_end"]).date()
            dfh_week = dfh_season.loc[(dfh_season[hist_date] >= ws) & (dfh_season[hist_date] <= we)].copy()
            m3 = dfp.merge(dfh_week, on=["_home_norm","_away_norm"], how="inner", suffixes=("_pred","_hist"))
            if not m3.empty:
                merged = m3; merge_name = "teams_only_week_bounded"

    # 4) Final fallback (same season only)
    if merged is None:
        if args.strategy == "strict":
            sys.exit("Strict date+teams merge produced zero rows.")
        m4 = dfp.merge(dfh_season, on=["_home_norm","_away_norm"], how="inner", suffixes=("_pred","_hist"))
        if m4.empty:
            sys.exit("Could not merge predictions with history within the same season.")
        merged = m4; merge_name = "teams_only"

    # Output
    out = pd.DataFrame()
    out["home_team"] = merged["_home_norm"]
    out["away_team"] = merged["_away_norm"]
    out["date"] = pd.to_datetime(merged["date"]).dt.date
    out["home_win_prob"] = pd.to_numeric(merged["home_win_prob"], errors="coerce")
    out["home_win"] = pd.to_numeric(merged[label_col], errors="coerce").astype("Int64")
    out = out[(out["home_win"].isin([0,1])) & out["home_win_prob"].notna()]
    if out.empty:
        sys.exit(f"Merged ({merge_name}) but no usable rows.")
    os.makedirs(OUT_DIR, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"Wrote {OUT_FILE} with {len(out)} rows using merge='{merge_name}' seasons={pred_seasons} hist='{os.path.basename(args.hist)}'")
if __name__ == "__main__":
    main()

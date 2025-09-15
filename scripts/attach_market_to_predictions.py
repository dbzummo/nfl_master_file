#!/usr/bin/env python3
import argparse, os, json, pandas as pd, numpy as np, glob
OUT_DEFAULT = "out/predictions_week_calibrated_with_market.csv"

TEAM_MAP_FILES = ["teams_lookup.json", "team_locations.csv"]
LINE_CANDS  = ["vegas_line","line","spread_home","spread","home_line"]
TOTAL_CANDS = ["vegas_total","total","totals","over_under"]

def load_team_map():
    # Try a JSON map first; else fall back to CSV of team_locations (name -> name)
    tm = {}
    for f in TEAM_MAP_FILES:
        if os.path.isfile(f) and f.endswith(".json"):
            try:
                tm = json.load(open(f))
                break
            except Exception:
                tm = {}
        if os.path.isfile(f) and f.endswith(".csv"):
            try:
                df = pd.read_csv(f)
                # allow any column containing 'team' to be a name; map to itself
                for col in df.columns:
                    if "team" in col.lower():
                        for s in df[col].astype(str):
                            tm[str(s).strip().upper()] = str(s).strip().upper()
                break
            except Exception:
                pass
    return tm

def norm_team(s, tm):
    s = str(s).strip().upper()
    return tm.get(s, s)

def pick_col(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None

def autodetect_market():
    for pat in ["weekly_odds_standard.csv", "odds_used.csv", "compare_odds.csv", "out/weekly_odds_standard.csv"]:
        if os.path.isfile(pat): return pat
    # fall back to any csv with obvious market headers
    for f in glob.glob("*.csv")+glob.glob("out/*.csv"):
        try:
            hdr = set(pd.read_csv(f, nrows=0).columns)
            if hdr & set(LINE_CANDS) and hdr & set(TOTAL_CANDS):
                return f
        except Exception:
            pass
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred_in",  default="out/predictions_week_calibrated_blend.csv")
    ap.add_argument("--market",   default=None, help="market CSV; if omitted, auto-detect")
    ap.add_argument("--out",      default=OUT_DEFAULT)
    ap.add_argument("--max_day_gap", type=int, default=2)
    args = ap.parse_args()

    pred = pd.read_csv(args.pred_in)
    need = {"home_team","away_team","date"}
    if not need.issubset(pred.columns):
        raise SystemExit(f"Predictions missing columns {need - set(pred.columns)}")
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.tz_localize(None).dt.date

    market_path = args.market or autodetect_market()
    if not market_path or not os.path.isfile(market_path):
        raise SystemExit("Could not find a market file; set --market=<file.csv>")

    mkt = pd.read_csv(market_path)
    # try to find home/away columns
    home_cands = [c for c in mkt.columns if c.lower() in ("home_team","home","team_home","home_name","home")]
    away_cands = [c for c in mkt.columns if c.lower() in ("away_team","away","team_away","away_name","away")]
    date_cands = [c for c in mkt.columns if c.lower() in ("date","game_date","gamedate","kickoff","kickoff_utc","start_time")]
    if not home_cands or not away_cands:
        raise SystemExit("Market CSV missing home/away columns")
    hcol, acol = home_cands[0], away_cands[0]
    dcol = date_cands[0] if date_cands else None

    # figure out line/total columns
    lcol  = pick_col(mkt, LINE_CANDS)
    tcol  = pick_col(mkt, TOTAL_CANDS)
    if lcol is None and "spread_away" in mkt.columns:
        # convert away spread to home line
        mkt["home_line"] = -pd.to_numeric(mkt["spread_away"], errors="coerce")
        lcol = "home_line"

    if lcol is None or tcol is None:
        raise SystemExit(f"Market CSV missing line/total columns (looked for line in {LINE_CANDS}, total in {TOTAL_CANDS})")

    # normalize teams
    tm = load_team_map()
    pred["_home"] = pred["home_team"].map(lambda s: norm_team(s, tm))
    pred["_away"] = pred["away_team"].map(lambda s: norm_team(s, tm))

    mkt = mkt.copy()
    mkt["_home"] = mkt[hcol].map(lambda s: norm_team(s, tm))
    mkt["_away"] = mkt[acol].map(lambda s: norm_team(s, tm))
    if dcol:
        mkt["_date"] = pd.to_datetime(mkt[dcol], errors="coerce").dt.tz_localize(None).dt.date
    else:
        mkt["_date"] = pd.NaT

    # reduce market to one row per matchup per date by averaging by book if multiple
    mred = mkt.groupby(["_home","_away","_date"], dropna=False).agg(
        line=(lcol,"mean"),
        total=(tcol,"mean")
    ).reset_index()

    # if we have exact date, merge on date; else nearest within ±max_day_gap
    left = pred.rename(columns={"date":"_pdate"})
    if mred["_date"].notna().any():
        merged = left.merge(mred, on=["_home","_away"], how="left")
        # choose nearest date row per game
        merged["_mdate"] = pd.to_datetime(merged["_date"])
        merged["_pdate_dt"] = pd.to_datetime(merged["_pdate"])
        merged["abs_gap"] = (merged["_mdate"] - merged["_pdate_dt"]).abs().dt.days
        merged.sort_values(["_home","_away","_pdate_dt","abs_gap"], inplace=True)
        merged = merged.groupby(["_home","_away","_pdate_dt"]).first().reset_index()
        merged.loc[merged["abs_gap"] > args.max_day_gap, ["line","total"]] = np.nan
    else:
        # no dates in market -> just aggregate by teams
        mteam = mred.groupby(["_home","_away"], dropna=False).agg(line=("line","mean"), total=("total","mean")).reset_index()
        merged = left.merge(mteam, on=["_home","_away"], how="left")

    out = merged.copy()
    # preserve your calibrated prob column name if present; else home_win_prob
    prob_col = "home_win_prob" if "home_win_prob" in out.columns else out.columns[out.columns.str.contains("prob")][0]
    out = out.rename(columns={
        "_pdate":"date"
    })
    cols = ["home_team","away_team","date", prob_col, "line","total"]
    # map back original names for clarity
    out["home_team"] = pred["home_team"].values
    out["away_team"] = pred["away_team"].values
    out["date"] = pred["date"].values
    out = out[cols]
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out} using market={os.path.basename(market_path)}; matched {out['line'].notna().sum()} lines, {out['total'].notna().sum()} totals out of {len(out)} games.")

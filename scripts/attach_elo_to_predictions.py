#!/usr/bin/env python3
"""
Attach latest pregame Elo to a predictions CSV (home_team, away_team, date[, home_win_prob]).
Outputs:
  out/predictions_with_elo.csv  (+ elo_home, elo_away, elo_diff, elo_prob_home)
  out/features_elo_week.csv     (minimal feature frame)
"""
import argparse, os
import pandas as pd

OUT_DIR = "out"

def expected_prob(elo_diff):
    import math
    return 1.0 / (1.0 + 10.0 ** (-(elo_diff) / 400.0))

def latest_elo_before(elo_df, team, date):
    rows = elo_df[(elo_df["team"] == team) & (elo_df["date"] <= date)]
    if rows.empty: return 1500.0
    return rows.sort_values("date").iloc[-1]["elo_post"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True)
    ap.add_argument("--elo", default="out/elo_ratings.csv")
    ap.add_argument("--hfa_elo", type=float, default=55.0)
    args = ap.parse_args()

    pred = pd.read_csv(args.pred)
    for c in ["home_team","away_team","date"]:
        if c not in pred.columns:
            raise SystemExit(f"Predictions missing required column: {c}")
    pred["date"] = pd.to_datetime(pred["date"], errors="coerce").dt.tz_localize(None).dt.date

    elo = pd.read_csv(args.elo)
    elo["date"] = pd.to_datetime(elo["date"], errors="coerce").dt.date

    elo_home, elo_away = [], []
    for _, r in pred.iterrows():
        h = str(r["home_team"]).strip().upper()
        a = str(r["away_team"]).strip().upper()
        d = r["date"]
        elo_home.append(latest_elo_before(elo, h, d))
        elo_away.append(latest_elo_before(elo, a, d))

    pred["elo_home"] = elo_home
    pred["elo_away"] = elo_away
    pred["elo_diff"] = (pred["elo_home"] + args.hfa_elo) - pred["elo_away"]
    pred["elo_prob_home"] = pred["elo_diff"].apply(expected_prob)

    os.makedirs(OUT_DIR, exist_ok=True)
    pred.to_csv(os.path.join(OUT_DIR, "predictions_with_elo.csv"), index=False)
    pred[["home_team","away_team","date","elo_home","elo_away","elo_diff"]] \
        .to_csv(os.path.join(OUT_DIR, "features_elo_week.csv"), index=False)

    print(f"Wrote out/predictions_with_elo.csv (rows={len(pred)}) and out/features_elo_week.csv")
if __name__ == "__main__":
    main()

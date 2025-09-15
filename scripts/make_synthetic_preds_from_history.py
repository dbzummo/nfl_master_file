#!/usr/bin/env python3
"""
Make a neutral baseline predictions CSV from a season's history file.

Input (auto-found):
  history/season_<SEASON>_from_site.csv with columns: home_team, away_team, date

Output:
  out/predictions_synth_<SEASON>.csv with:
    home_team, away_team, date, home_win_prob  (0.5 ± small jitter)
"""
import argparse, json, os, sys
import pandas as pd
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--out", default=None)
    ap.add_argument("--jitter_std", type=float, default=0.03)
    args = ap.parse_args()

    hist_path = f"history/season_{args.season}_from_site.csv"
    if not os.path.exists(hist_path):
        sys.exit(f"No history file: {hist_path}")

    df = pd.read_csv(hist_path)
    need = {"home_team","away_team","date"}
    if not need.issubset(df.columns):
        sys.exit(f"History missing required columns: {need}")

    df = df[["home_team","away_team","date"]].drop_duplicates().copy()
    rng = np.random.default_rng(7)
    df["home_win_prob"] = (0.5 + rng.normal(0, args.jitter_std, len(df))).clip(0.05, 0.95)

    os.makedirs("out", exist_ok=True)
    out_path = args.out or f"out/predictions_synth_{args.season}.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} rows={len(df)}")
if __name__ == "__main__":
    main()

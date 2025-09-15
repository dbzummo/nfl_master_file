#!/usr/bin/env python3
"""
Blend two probability columns and write out/blended_predictions.csv

Usage:
  --pred_in   out/predictions_with_elo_cal.csv   (must contain: home_win_prob, elo_logit_prob)
  --alpha     weight on your existing model (default 0.7)
"""
import argparse, os, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred_in", default="out/predictions_with_elo_cal.csv")
    ap.add_argument("--alpha", type=float, default=0.7)
    ap.add_argument("--out", default="out/blended_predictions.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.pred_in)
    need = ["home_win_prob","elo_logit_prob"]
    for c in need:
        if c not in df.columns:
            raise SystemExit(f"Missing column: {c}")
    a = args.alpha
    df["home_win_prob_blend"] = a*df["home_win_prob"] + (1-a)*df["elo_logit_prob"]
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} (alpha={a})")
if __name__ == "__main__":
    main()

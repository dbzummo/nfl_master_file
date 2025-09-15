#!/usr/bin/env python3
"""
Isotonic calibration for blended probabilities.

Usage:

# Train on last complete season backtest
python3 scripts/calibrate_blend.py --train_csv out/backtest_details.csv

# Apply to current predictions
python3 scripts/calibrate_blend.py \
  --apply_csv out/blended_predictions.csv \
  --model out/blend_isotonic.joblib \
  --out out/blended_predictions_cal.csv
"""

import argparse, os, json, joblib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss, brier_score_loss

OUT_DIR = "out"
ART_DIR = "artifacts"

def reliability_curve(y_true, y_prob, bins=10):
    df = pd.DataFrame({"y": y_true, "p": y_prob})
    df["bin"] = pd.cut(df["p"], bins=np.linspace(0,1,bins+1), include_lowest=True)
    out = df.groupby("bin").agg(
        avg_prob=("p","mean"),
        emp_rate=("y","mean"),
        n=("y","size")
    ).dropna()
    return out.reset_index(drop=True)

def train(train_csv, model_out, report_out, plot_out):
    df = pd.read_csv(train_csv)
    if not {"home_win_prob","home_win"}.issubset(df.columns):
        raise SystemExit("Train CSV must have columns: home_win_prob, home_win")

    y_true = df["home_win"].astype(int).values
    y_prob = df["home_win_prob"].astype(float).values

    # baseline metrics
    base_ll = log_loss(y_true, y_prob)
    base_br = brier_score_loss(y_true, y_prob)

    # fit isotonic
    iso = IsotonicRegression(out_of_bounds="clip")
    y_cal = iso.fit_transform(y_prob, y_true)

    # post metrics
    cal_ll = log_loss(y_true, y_cal)
    cal_br = brier_score_loss(y_true, y_cal)

    # reliability curves
    pre = reliability_curve(y_true, y_prob, bins=10)
    post = reliability_curve(y_true, y_cal, bins=10)

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(ART_DIR, exist_ok=True)

    # save model + report
    joblib.dump(iso, model_out)
    with open(report_out,"w") as f:
        json.dump({
            "n": int(len(y_true)),
            "baseline": {"logloss": base_ll, "brier": base_br},
            "calibrated": {"logloss": cal_ll, "brier": cal_br}
        }, f, indent=2)

    # plot
    plt.figure(figsize=(6,6))
    plt.plot(pre["avg_prob"], pre["emp_rate"], "o-", label="Pre-blend")
    plt.plot(post["avg_prob"], post["emp_rate"], "o-", label="Post-calibrated")
    plt.plot([0,1],[0,1],"k--")
    plt.xlabel("Predicted prob")
    plt.ylabel("Empirical win rate")
    plt.title("Isotonic calibration (pre vs post)")
    plt.legend()
    plt.savefig(plot_out, bbox_inches="tight")
    plt.close()

    print(f"[done] Trained isotonic → {model_out}, report {report_out}, plot {plot_out}")

def apply(apply_csv, model_in, out_csv):
    df = pd.read_csv(apply_csv)
    if not {"home_win_prob","elo_logit_prob"}.issubset(df.columns):
        raise SystemExit("Apply CSV must have columns incl. home_win_prob and elo_logit_prob")

    iso = joblib.load(model_in)
    df["home_win_prob_cal"] = iso.transform(df["home_win_prob"].astype(float))
    df.to_csv(out_csv, index=False)
    print(f"[done] Applied isotonic model → {out_csv} (rows={len(df)})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train_csv")
    ap.add_argument("--apply_csv")
    ap.add_argument("--model", default=os.path.join(OUT_DIR,"blend_isotonic.joblib"))
    ap.add_argument("--out", default=os.path.join(OUT_DIR,"blended_predictions_cal.csv"))
    ap.add_argument("--report", default=os.path.join(OUT_DIR,"calib_report.json"))
    ap.add_argument("--plot", default=os.path.join(ART_DIR,"calibration_pre_post.png"))
    args = ap.parse_args()

    if args.train_csv:
        train(args.train_csv, args.model, args.report, args.plot)
    elif args.apply_csv:
        apply(args.apply_csv, args.model, args.out)
    else:
        raise SystemExit("Must provide either --train_csv or --apply_csv")

if __name__ == "__main__":
    main()

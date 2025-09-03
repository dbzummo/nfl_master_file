#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Apply a saved calibrator onto predictions_week.csv:
- Supports {"method":"platt","A","B","feature":("logit"|"prob")}
- Supports {"method":"isotonic","x_":[...],"y_":[...]}
Outputs:
  - predictions_week_calibrated.csv (adds 'home_win_prob_cal')
  - also overwrites predictions_week.csv with that new column
"""

import json, pickle, sys
from pathlib import Path
import numpy as np
import pandas as pd

PRED   = Path("predictions_week.csv")
OUTCAL = Path("predictions_week_calibrated.csv")
CALPKL = Path("artifacts/calibrator.pkl")

def logit(p):
    p = np.clip(np.asarray(p, float), 1e-6, 1-1e-6)
    return np.log(p/(1-p))

def sigmoid(z):
    return 1.0/ (1.0 + np.exp(-z))

def apply_platt(df, col, A, B, feature="logit"):
    x = df[col].to_numpy(float)
    z = x if feature == "prob" else logit(x)
    return np.clip(sigmoid(A*z + B), 1e-6, 1-1e-6)

def apply_isotonic(df, col, x_thr, y_thr):
    x = df[col].to_numpy(float).clip(1e-6, 1-1e-6)
    return np.interp(x, x_thr, y_thr).clip(1e-6, 1-1e-6)

def main():
    if not PRED.exists():
        raise SystemExit(f"⛔ Missing {PRED}.")
    if not CALPKL.exists():
        raise SystemExit(f"⛔ Missing {CALPKL}.")

    df = pd.read_csv(PRED)

    # find a prob-like column
    cand = [c for c in ("home_winprob","home_prob","prob","p","pred_prob") if c in df.columns]
    if not cand:
        raise SystemExit("⛔ No probability-like column found in predictions_week.csv.")
    raw_col = cand[0]

    cal = pickle.load(open(CALPKL,"rb"))

    if isinstance(cal, dict) and cal.get("method") == "platt":
        A = float(cal["A"]); B = float(cal["B"])
        feature = cal.get("feature","logit")
        cal_arr = apply_platt(df, raw_col, A, B, feature=feature)
        method = f"platt({feature})"
    elif isinstance(cal, dict) and cal.get("method") == "isotonic":
        x_thr = np.array(cal["x_"], float)
        y_thr = np.array(cal["y_"], float)
        cal_arr = apply_isotonic(df, raw_col, x_thr, y_thr)
        method = "isotonic"
    else:
        raise SystemExit("⛔ Unknown calibrator format. Expect dict with method=platt|isotonic.")

    df["home_win_prob_cal"] = cal_arr
    df.to_csv(OUTCAL, index=False)
    PRED.write_text(df.to_csv(index=False))
    print(f"✅ Wrote {OUTCAL} with calibrated probs in 'home_win_prob_cal' (based on '{raw_col}', method={method}).")

if __name__ == "__main__":
    main()
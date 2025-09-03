#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Robust calibration pipeline (updated to handle y_home / p_home):
- Finds OOF predictions
- Detects label/probability/logit columns (now includes y_home, p_home)
- Fits Platt on logit(p) with guardrails; fallback to isotonic
Artifacts:
  artifacts/calibration.json
  artifacts/calibrator_meta.json
  artifacts/calibrator.pkl  (portable dict)
  artifacts/calibration_after.png
"""

import json, pickle
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.isotonic import IsotonicRegression

ART = Path("artifacts")
ART.mkdir(exist_ok=True)

# ---------- helpers ----------
def logit(p):
    p = np.clip(np.asarray(p, float), 1e-6, 1-1e-6)
    return np.log(p/(1-p))

def sigmoid(z):
    return 1.0/(1.0+np.exp(-z))

def brier(y, p):
    y = np.asarray(y, float); p = np.asarray(p, float)
    return np.mean((p - y)**2)

def logloss(y, p):
    p = np.clip(np.asarray(p, float), 1e-6, 1-1e-6)
    y = np.asarray(y, float)
    return -np.mean(y*np.log(p) + (1-y)*np.log(1-p))

def reliability_plot(y, p_raw, p_cal, out_png):
    bins = np.linspace(0, 1, 11)
    idx  = np.digitize(p_raw, bins) - 1
    cidx = np.digitize(p_cal, bins) - 1
    df = pd.DataFrame({"y": y, "p_raw": p_raw, "p_cal": p_cal, "b": idx, "bc": cidx})

    plt.figure(figsize=(6,5))
    g = df.groupby("b").agg(obs=("y","mean"), pred=("p_raw","mean"), n=("y","size"))
    plt.plot(g["pred"], g["obs"], marker="o", label="Raw")
    g2 = df.groupby("bc").agg(obs=("y","mean"), pred=("p_cal","mean"), n=("y","size"))
    plt.plot(g2["pred"], g2["obs"], marker="o", label="Calibrated")
    plt.plot([0,1],[0,1], "--", alpha=0.5, label="Perfect")
    plt.xlabel("Predicted probability")
    plt.ylabel("Observed fraction")
    plt.title("Reliability (Raw vs Calibrated)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()

def find_oof_path():
    cands = [
        ART / "backtest_details.csv",
        ART / "oof_predictions.csv",
        Path("backtest_details.csv"),
    ]
    for p in cands:
        if p.exists():
            return p
    raise SystemExit("⛔ Could not find an OOF file (tried artifacts/backtest_details.csv, artifacts/oof_predictions.csv, backtest_details.csv). Run `python backtest.py` first.")

def detect_columns(df):
    # Label candidates (now includes y_home)
    y_cands = ["home_win","label","y","target","outcome","home_win_obs","y_home"]
    # Probability candidates in [0,1] (now includes p_home)
    p_cands = ["home_winprob","prob","pred_prob","p","proba","prob1","home_prob","prediction","p_home"]
    # Logit/Margin candidates (need sigmoid)
    z_cands = ["logit","margin","score","home_logit","log_odds"]

    y_col = next((c for c in y_cands if c in df.columns), None)
    if y_col is None:
        raise SystemExit(f"⛔ OOF file missing label column. Tried {y_cands} — found: {list(df.columns)}")

    # Pick probability col if valid in [0,1]
    p_col = None
    for c in p_cands:
        if c in df.columns:
            p_tmp = pd.to_numeric(df[c], errors="coerce")
            if p_tmp.notna().any():
                mn, mx = float(p_tmp.min()), float(p_tmp.max())
                if mn >= -0.01 and mx <= 1.01:
                    p_col = c
                    break

    # If no probability col, fall back to a logit-like column
    z_col = next((c for c in z_cands if c in df.columns), None)

    if p_col is None and z_col is None:
        raise SystemExit("⛔ OOF file missing probability/logit columns. "
                         f"Tried prob {p_cands} and logit {z_cands}.")
    return y_col, p_col, z_col

# ---------- main ----------
def main():
    oof_path = find_oof_path()
    df = pd.read_csv(oof_path)

    y_col, p_col, z_col = detect_columns(df)
    y = df[y_col].astype(int).to_numpy()

    if p_col is not None:
        p = pd.to_numeric(df[p_col], errors="coerce").astype(float).to_numpy()
        p = np.clip(p, 1e-6, 1-1e-6)
    else:
        z = pd.to_numeric(df[z_col], errors="coerce").astype(float).to_numpy()
        p = sigmoid(z)

    if len(p) < 200:
        print(f"⚠️ OOF coverage looks small ({len(p)} rows). Calibration may be unstable.")

    # Fit Platt on logit(p)
    z_feat = logit(p)
    lr = LogisticRegression(penalty="l2", C=1e6, solver="lbfgs", max_iter=2000, fit_intercept=True)
    lr.fit(z_feat.reshape(-1,1), y)
    A = float(lr.coef_.ravel()[0])
    B = float(lr.intercept_.ravel()[0])
    p_platt = np.clip(sigmoid(A*z_feat + B), 1e-6, 1-1e-6)

    raw_ll, raw_br = logloss(y,p), brier(y,p)
    pl_ll,  pl_br  = logloss(y,p_platt), brier(y,p_platt)

    # Guardrails: fallback to isotonic if Platt is nearly flat
    p_range = float(p_platt.max() - p_platt.min())
    nearly_flat = (abs(A) < 0.1) or (p_range < 0.05)

    use_isotonic = False
    if nearly_flat:
        print(f"⚠️ Platt looks nearly flat (A={A:.4f}, cal range={p_range:.4f}). Trying isotonic fallback…")
        iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        iso.fit(p, y)             # isotonic on raw prob
        p_iso = iso.transform(p)
        iso_ll, iso_br = logloss(y,p_iso), brier(y,p_iso)
        if (iso_ll < pl_ll - 1e-6) or (abs(iso_ll - pl_ll) < 1e-6 and iso_br <= pl_br):
            use_isotonic = True
            calib = {"method":"isotonic",
                     "x_": iso.X_thresholds_.tolist(),
                     "y_": iso.y_thresholds_.tolist()}
            p_cal = p_iso
            scores = {"isotonic": {"logloss": iso_ll, "brier": iso_br},
                      "platt":    {"logloss": pl_ll,  "brier": pl_br}}
            choice = {"type":"isotonic","params":{"type":"isotonic"}}
            print("→ Using isotonic (better/safer).")
        else:
            calib = {"method":"platt","A":A,"B":B,"feature":"logit"}
            p_cal = p_platt
            scores = {"platt": {"logloss": pl_ll, "brier": pl_br}}
            choice = {"type":"platt","params":{"type":"platt","A":A,"B":B,"feature":"logit"}}
            print("→ Keeping Platt (wins by metrics).")
    else:
        calib = {"method":"platt","A":A,"B":B,"feature":"logit"}
        p_cal = p_platt
        scores = {"platt": {"logloss": pl_ll, "brier": pl_br}}
        choice = {"type":"platt","params":{"type":"platt","A":A,"B":B,"feature":"logit"}}

    # Save artifacts
    with open(ART/"calibrator.pkl","wb") as f:
        pickle.dump(calib, f)
    json.dump(choice, open(ART/"calibration.json","w"), indent=2)
    meta = {
        "selected_by": "guarded_logloss_then_brier",
        "oof_path": str(oof_path),
        "y_col": y_col, "p_col": p_col, "z_col": z_col,
        "raw_scores": {"logloss": raw_ll, "brier": raw_br, "mean_raw": float(p.mean())},
        "scores": scores
    }
    json.dump(meta, open(ART/"calibrator_meta.json","w"), indent=2)

    reliability_plot(y, p, p_cal, ART/"calibration_after.png")

    print("✅ Saved calibrator:", json.dumps(choice, indent=2))
    print(f"   Raw logloss={raw_ll:.4f} → Cal logloss={logloss(y,p_cal):.4f}")
    print(f"   Raw  brier ={raw_br:.4f} → Cal  brier ={brier(y,p_cal):.4f}")

if __name__ == "__main__":
    main()
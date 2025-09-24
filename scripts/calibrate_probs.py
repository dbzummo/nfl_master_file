#!/usr/bin/env python3
"""
calibrate_probs.py

Builds Platt (logistic) and Isotonic calibrators from historical Elo expectations,
then applies them to the current week's Elo expectations.

Inputs
------
- out/elo_games_enriched.csv        (from Elo step; needs exp_home, home_score, away_score)
- out/week_with_elo.csv             (joined week with elo_exp_home)

Outputs
-------
- out/calibration/platt.joblib
- out/calibration/isotonic.joblib
- out/week_predictions.csv          (date, teams, elo_exp_home, calibrated probs)
"""

import sys, json, pathlib
import numpy as np
import pandas as pd

from sklearn.model_selection import KFold
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from joblib import dump

CLIP = (1e-15, 1 - 1e-15)

# ---------- helpers ----------

def _ensure_exists(p: pathlib.Path, label: str):
    if not p.exists():
        sys.exit(f"[FATAL] Missing {label}: {p}")

def _cv_scores_platt(x, y, n_splits=5, random_state=42):
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=random_state)
    preds = np.zeros_like(y, dtype=float)
    lx = _safe_logit(x)
    for tr, te in kf.split(x):
        lr = LogisticRegression(solver="lbfgs", max_iter=1000)
        lr.fit(lx[tr].reshape(-1, 1), y[tr])
        preds[te] = lr.predict_proba(lx[te].reshape(-1, 1))[:, 1]
    preds = np.clip(preds, *CLIP)
    return {
        "brier": float(brier_score_loss(y, preds)),
        "logloss": float(log_loss(y, preds)),
    }, preds

def _cv_scores_iso(x, y, n_splits=5, random_state=42):
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=random_state)
    preds = np.zeros_like(y, dtype=float)
    for tr, te in kf.split(x):
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(x[tr], y[tr])
        preds[te] = iso.predict(x[te])
    preds = np.clip(preds, *CLIP)
    return {
        "brier": float(brier_score_loss(y, preds)),
        "logloss": float(log_loss(y, preds)),
    }, preds

def _safe_logit(p):
    p = np.clip(p, *CLIP)
    return np.log(p/(1.0 - p))

# ---------- main ----------

def main():
    hist_path = pathlib.Path("out/elo_games_enriched.csv")
    week_path = pathlib.Path("out/week_with_elo.csv")
    _ensure_exists(hist_path, "historical Elo file")
    _ensure_exists(week_path, "weekly Elo file")

    hist = pd.read_csv(hist_path)
    need_hist = {"exp_home", "home_score", "away_score"}
    if not need_hist.issubset(hist.columns):
        sys.exit(f"[FATAL] {hist_path} missing columns {sorted(need_hist - set(hist.columns))}")

    # training data
    x = hist["exp_home"].astype(float).to_numpy()
    y = (hist["home_score"].astype(float) > hist["away_score"].astype(float)).astype(int).to_numpy()

    # sanity clip
    x = np.clip(x, *CLIP)

    # cross-validated metrics
    platt_cv, platt_preds = _cv_scores_platt(x, y)
    iso_cv, iso_preds     = _cv_scores_iso(x, y)

    print("[CV] Platt   :", json.dumps(platt_cv))
    print("[CV] Isotonic:", json.dumps(iso_cv))

    # fit final calibrators on all history
    lr = LogisticRegression(solver="lbfgs", max_iter=1000)
    lr.fit(_safe_logit(x).reshape(-1,1), y)
    iso = IsotonicRegression(out_of_bounds="clip")
    iso.fit(x, y)

    cal_dir = pathlib.Path("out/calibration")
    cal_dir.mkdir(parents=True, exist_ok=True)
    dump(lr, cal_dir / "platt.joblib")
    dump(iso, cal_dir / "isotonic.joblib")
    print(f"[OK] Saved calibrators → {cal_dir}/platt.joblib, {cal_dir}/isotonic.joblib")

    # apply to this week's games
    week = pd.read_csv(week_path)
    need_week = {"date","away_team","home_team","elo_exp_home"}
    if not need_week.issubset(week.columns):
        sys.exit(f"[FATAL] {week_path} missing columns {sorted(need_week - set(week.columns))}")

    w = week.copy()
    w["p_home_raw"] = np.clip(w["elo_exp_home"].astype(float).to_numpy(), *CLIP)

    lx = _safe_logit(w["p_home_raw"].to_numpy()).reshape(-1,1)
    w["p_home_cal_platt"] = np.clip(lr.predict_proba(lx)[:,1], *CLIP)
    w["p_home_cal_iso"]   = np.clip(iso.predict(w["p_home_raw"].to_numpy()), *CLIP)

    # convenience: away probs
    for col in ("p_home_raw","p_home_cal_platt","p_home_cal_iso"):
        w[col.replace("home","away")] = 1.0 - w[col]

    outp = pathlib.Path("out/week_predictions.csv")
    cols = [
        "date","week","away_team","home_team","msf_game_id",
        "elo_home_pre","elo_away_pre","elo_diff_pre","elo_exp_home",
        "p_home_raw","p_home_cal_platt","p_home_cal_iso",
        "p_away_raw","p_away_cal_platt","p_away_cal_iso",
    ]
    have = [c for c in cols if c in w.columns]
    w[have].sort_values(["date","home_team","away_team"]).to_csv(outp, index=False)
    print(f"[OK] wrote {outp} rows={len(w)}")

    # spotlight WSH @ GB if present
    mask = (
        w["away_team"].astype(str).str.upper().isin(["WSH","WAS"]) &
        w["home_team"].astype(str).str.upper().isin(["GB","GNB","PACKERS","GREEN BAY"])
    )
    if mask.any():
        r = w[mask].iloc[0]
        print(f"[SPOTLIGHT] {r['away_team']} @ {r['home_team']} on {r['date']}")
        print(f"  elo_exp_home={float(r['elo_exp_home']):.3f}")
        print(f"  cal_platt   ={float(r['p_home_cal_platt']):.3f}")
        print(f"  cal_isotonic={float(r['p_home_cal_iso']):.3f}")

if __name__ == "__main__":
    main()

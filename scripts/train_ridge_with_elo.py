#!/usr/bin/env python3
"""
Train a 1-feature logistic on elo_diff to predict home_win using historical games.

Input:
  out/elo_games_enriched.csv   (from compute_elo.py; contains elo_diff_pre and result)
Output:
  out/elo_logit_model.json     {"intercept": ..., "coef": ..., "scale": "logistic"}
  out/elo_logit_eval.json      simple CV metrics (logloss/brier)
"""
import json, os, argparse
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, brier_score_loss
from sklearn.model_selection import KFold

OUT_DIR="out"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games_csv", default="out/elo_games_enriched.csv")
    ap.add_argument("--min_season", type=int, default=2019)
    ap.add_argument("--max_season", type=int, default=9999)
    ap.add_argument("--c", type=float, default=1.0)  # inverse of regularization strength
    args = ap.parse_args()

    df = pd.read_csv(args.games_csv)
    df = df[(df["season"] >= args.min_season) & (df["season"] <= args.max_season)].copy()
    df["home_win"] = (df["home_score"] > df["away_score"]).astype(int)

    X = df[["elo_diff_pre"]].values
    y = df["home_win"].values

    # Ridge-like regularization via L2 logistic
    lr = LogisticRegression(C=args.c, solver="lbfgs", penalty="l2", max_iter=500)
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    ll, br = [], []
    for tr, te in kf.split(X):
        lr.fit(X[tr], y[tr])
        p = lr.predict_proba(X[te])[:,1]
        ll.append(log_loss(y[te], p))
        br.append(brier_score_loss(y[te], p))

    # Fit on all data for final params
    lr.fit(X, y)
    model = {"intercept": float(lr.intercept_[0]), "coef": float(lr.coef_[0][0]), "scale": "logistic"}
    evalj = {"cv_logloss_mean": float(np.mean(ll)), "cv_logloss_std": float(np.std(ll)),
             "cv_brier_mean": float(np.mean(br)), "cv_brier_std": float(np.std(br)),
             "n": int(len(df))}
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR,"elo_logit_model.json"),"w") as f: json.dump(model,f,indent=2)
    with open(os.path.join(OUT_DIR,"elo_logit_eval.json"),"w") as f: json.dump(evalj,f,indent=2)
    print("Wrote out/elo_logit_model.json and out/elo_logit_eval.json")
    print(evalj)
if __name__ == "__main__":
    main()

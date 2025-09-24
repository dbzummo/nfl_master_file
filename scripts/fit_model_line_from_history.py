#!/usr/bin/env python3
"""
fit_model_line_from_history.py

Accuracy-first calibration of the mapping between closing home spread (home perspective)
and home win probability:

    P(home) = sigmoid(a + b * spread_home)

Inputs (must exist and contain: date, home_team, away_team, home_score, away_score, spread_home):
    history/season_2019_from_site.csv
    history/season_2020_from_site.csv
    history/season_2021_from_site.csv
    history/season_2022_from_site.csv
    history/season_2023_from_site.csv
    history/season_2024_from_site.csv

Outputs:
    out/calibration/model_line_calibration.json   # {"a": <float>, "b": <float>, "n_games": <int>}
    out/calibration/model_line_fit_points.csv     # binned diagnostic (spread bin -> freq, win rate)

We abort (no write) if validation fails or the fitted slope has the wrong sign.
"""

import json
import pathlib
import sys
from typing import List

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss


HISTORY_FILES: List[str] = [
    "history/season_2019_from_site.csv",
    "history/season_2020_from_site.csv",
    "history/season_2021_from_site.csv",
    "history/season_2022_from_site.csv",
    "history/season_2023_from_site.csv",
    "history/season_2024_from_site.csv",
]

REQUIRED_COLS = {"date", "home_team", "away_team", "home_score", "away_score", "spread_home"}
OUT_JSON = pathlib.Path("out/calibration/model_line_calibration.json")
OUT_BINS = pathlib.Path("out/calibration/model_line_fit_points.csv")


def _load_history() -> pd.DataFrame:
    missing = [p for p in HISTORY_FILES if not pathlib.Path(p).exists()]
    if missing:
        print("[FATAL] Missing required history files:", ", ".join(missing))
        sys.exit(2)

    frames = []
    for p in HISTORY_FILES:
        df = pd.read_csv(p)
        cols_lower = {c.lower(): c for c in df.columns}
        colmap = {
            "date": cols_lower.get("date", "date"),
            "home_team": cols_lower.get("home_team", "home_team"),
            "away_team": cols_lower.get("away_team", "away_team"),
            "home_score": cols_lower.get("home_score", "home_score"),
            "away_score": cols_lower.get("away_score", "away_score"),
            "spread_home": cols_lower.get("spread_home", "spread_home"),
        }
        if not REQUIRED_COLS.issubset({colmap[k] for k in colmap}):
            print(f"[FATAL] {p} is missing required columns. Found:", list(df.columns))
            sys.exit(2)

        df = df.rename(columns={v: k for k, v in colmap.items()})
        frames.append(df[["date", "home_team", "away_team", "home_score", "away_score", "spread_home"]])

    out = pd.concat(frames, ignore_index=True)
    out["home_score"] = pd.to_numeric(out["home_score"], errors="coerce")
    out["away_score"] = pd.to_numeric(out["away_score"], errors="coerce")
    out["spread_home"] = pd.to_numeric(out["spread_home"], errors="coerce")
    out["home_team"] = out["home_team"].astype(str).str.upper().str.strip()
    out["away_team"] = out["away_team"].astype(str).str.upper().str.strip()
    out = out.dropna(subset=["home_score", "away_score", "spread_home"]).copy()
    out = out[np.abs(out["spread_home"]) <= 30].copy()  # filter obvious bad rows
    out["home_win"] = (out["home_score"] > out["away_score"]).astype(int)
    out = out[out["home_score"] != out["away_score"]].copy()  # drop ties
    return out


def _validate_spread_convention(df: pd.DataFrame) -> None:
    """
    Sense check:
      - Big home favorites (very negative spread_home) should have HIGH win rates.
      - Moderate home dogs (positive spread_home) should have LOWER win rates.

    We use positional access on the binned series to avoid IntervalIndex label pitfalls.
    """
    bins = pd.cut(df["spread_home"], bins=[-50, -7, -3, 0, 3, 7, 50])
    grp = df.groupby(bins)["home_win"].mean().rename("home_win_rate")

    print("\n[CHECK] Home win rate by spread_home bins (home perspective):")
    print(grp.to_frame())

    if len(grp) < 3 or grp.isna().all():
        print("\n[FATAL] Not enough binned data to validate spread convention.")
        sys.exit(3)

    # Prefer full set of bins: [-50,-7],(-7,-3],(-3,0],(0,3],(3,7],(7,50]
    # Use .iloc to avoid IntervalIndex label issues.
    try:
        fav_rate = float(grp.iloc[0])       # most favorite bin (very negative)
        dog_rate = float(grp.iloc[-2])      # (3,7] bin if present
    except Exception:
        # Fallback: compute means over negative vs positive sides
        fav_rate = float(grp[pd.Index([iv for iv in grp.index if getattr(iv, "right", 0) <= -3])].mean())
        dog_rate = float(grp[pd.Index([iv for iv in grp.index if getattr(iv, "left", 0) >= 3])].mean())

    print(f"\n[CHECK] fav_rate≈{fav_rate:.3f}  vs  dog_rate≈{dog_rate:.3f}")
    if not np.isfinite(fav_rate) or not np.isfinite(dog_rate):
        print("\n[FATAL] Could not compute validation rates; bins too sparse.")
        sys.exit(3)

    if fav_rate <= dog_rate:
        print("\n[FATAL] Spread convention looks wrong: big home favorites are not "
              "winning more than home dogs. Verify 'spread_home' sign convention.")
        sys.exit(3)


def _fit_logistic(df: pd.DataFrame):
    X = df[["spread_home"]].to_numpy()
    y = df["home_win"].to_numpy().astype(int)

    lr = LogisticRegression(solver="lbfgs")
    lr.fit(X, y)

    b = float(lr.coef_[0][0])
    a = float(lr.intercept_[0])

    if b >= 0:
        print(f"\n[FATAL] Fitted slope b={b:.4f} is not negative. "
              f"This indicates wrong data or spread convention.")
        sys.exit(4)

    df = df.copy()
    df["p_hat"] = lr.predict_proba(X)[:, 1]
    bins = pd.cut(df["spread_home"], bins=[-50, -7, -3, 0, 3, 7, 50])
    diag = df.groupby(bins).agg(
        n=("home_win", "size"),
        win_rate=("home_win", "mean"),
        p_hat=("p_hat", "mean"),
    )

    try:
        ll = log_loss(y, df["p_hat"], eps=1e-15)
    except TypeError:
        ll = log_loss(y, df["p_hat"])
    bs = brier_score_loss(y, df["p_hat"])

    print("\n[DIAG] Bin diagnostics (count, empirical win_rate, model p_hat):")
    print(diag)
    print(f"\n[SCORING] logloss={ll:.4f}  brier={bs:.4f}")

    def sigmoid(z): return 1.0 / (1.0 + np.exp(-z))
    for s in [-7, -3, 0, 3, 7]:
        ph = sigmoid(a + b * s)
        print(f"[POINT] spread_home={s:+.1f} → P(home)={ph:.3f}")

    return a, b, diag.reset_index().rename(columns={"spread_home": "bin"})


def main():
    print("[STEP] Loading historical seasons …")
    df = _load_history()
    print(f"[INFO] Loaded rows: {len(df)} (ties dropped).")

    print("\n[STEP] Validating spread convention …")
    _validate_spread_convention(df)

    print("\n[STEP] Fitting logistic mapping P(home) ~ spread_home …")
    a, b, diag = _fit_logistic(df)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump({"a": a, "b": b, "n_games": int(len(df))}, f, indent=2)
    diag.to_csv(OUT_BINS, index=False)

    print(f"\n[OK] Saved coefficients → {OUT_JSON}")
    print(f"[OK] Wrote bin diagnostics → {OUT_BINS}")
    print(f"[SUMMARY] a={a:+.4f}  b={b:+.4f}  n={len(df)}")
    print("\nNext: point the board generator to READ these coefficients (do not refit).")


if __name__ == "__main__":
    main()

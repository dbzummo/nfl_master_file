#!/usr/bin/env python3
"""
Build the canonical model board for the current week.

Inputs (expected to exist from prior steps in Phase 0):
  - out/week_with_elo.csv
      required columns: date, week, home_team, away_team, elo_exp_home (as p_home prior)
  - OPTIONAL market spreads (first found is used):
      out/week_with_market.csv     (vegas_line_home or spread_home)
      out/odds_week.csv            (vegas_line_home or spread_home)
      out/odds/odds_week.csv       (vegas_line_home or spread_home)
  - OPTIONAL calibrated predictions:
      out/week_predictions.csv     (p_home_model or p_home)

Output:
  - out/model_board.csv
      guaranteed columns (at minimum):
        date, week, home_team, away_team,
        p_home_model,                 # model probability used downstream
        vegas_line_home               # numeric, default 0.0 if no market available

Fail-closed behaviour:
  - If out/week_with_elo.csv is missing or malformed -> fatal.
  - If calibrated preds missing -> derive p_home_model from elo_exp_home.
  - If market spreads missing -> vegas_line_home = 0.0 (explicit, deterministic fallback).
"""
import sys, pathlib
import pandas as pd
from typing import Optional

OUT = pathlib.Path("out/model_board.csv")

def fatal(msg, code=1):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)

def read_week_with_elo() -> pd.DataFrame:
    p = pathlib.Path("out/week_with_elo.csv")
    if not p.exists():
        fatal("Missing out/week_with_elo.csv (run join_week_with_elo.py first).", 21)
    df = pd.read_csv(p)
    need = {"date","week","home_team","away_team","elo_exp_home"}
    if not need.issubset(df.columns):
        missing = sorted(need - set(df.columns))
        fatal(f"out/week_with_elo.csv missing columns: {missing}", 22)
    # normalize
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date","home_team","away_team"]).copy()
    return df

def read_market() -> Optional[pd.DataFrame]:
    cands = [
        "out/week_with_market.csv",
        "out/odds_week.csv",
        "out/odds/odds_week.csv",
    ]
    for path in cands:
        p = pathlib.Path(path)
        if not p.exists():
            continue
        try:
            m = pd.read_csv(p)
            # find a spread column and normalize to vegas_line_home (float)
            col = None
            for k in ("vegas_line_home","spread_home","home_spread","line_home"):
                if k in m.columns:
                    col = k; break
            if col is None:
                continue
            keep = ["date","home_team","away_team", col]
            keep = [k for k in keep if k in m.columns]
            m = m[keep].copy()
            m.rename(columns={col:"vegas_line_home"}, inplace=True)
            m["date"] = pd.to_datetime(m["date"], errors="coerce").dt.date
            m["vegas_line_home"] = pd.to_numeric(m["vegas_line_home"], errors="coerce")
            return m
        except Exception:
            continue
    return None

def read_preds() -> Optional[pd.DataFrame]:
    p = pathlib.Path("out/week_predictions.csv")
    if not p.exists():
        return None
    try:
        w = pd.read_csv(p)
        # find model probability column
        col = None
        for k in ("p_home_model","p_home","p_model_home"):
            if k in w.columns:
                col = k; break
        if col is None:
            return None
        keep = ["date","home_team","away_team","week", col]
        keep = [k for k in keep if k in w.columns]
        w = w[keep].copy()
        w.rename(columns={col:"p_home_model"}, inplace=True)
        w["date"] = pd.to_datetime(w["date"], errors="coerce").dt.date
        w["p_home_model"] = pd.to_numeric(w["p_home_model"], errors="coerce")
        return w
    except Exception:
        return None

def main():
    base = read_week_with_elo()
    market = read_market()
    preds  = read_preds()

    # start board with base features
    board = base.copy()
    board["p_home_model"] = pd.NA  # will fill from preds or elo prior
    # default market line = 0.0 if we have no market
    board["vegas_line_home"] = 0.0

    if preds is not None and not preds.empty:
        board = board.merge(preds, on=["date","home_team","away_team","week"], how="left", suffixes=("",""))
        # prefer calibrated predictions when available
        mask = board["p_home_model"].notna()
        filled = mask.sum()
        print(f"[BOARD] merged calibrated predictions: filled {filled} rows")
    else:
        print("[BOARD] no calibrated predictions found; will derive p_home_model from elo_exp_home")

    # Fill p_home_model with elo prior where missing
    board["p_home_model"] = board["p_home_model"].astype(float)
    board["p_home_model"] = board["p_home_model"].fillna(board["elo_exp_home"])

    # Merge market spreads if available
    if market is not None and not market.empty:
        board = board.merge(market, on=["date","home_team","away_team"], how="left", suffixes=("",""))
        # ensure numeric with explicit 0.0 fallback
        board["vegas_line_home"] = board["vegas_line_home"].fillna(0.0).astype(float)
        print("[BOARD] merged market spreads into vegas_line_home")
    else:
        print("[BOARD] no market file found; vegas_line_home set to 0.0 for all rows (deterministic fallback)")

    # Minimal schema the downstream pipeline expects; keep helpful extras
    cols_order = [
        "date","week","away_team","home_team",
        "p_home_model","vegas_line_home",
        "elo_exp_home","elo_diff_pre","elo_home_pre","elo_away_pre","msf_game_id"
    ]
    cols = [c for c in cols_order if c in board.columns]
    # Include any remaining columns at the end
    rest = [c for c in board.columns if c not in cols]
    out = board[cols + rest].copy()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()

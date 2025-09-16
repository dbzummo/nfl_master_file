#!/usr/bin/env python3
"""
Build the canonical model board for the current week.

Inputs:
  - out/week_with_elo.csv                (required)
  - out/week_predictions.csv             (optional; supplies p_home_model)
  - out/week_with_market.csv / odds*.csv (optional; supplies vegas_line_home)

Output:
  - out/model_board.csv  (deterministic, fail-closed)
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
            col = next((k for k in ("vegas_line_home","spread_home","home_spread","line_home") if k in m.columns), None)
            if not col:
                continue
            keep = [k for k in ["date","home_team","away_team", col] if k in m.columns]
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
        col = next((k for k in ("p_home_model","p_home","p_model_home") if k in w.columns), None)
        if not col:
            return None
        keep = [k for k in ["date","home_team","away_team","week", col] if k in w.columns]
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

    board = base.copy()
    board["p_home_model"] = pd.NA
    board["vegas_line_home"] = 0.0

    if preds is not None and not preds.empty:
        board = board.merge(preds, on=["date","home_team","away_team","week"], how="left")
        filled = board["p_home_model"].notna().sum()
        print(f"[BOARD] merged calibrated predictions: filled {filled} rows")
    else:
        print("[BOARD] no calibrated predictions found; will derive p_home_model from elo_exp_home")

    # Robust numeric handling with deterministic fallbacks
    board["elo_exp_home"] = pd.to_numeric(board["elo_exp_home"], errors="coerce")
    board["p_home_model"] = pd.to_numeric(board["p_home_model"], errors="coerce")
    board["p_home_model"] = board["p_home_model"].fillna(board["elo_exp_home"])
    board["p_home_model"] = board["p_home_model"].fillna(0.5).astype(float)  # final guarantee

    if market is not None and not market.empty:
        board = board.merge(market, on=["date","home_team","away_team"], how="left")
        board["vegas_line_home"] = pd.to_numeric(board["vegas_line_home"], errors="coerce").fillna(0.0).astype(float)
        print("[BOARD] merged market spreads into vegas_line_home")
    else:
        print("[BOARD] no market file found; vegas_line_home set to 0.0 for all rows (deterministic fallback)")

    cols_order = [
        "date","week","away_team","home_team",
        "p_home_model","vegas_line_home",
        "elo_exp_home","elo_diff_pre","elo_home_pre","elo_away_pre","msf_game_id"
    ]
    cols = [c for c in cols_order if c in board.columns]
    rest = [c for c in board.columns if c not in cols]
    out = board[cols + rest].copy()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()

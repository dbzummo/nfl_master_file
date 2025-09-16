#!/usr/bin/env python3
"""
Build canonical out/model_board.csv for the resolved week.

Requires:
  - out/week_with_elo.csv  (date, week, home_team, away_team, elo_exp_home, msf_game_id?)
  - out/week_predictions.csv (optional; p_home_model)  -- else fallback to elo_exp_home
  - out/week_with_market.csv (optional; vegas_line_home) -- else 0.0
  - out/calibration/model_line_calibration.json (a,b) for transforms

Emits the columns expected by validate_and_manifest.py:
  game_id, vegas_line_home, model_line_home, p_home_market, p_home_model,
  confidence, (plus pass-throughs like inj_* if later joined)
"""
import sys, json, pathlib
import pandas as pd
from src.nfl_model.probs import prob_from_home_line, line_from_prob

OUT = pathlib.Path("out/model_board.csv")
CAL = pathlib.Path("out/calibration/model_line_calibration.json")

def fatal(msg, code=1):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)

def read_cal():
    if not CAL.exists():
        fatal(f"Missing {CAL}. Run scripts/ensure_model_line_calibration.py first.", 31)
    d = json.loads(CAL.read_text(encoding="utf-8"))
    return float(d["a"]), float(d["b"])

def read_week_with_elo():
    p = pathlib.Path("out/week_with_elo.csv")
    if not p.exists(): fatal("Missing out/week_with_elo.csv (run join_week_with_elo.py)", 21)
    df = pd.read_csv(p)
    need = {"date","week","home_team","away_team","elo_exp_home"}
    miss = sorted(need - set(df.columns))
    if miss: fatal(f"out/week_with_elo.csv missing columns: {miss}", 22)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
    df["week"] = pd.to_numeric(df["week"], errors="coerce").astype("Int64")
    return df

def read_preds():
    p = pathlib.Path("out/week_predictions.csv")
    if not p.exists(): return None
    w = pd.read_csv(p)
    for col in ("p_home_model","p_home","p_model_home"):
        if col in w.columns:
            w = w.rename(columns={col:"p_home_model"})
            break
    if "p_home_model" not in w.columns: return None
    w["date"] = pd.to_datetime(w["date"], errors="coerce").dt.strftime("%Y%m%d")
    return w[["date","home_team","away_team","week","p_home_model"]].copy()

def read_market():
    for cand in ("out/week_with_market.csv","out/odds/week_odds.csv","out/odds_week.csv","out/odds/odds_week.csv"):
        p = pathlib.Path(cand)
        if p.exists():
            m = pd.read_csv(p)
            col = next((k for k in ("vegas_line_home","spread_home","home_spread","line_home") if k in m.columns), None)
            if not col: continue
            m = m.rename(columns={col:"vegas_line_home"})
            m["date"] = pd.to_datetime(m["date"], errors="coerce").dt.strftime("%Y%m%d")
            m["vegas_line_home"] = pd.to_numeric(m["vegas_line_home"], errors="coerce")
            return m[["date","home_team","away_team","vegas_line_home"]].copy()
    return None

def main():
    a,b = read_cal()
    base = read_week_with_elo()
    preds = read_preds()
    market = read_market()

    board = base.copy()
    # Attach predictions
    board["p_home_model"] = pd.NA
    if preds is not None:
        board = board.merge(preds, on=["date","home_team","away_team","week"], how="left")
    # Fallback: elo expectation if no calibrated probs
    board["elo_exp_home"] = pd.to_numeric(board["elo_exp_home"], errors="coerce")
    board["p_home_model"] = pd.to_numeric(board["p_home_model"], errors="coerce")
    board["p_home_model"] = board["p_home_model"].fillna(board["elo_exp_home"]).fillna(0.5).astype(float)

    # Attach market spread
    board["vegas_line_home"] = 0.0
    if market is not None:
        board = board.merge(market, on=["date","home_team","away_team"], how="left")
        board["vegas_line_home"] = pd.to_numeric(board["vegas_line_home"], errors="coerce").fillna(0.0).astype(float)

    # Deterministic fields the validator expects
    # p_home_market from vegas_line_home
    board["p_home_market"] = board["vegas_line_home"].map(lambda x: prob_from_home_line(x, a, b))
    # model_line_home from p_home_model
    board["model_line_home"] = board["p_home_model"].map(lambda p: line_from_prob(p, a, b))
    # confidence as abs diff of probabilities
    board["confidence"] = (board["p_home_model"] - board["p_home_market"]).abs()

    # game_id: prefer msf_game_id, else deterministic composite
    if "msf_game_id" in board.columns:
        board["game_id"] = board["msf_game_id"]
    else:
        board["game_id"] = None
    mask = board["game_id"].isna() | (board["game_id"].astype(str) == "") | (board["game_id"].astype(str) == "nan")
    board.loc[mask, "game_id"] = board.loc[mask].apply(
        lambda r: f"{r['date']}-{str(r['away_team']).upper()}-{str(r['home_team']).upper()}", axis=1
    )

    cols = [
        "date","week","away_team","home_team","game_id",
        "vegas_line_home","model_line_home","p_home_market","p_home_model","confidence",
        "elo_exp_home","elo_diff_pre","elo_home_pre","elo_away_pre","msf_game_id"
    ]
    cols = [c for c in cols if c in board.columns]
    board = board[cols].copy()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    board.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(board)}")

#!/usr/bin/env python3
"""
Deterministic board builder.

Inputs (required):
  - out/week_with_elo.csv   (date, week, home_team, away_team, elo_exp_home, elo_diff_pre, elo_home_pre, elo_away_pre, msf_game_id)

Inputs (optional, handled safely):
  - out/week_predictions.csv  (can contain p_home_model or p_home/_cal_*; if absent, derive from elo_exp_home)
  - out/odds/week_odds.csv    (vegas_line_home; if absent, default 0.0)
  - out/injuries/injury_impacts.csv (inj_*, matched by team)

Output (required):
  - out/model_board.csv

Fail-closed: if required inputs are missing or merge produces zero rows, exit with clear [FATAL].
"""

import sys, os, math, json
from pathlib import Path
import pandas as pd

REQ_ELO  = Path("out/week_with_elo.csv")
OPT_PREDS= Path("out/week_predictions.csv")
OPT_ODDS = Path("out/odds/week_odds.csv")
OPT_INJ  = Path("out/injuries/injury_impacts.csv")
OUT_CSV  = Path("out/model_board.csv")

def fatal(msg: str, code: int = 2):
    print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(code)

def canon_team(t: str) -> str:
    if t is None: return ""
    t = str(t).upper().strip()
    # Allow both older and current aliases; keep simple map
    return {"WSH":"WAS"}.get(t, t)

def safe_read_csv(path: Path, need: set|None=None):
    if not path.exists(): return None
    try:
        df = pd.read_csv(path)
        if need and not need.issubset(df.columns):
            missing = sorted(need - set(df.columns))
            print(f"[WARN] {path} missing {missing}; continuing with available columns", file=sys.stderr)
        return df
    except Exception as e:
        fatal(f"Failed to read {path}: {e}")

def derive_p_from_elo(elo_exp_home):
    # elo_exp_home is already a 0..1 “expected” home prob from Elo diff
    try:
        x = float(elo_exp_home)
        if not (0.0 <= x <= 1.0): return 0.5
        return x
    except Exception:
        return 0.5

def main():
    # Required ELO join
    elo = safe_read_csv(REQ_ELO, need={"date","home_team","away_team","elo_exp_home"})
    if elo is None:
        fatal(f"Missing required input: {REQ_ELO}")
    # Canonicalize minimal columns and types
    for c in ("home_team","away_team"):
        elo[c] = elo[c].map(canon_team)
    elo["date"] = pd.to_datetime(elo["date"], errors="coerce").dt.date
    if elo["date"].isna().any():
        fatal("week_with_elo.csv has invalid dates")
    if "week" in elo.columns:
        elo["week"] = pd.to_numeric(elo["week"], errors="coerce").astype("Int64")

    # Optional predictions
    preds = safe_read_csv(OPT_PREDS)
    if preds is not None and not preds.empty:
        # Normalize keys we might match on
        for c in ("home_team","away_team"):
            if c in preds.columns:
                preds[c] = preds[c].map(canon_team)
        if "date" in preds.columns:
            preds["date"] = pd.to_datetime(preds["date"], errors="coerce").dt.date

        # Choose a p_home_model column (priority order)
        pcols = [c for c in preds.columns if c.lower() in (
            "p_home_model","p_home","p_home_cal_platt","p_home_cal_iso"
        )]
        pcol = pcols[0] if pcols else None

        # Try to merge on strongest keys; fall back progressively
        merged = None
        for keys in (["date","home_team","away_team"],
                     ["home_team","away_team"],
                     ["date","home_team"],
                     ["date","away_team"]):
            if all(k in preds.columns for k in keys):
                merged = pd.merge(
                    elo, preds[[*keys] + ([pcol] if pcol else [])],
                    on=keys, how="left"
                )
                if len(merged) == len(elo):  # no accidental row explosion
                    elo = merged
                    break

        if pcol:
            elo["p_home_model"] = pd.to_numeric(elo[pcol], errors="coerce")
        # else we’ll derive below
        print(f"[BOARD] predictions merged on available keys; pcol={pcol or 'derived-from-elo'}")
    else:
        print("[BOARD] no week_predictions.csv found; will derive p_home_model from elo_exp_home")

    # Optional odds / vegas line
    odds = safe_read_csv(OPT_ODDS)
    if odds is not None and not odds.empty:
        for c in ("home_team","away_team"):
            if c in odds.columns:
                odds[c] = odds[c].map(canon_team)
        if "date" in odds.columns:
            odds["date"] = pd.to_datetime(odds["date"], errors="coerce").dt.date

        # Normalize a Vegas line column name
        vcol = None
        for candidate in ("vegas_line_home","line_home","home_line","spread_home"):
            if candidate in odds.columns:
                vcol = candidate; break
        if vcol is None:
            print("[WARN] odds file present but no recognizable line column; defaulting 0.0", file=sys.stderr)

        # Merge odds (best-effort on strongest keys)
        merged = None
        for keys in (["date","home_team","away_team"], ["home_team","away_team"], ["date","home_team"]):
            if all(k in odds.columns for k in keys):
                cols = keys + ([vcol] if vcol else [])
                merged = pd.merge(elo, odds[cols], on=keys, how="left", suffixes=("",""))
                if len(merged) == len(elo):
                    elo = merged; break

        if vcol and vcol != "vegas_line_home":
            elo.rename(columns={vcol:"vegas_line_home"}, inplace=True)

    # Injuries (optional): expect team-level impacts, join by team and/or by home/away
    inj = safe_read_csv(OPT_INJ)
    if inj is not None and not inj.empty:
        if "team" in inj.columns:
            inj["team"] = inj["team"].map(canon_team)
            # home impacts
            elo = pd.merge(
                elo, inj.add_prefix("home_"), left_on="home_team", right_on="home_team", how="left"
            )
            # away impacts
            elo = pd.merge(
                elo, inj.add_prefix("away_"), left_on="away_team", right_on="away_team", how="left"
            )
            # reduce to expected columns if present
            for want_src, want_dst in (("home_points","inj_home_pts"), ("away_points","inj_away_pts")):
                col = None
                for cand in (f"home_{want_src}", f"away_{want_src}"):
                    if cand in elo.columns:
                        col = cand; break
                if want_dst not in elo.columns:
                    elo[want_dst] = pd.to_numeric(elo[col], errors="coerce") if col else 0.0
        print("[BOARD] injury impacts merged (best-effort)")
    else:
        # ensure presence
        elo["inj_home_pts"] = elo.get("inj_home_pts", 0.0)
        elo["inj_away_pts"] = elo.get("inj_away_pts", 0.0)

    # Guarantee core columns
    need_core = ["date","home_team","away_team","elo_exp_home"]
    for c in need_core:
        if c not in elo.columns:
            fatal(f"week_with_elo.csv missing required column after joins: {c}")

    # p_home_model: use column if present; else derive from elo
    if "p_home_model" not in elo.columns or elo["p_home_model"].isna().all():
        elo["p_home_model"] = elo["elo_exp_home"].apply(derive_p_from_elo)

    # vegas_line_home default to 0.0 if absent
    if "vegas_line_home" not in elo.columns:
        elo["vegas_line_home"] = 0.0
    elo["vegas_line_home"] = pd.to_numeric(elo["vegas_line_home"], errors="coerce").fillna(0.0)

    # confidence: if market present compute |p_model - p_market|, else 0.0
    def p_from_line(spread):
        try:
            spread = float(spread)
        except Exception:
            return None
        # Use the same transform constants used elsewhere
        A, B = -0.1794, -0.1655
        return 1.0/(1.0 + math.exp(-(A + B*spread)))

    p_market = elo["vegas_line_home"].apply(p_from_line)
    elo["confidence"] = (elo["p_home_model"] - p_market).abs().fillna(0.0)

    # Select & order minimal schema plus helpful extras
    out_cols = [
        "date","week","away_team","home_team",
        "p_home_model","vegas_line_home",
        "elo_exp_home","elo_diff_pre","elo_home_pre","elo_away_pre",
        "inj_home_pts","inj_away_pts","msf_game_id"
    ]
    cols_present = [c for c in out_cols if c in elo.columns]
    rest = [c for c in elo.columns if c not in cols_present]
    out = elo[cols_present + rest].copy()

    # Final sanity
    if out.empty:
        fatal("board is empty after merges — cannot proceed")
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.sort_values(["date","home_team","away_team"], inplace=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"[OK] wrote {OUT_CSV} rows={len(out)}")

if __name__ == "__main__":
    main()

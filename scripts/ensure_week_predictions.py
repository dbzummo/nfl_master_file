#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[0].parents[0] if Path(__file__).name != "ensure_week_predictions.py" else Path.cwd()

BASE = ROOT / "out" / "week_with_elo.csv"      # may or may not have elo
MKT  = ROOT / "out" / "week_with_market.csv"   # should have market_p_home
OUT  = ROOT / "out" / "week_predictions.csv"

def fatal(msg: str):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)

def main():
    # Load market (required to write anything)
    if not MKT.exists():
        fatal(f"Missing {MKT}; run join_week_with_market.py first.")
    m = pd.read_csv(MKT)

    # Prefer a model probability if it exists in BASE (elo or precomputed)
    ph_col = None
    if BASE.exists():
        b = pd.read_csv(BASE)
        # Look for typical model prob columns
        for c in b.columns:
            lc = c.lower()
            if lc in ("p_home_model","p_home","model_prob_home","prob_home","home_win_prob"):
                ph_col = c
                break
        if ph_col:
            # join by (home,away,game_start) if present, else (home,away)
            on = [c for c in ["msf_game_id","game_start","home_abbr","away_abbr"] if c in b.columns and c in m.columns]
            if not on:
                on = [c for c in ["home_abbr","away_abbr"] if c in b.columns and c in m.columns]
            if not on:
                fatal("Could not align BASE and market to bring model probabilities through.")
            m = m.merge(b[on+[ph_col]], on=on, how="left")

    # Derive p_home selection:
    # 1) If model prob present -> use it
    # 2) Else use market implied probability (real, no 0.5)
    if ph_col and m[ph_col].notna().any():
        m["p_home"] = m[ph_col]
        m["p_source"] = "model"
    elif "market_p_home" in m.columns and m["market_p_home"].notna().any():
        m["p_home"] = m["market_p_home"]
        m["p_source"] = "market"
    else:
        fatal("No real probability available (neither model nor market).")

    # Write the minimal predictions file used downstream
    keep = [c for c in ["msf_game_id","game_start","home_abbr","away_abbr","venue","status"] if c in m.columns]
    keep += [c for c in ["p_home","p_source"] if c in m.columns]
    out = m[keep].copy()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} rows={len(out)} using p_source={out['p_source'].iloc[0] if len(out) else 'n/a'}")

if __name__ == "__main__":
    main()

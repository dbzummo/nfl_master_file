#!/usr/bin/env python3
from __future__ import annotations
import sys, pandas as pd, pathlib

BASE = pathlib.Path("out/week_with_elo.csv")
ODDS_CANDIDATES = [
    pathlib.Path("out/odds_week_norm.csv"),
    pathlib.Path("out/odds/odds_combined.csv"),  # fallback if someone moved it
]
OUT = pathlib.Path("out/week_with_market.csv")

def fatal(msg):
    print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(2)

def load_base():
    if not BASE.exists(): fatal(f"Missing {BASE}. Run join_week_with_elo.py first.")
    df = pd.read_csv(BASE)
    for c in ("home_abbr","away_abbr","game_start"):
        if c not in df.columns: fatal(f"{BASE} missing {c}")
    df = df.copy()
    df["game_date"] = pd.to_datetime(df["game_start"]).dt.strftime("%Y%m%d")
    if "msf_game_id" not in df.columns:
        df["msf_game_id"] = df.apply(lambda r: f"{r['game_date']}-{r['away_abbr']}-{r['home_abbr']}", axis=1)
    return df

def load_odds():
    for p in ODDS_CANDIDATES:
        if p.exists():
            df = pd.read_csv(p)
            # Accept either slim weekly or combined; require msf_game_id & market_p_home
            if "msf_game_id" in df.columns and "market_p_home" in df.columns:
                return df[["msf_game_id","market_p_home"]], str(p)
            # If combined, reduce here (shouldn’t hit if odds_prep wrote slim)
            if {"msf_game_id","p_home_book"}.issubset(df.columns):
                red = (df.groupby("msf_game_id", as_index=False)
                         .agg(market_p_home=("p_home_book","median")))
                return red, str(p)
    fatal(f"No odds file found. Run scripts/odds_prep.py first.")

def main():
    base = load_base()
    odds, chosen = load_odds()
    merged = base.merge(odds, on="msf_game_id", how="left")
    base_n = len(base); matched = merged["market_p_home"].notna().sum()
    print(f"[INFO] Using odds file: {chosen}")
    print(f"[DEBUG] Join candidates: base={base_n} matched={matched}")
    if matched != base_n:
        missing = merged.loc[merged["market_p_home"].isna(), ["msf_game_id","home_abbr","away_abbr","game_date"]]
        print(missing.head(20).to_string(index=False))
        fatal(f"Odds join incomplete ({matched}/{base_n}).")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} (rows={len(merged)}) | market_p_home present: {merged['market_p_home'].notna().all()}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations
import argparse, sys, pandas as pd, pathlib

ELO = pathlib.Path("out/week_with_elo.csv")
MKT = pathlib.Path("out/week_with_market.csv")
OUT = pathlib.Path("out/week_predictions.csv")

def fatal(m): print(f"[FATAL] {m}", file=sys.stderr); sys.exit(2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["model","market","auto"], default="auto",
                    help="probability source to emit")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    if not ELO.exists(): fatal("Run join_week_with_elo.py first.")
    if not MKT.exists(): fatal("Run join_week_with_market.py first.")
    elo = pd.read_csv(ELO)
    mkt = pd.read_csv(MKT)

    # Canonical id + date columns
    for df in (elo, mkt):
        if "game_start" in df.columns:
            df["game_date"] = pd.to_datetime(df["game_start"]).dt.strftime("%Y%m%d")
        if "msf_game_id" not in df.columns:
            df["msf_game_id"] = df.apply(lambda r: f"{r['game_date']}-{r['away_abbr']}-{r['home_abbr']}", axis=1)

    merged = elo.merge(mkt[["msf_game_id","market_p_home"]], on="msf_game_id", how="left")

    # Choose source
    if args.source == "model":
        merged["p_home"] = merged["p_home_model"]
        src = "model"
    elif args.source == "market":
        merged["p_home"] = merged["market_p_home"]
        src = "market"
    else:
        # auto: prefer model when present, fallback to market
        merged["p_home"] = merged["p_home_model"].where(merged["p_home_model"].notna(), merged["market_p_home"])
        src = "model_auto"

    # Minimal columns required by results/backtest
    out = merged[[
        "game_date", "msf_game_id",
        "away_abbr", "home_abbr",
        "p_home"
    ]].rename(columns={
        "game_date":"date",
        "away_abbr":"away_team",
        "home_abbr":"home_team",
    })

    if out["p_home"].isna().any():
        bad = out[out["p_home"].isna()]
        fatal(f"Predictions contain null probs:\n{bad.head(10)}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"[OK] Wrote {OUT} rows={len(out)} using p_source={src}")

if __name__ == "__main__":
    main()

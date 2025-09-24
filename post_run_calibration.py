#!/usr/bin/env python3
"""
post_run_calibration.py — produce model-vs-market deltas for weekly & season logs.

Inputs:
  - preds_df: from run_simulation (required columns include vegas_line, vegas_total, sigma, kickoff_utc, neutral_site)
  - cards_df: from run_simulation (required columns include modeled_spread_home, modeled_total)

Outputs:
  - out_calibration_week.csv
  - all_seasons_calibration_log.csv (append)

Design:
  - Joins on (home_team, away_team, kickoff_utc).
  - Computes delta_spread = modeled_spread_home - vegas_line
           delta_total  = modeled_total - vegas_total
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd

def write_calibration(preds_df: pd.DataFrame,
                      cards_df: pd.DataFrame,
                      week_out: Path,
                      season_log: Path) -> pd.DataFrame:
    # Key join
    keys = ["home_team","away_team","kickoff_utc"]
    for df, name, need in [(preds_df,"preds_df",keys+["vegas_line","vegas_total","sigma","neutral_site"]),
                           (cards_df,"cards_df",keys+["modeled_spread_home","modeled_total"])]:
        missing = [c for c in need if c not in df.columns]
        if missing:
            raise RuntimeError(f"{name} missing columns: {missing}")

    p = preds_df[keys + ["vegas_line","vegas_total","sigma","neutral_site"]].copy()
    c = cards_df[keys + ["modeled_spread_home","modeled_total"]].copy()

    m = p.merge(c, on=keys, how="inner")

    m["delta_spread"] = m["modeled_spread_home"] - m["vegas_line"]
    m["delta_total"]  = m["modeled_total"] - m["vegas_total"]

    # Order cols
    out_cols = [
        "home_team","away_team","kickoff_utc","neutral_site",
        "vegas_line","modeled_spread_home","delta_spread",
        "vegas_total","modeled_total","delta_total",
        "sigma"
    ]
    m = m[out_cols].copy()

    # Write weekly file (overwrite) and append to season log
    m.to_csv(week_out, index=False)

    # Append or create season log
    if season_log.exists():
        prev = pd.read_csv(season_log)
        all_df = pd.concat([prev, m], ignore_index=True)
        # Drop dupes based on the unique key triple
        all_df = all_df.drop_duplicates(subset=["home_team","away_team","kickoff_utc"], keep="last")
        all_df.to_csv(season_log, index=False)
    else:
        m.to_csv(season_log, index=False)

    print(f"📈 Calibration written: {week_out.name}")
    print(f"📚 Season log updated: {season_log.name}")
    return m

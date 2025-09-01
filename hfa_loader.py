#!/usr/bin/env python3
"""
hfa_loader.py — never zero-out HFA; always merge your precomputed stadium HFA
"""

from __future__ import annotations
from typing import Tuple
import pandas as pd

REQUIRED_RATINGS_COLS = ["team_code", "power_rating", "uncertainty", "last_updated_utc", "week_ended"]
# We will tolerate either "power_rating" as model rating, or an already-present "rating".
# If both exist, we keep "rating" and drop "power_rating".

def merge_hfa(
    ratings_csv: str,
    stadium_hfa_csv: str
) -> pd.DataFrame:
    """
    Returns ratings DataFrame with guaranteed columns:
      team_code, rating, uncertainty, last_updated_utc, week_ended, hfa
    Will preserve any existing rating column, or derive from power_rating. HFA is taken
    from stadium file when present; falls back to ratings file 'hfa' if already present; else 0.0.
    """
    r = pd.read_csv(ratings_csv)
    # basic schema tolerances
    for c in ["team_code", "uncertainty", "last_updated_utc", "week_ended"]:
        if c not in r.columns:
            raise RuntimeError(f"kalman_state_preseason.csv missing column: {c}")

    # harmonize rating column
    if "rating" not in r.columns:
        if "power_rating" in r.columns:
            r["rating"] = r["power_rating"]
        else:
            raise RuntimeError("ratings file must contain either 'rating' or 'power_rating'.")

    # normalize team code
    r["team_code"] = r["team_code"].astype(str).str.upper().str.strip()

    # load stadium HFA
    h = pd.read_csv(stadium_hfa_csv)
    h.columns = [c.strip().lower() for c in h.columns]
    team_col = "team_code" if "team_code" in h.columns else h.columns[0]
    h.rename(columns={team_col: "team_code"}, inplace=True)
    if "hfa" not in h.columns:
        raise RuntimeError("stadium_hfa_advanced.csv found but has no 'hfa' column after normalization.")
    h["team_code"] = h["team_code"].astype(str).str.upper().str.strip()
    h["hfa"] = pd.to_numeric(h["hfa"], errors="coerce").fillna(0.0).clip(-3.0, 3.0)

    m = r.merge(h[["team_code", "hfa"]], on="team_code", how="left", suffixes=("", "_stadium"))
    # If ratings already had an hfa, keep it only where stadium file is missing
    if "hfa" in r.columns:
        m["hfa"] = m["hfa"].where(m["hfa"].notna(), r["hfa"])
    m["hfa"] = m["hfa"].fillna(0.0)

    # final column order
    keep = ["team_code", "rating", "uncertainty", "last_updated_utc", "week_ended", "hfa"]
    return m[keep].copy()
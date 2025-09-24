#!/usr/bin/env python3
"""
Normalize raw odds (from MSF or The Odds API) into 3-letter team abbreviations,
ensure consistent columns, and write a cleaned file for the join step.

Inputs (auto-detected):
  - out/odds_week.csv           # your current raw odds dump (home/away may be long names)
  - teams_lookup.json           # mapping of names/aliases -> 3-letter abbreviations

Output:
  - out/odds_week_norm.csv      # normalized odds with home_abbr/away_abbr + market_p_home

This script is idempotent and safe to re-run.
"""

from __future__ import annotations
from pathlib import Path
import json
import math
import sys
import csv

import pandas as pd

RAW = Path("out/odds_week.csv")
OUT = Path("out/odds_week_norm.csv")
LK  = Path("teams_lookup.json")

EXPECTED_COLS = [
    "home_abbr","away_abbr","commence_time","book_count","ml_home","ml_away","market_p_home"
]

def fail(msg: str) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)

def load_lookup() -> dict:
    if not LK.exists():
        fail("teams_lookup.json is missing. Please place it at repo root.")
    with LK.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Normalize keys to lowercase and strip
    norm = {}
    for k, v in data.items():
        if isinstance(v, dict) and "abbr" in v:
            abbr = str(v["abbr"]).strip().upper()
            norm[str(k).strip().lower()] = abbr
            # also include simple aliases if present
            for alias in v.get("aliases", []):
                norm[str(alias).strip().lower()] = abbr
        else:
            # allow simple "Name": "ABR" form
            norm[str(k).strip().lower()] = str(v).strip().upper()

    return norm

def to_prob_from_american(american: float|int|str|None) -> float|None:
    """Convert american moneyline to implied probability. Return None if impossible."""
    if american is None or (isinstance(american, float) and math.isnan(american)):
        return None
    try:
        a = float(american)
    except Exception:
        return None
    if a > 0:
        return 100.0 / (a + 100.0)
    elif a < 0:
        return (-a) / ((-a) + 100.0)
    else:
        return None

def normalize_team(x: str|None, lk: dict) -> str|None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # try lookup by lowercase full string
    hit = lk.get(s.lower())
    if hit:
        return hit
    # try removing common suffixes/prefixes (e.g., city names duplicated, "the")
    s2 = s.replace("The ", "").replace("the ", "").strip()
    hit = lk.get(s2.lower())
    if hit:
        return hit
    # fall back: if already looks like 2–4 caps, keep it
    if s.isupper() and 2 <= len(s) <= 4:
        return s
    # else return original (join step will not match if it’s not in lookup)
    return s

def main():
    if not RAW.exists():
        fail(f"{RAW} not found. Run your odds fetch first.")

    # Load odds
    df = pd.read_csv(RAW)
    if df.empty:
        print("[WARN] Raw odds file has header only / no rows; writing empty normalized file.")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        with OUT.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=EXPECTED_COLS).writeheader()
        print(f"[OK] Wrote {OUT} (rows=0)")
        return

    # Load lookup
    lk = load_lookup()

    # Ensure expected columns exist
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    # Normalize teams → abbreviations
    df["home_abbr"] = df["home_abbr"].apply(lambda x: normalize_team(x, lk))
    df["away_abbr"] = df["away_abbr"].apply(lambda x: normalize_team(x, lk))

    # Ensure numeric moneylines
    for c in ("ml_home","ml_away"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Build/repair market_p_home if missing
    if "market_p_home" not in df.columns:
        df["market_p_home"] = pd.NA

    need_prob = df["market_p_home"].isna() | (df["market_p_home"] == "")
    if need_prob.any():
        # compute from ml_home, else invert ml_away
        ph = df.loc[need_prob, "ml_home"].apply(to_prob_from_american)
        inv = df.loc[need_prob, "ml_away"].apply(to_prob_from_american)
        inv = inv.apply(lambda p: (1.0 - p) if (p is not None) else None)
        df.loc[need_prob, "market_p_home"] = ph.fillna(inv)

    # Coerce to float
    df["market_p_home"] = pd.to_numeric(df["market_p_home"], errors="coerce")

    # Drop rows without a usable home/away or prob
    before = len(df)
    df = df.dropna(subset=["home_abbr","away_abbr"]).copy()
    # Keep even if market_p_home is NaN (join will still carry other market fields)
    after = len(df)

    # Write normalized file with exact expected columns
    df_out = df[EXPECTED_COLS].copy()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(OUT, index=False)

    print(f"[OK] Normalized odds: {before} -> {after} rows; wrote {OUT} (rows={len(df_out)})")

if __name__ == "__main__":
    main()

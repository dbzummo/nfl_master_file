#!/usr/bin/env python3
"""
fix_depth_csv.py — normalize team_depth_charts_with_values.csv to required schema:
  team_code, position, player, value

Usage:
  python fix_depth_csv.py [--in <input_csv>] [--out <output_csv>] [--force]

Behavior:
  - Tries to map common header variants to required names.
  - If no numeric 'value' is present, derives it from depth rank by position.
  - Fills missing positions with 'UNK' and value with conservative defaults.
  - Prints a short summary of transformations.
"""

from __future__ import annotations
import argparse
import sys
import math
import pandas as pd
import numpy as np
from pathlib import Path

COMMON_MAPS = {
    # required -> possible aliases
    "team_code": ["team_code", "team", "teamid", "team_id", "club", "abbr", "abbrv", "code"],
    "position":  ["position", "pos", "slot", "role"],
    "player":    ["player", "name", "player_name", "fullname", "full_name"],
    "value":     ["value", "val", "weight", "score", "grade", "rating", "power", "strength"]
}

def _pick_col(df: pd.DataFrame, want: str) -> str | None:
    aliases = COMMON_MAPS[want]
    cols_norm = {c.lower().strip(): c for c in df.columns}
    for a in aliases:
        if a in cols_norm:
            return cols_norm[a]
    return None

def _derive_value_from_depth(df: pd.DataFrame) -> pd.Series:
    """
    Derive a numeric 'value' using per-position depth order.
    - We assume the input is already grouped by position; we'll create a rank per (team_code, position)
    - Value curve (starter->bench): 8.0, 6.0, 4.0, 3.0, 2.5, 2.0, then decay
    """
    # stable position order (QB/WR/LT/etc. doesn’t matter; we just need rank per position)
    df = df.copy()
    # Create a stable row order per (team, position) using original index
    df["__idx"] = np.arange(len(df))
    df["__rank"] = df.groupby(["team_code","position"])["__idx"].rank(method="first", ascending=True)

    # piecewise value curve
    def value_from_rank(r):
        r = int(r)
        curve = {1:8.0, 2:6.0, 3:4.0, 4:3.0, 5:2.5, 6:2.0}
        if r in curve:
            return curve[r]
        # gentle tail
        return max(1.0, 2.0 - 0.1*(r-6))

    return df["__rank"].map(value_from_rank)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="team_depth_charts_with_values.csv")
    ap.add_argument("--out", dest="out", default="team_depth_charts_with_values.fixed.csv")
    ap.add_argument("--force", action="store_true", help="fill missing fields with safe defaults")
    args = ap.parse_args()

    inp = Path(args.inp)
    if not inp.exists():
        print(f"⛔ Input file not found: {inp}")
        sys.exit(2)

    df = pd.read_csv(inp)
    orig_cols = list(df.columns)

    # Normalize whitespace/case in headers
    df.columns = [c.strip() for c in df.columns]

    # Map to required names
    col_team = _pick_col(df, "team_code")
    col_pos  = _pick_col(df, "position")
    col_player = _pick_col(df, "player")
    col_value  = _pick_col(df, "value")

    notes = []

    if col_team is None or col_pos is None or col_player is None:
        if not args.force:
            print("⛔ Missing essential columns and --force not set.")
            print(f"   Detected columns: {orig_cols}")
            print("   Need to map at least team_code, position, player. "
                  "You can re-run with --force to fill missing with conservative defaults.")
            sys.exit(2)

    # Build normalized frame
    out = pd.DataFrame()
    out["team_code"] = (df[col_team] if col_team in df.columns else "UNK").astype(str).str.upper().str.strip()
    out["position"]  = (df[col_pos]  if col_pos  in df.columns else "UNK").astype(str).str.upper().str.strip()
    out["player"]    = (df[col_player] if col_player in df.columns else "Unknown Player").astype(str).str.strip()

    if col_value and col_value in df.columns:
        out["value"] = pd.to_numeric(df[col_value], errors="coerce")
        missing_vals = out["value"].isna().sum()
        if missing_vals > 0:
            notes.append(f"Filled {missing_vals} missing numeric values with depth-derived weights.")
            # fill only the missing via depth-derived weights
            tmp = df.copy()
            tmp["team_code"] = out["team_code"]
            tmp["position"]  = out["position"]
            fill_vals = _derive_value_from_depth(tmp)
            out.loc[out["value"].isna(), "value"] = fill_vals[out["value"].isna()]
    else:
        notes.append("No numeric value/grade column detected; deriving from depth order.")
        tmp = df.copy()
        tmp["team_code"] = out["team_code"]
        tmp["position"]  = out["position"]
        out["value"] = _derive_value_from_depth(tmp)

    # Cleanups
    out["value"] = out["value"].fillna(2.0).clip(lower=0.5, upper=10.0)

    # Basic sanity
    missing_core = out[ (out["team_code"]=="") | (out["player"]=="") ]
    if len(missing_core) > 0 and not args.force:
        print("⛔ Some rows missing team_code or player; re-run with --force to fill with defaults.")
        sys.exit(2)
    if args.force:
        out.loc[out["team_code"]=="", "team_code"] = "UNK"
        out.loc[out["player"]=="", "player"] = "Unknown Player"
        out.loc[out["position"]=="", "position"] = "UNK"

    # Save
    out_cols = ["team_code","position","player","value"]
    out[out_cols].to_csv(args.out, index=False)

    print("✅ Fixed depth chart written:", args.out)
    print("   Original columns:", orig_cols)
    print("   Output columns:  ", out_cols)
    if notes:
        print("   Notes:")
        for n in notes:
            print("    -", n)

if __name__ == "__main__":
    main()
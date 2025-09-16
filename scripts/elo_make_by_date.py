#!/usr/bin/env python3
"""
Produce out/elo_ratings_by_date.csv (date,team,elo) from out/elo_ratings.csv.

Input (must exist):
  out/elo_ratings.csv  with columns at least: date, team, elo_post

Output:
  out/elo_ratings_by_date.csv  with columns: date, team, elo
"""
import sys, pathlib
import pandas as pd

SRC = pathlib.Path("out/elo_ratings.csv")
DST = pathlib.Path("out/elo_ratings_by_date.csv")

def fatal(msg, code=1):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)

if not SRC.exists():
    fatal(f"Missing {SRC}; run compute_elo.py first.")

df = pd.read_csv(SRC)
need = {"date","team","elo_post"}
if not need.issubset(df.columns):
    missing = sorted(need - set(df.columns))
    fatal(f"{SRC} missing required columns: {missing}")

# Normalize and produce canonical by-date ratings
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
df = df.dropna(subset=["date","team","elo_post"]).copy()

# If multiple rows exist per (date,team), keep the last snapshot of that date.
df = df.sort_values(["team","date"])
df = df.drop_duplicates(subset=["team","date"], keep="last").copy()

out = df.rename(columns={"elo_post":"elo"})[["date","team","elo"]].sort_values(["date","team"])
DST.parent.mkdir(parents=True, exist_ok=True)
out.to_csv(DST, index=False)

print(f"[OK] wrote {DST} rows={len(out)} range={out['date'].min()}..{out['date'].max()}")

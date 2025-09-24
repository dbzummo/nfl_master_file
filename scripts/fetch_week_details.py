#!/usr/bin/env python3
"""
Compliance mode: do NOT call add-on endpoints until MSF confirms access.
We only ensure downstream files exist so later stages won’t crash.
"""
import argparse, pathlib, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--season", required=True)
ap.add_argument("--week", type=int)
ap.add_argument("--date")
args = ap.parse_args()

base = pathlib.Path("out/msf_details"); base.mkdir(parents=True, exist_ok=True)
p = base / "msf_week.csv"

if not p.exists() or p.stat().st_size == 0:
    print("[details][WARN] msf_week.csv missing/empty; skipping add-on endpoints (per provider guidance).")
else:
    try:
        df = pd.read_csv(p)
        print(f"[details] have {len(df)} games; not calling boxscore/lineups/PBP/injuries until access is confirmed.")
    except Exception:
        print("[details][WARN] msf_week.csv unreadable; continuing with stubs.")

for name in ["boxscores_week.csv", "lineups_week.csv", "injuries_week.csv"]:
    f = base / name
    if not f.exists():
        f.write_text("")
print("[details][ok] wrote/kept empty detail stubs.")

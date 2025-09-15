#!/usr/bin/env python3
import sys, pathlib, pandas as pd

p_msf = pathlib.Path("out/msf_details/msf_week.csv")
if not p_msf.exists():
    print("[week][ERR] msf_week.csv not found at out/msf_details/msf_week.csv", file=sys.stderr)
    sys.exit(3)

df = pd.read_csv(p_msf)
if df.empty or "week" not in df.columns:
    print("[week][ERR] msf_week.csv missing or has no 'week' column", file=sys.stderr)
    sys.exit(3)

wk = pd.to_numeric(df["week"], errors="coerce").dropna()
if wk.empty:
    print("[week][ERR] could not parse week from msf_week.csv", file=sys.stderr)
    sys.exit(3)

wk_mode = int(wk.mode().iloc[0])
pathlib.Path("out/msf_details/.detected_week").write_text(str(wk_mode))
print(f"[week] Detected week={wk_mode}")

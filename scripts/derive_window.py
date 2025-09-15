#!/usr/bin/env python3
import sys, pathlib, pandas as pd

p_det = pathlib.Path("out/msf_details/.detected_week")
p_box = pathlib.Path("out/msf_details/boxscores_week.csv")

if not p_det.exists():
    print("", "", sep=" ", end="")
    sys.exit(4)

w_txt = p_det.read_text().strip()
try:
    w = int(w_txt)
except:
    print("", "", sep=" ", end="")
    sys.exit(4)

if not p_box.exists():
    print("", "", sep=" ", end="")
    sys.exit(4)

df = pd.read_csv(p_box)
if "date" not in df.columns or "week" not in df.columns:
    print("", "", sep=" ", end="")
    sys.exit(4)

df["date"] = pd.to_datetime(df["date"].astype(str).str[:10], errors="coerce")
df = df[(df["week"] == w) & df["date"].notna()].copy()
if df.empty:
    print("", "", sep=" ", end="")
    sys.exit(4)

mn = df["date"].min().strftime("%Y%m%d")
mx = df["date"].max().strftime("%Y%m%d")
print(mn, mx)

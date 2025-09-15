#!/usr/bin/env python3
import os, pandas as pd, numpy as np

# choose source: calibrated blend if present & valid; else raw blend
src = "out/blended_predictions.csv"
bsrc_txt = "out/BLEND_SOURCE.txt"
if os.path.exists(bsrc_txt):
    cand = open(bsrc_txt).read().strip()
    if cand and os.path.exists(cand):
        src = cand

df = pd.read_csv(src, usecols=["home_team","away_team","date","home_win_prob"]).copy()
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date","home_win_prob"])

# Week 1 window: earliest game date → +4 days (Thu→Mon)
start = df["date"].min()
end   = start + pd.Timedelta(days=4)
w1 = (df[(df["date"] >= start) & (df["date"] <= end)]
        .sort_values(["date","home_team","away_team"])
        .copy())

# sanity checks
p = w1["home_win_prob"].astype(float)
assert len(w1) > 0, "No games in week-1 window"
assert p.min() >= 0 and p.max() <= 1, "probs out of [0,1]"
assert p.nunique() >= max(8, len(w1)//2), "probs look degenerate"
assert len(w1) <= 20, f"Weekly file too large ({len(w1)})"

w1["date"] = w1["date"].dt.strftime("%Y-%m-%d")
w1 = w1[["home_team","away_team","date","home_win_prob"]]
out = "out/predictions_week_calibrated_blend.csv"
w1.to_csv(out, index=False)
print(f"wrote {out} rows={len(w1)} range=({p.min():.3f},{p.max():.3f}) window={start.date()}→{end.date()}")

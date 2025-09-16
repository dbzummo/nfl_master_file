#!/usr/bin/env python3
import json, math, pandas as pd
CAL_PATH = "out/calibration/model_line_calibration.json"

def read_cal():
    try:
        d = json.load(open(CAL_PATH,"r",encoding="utf-8"))
        return float(d.get("a",0.0)), float(d.get("b",0.0))
    except Exception:
        return 0.0, 1.0  # identity-ish fallback so we never explode

def prob_from_home_line(line, a, b):
    # inverse of line_from_prob; probability from a spread using logistic
    z = a + b*float(line)
    try:
        return 1.0/(1.0+math.exp(-z))
    except OverflowError:
        return 1.0 if z>0 else 0.0

def line_from_prob(p, a, b):
    # clamp + logit back to spread
    p = max(min(float(p), 1-1e-9), 1e-9)
    z = math.log(p/(1-p))
    # b could be 0 in fallback; avoid div by zero.
    return (z - a)/(b if b!=0 else 1.0)

def synth_game_id(row):
    try:
        d = pd.to_datetime(row["date"]).strftime("%Y%m%d")
    except Exception:
        d = "00000000"
    away = str(row.get("away_team","UNK")).upper()
    home = str(row.get("home_team","UNK")).upper()
    return f"{d}_{away}_AT_{home}"

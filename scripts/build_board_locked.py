#!/usr/bin/env python3
from __future__ import annotations
import os, sys, json, math, csv
from pathlib import Path

# Required inputs
WK = Path("out/week_with_market.csv")          # date,week,away_team,home_team,msf_game_id,book,vegas_line_home,vegas_total
PR = Path("out/week_predictions.csv")          # must have a home-win prob: p_home or p_home_cal_platt/iso or elo_exp_home
CAL = Path("out/calibration/model_line_calibration.json")
INJ = Path("out/injuries/team_adjustments.csv") # optional: team,points or points_capped
OUT = Path("out/model_board.csv")

def fatal(msg): print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(1)

for p in [WK, PR, CAL]:
    if not p.exists(): fatal(f"Missing {p}")

cal = json.load(CAL.open())
a, b = float(cal["a"]), float(cal["b"])

def logit(p: float) -> float:
    p = min(max(p, 1e-12), 1-1e-12)
    return math.log(p/(1-p))

def prob_from_line(line_home: float) -> float:
    return 1.0/(1.0+math.exp(-(a + b*line_home)))

def line_from_prob(p_home: float) -> float:
    return (logit(p_home) - a)/b

# Load injuries (optional)
inj_map = {}
if INJ.exists():
    with INJ.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            team = (row.get("team") or "").strip()
            pts  = row.get("points_capped") or row.get("points") or "0"
            try: inj_map[team] = float(pts)
            except: inj_map[team] = 0.0

# Read week_with_market
with WK.open(newline="", encoding="utf-8") as f:
    wk = list(csv.DictReader(f))
    needed = ["date","week","away_team","home_team","msf_game_id","book","vegas_line_home","vegas_total"]
    miss = [c for c in needed if c not in wk[0].keys()]
    if miss: fatal(f"week_with_market.csv missing columns: {miss}")

# Read preds
with PR.open(newline="", encoding="utf-8") as f:
    pr = list(csv.DictReader(f))

# Index preds by game
def pick_prob(row: dict) -> float | None:
    for k in ["p_home","p_home_cal_platt","p_home_cal_iso","home_win_prob","cal_platt","elo_exp_home"]:
        if k in row and row[k] not in (None,"","nan"):
            try:
                return float(row[k])
            except:
                pass
    return None

pred_by_id = {}
for row in pr:
    gid = (row.get("msf_game_id") or row.get("game_id") or "").strip()
    if not gid:
        continue
    p = pick_prob(row)
    if p is not None:
        pred_by_id[gid] = p

# Build final board rows
rows = []
missing_pred = 0
for row in wk:
    gid = row["msf_game_id"].strip()
    if not gid:
        continue

    vegas_line = float(row["vegas_line_home"]) if row["vegas_line_home"] not in ("","nan") else 0.0
    vegas_total = float(row["vegas_total"]) if row["vegas_total"] not in ("","nan") else 0.0
    book = row.get("book","").strip()

    p_model = pred_by_id.get(gid, None)
    if p_model is None:
        missing_pred += 1
        continue

    # Market prob from spread
    p_market = prob_from_line(vegas_line)

    # Model line from prob + injury net (Away - Home)
    base_model_line = line_from_prob(p_model)
    inj_home = inj_map.get(row["home_team"].strip(), 0.0)
    inj_away = inj_map.get(row["away_team"].strip(), 0.0)
    inj_net  = inj_away - inj_home
    model_line = base_model_line + inj_net

    # Edge & confidence
    edge_pts = model_line - vegas_line
    confidence = abs(p_model - p_market)

    rows.append({
        "date": row["date"],
        "week": row["week"],
        "away_team": row["away_team"],
        "home_team": row["home_team"],
        "msf_game_id": gid,
        "book": book,
        "vegas_line_home": f"{vegas_line:.1f}",
        "vegas_total": f"{vegas_total:.1f}",
        "model_line_home": f"{model_line:.2f}",
        "edge": f"{edge_pts:.2f}",
        "confidence": f"{confidence:.4f}",
        "p_home_market": f"{p_market:.6f}",
        "p_home_model": f"{p_model:.6f}",
        "inj_home_pts": f"{inj_home:.2f}",
        "inj_away_pts": f"{inj_away:.2f}",
        "inj_net_pts":  f"{inj_net:.2f}",
    })

if not rows:
    fatal("No board rows built (likely no matching msf_game_id between preds and week_with_market).")

# Deterministic sort
rows.sort(key=lambda r: (r["date"], r["home_team"], r["away_team"]))

# Write locked schema
OUT.parent.mkdir(parents=True, exist_ok=True)
cols = [
 "date","week","away_team","home_team","msf_game_id","book",
 "vegas_line_home","vegas_total",
 "model_line_home","edge","confidence",
 "p_home_market","p_home_model",
 "inj_home_pts","inj_away_pts","inj_net_pts"
]
with OUT.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
    w.writerows(rows)

print(f"[OK] wrote {OUT} rows={len(rows)}  missing_preds={missing_pred}")

#!/usr/bin/env python3
import csv, sys

SRC = "out/model_board.csv"
DST = "out/week_predictions.csv"

def pick_prob(row):
    # Board column preference (post-blend first)
    for k in ("p_home_blend","p_home_model_adj","p_home_model","p_home"):
        v = row.get(k)
        if v not in (None, ""):
            try:
                p = float(v)
                if 0.0 <= p <= 1.0:
                    return p
            except:
                pass
    return None

with open(SRC, newline='', encoding='utf-8') as f, \
     open(DST, 'w', newline='', encoding='utf-8') as g:
    r = csv.DictReader(f)
    w = csv.writer(g)
    w.writerow(["game_id","p_home"])
    n = 0
    for row in r:
        gid = (row.get("game_id") or row.get("msf_game_id") or "").strip()
        if not gid:
            continue
        p = pick_prob(row)
        if p is None:
            continue
        w.writerow([gid, f"{p:.6f}"])
        n += 1

print(f"[OK] wrote {DST} rows={n} (sourced from board post-blend)")

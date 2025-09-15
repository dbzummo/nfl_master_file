#!/usr/bin/env python3
import csv, math, sys
SRC='out/model_board.csv'; TMP='out/model_board.csv.tmp'
# inverse of market logistic (your fit)
A,B=-0.1794,-0.1655
def line_from_p(p):
    p=max(min(float(p),1-1e-9),1e-9)
    return (math.log(p/(1-p))-A)/B

need = [
  "game_id","date","week","away_team","home_team",
  "book","vegas_line_home","vegas_total",
  "p_home_model","p_home_blend",
  "model_line_home","edge","confidence",
  "inj_home_pts","inj_away_pts","inj_net_pts"
]

with open(SRC, newline='', encoding='utf-8') as f, open(TMP,'w',newline='',encoding='utf-8') as g:
    r=csv.DictReader(f); w=csv.DictWriter(g, fieldnames=need); w.writeheader()
    for x in r:
        gid=(x.get("game_id") or x.get("msf_game_id") or "").strip()
        if not gid: continue
        # prefer already-computed fields; enforce presence
        p_model = (x.get("p_home_model") or x.get("p_home") or "").strip()
        p_blend = (x.get("p_home_blend") or x.get("p_home_model_adj") or "").strip()
        vegas   = (x.get("vegas_line_home") or "").strip()
        total   = (x.get("vegas_total") or "").strip()
        book    = (x.get("book") or "draftkings").strip()

        # model line should reflect the *blend* if present, else p_model
        model_line = x.get("model_line_home") or ""
        if p_blend:
            try: model_line = f"{line_from_p(float(p_blend)):.2f}"
            except: pass
        elif p_model and not model_line:
            try: model_line = f"{line_from_p(float(p_model)):.2f}"
            except: pass

        row = {
          "game_id": gid,
          "date": x.get("date",""),
          "week": x.get("week",""),
          "away_team": x.get("away_team",""),
          "home_team": x.get("home_team",""),
          "book": book,
          "vegas_line_home": vegas,
          "vegas_total": total,
          "p_home_model": p_model,
          "p_home_blend": p_blend,
          "model_line_home": model_line,
          "edge": x.get("edge",""),
          "confidence": x.get("confidence",""),
          "inj_home_pts": x.get("inj_home_pts",""),
          "inj_away_pts": x.get("inj_away_pts",""),
          "inj_net_pts":  x.get("inj_net_pts",""),
        }
        w.writerow(row)
import os; os.replace(TMP,SRC)
print("[OK] board schema locked → out/model_board.csv")

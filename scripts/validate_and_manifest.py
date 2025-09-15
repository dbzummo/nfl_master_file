#!/usr/bin/env python3
from __future__ import annotations
import os, csv, json, sys
from src.nfl_model.contracts import BoardRow
from src.nfl_model.probs import prob_from_home_line, sanity_roundtrip
CAL_PATH="out/calibration/model_line_calibration.json"
def _read_cal():
    with open(CAL_PATH,"r",encoding="utf-8") as f: d=json.load(f)
    return float(d["a"]), float(d["b"])
def main()->None:
    in_csv="out/model_board.csv"
    if not os.path.exists(in_csv): print("[FATAL] Missing out/model_board.csv", file=sys.stderr); sys.exit(1)
    if not os.path.exists(CAL_PATH): print("[FATAL] Missing calibration JSON", file=sys.stderr); sys.exit(1)
    a,b=_read_cal(); sanity_roundtrip(0.0,a,b)
    with open(in_csv,newline="",encoding="utf-8") as f: rows=list(csv.DictReader(f))
    # ensure p_home_model exists
    for r in rows:
        if not r.get("p_home_model"):
            r["p_home_model"]=r.get("p_home") or r.get("p_home_cal_platt") or r.get("p_home_cal_iso") or ""
            if not r["p_home_model"]:
                # Try from model_line_home
                r["p_home_model"]=f"{prob_from_home_line(float(r['model_line_home']), a, b):.6f}"
        _=BoardRow(
            game_id=r["game_id"], vegas_line_home=float(r["vegas_line_home"]), model_line_home=float(r["model_line_home"]),
            p_home_market=float(r["p_home_market"]) if r.get("p_home_market") else None, p_home_model=float(r["p_home_model"]),
            inj_home_pts=float(r.get("inj_home_pts",0.0)), inj_away_pts=float(r.get("inj_away_pts",0.0)), confidence=float(r["confidence"])
        )
    with open(in_csv,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    # write manifest
    from src.nfl_model.run_manifest import write_manifest
    write_manifest(
        "out/run_manifest.json",
        calibration={"a":a,"b":b,"window":[os.environ.get("START",""), os.environ.get("END","")]},
        inputs={"odds":"out/odds/week_odds.csv","elo":"out/week_with_elo.csv","injuries":"out/injuries/injuries_feed.csv","board":"out/model_board.csv"},
        strict=bool(int(os.environ.get("STRICT_MODE","1"))),
    )
    print("[OK] Validation + manifest complete.")
if __name__=="__main__": main()

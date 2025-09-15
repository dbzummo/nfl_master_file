#!/usr/bin/env python3
import csv, sys
board = {(r.get("game_id") or r.get("msf_game_id") or "").strip(): r
         for r in csv.DictReader(open("out/model_board.csv", newline="", encoding="utf-8"))}
preds = {r["game_id"].strip(): float(r["p_home"])
         for r in csv.DictReader(open("out/week_predictions.csv", newline="", encoding="utf-8"))}
missing_in_preds = [gid for gid in board if gid and gid not in preds]
missing_in_board = [gid for gid in preds if gid and gid not in board]
def pick_prob(row):
    for k in ("p_home_blend","p_home_model_adj","p_home_model","p_home"):
        v=row.get(k)
        if v not in (None,""):
            try: return float(v)
            except: pass
    return None
mismatch=[]
for gid,brow in board.items():
    if not gid or gid not in preds: continue
    pb=pick_prob(brow); 
    if pb is None: mismatch.append((gid,"no_board_prob")); continue
    pe=preds[gid]
    if not (0<=pe<=1): mismatch.append((gid,"pred_out_of_range")); continue
    if abs(pb-pe) > 1e-6:
        mismatch.append((gid, f"Δ={pb-pe:+.6f} (board={pb:.6f}, eval={pe:.6f})"))
ok = (not missing_in_preds) and (not missing_in_board) and (not mismatch)
if not ok:
    if missing_in_preds: print("[FATAL] board game_ids missing in week_predictions:", missing_in_preds[:10], "…", file=sys.stderr)
    if missing_in_board: print("[FATAL] week_predictions game_ids missing in board:", missing_in_board[:10], "…", file=sys.stderr)
    if mismatch: print("[FATAL] probability mismatch(s):", mismatch[:10], "…", file=sys.stderr)
    sys.exit(1)
print("[OK] board ↔ eval alignment verified")

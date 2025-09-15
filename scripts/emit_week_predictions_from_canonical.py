#!/usr/bin/env python3
import os, csv, sys
src='out/predictions_week.csv'; dst='out/week_predictions.csv'
if not os.path.exists(src):
    print("[FATAL] missing", src, file=sys.stderr); sys.exit(1)
os.makedirs('out', exist_ok=True)
with open(src, newline='', encoding='utf-8') as f, open(dst,'w',newline='',encoding='utf-8') as g:
    r=csv.DictReader(f); w=csv.writer(g)
    w.writerow(['game_id','p_home'])
    for x in r:
        gid=(x.get('game_id') or '').strip()
        p  =(x.get('p_home_model') or '').strip()
        if gid and p: w.writerow([gid,p])
print("[OK] wrote", dst)

#!/usr/bin/env python3
import os,csv,sys
def must_exist(path, hdr=None):
    if not os.path.exists(path):
        print(f"[FATAL] missing file: {path}", file=sys.stderr); sys.exit(1)
    if hdr:
        r=csv.reader(open(path, newline='', encoding='utf-8'))
        h=next(r, [])
        if [c.lower() for c in h] != [c.lower() for c in hdr]:
            print(f"[FATAL] bad header in {path}: {h} expected {hdr}", file=sys.stderr); sys.exit(1)
for k in ("MSF_KEY","MSF_PASS"):
    if not os.environ.get(k):
        print(f"[FATAL] env var {k} not set", file=sys.stderr); sys.exit(1)
must_exist("out/model_board.csv")
must_exist("out/results/finals.csv", ["game_id","home_score","away_score"])
print("[OK] preflight passed")

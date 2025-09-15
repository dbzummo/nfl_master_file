#!/usr/bin/env python3
# scripts/evaluate_metrics.py
# Compute Brier, ECE, reliability table, and stratified performance.

import json, math, os, sys, csv
from typing import List, Dict, Tuple

IN_CSV = os.environ.get("BACKTEST_CSV", "backtest_details.csv")
PROB_COL = os.environ.get("PROB_COL", "home_win_prob_cal")
FALLBACK_PROB_COL = os.environ.get("FALLBACK_PROB_COL", "home_win_prob")
LABEL_COL = os.environ.get("LABEL_COL", "home_win")
BINS = int(os.environ.get("ECE_BINS", "20"))
OUT_DIR = os.environ.get("OUT_DIR", "out")
SLICE_COLS = ["high_injury_risk", "high_sunk_cost", "high_cohesion", "bad_weather"]

def read_rows(path: str):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def to_float(x, default=None):
    try: return float(x)
    except: return default

def brier_score(labels, probs):
    return sum((p - y)**2 for y,p in zip(labels,probs) if y in (0,1) and p is not None)/len(labels)

def log_loss(labels, probs, eps=1e-15):
    s=0; n=0
    for y,p in zip(labels,probs):
        if y in (0,1) and p is not None:
            p=min(max(p,eps),1-eps)
            s+=-(y*math.log(p)+(1-y)*math.log(1-p)); n+=1
    return s/n if n else float("nan")

def calc_ece(labels, probs, bins):
    counts=[0]*bins; sump=[0]*bins; sumy=[0]*bins
    for y,p in zip(labels,probs):
        if y in (0,1) and p is not None:
            idx=min(int(p*bins), bins-1); counts[idx]+=1; sump[idx]+=p; sumy[idx]+=y
    table=[]; ece=0; total=sum(counts)
    for i in range(bins):
        if counts[i]==0: avg_p=emp=gap=float("nan")
        else:
            avg_p=sump[i]/counts[i]; emp=sumy[i]/counts[i]; gap=abs(avg_p-emp); ece+=counts[i]*gap
        table.append({"bin":i,"p_lower":i/bins,"p_upper":(i+1)/bins,"avg_prob":avg_p,"empirical":emp,"count":counts[i],"abs_gap":gap})
    return ece/total if total else float("nan"), table

def main():
    os.makedirs(OUT_DIR,exist_ok=True)
    rows=read_rows(IN_CSV)
    labels=[int(to_float(r.get(LABEL_COL))) if r.get(LABEL_COL) else None for r in rows]
    probs=[to_float(r.get(PROB_COL)) if r.get(PROB_COL) else to_float(r.get(FALLBACK_PROB_COL)) for r in rows]
    filtered=[(y,p) for y,p in zip(labels,probs) if y in (0,1) and p is not None]
    if not filtered: sys.exit("No valid rows")
    ys,ps=zip(*filtered)
    metrics={"logloss":log_loss(ys,ps),"brier":brier_score(ys,ps)}
    ece,table=calc_ece(ys,ps,BINS); metrics["ece"]=ece; metrics["n"]=len(ys)
    with open(os.path.join(OUT_DIR,"metrics_summary.json"),"w") as f: json.dump(metrics,f,indent=2)
    with open(os.path.join(OUT_DIR,"reliability_table.csv"),"w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=table[0].keys()); w.writeheader(); w.writerows(table)
    print("Wrote metrics_summary.json and reliability_table.csv")

if __name__=="__main__": main()

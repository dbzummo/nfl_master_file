#!/usr/bin/env python3
import csv, json, math
from pathlib import Path

CFG = json.load(open("config/bayes_weights_2025.json"))
A, B = -0.1794, -0.1655  # historical logistic fit: P(home)=sigmoid(A + B*spread_home)

def w2025_for_week(week:str)->float:
    try:
        return float(CFG["w2025_by_week"].get(str(int(week)), CFG["default_w2025"]))
    except:
        return float(CFG["default_w2025"])

def p_from_line(spread):
    return 1/(1+math.exp(-(A + B*spread)))

def line_from_p(p):
    p=max(min(p,1-1e-9),1e-9)
    logit=math.log(p/(1-p))
    return (logit - A)/B

IN  = Path("out/model_board.csv")
TMP = Path("out/model_board.csv.tmp")

with IN.open(newline="",encoding="utf-8") as f, TMP.open("w",newline="",encoding="utf-8") as g:
    r = csv.DictReader(f); cols = r.fieldnames or []
    for add in ["p_home_model_adj","model_line_home_adj","edge_adj","confidence_adj","w2025_used"]:
        if add not in cols: cols.append(add)
    w = csv.DictWriter(g, fieldnames=cols); w.writeheader()

    for row in r:
        week = row.get("week","3")
        w2025 = w2025_for_week(week)

        # base model prob (fallbacks kept to avoid breaks)
        try: p_model = float(row.get("p_home_model") or row.get("p_home") or 0.5)
        except: p_model = 0.5

        try: vegas = float(row.get("vegas_line_home") or 0.0)
        except: vegas = 0.0

        p_market = p_from_line(vegas)
        p_adj = w2025*p_market + (1.0-w2025)*p_model

        ml_adj = line_from_p(p_adj)
        p_mkt  = p_market
        edge   = ml_adj - vegas
        conf   = abs(p_adj - p_mkt)

        row["p_home_model_adj"]   = f"{p_adj:.6f}"
        row["model_line_home_adj"]= f"{ml_adj:.2f}"
        row["edge_adj"]           = f"{edge:+.2f}"
        row["confidence_adj"]     = f"{conf:.4f}"
        row["w2025_used"]         = f"{w2025:.2f}"
        w.writerow(row)

TMP.replace(IN)
print("[OK] bayes blend applied; recomputed adjusted line/edge/conf → out/model_board.csv")

#!/usr/bin/env python3
"""
Apply learned logistic (intercept + coef on elo_diff) to out/predictions_with_elo.csv.
Outputs:
  out/predictions_with_elo_cal.csv  (adds elo_logit_prob)
"""
import json, argparse, os
import pandas as pd
import numpy as np

def sigmoid(z): return 1.0/(1.0+np.exp(-z))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred_in", default="out/predictions_with_elo.csv")
    ap.add_argument("--model", default="out/elo_logit_model.json")
    ap.add_argument("--out", default="out/predictions_with_elo_cal.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.pred_in)
    with open(args.model,"r") as f:
        m = json.load(f)
    z = m["intercept"] + m["coef"]*df["elo_diff"].astype(float)
    df["elo_logit_prob"] = sigmoid(z)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} (rows={len(df)})")
if __name__ == "__main__":
    main()

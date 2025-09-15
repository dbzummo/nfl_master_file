#!/usr/bin/env python3
"""
Reliability (calibration) plot with:
  • 45° reference line
  • error bars = 95% Wilson intervals for empirical frequency
  • bar overlay showing counts per bin (secondary axis)

Inputs (choose one source):
  1) --reliability_csv  (default: out/reliability_table.csv produced by scripts/evaluate_metrics.py)
  2) --backtest_csv + --prob_col/--label_col to compute bins on the fly

Usage examples:
  python3 scripts/plot_reliability.py
  python3 scripts/plot_reliability.py --title "Week 1 Blend" --out artifacts/reliability_week1.png
  python3 scripts/plot_reliability.py --backtest_csv out/backtest_details.csv --prob_col home_win_prob --label_col home_win --bins 20
"""
import os, math, argparse
import pandas as pd
import matplotlib.pyplot as plt

DEF_REL = "out/reliability_table.csv"
DEF_OUT = "artifacts/reliability_plot.png"

def wilson_interval(p_hat: float, n: int, z: float = 1.96):
    if n <= 0 or not (0.0 <= p_hat <= 1.0):
        return (float("nan"), float("nan"))
    denom = 1 + (z*z)/n
    center = (p_hat + (z*z)/(2*n)) / denom
    rad = (z * math.sqrt((p_hat*(1-p_hat)/n) + (z*z)/(4*n*n))) / denom
    return (max(0.0, center - rad), min(1.0, center + rad))

def compute_bins(backtest_csv: str, prob_col: str, label_col: str, bins: int):
    df = pd.read_csv(backtest_csv)
    p = pd.to_numeric(df[prob_col], errors="coerce")
    y = pd.to_numeric(df[label_col], errors="coerce")
    m = (y.isin([0,1])) & p.notna()
    p, y = p[m], y[m].astype(int)

    # bin edges [0,1], right-open except last
    edges = [i/bins for i in range(bins+1)]
    # bin index: put 1.0 into last bin
    idx = (p*bins).clip(upper=bins-1e-12).astype(int)

    rows = []
    for i in range(bins):
        sel = (idx == i)
        n = int(sel.sum())
        if n == 0:
            rows.append({
                "bin": i,
                "p_lower": edges[i],
                "p_upper": edges[i+1],
                "avg_prob": float("nan"),
                "empirical": float("nan"),
                "count": 0,
            })
        else:
            avg_prob = float(p[sel].mean())
            emp = float(y[sel].mean())
            rows.append({
                "bin": i,
                "p_lower": edges[i],
                "p_upper": edges[i+1],
                "avg_prob": avg_prob,
                "empirical": emp,
                "count": n,
            })
    return pd.DataFrame(rows)

def load_or_build(args):
    if args.reliability_csv and os.path.isfile(args.reliability_csv):
        df = pd.read_csv(args.reliability_csv)
        # Normalize expected columns in case of capitalization drift
        needed = {"bin","p_lower","p_upper","avg_prob","empirical","count"}
        missing = needed - set(df.columns)
        if missing:
            raise SystemExit(f"[plot] reliability CSV missing columns: {missing}")
        return df
    if not args.backtest_csv:
        raise SystemExit(f"[plot] {args.reliability_csv} not found and no --backtest_csv provided.")
    return compute_bins(args.backtest_csv, args.prob_col, args.label_col, args.bins)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reliability_csv", default=DEF_REL)
    ap.add_argument("--backtest_csv", default=None)
    ap.add_argument("--prob_col", default="home_win_prob")
    ap.add_argument("--label_col", default="home_win")
    ap.add_argument("--bins", type=int, default=10)
    ap.add_argument("--out", default=DEF_OUT)
    ap.add_argument("--title", default="Calibration (Reliability Diagram)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = load_or_build(args).copy()

    # Midpoints for x, and Wilson intervals
    df["mid"] = (df["p_lower"] + df["p_upper"]) / 2.0
    lo, hi = [], []
    for _, r in df.iterrows():
        l, h = wilson_interval(float(r["empirical"]) if pd.notna(r["empirical"]) else float("nan"),
                               int(r["count"]))
        lo.append(l); hi.append(h)
    df["emp_lo"] = lo
    df["emp_hi"] = hi

    # Drop NaN points for the line
    plot_df = df.dropna(subset=["avg_prob","empirical"]).copy()

    # Plot
    plt.figure(figsize=(7,7))
    # Main line with markers
    plt.plot(plot_df["avg_prob"], plot_df["empirical"], marker="o")
    # 45deg reference
    plt.plot([0,1],[0,1], linestyle="--")
    # Error bars (Wilson CI)
    plt.errorbar(plot_df["avg_prob"], plot_df["empirical"],
                 yerr=[plot_df["empirical"]-plot_df["emp_lo"],
                       plot_df["emp_hi"]-plot_df["empirical"]],
                 fmt="none", capsize=3)

    # Secondary axis: counts per bin as bars at bin midpoints
    ax = plt.gca()
    ax2 = ax.twinx()
    ax2.bar(df["mid"], df["count"], width=1.0/args.bins*0.9, alpha=0.3)
    ax2.set_ylabel("Count per bin")

    plt.xlim(0,1); plt.ylim(0,1)
    plt.xlabel("Predicted probability")
    plt.ylabel("Empirical frequency")
    plt.title(args.title)
    plt.tight_layout()
    plt.savefig(args.out, dpi=150)
    print(f"Wrote {args.out}")
if __name__ == "__main__":
    main()

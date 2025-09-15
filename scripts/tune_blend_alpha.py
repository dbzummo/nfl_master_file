#!/usr/bin/env python3
"""
Tune blend alpha between baseline probs and Elo-logit probs on a given season.

It will:
  - Attach Elo to the provided predictions
  - Apply your learned Elo logistic
  - For each alpha in --alphas, try merging with history using:
        strict -> date_then_fallback -> teams_only
  - Evaluate metrics; collect results even if some alphas fail
  - Write out/blend_alpha_scan.csv and print the best alpha by logloss
"""
import argparse, csv, json, os, subprocess, sys

OUT_DIR = "out"

def sh(cmd, env=None, check=True):
    print(f"\n[sh] {cmd}")
    res = subprocess.run(cmd, shell=True, env=env, text=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if res.stdout:
        print(res.stdout.strip())
    if check and res.returncode != 0:
        raise SystemExit(f"Command failed (rc={res.returncode}): {cmd}")
    return res

def try_build_details(pred_path, hist_path):
    """
    Returns True on success, False if all strategies fail.
    """
    # 1) strict
    r = subprocess.run(
        f'python3 scripts/build_backtest_details_from_weekly.py '
        f'--strategy strict --pred "{pred_path}" --hist "{hist_path}"',
        shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    print(r.stdout.strip())
    if r.returncode == 0:
        return True

    # 2) date_then_fallback
    r = subprocess.run(
        f'python3 scripts/build_backtest_details_from_weekly.py '
        f'--strategy date_then_fallback --pred "{pred_path}" --hist "{hist_path}"',
        shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    print(r.stdout.strip())
    if r.returncode == 0:
        return True

    # 3) teams_only
    r = subprocess.run(
        f'python3 scripts/build_backtest_details_from_weekly.py '
        f'--strategy teams_only --pred "{pred_path}" --hist "{hist_path}"',
        shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    print(r.stdout.strip())
    return r.returncode == 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--pred", required=True, help="Baseline predictions CSV (home_team,away_team,date,home_win_prob)")
    ap.add_argument("--alphas", default="0.5,0.6,0.7,0.8,0.9")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)

    # 0) Find matching history file
    hist = sh(f'scripts/find_history_for_season.py {args.season}').stdout.strip()
    if not hist or not os.path.exists(hist):
        print(f"[fatal] No history found for season {args.season}. Got: '{hist}'")
        sys.exit(1)
    print(f"[info] Using history: {hist}")

    # 1) Attach Elo to the given predictions (writes out/predictions_with_elo.csv)
    sh(f'python3 scripts/attach_elo_to_predictions.py --pred "{args.pred}"')

    # 2) Apply Elo logit (writes out/predictions_with_elo_cal.csv)
    sh('python3 scripts/apply_elo_logit.py --pred_in out/predictions_with_elo.csv --out out/predictions_with_elo_cal.csv')

    # 3) Prepare alpha list
    alphas = [float(x.strip()) for x in args.alphas.split(",") if x.strip()]
    results = []

    for a in alphas:
        print(f"\n=== Alpha {a:.2f} ===")
        # Blend -> write compact eval preds
        blend_in = "out/predictions_with_elo_cal.csv"
        blend_out = f"out/blended_for_eval_{args.season}_a{a:.2f}.csv"
        cmd = (
          f'python3 - << "PY"\n'
          f'import pandas as pd\n'
          f'df = pd.read_csv("{blend_in}")\n'
          f'need=["home_team","away_team","date","home_win_prob","elo_logit_prob"]\n'
          f'assert set(need).issubset(df.columns), "Missing columns in predictions_with_elo_cal.csv"\n'
          f'a = {a}\n'
          f'df["home_win_prob"] = a*df["home_win_prob"] + (1-a)*df["elo_logit_prob"]\n'
          f'df = df[["home_team","away_team","date","home_win_prob"]]\n'
          f'df.to_csv("{blend_out}", index=False)\n'
          f'print("Wrote {blend_out}", len(df))\n'
          f'PY'
        )
        try:
            sh(cmd)
        except SystemExit as e:
            print(f"[warn] Blend step failed for alpha {a:.2f}: {e}")
            continue

        # Build backtest details (try 3 strategies)
        ok = try_build_details(blend_out, hist)
        if not ok:
            print(f"[warn] Could not build details for alpha {a:.2f}; skipping metrics.")
            continue

        # Evaluate
        env = os.environ.copy()
        env["BACKTEST_CSV"] = "out/backtest_details.csv"
        env["PROB_COL"] = "home_win_prob"
        try:
            sh('python3 scripts/evaluate_metrics.py', env=env, check=True)
            with open(os.path.join(OUT_DIR,"metrics_summary.json"), "r") as f:
                m = json.load(f)
            print(f"[alpha {a:.2f}] logloss={m.get('logloss'):.6f}  "
                  f"brier={m.get('brier'):.6f}  ece={m.get('ece'):.6f}  n={m.get('n')}")
            results.append({
                "alpha": a,
                "logloss": m.get("logloss"),
                "brier": m.get("brier"),
                "ece": m.get("ece"),
                "n": m.get("n")
            })
        except SystemExit as e:
            print(f"[warn] Metrics failed for alpha {a:.2f}: {e}")
            continue

    # Write results if any
    out_csv = os.path.join(OUT_DIR, "blend_alpha_scan.csv")
    if results:
        with open(out_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["alpha","logloss","brier","ece","n"])
            w.writeheader()
            for r in sorted(results, key=lambda x: x["alpha"]):
                w.writerow(r)
        best = min(results, key=lambda x: x["logloss"])
        print("\n=== Alpha scan complete ===")
        print(f"Best alpha by logloss: {best['alpha']:.2f} "
              f"(logloss={best['logloss']:.6f}, brier={best['brier']:.6f}, ece={best['ece']:.6f}, n={best['n']})")
        print(f"Wrote {out_csv}")
    else:
        print("[fatal] No successful alpha runs; check merge or inputs.")
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
run_week.py

Always-fresh weekly pipeline:
1) Fetch MSF schedule/results for a window.
2) Fetch Bwin odds via OddsAPI (optional: allow-missing).
3) Compute OC/DC scheme adjustments (resilient; writes even if data absent).
4) Render full HTML (table → cards → summary).

Examples:
  python3 scripts/run_week.py --start 20250911 --end 20250916 --season 2025-regular --allow-missing-odds
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

def run(cmd, fail_ok=False):
    print(f"[cmd] {cmd}")
    p = subprocess.run(cmd, shell=True)
    if p.returncode != 0 and not fail_ok:
        print(f"[RUN][FAIL] command failed: {cmd}", file=sys.stderr)
        sys.exit(1)
    return p.returncode

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=True, help="YYYYMMDD")
    ap.add_argument("--end", type=str, required=True, help="YYYYMMDD")
    ap.add_argument("--season", type=str, default="2025-regular")
    ap.add_argument("--allow-missing-odds", action="store_true",
                    help="Do not hard-fail if odds are missing")
    args = ap.parse_args()

    py = sys.executable

    # 1) MSF schedule/results
    run(f"{py} {os.path.join(HERE, 'fetch_week_msf.py')} "
        f"--start {args.start} --end {args.end} --season {args.season}")

    # 2) Odds (optional fail)
    allow_flag = "--allow-missing" if args.allow_missing_odds else ""
    run(f"{py} {os.path.join(HERE, 'fetch_odds_bwin.py')} "
        f"--start {args.start} --end {args.end} --season {args.season} {allow_flag}",
        fail_ok=args.allow_missing_odds)

    # 3) Scheme (never hard-fails)
    run(f"{py} {os.path.join(HERE, 'compute_scheme_features.py')} "
        f"--start {args.start} --end {args.end} --season {args.season}",
        fail_ok=True)

    # 4) Render full HTML
    run(f"{py} {os.path.join(HERE, 'render_full_week.py')}")
    print("[ok] RUN WEEK PASS")

if __name__ == "__main__":
    main()

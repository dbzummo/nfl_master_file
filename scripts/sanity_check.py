#!/usr/bin/env python3
# scripts/sanity_check.py
#
# One-button health check for the weekly report pipeline:
# - Verifies MSF creds and required scripts/files
# - Runs fetch_week_msf.py for the requested window
# - Normalizes out/msf_week.csv (adds status if only played_status exists)
# - (Optionally) checks predictions + market lines format
# - Runs render_full_week.py and verifies the HTML was written
# - Prints a crisp PASS/FAIL summary
#
# Usage (Week 1 default):
#   python3 scripts/sanity_check.py
# Custom window/season:
#   python3 scripts/sanity_check.py --start 20250904 --end 20250909 --season 2025-regular
#
# Exit code: 0 on PASS, 1 on FAIL (CI-friendly)

from __future__ import annotations
import os
import re
import sys
import json
import time
import argparse
import subprocess
from pathlib import Path
from typing import List, Tuple

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"

FETCH = SCRIPTS / "fetch_week_msf.py"
RENDER = SCRIPTS / "render_full_week.py"

OUT_DIR = REPO / "out"
REPORTS_DIR = REPO / "reports"

MSF_WEEK = OUT_DIR / "msf_week.csv"
PRED_FILE = OUT_DIR / "predictions_week_calibrated_blend.csv"
LINES_FILE = REPO / "overrides" / "market_lines.csv"

def sh(cmd: List[str], env=None) -> Tuple[int, str]:
    """Run a command and return (rc, combined_output)."""
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(REPO), env=env
    )
    out = p.communicate()[0]
    return p.returncode, out

def ok(msg: str):  print(f"[ok] {msg}")
def info(msg: str): print(f"[i] {msg}")
def warn(msg: str): print(f"[warn] {msg}")
def fail(msg: str): print(f"[FAIL] {msg}")

def check_creds() -> bool:
    key = os.environ.get("MSF_KEY")
    pwd = os.environ.get("MSF_PASS")
    if not key or not pwd:
        fail("missing MSF_KEY / MSF_PASS in environment")
        return False
    ok("creds present")
    return True

def check_scripts() -> bool:
    missing = []
    for p in (FETCH, RENDER):
        if not p.exists():
            missing.append(str(p))
    if missing:
        fail("missing scripts: " + ", ".join(missing))
        return False
    ok("scripts located")
    return True

def normalize_msf_week() -> None:
    """Add status column if only played_status exists; no-op otherwise."""
    if not MSF_WEEK.exists():
        return
    import pandas as pd
    df = pd.read_csv(MSF_WEEK)
    touched = False
    if "status" not in df.columns and "played_status" in df.columns:
        df["status"] = df["played_status"]
        touched = True
    # normalize team column names if fetch changed them
    rename_map = {}
    if "away" in df.columns and "away_team" not in df.columns:
        rename_map["away"] = "away_team"
    if "home" in df.columns and "home_team" not in df.columns:
        rename_map["home"] = "home_team"
    if rename_map:
        df = df.rename(columns=rename_map)
        touched = True
    if touched:
        df.to_csv(MSF_WEEK, index=False)
        ok("normalized out/msf_week.csv (columns)")

def validate_msf_week() -> bool:
    if not MSF_WEEK.exists():
        fail("missing out/msf_week.csv (did fetch succeed?)")
        return False
    import pandas as pd
    df = pd.read_csv(MSF_WEEK)
    required_any = [
        ["status", "played_status"],
    ]
    required_all = ["date", "away_team", "home_team"]
    missing_all = [c for c in required_all if c not in df.columns]
    if missing_all:
        fail("msf_week.csv missing required columns: " + ", ".join(missing_all))
        return False
    for group in required_any:
        if not any(col in df.columns for col in group):
            fail("msf_week.csv missing one-of columns: " + " | ".join(group))
            return False
    ok(f"msf_week.csv looks good (rows={len(df)})")
    return True

def validate_predictions(optional: bool = True) -> bool:
    if not PRED_FILE.exists():
        if optional:
            info("predictions file not found (optional): out/predictions_week_calibrated_blend.csv")
            return True
        fail("predictions file missing")
        return False
    import pandas as pd
    df = pd.read_csv(PRED_FILE)
    required = ["date", "away_team", "home_team", "home_win_prob"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        fail("predictions missing columns: " + ", ".join(miss))
        return False
    ok(f"predictions present (rows={len(df)})")
    return True

def validate_lines(optional: bool = True) -> bool:
    if not LINES_FILE.exists():
        if optional:
            info("market lines not found (optional): overrides/market_lines.csv")
            return True
        fail("market lines missing")
        return False
    import pandas as pd
    df = pd.read_csv(LINES_FILE)
    required = ["date", "away", "home", "market_spread", "market_total"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        # Also allow already-normalized variant (away_team/home_team)
        alt_ok = all(c in df.columns for c in ["date", "away_team", "home_team"])
        if not alt_ok:
            fail("market lines missing columns: " + ", ".join(miss))
            return False
    ok(f"market lines present (rows={len(df)})")
    return True

def parse_render_output_for_path(text: str) -> Path | None:
    # render_full_week prints like: "[done] reports/<dir>/weekly_report.html"
    m = re.search(r"\[done\]\s+(reports/.+?/weekly_report\.html)", text.strip())
    if m:
        return REPO / m.group(1)
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20250904", help="YYYYMMDD")
    ap.add_argument("--end", default="20250909", help="YYYYMMDD")
    ap.add_argument("--season", default="2025-regular")
    ap.add_argument("--no-open", action="store_true", help="do not open the HTML")
    args = ap.parse_args()

    all_ok = True

    # 0) Basics
    if not check_creds(): return sys.exit(1)
    if not check_scripts(): return sys.exit(1)

    # 1) Fetch
    cmd_fetch = [
        sys.executable, str(FETCH),
        "--start", args.start,
        "--end", args.end,
        "--season", args.season,
    ]
    print(f"[cmd] {' '.join(cmd_fetch)}")
    rc, out = sh(cmd_fetch, env=os.environ.copy())
    print(out, end="")
    if rc != 0:
        fail(f"command failed: {' '.join(cmd_fetch)}")
        return sys.exit(1)

    # 2) Normalize + validate msf_week.csv
    normalize_msf_week()
    if not validate_msf_week():
        return sys.exit(1)

    # 3) Validate optional inputs
    if not validate_predictions(optional=True):  # set False to enforce
        all_ok = False
    if not validate_lines(optional=True):
        all_ok = False

    # 4) Render
    cmd_render = [sys.executable, str(RENDER)]
    print(f"[cmd] {' '.join(cmd_render)}")
    rc, out = sh(cmd_render, env=os.environ.copy())
    print(out, end="")
    if rc != 0:
        fail(f"command failed: {' '.join(cmd_render)}")
        return sys.exit(1)

    # 5) Verify HTML path
    html_path = parse_render_output_for_path(out)
    if not html_path or not html_path.exists():
        fail("could not locate the generated weekly_report.html from render output")
        return sys.exit(1)

    ok(f"report generated → {html_path}")

    # 6) Optionally open
    if not args.no_open:
        if sys.platform == "darwin":
            sh(["open", str(html_path)])
        elif sys.platform.startswith("linux"):
            sh(["xdg-open", str(html_path)])

    if all_ok:
        ok("SANITY CHECK PASS")
        sys.exit(0)
    else:
        warn("SANITY CHECK PASS (with warnings)")
        sys.exit(0)

if __name__ == "__main__":
    main()
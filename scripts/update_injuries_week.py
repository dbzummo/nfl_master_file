#!/usr/bin/env python3
import subprocess, sys

def run(cmd):
    print("+", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    try:
        run(["python3","scripts/fetch_injuries_per_team.py"])
    except Exception as e:
        print("[WARN] injuries fetch failed; continuing with any existing out/injuries_week.csv. Reason:", e)
    try:
        run(["python3","scripts/compute_injury_adjustments.py"])
    except Exception as e:
        print("[FATAL] compute_injury_adjustments failed:", e)
        sys.exit(1)
    print("[OK] Injuries pipeline complete.")

if __name__ == "__main__":
    main()

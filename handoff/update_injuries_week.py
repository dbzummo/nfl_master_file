import sys, os, subprocess

def run(cmd):
    print("+"," ".join(cmd)); subprocess.check_call(cmd)

def main():
    # 1) fetch injuries per team
    run(["python3","scripts/fetch_injuries_per_team.py"])
    # 2) compute adjustments (supports "pure" or "relative")
    mode = os.environ.get("INJURY_MODE","pure")
    run(["python3","scripts/compute_injury_adjustments.py", mode])
    print("[OK] injuries pipeline complete.")
if __name__=="__main__":
    main()

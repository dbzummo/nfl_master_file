#!/usr/bin/env python3
import os, shutil, sys, json, subprocess

def fatal(msg, code=1):
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(code)

# Prime Directive: preflight checks inputs & environment, not outputs
REQUIRED_ENVS = ["START","END","SEASON"]
missing = [k for k in REQUIRED_ENVS if not os.environ.get(k)]
if missing:
    fatal(f"Missing required env vars: {', '.join(missing)}", 10)

# Tools
for exe in ("python3","jq","make"):
    if shutil.which(exe) is None:
        fatal(f"Required tool not found in PATH: {exe}", 11)

# Optional: show resolved window
print(json.dumps({
    "preflight":"ok",
    "python": subprocess.getoutput("python3 -V"),
    "make": subprocess.getoutput("make -v | head -n 1"),
    "window": {"start": os.environ["START"], "end": os.environ["END"], "season": os.environ["SEASON"]},
}, indent=2))

print("[OK] preflight passed")

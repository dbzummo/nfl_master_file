#!/usr/bin/env python3
import sys, csv, os
from pathlib import Path

def die(msg, code=2):
    print(f"[FATAL] ensure_nonempty_csv: {msg}", file=sys.stderr)
    sys.exit(code)

if len(sys.argv) != 2:
    die("usage: ensure_nonempty_csv.py <path-to-csv>")

p = Path(sys.argv[1])
if not p.exists():
    die(f"missing {p}")

with p.open(newline="", encoding="utf-8") as f:
    r = csv.reader(f)
    try:
        header = next(r, None)
    except Exception as e:
        die(f"unable to read {p}: {e}")
    if not header:
        die(f"{p} has no header/rows")
    # Count one row cheaply
    firstrow = next(r, None)
    if firstrow is None:
        die(f"{p} has 0 data rows")

print(f"[OK] {p} is non-empty and readable")

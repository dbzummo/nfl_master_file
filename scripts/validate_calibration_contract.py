#!/usr/bin/env python3
import csv, json, os, sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
meta_p  = ROOT / "out/calibration/meta.json"
model_p = ROOT / "out/calibration/model_line_calibration.json"
sample_p= ROOT / "out/calibration/train_sample.csv"

def fail(msg):
    print(f"[FAIL] {msg}", file=sys.stderr); sys.exit(1)

def need(p, what):
    if not p.exists():
        fail(f"Missing {what}: {p}")
    return p

# Load artifacts
need(meta_p,  "meta.json")
need(model_p, "model_line_calibration.json")
need(sample_p,"train_sample.csv")

meta  = json.loads(meta_p.read_text())
model = json.loads(model_p.read_text())

# Read expected from environment (single source of truth)
try:
    exp_hist  = os.environ["CAL_TRAIN_HISTORY_GLOB"]
    exp_start = int(os.environ["CAL_TRAIN_START_SEASON"])
    exp_end   = int(os.environ["CAL_TRAIN_END_SEASON"])
    min_rows  = int(os.environ.get("MIN_CAL_ROWS", "200"))
except KeyError as e:
    fail(f"Required env var not set: {e}")

errs = []

# Contract checks
if meta.get("hist_glob") != exp_hist:
    errs.append(f"hist_glob mismatch: meta={meta.get('hist_glob')} env={exp_hist}")

if meta.get("season_start") != exp_start:
    errs.append(f"season_start mismatch: meta={meta.get('season_start')} env={exp_start}")

if meta.get("season_end") != exp_end:
    errs.append(f"season_end mismatch: meta={meta.get('season_end')} env={exp_end}")

n_rows = meta.get("n_rows")
if not isinstance(n_rows, int) or n_rows < min_rows:
    errs.append(f"n_rows too small: meta.n_rows={n_rows} min_required={min_rows}")

# Model must be non-identity (a,b present) and consistent with sample size
a = model.get("a"); b = model.get("b"); n = model.get("n")
if a is None or b is None:
    errs.append("model is identity (a or b is null)")

if isinstance(n_rows, int) and isinstance(n, int) and n != n_rows:
    errs.append(f"model.n != meta.n_rows: {n} != {n_rows}")

# No coinflips in training sample (no p == 0.5)
with sample_p.open(newline="") as f:
    rdr = csv.DictReader(f)
    for i,row in enumerate(rdr, 1):
        try:
            p = float(row["p"])
            if p == 0.5:
                errs.append(f"coinflip probability (p==0.5) at sample row {i}")
                break
        except Exception:
            errs.append("train_sample.csv missing/invalid 'p' column")
            break

if errs:
    print("\n".join(f"[CONTRACT] {e}" for e in errs), file=sys.stderr)
    sys.exit(2)

print("[OK] calibration contract honored:")
print(f"     hist_glob={exp_hist} seasons={exp_start}-{exp_end} n_rows={n_rows} a={a:.4f} b={b:.4f}")

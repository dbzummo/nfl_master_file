#!/usr/bin/env python3
import json, os, sys, pathlib, shutil
CFG = pathlib.Path("config/model_line_calibration.json")
OUT = pathlib.Path("out/calibration/model_line_calibration.json")
def fatal(m, c=1): print(f"[FATAL] {m}", file=sys.stderr); sys.exit(c)
if not CFG.exists(): fatal(f"Missing {CFG}")
d = json.loads(CFG.read_text(encoding="utf-8"))
for k in ("a","b"):
    if k not in d: fatal(f"{CFG} missing '{k}'")
OUT.parent.mkdir(parents=True, exist_ok=True)
# normalize to only what's needed at runtime
OUT.write_text(json.dumps({"a": float(d["a"]), "b": float(d["b"])}, indent=2), encoding="utf-8")
print(f"[OK] wrote {OUT}")

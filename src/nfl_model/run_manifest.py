from __future__ import annotations
import json, os, subprocess, hashlib, time
from pathlib import Path
from typing import Dict, Any

def _sha256(path: Path) -> str | None:
    if not path.exists(): return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()

def _git_sha() -> str:
    try:
        return subprocess.check_output(["git","rev-parse","HEAD"], text=True).strip()
    except Exception:
        return ""

def write_manifest(dst: str,
                   calibration: Dict[str, Any],
                   inputs: Dict[str, str],
                   strict: bool) -> None:
    outp = Path(dst)
    outp.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "git_sha": _git_sha(),
        "window": {"start": os.environ.get("START",""), "end": os.environ.get("END",""), "season": os.environ.get("SEASON","")},
        "calibration": calibration,
        "strict_mode": bool(strict),
        "inputs": {},
    }
    for k, v in (inputs or {}).items():
        p = Path(v)
        payload["inputs"][k] = {"path": v, "exists": p.exists(), "sha256": _sha256(p) if p.exists() else None}
    outp.write_text(json.dumps(payload, indent=2), encoding="utf-8")

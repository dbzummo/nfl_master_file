#!/usr/bin/env python3
"""
manifest_writer.py — write run provenance to a JSON manifest
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Optional, Union
import json

def write_manifest(
    path: Union[Path, str],
    run_meta: Dict[str, Any],
    inputs: Dict[str, Any],
    outputs: Dict[str, Any],
    audits: Dict[str, Any],
    extras: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a structured manifest.json for provenance/debugging."""
    manifest: Dict[str, Any] = {
        "run_meta": run_meta or {},
        "inputs": inputs or {},
        "outputs": outputs or {},
        "audits": audits or {},
    }
    if extras is not None:
        manifest["extras"] = extras

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Saved: {path.name}")

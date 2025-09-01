#!/usr/bin/env python3
"""
gates.py — centralizes strict validations so we never silently run on stale/bad data
"""

from __future__ import annotations
from typing import Dict, Any, Iterable, Optional, List
import sys

def die(msg: str, code: int = 2) -> None:
    print(f"\n⛔ FATAL: {msg}")
    sys.exit(code)

def warn(msg: str) -> None:
    print(f"⚠️  {msg}")

def require_env(env: Dict[str, Optional[str]], required_keys: Iterable[str]) -> None:
    missing = [k for k in required_keys if not env.get(k)]
    if missing:
        die(f"Missing required environment vars: {missing}. "
            f"Create a .env with these keys (or export them) before running.")

def require_columns(df, name: str, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        die(f"{name} is missing required columns: {missing}")

def enforce_roster_audit(audit_log: Dict[str, Any]) -> None:
    """
    Expects the structure produced by run_audit.run_roster_audit()
    {
      "checked": [...],
      "conflicts": {
         "TEAM": [{"severity":"BLOCK"|"HOLD", "details":"..."}], ...
      }
    }
    """
    conflicts = (audit_log or {}).get("conflicts", {})
    if not conflicts:
        print("✅ Audit complete. No conflicts found.")
        return

    print("\n--- 🚨 AUDIT WARNINGS 🚨 ---")
    blocked = False
    for team, items in conflicts.items():
        for c in items:
            sev = str(c.get("severity", "HOLD")).upper()
            details = c.get("details", "No details.")
            if sev == "BLOCK":
                blocked = True
                print(f"⛔ BLOCK on {team}: {details}")
            else:
                print(f"⚠️ HOLD on {team}: {details}")
    print("-----------------------------\n")

    if blocked:
        die("Roster audit returned BLOCK conflicts. Resolve before running simulations.")
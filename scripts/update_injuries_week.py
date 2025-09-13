#!/usr/bin/env python3
# scripts/update_injuries_week.py
#
# Bulk-pull NFL injuries (MSF v2.1) with pagination + retry/backoff.
# Outputs: out/injuries/injuries_feed.csv
# Logs errors: out/out_provider_errors.jsonl
#
# Env vars required:
#   MSF_KEY, MSF_PASS   (for Basic Auth; for API key auth, PASS must be 'MYSPORTSFEEDS')
#
# Optional env knobs:
#   INJURIES_LIMIT         (default: 500)
#   INJURIES_FORCE         (default: false)  # use 'true' only when you *need* a fresh pull
#   INJURIES_TIMEOUT       (default: 8.0)    # seconds
#   INJURIES_RETRIES       (default: 3)
#   INJURIES_BACKOFF_BASE  (default: 0.6)    # seconds; exponential backoff
#   STRICT_MODE            (default: 1)      # 1 ⇒ raise on failure, 0 ⇒ continue silently
#
# Usage:
#   python3 scripts/update_injuries_week.py
#

import os
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ---------------- Config ----------------
MSF_KEY = os.getenv("MSF_KEY", "").strip()
MSF_PASS = os.getenv("MSF_PASS", "").strip()

STRICT_MODE = os.getenv("STRICT_MODE", "1") == "1"

INJURIES_LIMIT = int(os.getenv("INJURIES_LIMIT", "500"))
INJURIES_FORCE = os.getenv("INJURIES_FORCE", "false").lower() == "true"
INJURIES_TIMEOUT = float(os.getenv("INJURIES_TIMEOUT", "8.0"))
INJURIES_RETRIES = int(os.getenv("INJURIES_RETRIES", "3"))
INJURIES_BACKOFF_BASE = float(os.getenv("INJURIES_BACKOFF_BASE", "0.6"))

PROVIDER_ERROR_LOG = os.getenv("PROVIDER_ERROR_LOG", "out/out_provider_errors.jsonl")

BASE_URL = "https://api.mysportsfeeds.com/v2.1/pull/nfl/injuries.json"

TRANSIENT_HTTP = {429, 500, 502, 503, 504}


# ---------------- Utils ----------------
def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _log_error(note: str, status: Optional[int] = None, body_snippet: Optional[str] = None,
               endpoint: str = BASE_URL, params: Optional[Dict[str, Any]] = None) -> None:
    _ensure_parent(Path(PROVIDER_ERROR_LOG))
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "provider": "MySportsFeeds",
        "endpoint": endpoint,
        "params": params or {},
        "status": status,
        "body_snippet": (body_snippet[:500] if body_snippet else None),
        "note": note,
    }
    with open(PROVIDER_ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _http_get(session: requests.Session, offset: int, limit: int, force: bool) -> Dict[str, Any]:
    params = {
        "offset": offset,
        "limit": limit,
        "force": "true" if force else "false",
    }
    last_err: Optional[Exception] = None
    for attempt in range(INJURIES_RETRIES):
        try:
            resp = session.get(
                BASE_URL,
                params=params,
                timeout=INJURIES_TIMEOUT,
                auth=(MSF_KEY, MSF_PASS),
            )
            if resp.status_code != 200:
                # Retry on transient
                if resp.status_code in TRANSIENT_HTTP and attempt < INJURIES_RETRIES - 1:
                    time.sleep(INJURIES_BACKOFF_BASE * (2 ** attempt))
                    continue
                # Log and raise
                _log_error(
                    note=f"HTTP error {resp.status_code}",
                    status=resp.status_code,
                    body_snippet=(resp.text or "")[:500],
                    params=params,
                )
                resp.raise_for_status()

            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype:
                _log_error(note=f"Unexpected content-type: {ctype}", status=resp.status_code, params=params)
                raise RuntimeError(f"Unexpected content-type: {ctype}")

            data = resp.json()
            if not isinstance(data, dict):
                _log_error(note="Invalid JSON (not a dict).", status=resp.status_code, params=params)
                raise RuntimeError("Invalid JSON (not a dict).")

            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt < INJURIES_RETRIES - 1:
                time.sleep(INJURIES_BACKOFF_BASE * (2 ** attempt))
                continue
            _log_error(note=f"Network error: {e}", params=params)

        except Exception as e:
            last_err = e
            # Non-retryable or exhausted retry
            break

    if STRICT_MODE:
        raise last_err or RuntimeError("Unknown injuries fetch failure.")
    return {}


def _normalize_to_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten MSF injuries payload to rows suitable for CSV.
    """
    rows: List[Dict[str, Any]] = []
    if not payload or "players" not in payload or not isinstance(payload["players"], list):
        return rows

    updated = payload.get("lastUpdatedOn")

    for p in payload["players"]:
        # base player info
        pid = p.get("id")
        first = p.get("firstName")
        last = p.get("lastName")
        pos = p.get("primaryPosition")
        jersey = p.get("jerseyNumber")

        # team
        t = p.get("currentTeam") or {}
        team_id = t.get("id")
        team_abbr = t.get("abbreviation")

        roster_status = p.get("currentRosterStatus")

        inj = p.get("currentInjury") or {}
        inj_desc = inj.get("description")
        inj_prob = inj.get("playingProbability")

        rows.append({
            "player_id": pid,
            "first_name": first,
            "last_name": last,
            "position": pos,
            "jersey_number": jersey,
            "team_id": team_id,
            "team_abbr": team_abbr,
            "roster_status": roster_status,
            "injury_description": inj_desc,
            "playing_probability": inj_prob,
            "last_updated_on": updated,
        })
    return rows


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    _ensure_parent(path)
    fieldnames = [
        "player_id",
        "first_name",
        "last_name",
        "position",
        "jersey_number",
        "team_id",
        "team_abbr",
        "roster_status",
        "injury_description",
        "playing_probability",
        "last_updated_on",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    if not MSF_KEY or not MSF_PASS:
        msg = "Missing MSF_KEY/MSF_PASS env vars."
        _log_error(note=msg)
        if STRICT_MODE:
            raise RuntimeError(msg)
        print(f"[WARN] {msg}")
        return

    out_csv = Path("out/injuries/injuries_feed.csv")
    all_rows: List[Dict[str, Any]] = []

    session = requests.Session()
    offset = 0
    total_fetched = 0

    while True:
        data = _http_get(session, offset=offset, limit=INJURIES_LIMIT, force=INJURIES_FORCE)
        page_rows = _normalize_to_rows(data)
        if not page_rows:
            # No rows returned => end of pagination
            break

        all_rows.extend(page_rows)
        total_fetched += len(page_rows)

        # If fewer than limit came back, pagination complete
        if len(page_rows) < INJURIES_LIMIT:
            break

        offset += INJURIES_LIMIT
        # Gentle throttle between pages
        time.sleep(0.25)

    _write_csv(out_csv, all_rows)
    print(f"[OK] normalized {len(all_rows)} rows → {out_csv}")


if __name__ == "__main__":
    main()
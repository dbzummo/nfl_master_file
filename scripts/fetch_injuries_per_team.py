#!/usr/bin/env python3
"""
Team-scoped MSF injuries fetcher (force=false) with polite backoff.
Writes raw per-team JSON and a normalized union CSV for downstream steps.

Env:
  MSF_KEY, MSF_PASS  (required)
Optional:
  TEAMS               Comma-separated list of team abbreviations to limit scope.
  SLEEP_BASE          Base sleep between teams (seconds, default 0.8)
  RETRY_MAX           Max retries per team (default 6)
  OUTDIR              Base output dir (default: out/injuries)

Outputs:
  out/injuries/raw/<UTC stamp>/<TEAM>.json
  out/injuries/injuries_feed.csv
"""

from __future__ import annotations
import os, sys, time, json, csv, math, pathlib, datetime, typing as T
import requests

MSF_KEY = os.getenv("MSF_KEY", "").strip()
MSF_PASS = os.getenv("MSF_PASS", "").strip()
if not MSF_KEY or not MSF_PASS:
    print("[FATAL] MSF_KEY/MSF_PASS not set in environment", file=sys.stderr)
    sys.exit(2)

# Default all 32 NFL teams (standard abbreviations used by MSF).
ALL_TEAMS = [
    "ARI","ATL","BAL","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GB",
    "HOU","IND","JAX","KC","LV","LAC","LAR","MIA","MIN","NE","NO","NYG","NYJ",
    "PHI","PIT","SEA","SF","TB","TEN","WAS",
]
TEAMS = [t.strip().upper() for t in os.getenv("TEAMS", ",".join(ALL_TEAMS)).split(",") if t.strip()]

SLEEP_BASE = float(os.getenv("SLEEP_BASE", "0.8"))
RETRY_MAX  = int(os.getenv("RETRY_MAX", "6"))
OUTDIR     = pathlib.Path(os.getenv("OUTDIR", "out/injuries")).resolve()

RAW_DIR = OUTDIR / "raw" / datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUTDIR / "injuries_feed.csv"
OUTDIR.mkdir(parents=True, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})

def backoff_sleep(attempt: int, retry_after: float | None) -> None:
    if retry_after and retry_after > 0:
        time.sleep(min(retry_after, 20.0))
    else:
        # exponential backoff with jitter
        sleep = min(20.0, SLEEP_BASE * (2 ** attempt)) + (0.05 * attempt)
        time.sleep(sleep)

def fetch_team(team: str) -> dict:
    url = "https://api.mysportsfeeds.com/v2.1/pull/nfl/injuries.json"
    params = {"team": team, "force": "false"}
    for attempt in range(RETRY_MAX + 1):
        try:
            resp = SESSION.get(url, params=params, auth=(MSF_KEY, MSF_PASS), timeout=30)
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                retry_after = float(ra) if ra and ra.isdigit() else None
                print(f"[429] team={team} attempt={attempt+1}/{RETRY_MAX}; backing off{f' {retry_after}s' if retry_after else ''}...", file=sys.stderr)
                backoff_sleep(attempt, retry_after)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"[WARN] team={team} attempt={attempt+1}/{RETRY_MAX} error={e}; backing off...", file=sys.stderr)
            backoff_sleep(attempt, None)
    raise RuntimeError(f"Failed to fetch injuries for team={team} after {RETRY_MAX} retries")

def write_raw(team: str, payload: dict) -> None:
    p = RAW_DIR / f"{team}.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def normalize_payload(team: str, payload: dict) -> T.List[dict]:
    """
    Normalize MSF injuries feed into a conservative, pipeline-friendly schema.
    We cover the most common fields used downstream:
      team, player_id, player_name, position, status, injury_desc, last_update
    """
    out: T.List[dict] = []
    last_update = payload.get("lastUpdatedOn") or ""
    # MSF "injuries" feed typically returns an 'injuries' array with 'player' and 'team' refs.
    # If the shape differs, we do best-effort extraction of status/desc/position.
    items = payload.get("injuries") or []
    if not isinstance(items, list):
        items = []

    def get(d: dict, path: T.List[str], default=""):
        cur = d
        for k in path:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    for it in items:
        player = get(it, ["player"], {})
        status = get(it, ["injury","status"]) or get(it, ["injury","injuryStatus"]) or ""
        desc   = get(it, ["injury","comment"]) or get(it, ["injury","description"]) or ""
        pos    = player.get("primaryPosition") or player.get("position") or ""
        pid    = player.get("id")
        fname  = player.get("firstName","")
        lname  = player.get("lastName","")
        name   = (fname + " " + lname).strip() or get(it, ["player","fullName"], "")
        out.append({
            "team": team,
            "player_id": str(pid) if pid is not None else "",
            "player_name": name,
            "position": pos,
            "status": status,
            "injury_desc": desc,
            "last_update": last_update,
        })
    return out

def main() -> None:
    print(f"[RUN] teams={len(TEAMS)} mode=force=false out={OUT_CSV}")
    union: T.List[dict] = []
    failures: T.List[str] = []

    for i, team in enumerate(TEAMS, 1):
        print(f"[STEP] {i}/{len(TEAMS)} team={team}")
        try:
            payload = fetch_team(team)
            write_raw(team, payload)
            rows = normalize_payload(team, payload)
            print(f"[OK] {team}: normalized {len(rows)} rows")
            union.extend(rows)
            time.sleep(SLEEP_BASE)  # polite gap between teams
        except Exception as e:
            print(f"[FAIL] {team}: {e}", file=sys.stderr)
            failures.append(team)

    # write union CSV
    fieldnames = ["team","player_id","player_name","position","status","injury_desc","last_update"]
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in union:
            w.writerow({k: r.get(k,"") for k in fieldnames})

    print(f"[SUMMARY] wrote {OUT_CSV} rows={len(union)} teams_ok={len(TEAMS)-len(failures)} teams_fail={len(failures)}")
    if failures:
        print(f"[WARN] team fetch failures: {','.join(failures)}", file=sys.stderr)

if __name__ == "__main__":
    main()

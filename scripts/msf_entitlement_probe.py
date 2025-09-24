#!/usr/bin/env python3
"""
MSF Entitlement Probe
---------------------
Verifies which MySportsFeeds endpoints your account can access and why failures occur.

Outputs:
  - out/msf_entitlements_report.csv  (endpoint, version, url, status, reason, notes)
  - Console summary with quick interpretations.

Usage examples:
  export MSF_KEY="your-new-token"
  export MSF_PASS="MYSPORTSFEEDS"
  python3 scripts/msf_entitlement_probe.py --start 20250904 --end 20250909 --season 2025-regular

  # (Optional) specify a known game id to skip discovery:
  python3 scripts/msf_entitlement_probe.py --season 2025-regular --game-id 148741
"""
from __future__ import annotations
import os
import sys
import argparse
import json
import time
import textwrap
import csv
from typing import Dict, Any, Tuple, List, Optional
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlencode

BASE_V2 = "https://api.mysportsfeeds.com/v2.1/pull/nfl"
BASE_V1 = "https://api.mysportsfeeds.com/v1.2/pull/nfl"

def env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        print(f"[ERR] Missing env var {key}. Export MSF_KEY and MSF_PASS.", file=sys.stderr)
        sys.exit(2)
    return val

def short_reason(status: int, body: bytes) -> str:
    txt = (body or b"")[:300].decode(errors="ignore").lower()
    if status == 200:
        return "OK"
    if status == 401 or "authentication required" in txt:
        return "Authentication Required (401)"
    if status == 403 or "access restricted" in txt:
        return "Access Restricted (403)"
    if status == 404 or "feed not found" in txt:
        return "Feed Not Found (404)"
    return f"HTTP {status}"

def get_json(url: str, auth: HTTPBasicAuth, params: Optional[Dict[str, str]]=None, timeout: int=30) -> Tuple[int, Any, bytes]:
    q = f"?{urlencode(params)}" if params else ""
    full = f"{url}{q}"
    try:
        r = requests.get(full, auth=auth, timeout=timeout)
        body = r.content or b""
        if r.headers.get("content-type","").startswith("application/json"):
            try:
                return r.status_code, r.json(), body
            except Exception:
                pass
        return r.status_code, None, body
    except requests.RequestException as e:
        return 0, None, str(e).encode("utf-8")

def discover_game_id(season: str, start: str, end: str, auth: HTTPBasicAuth) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Returns (gameId, away, home) for the first game in the window; None if not found.
    """
    url = f"{BASE_V2}/{season}/games.json"
    status, js, body = get_json(url, auth, params={"date": f"{start}-{end}"})
    if status != 200 or not js or "games" not in js or not js["games"]:
        print(f"[WARN] Could not discover a game in {start}→{end}. status={status} reason={short_reason(status, body)}")
        return None, None, None
    g = js["games"][0]
    gid = g.get("schedule",{}).get("id")
    away = g.get("schedule",{}).get("awayTeam",{}).get("abbreviation")
    home = g.get("schedule",{}).get("homeTeam",{}).get("abbreviation")
    return gid, away, home

def probe(url: str, label: str, version: str, auth: HTTPBasicAuth, params: Optional[Dict[str,str]]=None) -> Dict[str,str]:
    status, js, body = get_json(url, auth, params=params)
    reason = short_reason(status, body)
    notes = ""
    if status == 200 and js:
        # tiny helpful note for a couple feeds
        if "lastUpdatedOn" in js:
            notes = f"lastUpdatedOn={js['lastUpdatedOn']}"
        elif "games" in js:
            notes = f"games={len(js['games'])}"
    return {
        "endpoint": label,
        "version": version,
        "url": f"{url}{'?' + urlencode(params) if params else ''}",
        "status": str(status),
        "reason": reason,
        "notes": notes,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="2025-regular")
    ap.add_argument("--start", default="20250904")
    ap.add_argument("--end",   default="20250909")
    ap.add_argument("--game-id", type=int, default=None, help="If provided, use this instead of discovery")
    args = ap.parse_args()

    key = env("MSF_KEY")
    pw  = env("MSF_PASS")
    auth = HTTPBasicAuth(key, pw)

    os.makedirs("out", exist_ok=True)

    # Step 1: Find a real gameId (or use provided)
    gid, away, home = args.game_id, None, None
    if gid is None:
        gid, away, home = discover_game_id(args.season, args.start, args.end, auth)
    if gid is None:
        # Still proceed with non game-specific probes.
        print("[WARN] No gameId discovered; will skip game-specific detailed endpoints.\n")

    # Build probe list
    rows: List[Dict[str,str]] = []

    # --- V2 schedule/games (should be OK with CORE) ---
    rows.append(probe(f"{BASE_V2}/{args.season}/games.json", "v2.games", "v2",
                      auth, {"date": f"{args.start}-{args.end}"}))

    # --- V2 DETAILED endpoints (require DETAILED add-on) ---
    if gid is not None:
        rows.append(probe(f"{BASE_V2}/{args.season}/game_boxscore.json", "v2.game_boxscore", "v2",
                          auth, {"game": str(gid)}))
        rows.append(probe(f"{BASE_V2}/{args.season}/game_playbyplay.json", "v2.game_playbyplay", "v2",
                          auth, {"game": str(gid)}))
        rows.append(probe(f"{BASE_V2}/{args.season}/game_lineups.json", "v2.game_lineups", "v2",
                          auth, {"game": str(gid)}))
    # Player injuries (window-based)
    rows.append(probe(f"{BASE_V2}/{args.season}/player_injuries.json", "v2.player_injuries", "v2",
                      auth, {"date": f"{args.start}-{args.end}"}))

    # --- V2 STATS endpoints (require STATS add-on) ---
    rows.append(probe(f"{BASE_V2}/{args.season}/weekly_team_gamelogs.json", "v2.weekly_team_gamelogs", "v2",
                      auth, {"date": f"{args.start}-{args.end}"}))
    rows.append(probe(f"{BASE_V2}/{args.season}/weekly_player_gamelogs.json", "v2.weekly_player_gamelogs", "v2",
                      auth, {"date": f"{args.start}-{args.end}"}))
    rows.append(probe(f"{BASE_V2}/{args.season}/seasonal_team_stats.json", "v2.seasonal_team_stats", "v2",
                      auth, None))
    rows.append(probe(f"{BASE_V2}/{args.season}/seasonal_player_stats.json", "v2.seasonal_player_stats", "v2",
                      auth, None))
    rows.append(probe(f"{BASE_V2}/{args.season}/standings.json", "v2.standings", "v2",
                      auth, None))

    # --- V1 fallbacks for DETAILED (older names) ---
    if gid is not None:
        # V1 uses YYYY-YYYY regular-season token; we assume MSF maps season token; many accounts still accept v1 auth.
        # Most helpful for understanding entitlement: v1 often returns 403 "Access Restricted" when you lack add-on.
        rows.append(probe(f"{BASE_V1}/{args.season}/game_boxscore.json", "v1.game_boxscore", "v1",
                          auth, {"gameid": str(gid)}))
        rows.append(probe(f"{BASE_V1}/{args.season}/game_playbyplay.json", "v1.game_playbyplay", "v1",
                          auth, {"gameid": str(gid)}))
        rows.append(probe(f"{BASE_V1}/{args.season}/game_startinglineup.json", "v1.game_startinglineup", "v1",
                          auth, {"gameid": str(gid)}))
    rows.append(probe(f"{BASE_V1}/{args.season}/player_injuries.json", "v1.player_injuries", "v1",
                      auth, {"date": f"{args.start}-{args.end}"}))

    # Write CSV
    out_csv = "out/msf_entitlements_report.csv"
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["endpoint","version","url","status","reason","notes"])
        w.writeheader()
        w.writerows(rows)

    # Console summary with interpretations
    print(f"[ok] wrote {out_csv}")
    ok = [r for r in rows if r["status"] == "200"]
    not_ok = [r for r in rows if r["status"] != "200"]
    print(f"[summary] OK={len(ok)}  Non-OK={len(not_ok)}")

    def any_reason(name: str) -> bool:
        return any(name in r["reason"] for r in rows)

    # Quick interpretations:
    hints: List[str] = []
    # If v2.games is OK but v2 detailed feeds fail → DETAILED likely not enabled.
    v2_games_ok = any(r["endpoint"] == "v2.games" and r["status"] == "200" for r in rows)
    v2_detailed = [r for r in rows if r["endpoint"] in ("v2.game_boxscore","v2.game_playbyplay","v2.game_lineups")]
    if v2_games_ok and v2_detailed and all(r["status"] in ("401","403","404") for r in v2_detailed):
        hints.append("• v2 DETAILED endpoints (boxscore/playbyplay/lineups) are not accessible. This usually means the DETAILED add-on is not enabled on this key, or the feed hasn’t been published yet for that game.")

    # If v2 injuries 404 but v1 injuries returns OK/403 → v2 may not be populated yet or not included; try v1 in pipeline.
    v2_inj = next((r for r in rows if r["endpoint"] == "v2.player_injuries"), None)
    v1_inj = next((r for r in rows if r["endpoint"] == "v1.player_injuries"), None)
    if v2_inj and v2_inj["status"] in ("404","403","401"):
        if v1_inj and v1_inj["status"] == "200":
            hints.append("• v2 injuries unavailable but v1 injuries returns data. Use v1.player_injuries as a fallback.")
        else:
            hints.append("• Injuries feed not accessible (v2 failed and v1 not OK). It may be unpublished for the window or not in your plan.")

    # If all STATS endpoints fail but schedule works → STATS add-on likely missing.
    stats_targets = {"v2.weekly_team_gamelogs","v2.weekly_player_gamelogs","v2.seasonal_team_stats","v2.seasonal_player_stats","v2.standings"}
    stats_all = [r for r in rows if r["endpoint"] in stats_targets]
    if stats_all and all(r["status"] != "200" for r in stats_all) and v2_games_ok:
        hints.append("• STATS endpoints are not accessible. This usually means the STATS add-on is not enabled.")

    # Generic hints based on reasons
    if any_reason("Authentication Required"):
        hints.append("• Some requests returned 401 (Authentication Required). Double-check MSF_KEY / MSF_PASS and that the token matches this subscription.")
    if any_reason("Access Restricted"):
        hints.append("• Some requests returned 403 (Access Restricted). That typically means your auth is valid but your plan doesn’t include that endpoint.")
    if any_reason("Feed Not Found"):
        hints.append("• Some requests returned 404 (Feed Not Found). This can be a) wrong endpoint name, b) feed not yet published for that game/date, or c) a known doc mismatch—try the v1 fallback listed above.")

    if hints:
        print("\n[interpretation]")
        print("\n".join(hints))

    # Small, tidy console table
    print("\n[results]")
    max_ep = max(len(r["endpoint"]) for r in rows)
    max_rs = max(len(r["reason"]) for r in rows)
    print(f"{'endpoint'.ljust(max_ep)}  {'status':>6}  {'reason'.ljust(max_rs)}  url")
    for r in rows:
        print(f"{r['endpoint'].ljust(max_ep)}  {r['status']:>6}  {r['reason'].ljust(max_rs)}  {r['url']}")

if __name__ == "__main__":
    main()

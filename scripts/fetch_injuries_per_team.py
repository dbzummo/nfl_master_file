#!/usr/bin/env python3
import os, time, sys, json, pathlib, math
from datetime import datetime, timezone
import requests
import pandas as pd

MSF_KEY   = os.environ.get("MSF_API_KEY", "")
MSF_SEASON= os.environ.get("MSF_SEASON", "2025-regular")  # not needed for injuries, but kept for consistency

RAW_DIR   = pathlib.Path("raw/msf/injuries")
OUT_WEEK  = pathlib.Path("out/injuries_week.csv")

URL = "https://api.mysportsfeeds.com/v2.1/pull/nfl/injuries.json?force=false"

HEADERS = {}
AUTH    = (MSF_KEY, "MYSPORTSFEEDS") if MSF_KEY else None
TIMEOUT = 30

def backoff_fetch(max_tries=5):
    delay = 1.0
    for i in range(1, max_tries+1):
        r = requests.get(URL, headers=HEADERS, auth=AUTH, timeout=TIMEOUT)
        if r.status_code == 200:
            return r
        if r.status_code in (429, 503):
            print(f"[WARN] injuries HTTP {r.status_code}: throttled. sleeping {delay:.1f}s then retry...")
            time.sleep(delay); delay *= 1.8
            continue
        # other statuses are fatal
        r.raise_for_status()
    raise RuntimeError("injuries fetch failed after %d attempts" % max_tries)

# map MSF playingProbability to our normalized buckets
STATUS_MAP = {
    "OUT":"OUT",
    "DOUBTFUL":"DOUBTFUL",
    "QUESTIONABLE":"QUESTIONABLE",
    "PROBABLE":"PROBABLE",
    "LIKELY":"PROBABLE",
    "ACTIVE":"ACTIVE",
    "INACTIVE":"OUT",
    # common alternates / typos just in case
    "DNP":"OUT",
    "NA":""
}

def normalize_to_rows(js: dict):
    rows = []
    players = js.get("players") or []
    last_updated = js.get("lastUpdatedOn", "")
    for p in players:
        cur_team = (p.get("currentTeam") or {})
        abbr = (cur_team.get("abbreviation") or "").strip().upper()
        cur_inj = (p.get("currentInjury") or {})
        # Only keep players that actually have an injury object
        if not cur_inj:
            continue
        status_raw = (cur_inj.get("playingProbability") or "").strip().upper()
        status_norm = STATUS_MAP.get(status_raw, status_raw)
        rows.append({
            "team_abbr": abbr,
            "player_id": p.get("id"),
            "player_name": f"{p.get('firstName','').strip()} {p.get('lastName','').strip()}".strip(),
            "position": (p.get("primaryPosition") or "").strip().upper(),
            "status_norm": status_norm,
            "designation": "",                       # MSF doesn't provide a separate Q/IR tag here
            "practice": "",                          # not present in this endpoint payload
            "injury_desc": (cur_inj.get("description") or "").strip(),
            "status_raw": status_raw,
            "last_updated": last_updated
        })
    # Drop empties / null team
    rows = [r for r in rows if r["team_abbr"]]
    return rows

def main():
    if not MSF_KEY or len(MSF_KEY) < 20:
        print("[FATAL] MSF_API_KEY missing/placeholder. Export your real key in .env.", file=sys.stderr)
        sys.exit(1)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    try:
        r = backoff_fetch()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        raw_path = RAW_DIR / f"{ts}.json"
        raw_path.write_text(r.text, encoding="utf-8")
        print(f"[OK] injuries snapshot -> {raw_path}")

        js = r.json()
        rows = normalize_to_rows(js)

        # Write out CSV in the schema downstream expects
        out_cols = ["team_abbr","player_id","player_name","position","status_norm",
                    "designation","practice","injury_desc","status_raw","last_updated"]
        df = pd.DataFrame(rows, columns=out_cols)
        OUT_WEEK.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUT_WEEK, index=False)
        print(f"[OK] injuries normalized -> {OUT_WEEK} rows={len(df)}")

        if len(df):
            by_team = df.groupby("team_abbr").size().sort_values(ascending=False)
            top = ", ".join(f"{k}:{v}" for k,v in by_team.head(8).items())
            print(f"[OK] team counts -> {top}")
        else:
            print("[WARN] injuries feed parsed to 0 rows (no currentInjury objects).")

    except Exception as e:
        # Keep pipeline alive with header-only file
        OUT_WEEK.parent.mkdir(parents=True, exist_ok=True)
        if not OUT_WEEK.exists():
            pd.DataFrame(columns=["team_abbr","player_id","player_name","position","status_norm",
                                  "designation","practice","injury_desc","status_raw","last_updated"]
                        ).to_csv(OUT_WEEK, index=False)
        print("[WARN] injuries fetch/normalize failed; wrote header-only injuries_week.csv. Reason:", e)
        raise

if __name__ == "__main__":
    main()

import os, json, time, pathlib, sys
import requests
import pandas as pd

MSF_SEASON = os.environ.get("MSF_SEASON","current")
MSF_API_KEY = os.environ.get("MSF_API_KEY")
BASE = "https://api.mysportsfeeds.com/v2.1/pull/nfl"
OUT_JSON = pathlib.Path("out/msf/injuries_week.json")
OUT_CSV  = pathlib.Path("out/injuries_week.csv")

STATUS_OUT = {"OUT","IR","INJURED_RESERVE","PUP","SUSPENDED","DOUBTFUL"}
def is_out_like(s):
    if not s: return False
    s = str(s).strip().upper()
    return any(tag in s for tag in STATUS_OUT)

def fetch_injuries_for_team(session, team):
    url = f"{BASE}/injuries.json"
    r = session.get(url, params={"team":team, "force":"false"}, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize(j):
    rows=[]
    for it in j.get("injuries",[]):
        team = (it.get("team") or {}).get("abbreviation")
        p    = it.get("player") or {}
        pid  = p.get("id")
        name = " ".join(x for x in [p.get("firstName"), p.get("lastName")] if x)
        pos  = p.get("position")
        det  = it.get("injury") or {}
        status = det.get("status")
        rows.append({
            "team_abbr":team, "player_id":pid, "player_name":name,
            "position":pos, "status":status,
            "details":det.get("desc") or det.get("detail") or "",
            "is_out": bool(is_out_like(status)),
        })
    return rows

def main():
    if not MSF_API_KEY:
        print("[FATAL] MSF_API_KEY not set", file=sys.stderr); sys.exit(2)
    wk = pd.read_csv("out/msf/week_games.csv")
    teams = sorted(set(wk["home_abbr"]) | set(wk["away_abbr"]))
    auth = requests.auth.HTTPBasicAuth(MSF_API_KEY,"MYSPORTSFEEDS")
    s = requests.Session(); s.auth = auth
    all_raw=[]; all_rows=[]
    for t in teams:
        time.sleep(0.2)
        try:
            j = fetch_injuries_for_team(s,t)
            all_raw.append({"team":t,"payload":j})
            all_rows.extend(normalize(j))
        except requests.HTTPError as e:
            print(f"[WARN] injuries {t} HTTP {getattr(e.response,'status_code',None)}", file=sys.stderr)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(all_raw, indent=2), encoding="utf-8")
    pd.DataFrame(all_rows).to_csv(OUT_CSV, index=False)
    print(f"[OK] injuries -> {OUT_CSV} rows={len(all_rows)}")
if __name__ == "__main__":
    main()

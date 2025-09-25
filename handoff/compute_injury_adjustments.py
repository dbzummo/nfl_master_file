import os, sys, math, json, time, pathlib, itertools
from collections import defaultdict, Counter
from datetime import datetime
import pandas as pd, requests

MSF_API_KEY = os.environ.get("MSF_API_KEY")
MSF_SEASON  = os.environ.get("MSF_SEASON","current")

BASE = "https://api.mysportsfeeds.com/v2.1/pull/nfl"
LOG  = pathlib.Path("out/injury_log.jsonl")
OUT  = pathlib.Path("out/injury_adjustments.csv")

# default Elo penalties by position (tunable; auditable)
POS_WEIGHTS = {
  "QB": 65, "RB": 18, "WR": 16, "WR2": 12, "TE": 12,
  "T": 10, "OT":10, "G":10, "C":10,
  "EDGE": 14, "DT": 10, "LB": 10, "CB": 14, "S": 10
}
# status scalers (Out=1.0, Doubtful=0.7, Questionable=0.35, Probable=0.15)
STATUS_SCALE = {
  "OUT":1.0, "INJURED_RESERVE":1.0, "IR":1.0, "PUP":1.0, "SUSPENDED":1.0,
  "DOUBTFUL":0.7, "QUESTIONABLE":0.35, "PROBABLE":0.15, "ACTIVE":0.0
}

PRI_POS_MAP = {"OT":"T"}  # normalize
DEF_POS = {"EDGE","DT","LB","CB","S"}
OFF_POS = {"QB","RB","WR","TE","T","OT","G","C"}

def norm_pos(p):
    if not p: return None
    p = p.upper()
    p = PRI_POS_MAP.get(p,p)
    if p in POS_WEIGHTS: return p
    # normalize variants
    if p in {"LT","RT"}: return "T"
    if p in {"RG","LG"}: return "G"
    if p in {"FS","SS"}: return "S"
    if p.startswith("WR"): return "WR"
    return p

def ln10_over_400():
    return math.log(10)/400.0

def game_slug(row):
    d = str(row["game_date"]).replace("-","").replace("\"","")
    return f"{d}-{row['away_abbr']}-{row['home_abbr']}"

def fetch_lineup_expected(sess, season, slug_or_id):
    url = f"{BASE}/{season}/games/{slug_or_id}/lineup.json"
    r = sess.get(url, params={"lineuptype":"expected","force":"false"}, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_week_gamelogs(sess, season, week):
    url = f"{BASE}/{season}/week/{week}/player_gamelogs.json"
    r = sess.get(url, params={"limit":5000,"force":"false"}, timeout=90)
    r.raise_for_status()
    return r.json()

def usage_score(pos, stats):
    # robust fallbacks by pos
    if pos == "QB":
        return (stats.get("passing",{}) or {}).get("passAttempts", 0)
    if pos == "RB":
        rush = (stats.get("rushing",{}) or {}).get("rushAttempts", 0)
        rec_t = (stats.get("receiving",{}) or {}).get("targets", 0)
        return rush + 0.7*rec_t
    if pos in {"WR","TE"}:
        return (stats.get("receiving",{}) or {}).get("targets", 0)
    # OL/DEF: prefer snaps if available
    so = stats.get("snaps",{}).get("offense", None) if isinstance(stats.get("snaps",{}),dict) else None
    sd = stats.get("snaps",{}).get("defense", None) if isinstance(stats.get("snaps",{}),dict) else None
    if so is not None or sd is not None:
        return (so or 0) + (sd or 0)
    # last resort: total tackles for DEF
    if pos in DEF_POS:
        return (stats.get("defense",{}) or {}).get("tackleTotal", 0)
    return 0

def recency_weighted_canon(wk_numbers, gamelogs, team):
    # gamelogs: list of json for each week
    pool=defaultdict(float)
    for i, (w, j) in enumerate(wk_numbers):
        wgt = 1.0 if i==0 else (0.6 if i==1 else 0.4)
        for gl in (j.get("gamelogs") or []):
            t = (gl.get("team") or {}).get("abbreviation")
            if t != team: continue
            p = gl.get("player") or {}
            pos = norm_pos(p.get("position"))
            if not pos: continue
            s  = gl.get("stats") or {}
            pool[(pos, p.get("id"), p.get("firstName"), p.get("lastName"))] += wgt*usage_score(pos, s)
    by_pos=defaultdict(list)
    for (pos,pid,fn,ln),score in pool.items():
        by_pos[pos].append((score, pid, f"{fn or ''} {ln or ''}".strip()))
    canon={}
    for pos, lst in by_pos.items():
        lst.sort(reverse=True)
        if pos=="WR" and len(lst)>=2:
            canon["WR1"]=lst[0][1:]
            canon["WR2"]=lst[1][1:]
        else:
            canon[pos]=lst[0][1:]
    return canon  # map of pos/WR1/WR2 -> (player_id, player_name)

def main():
    mode = "pure"
    if len(sys.argv)>=2 and sys.argv[1] in {"pure","relative"}:
        mode = sys.argv[1]
    if not MSF_API_KEY:
        print("[FATAL] MSF_API_KEY not set", file=sys.stderr); sys.exit(2)

    wk = pd.read_csv("out/msf/week_games.csv")
    week = int(pd.Series(wk["week"].unique()).iloc[0])
    teams = sorted(set(wk["home_abbr"]) | set(wk["away_abbr"]))

    auth = requests.auth.HTTPBasicAuth(MSF_API_KEY,"MYSPORTSFEEDS")
    s = requests.Session(); s.auth = auth

    # Pull injuries once (already normalized by sibling script if present)
    inj_path = pathlib.Path("out/injuries_week.csv")
    if inj_path.exists():
        injuries = pd.read_csv(inj_path)
    else:
        # light inline fetch if user didn’t run the fetcher yet
        from fetch_injuries_per_team import fetch_injuries_for_team, normalize
        all_rows=[]
        for t in teams:
            time.sleep(0.2)
            try:
                j = fetch_injuries_for_team(s,t)
                all_rows.extend(normalize(j))
            except requests.HTTPError:
                pass
        injuries = pd.DataFrame(all_rows)

    # Recency window: W-1, W-2, W-3 (clamped to >=1)
    weeks = [w for w in [week-1, week-2, week-3] if w>=1]
    wk_gl=[]
    for w in weeks:
        time.sleep(0.3)
        try:
            j = fetch_week_gamelogs(s, MSF_SEASON, w)
            wk_gl.append((w,j))
        except requests.HTTPError:
            pass

    # Build canonical starters by team
    canon_by_team={}
    for t in teams:
        canon_by_team[t] = recency_weighted_canon(wk_gl, wk_gl, t) if wk_gl else {}

    # Expected lineup for current 16 games
    lineup_by_game={}
    for _,row in wk.iterrows():
        slug = f"{str(row['game_date']).replace('\"','')}-{row['away_abbr']}-{row['home_abbr']}"
        time.sleep(0.25)
        try:
            lj = fetch_lineup_expected(s, MSF_SEASON, slug)
            lineup_by_game[slug]=lj
        except requests.HTTPError:
            lineup_by_game[slug]={"lineups":[]}

    # Convert lineups to quick lookup: team -> set(player_id) expected starters
    expected_starters = defaultdict(lambda: defaultdict(set))  # game_slug -> {team_abbr -> set(pids)}
    for slug, j in lineup_by_game.items():
        for L in (j.get("lineups") or []):
            team = (L.get("team") or {}).get("abbreviation")
            for unit in (L.get("expected") or []):
                pos = norm_pos((unit.get("position") or {}).get("abbreviation"))
                for pl in (unit.get("players") or []):
                    pid = (pl.get("player") or {}).get("id")
                    if pid: expected_starters[slug][team].add(pid)

    # Prepare injuries lookup
    injuries["pos_n"] = injuries["position"].map(norm_pos)
    injuries["status_u"] = injuries["status"].fillna("").str.upper()
    def scale_status(s):
        return STATUS_SCALE.get(s, 0.0)
    injuries["status_scale"] = injuries["status_u"].map(scale_status)
    inj_by_team = defaultdict(list)
    for r in injuries.itertuples(index=False):
        inj_by_team[r.team_abbr].append(r)

    # Compute Elo deltas per game
    out_rows=[]
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("w", encoding="utf-8") as logf:
        for _,g in wk.iterrows():
            slug = game_slug(g)
            ha = g["home_abbr"]; aa = g["away_abbr"]
            canon_h = canon_by_team.get(ha,{})
            canon_a = canon_by_team.get(aa,{})
            starters_h = expected_starters.get(slug,{}).get(ha,set())
            starters_a = expected_starters.get(slug,{}).get(aa,set())

            def side_delta(team, canon, starters):
                delta = 0.0; details=[]
                for key,pos_weight in POS_WEIGHTS.items():
                    # expand WR1/WR2 for WR if registry provided
                    keys = [key]
                    if key=="WR":
                        keys=["WR","WR1","WR2"]
                    for k in keys:
                        tup = canon.get(k)
                        if not tup: continue
                        pid, pname = tup
                        # Is that starter injured?
                        inj_list = inj_by_team.get(team,[])
                        inj_match = next((ir for ir in inj_list if int(ir.player_id)==int(pid) if str(ir.player_id).isdigit()), None)
                        if not inj_match:
                            continue
                        # Is he *not* expected to start now?
                        displaced = (pid not in starters) if starters else True
                        if not displaced and inj_match.status_u in {"QUESTIONABLE","PROBABLE"}:
                            # If listed as starter but Q/Probable, apply light penalty
                            displaced = True
                        if displaced:
                            scale = float(inj_match.status_scale or 0.0)
                            w = pos_weight * scale
                            if mode=="relative":
                                # crude replacement: if expected starters exist, assume someone else is listed; shrink penalty by 25%
                                w *= 0.75 if starters else 1.0
                            delta += w
                            details.append({
                                "team":team,"pos":k,"player_id":pid,"player_name":pname,
                                "status": inj_match.status_u, "elo_penalty": w
                            })
                return delta, details

            d_home, det_h = side_delta(ha, canon_h, starters_h)
            d_away, det_a = side_delta(aa, canon_a, starters_a)

            out_rows.append({
                "home_abbr":ha, "away_abbr":aa,
                "elo_delta_home": round(d_home,2),
                "elo_delta_away": round(d_away,2)
            })
            for d in det_h+det_a:
                d.update({"game":slug})
                logf.write(json.dumps(d)+"\n")

    pd.DataFrame(out_rows).to_csv(OUT, index=False)
    print(f"[OK] injury adjustments -> {OUT} rows={len(out_rows)}")
    print(f"[OK] audit -> {LOG}")
if __name__ == "__main__":
    main()

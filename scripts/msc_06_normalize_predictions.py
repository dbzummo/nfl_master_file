#!/usr/bin/env python3
import os, csv, sys, requests

# Inputs
SRC   = sys.argv[1] if len(sys.argv)>1 else "out/week_predictions.csv"
START = os.environ.get("START") or sys.argv[2]  if len(sys.argv)>2 else None
END   = os.environ.get("END")   or sys.argv[3]  if len(sys.argv)>3 else None
if not START or not END:
    print("[FATAL] Provide START and END (YYYYMMDD)", file=sys.stderr); sys.exit(1)

key=os.environ.get('MSF_KEY'); pw=os.environ.get('MSF_PASS','MYSPORTSFEEDS')

# Fetch MSF schedule for the window
dates=[]; 
from datetime import datetime, timedelta
d0=datetime.strptime(START,"%Y%m%d"); d1=datetime.strptime(END,"%Y%m%d")
for i in range((d1-d0).days+1): dates.append((d0+timedelta(days=i)).strftime("%Y%m%d"))

lookup={}
if not key:
    print("[FATAL] MSF_KEY not set", file=sys.stderr); sys.exit(1)
for d in dates:
    r=requests.get(f'https://api.mysportsfeeds.com/v2.1/pull/nfl/2025-regular/date/{d}/games.json',
                   auth=(key,pw), timeout=30); r.raise_for_status()
    for g in r.json().get('games',[]):
        s=g.get('schedule',{})
        gid=str(s.get('id') or s.get('msfGameId') or '')
        home=(s.get('homeTeam') or {}).get('abbreviation') or ''
        away=(s.get('awayTeam') or {}).get('abbreviation') or ''
        if gid and home and away:
            lookup[(home.upper(),away.upper())]=gid

ALIASES={'LAR':'LA','WSH':'WAS'}
def norm(t): return ALIASES.get(t,t)

rows=list(csv.DictReader(open(SRC, newline='', encoding='utf-8')))

su=[]; ats=[]
for x in rows:
    home = norm((x.get('home_team') or '').strip().upper())
    away = norm((x.get('away_team') or '').strip().upper())
    gid  = (x.get('msf_game_id') or '').strip() or lookup.get((home,away))
    if not gid: continue

    # try a list of candidate fields
    p_su = None
    for k in ('p_home','p_home_cal_platt','p_home_cal_iso','win_prob_home','p_home_calibrated','p_home_model'):
        v = x.get(k)
        if v not in (None, ''):
            try: p_su=float(v); break
            except: pass

    p_cov = None
    for k in ('p_cover','cover_prob_home'):
        v = x.get(k)
        if v not in (None, ''):
            try: p_cov=float(v); break
            except: pass

    if p_su is not None:   su.append((gid, p_su))
    if p_cov is not None: ats.append((gid, p_cov))

os.makedirs("out", exist_ok=True)
with open("out/week_predictions_norm_su.csv","w",newline="",encoding="utf-8") as f:
    w=csv.writer(f); w.writerow(["game_id","p_home"]); w.writerows(su)
with open("out/week_predictions_norm_ats.csv","w",newline="",encoding="utf-8") as f:
    w=csv.writer(f); w.writerow(["game_id","p_cover"]); w.writerows(ats)
print(f"[OK] wrote SU={len(su)} ATS={len(ats)} → out/week_predictions_norm_*")

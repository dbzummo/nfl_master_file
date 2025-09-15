#!/usr/bin/env python3
import os,csv,requests,datetime,sys
key=os.environ['MSF_KEY']; pw=os.environ.get('MSF_PASS','MYSPORTSFEEDS')
start=os.environ.get('START'); end=os.environ.get('END')
if not (key and start and end):
    print("[FATAL] MSF_KEY/START/END not set", file=sys.stderr); sys.exit(1)
d0=datetime.datetime.strptime(start,"%Y%m%d"); d1=datetime.datetime.strptime(end,"%Y%m%d")
rows=[]
for i in range((d1-d0).days+1):
    d=(d0+datetime.timedelta(days=i)).strftime("%Y%m%d")
    r=requests.get(f'https://api.mysportsfeeds.com/v2.1/pull/nfl/2025-regular/date/{d}/games.json',auth=(key,pw),timeout=30)
    r.raise_for_status()
    for g in r.json().get('games',[]):
        s=g.get('schedule',{}); sc=g.get('score',{})
        gid=str(s.get('id') or s.get('msfGameId') or '')
        hs=sc.get('homeScoreTotal'); aw=sc.get('awayScoreTotal')
        if gid and isinstance(hs,int) and isinstance(aw,int): rows.append((gid,hs,aw))
os.makedirs('out/results',exist_ok=True)
with open('out/results/finals.csv','w',newline='',encoding='utf-8') as f:
    csv.writer(f).writerows([('game_id','home_score','away_score'),*rows])
print('[OK] finals rows=',len(rows))

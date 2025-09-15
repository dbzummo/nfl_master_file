#!/usr/bin/env python3
import csv, sys, math
from pathlib import Path

BOARD  = Path("out/model_board.csv")
FINALS = Path("out/results/finals.csv")
OUT    = Path("reports/eval_ats.html")

def read_finals(path):
    rows={}
    with open(path, newline='', encoding='utf-8') as f:
        r=csv.DictReader(f)
        for x in r:
            gid=(x.get('game_id') or '').strip()
            try:
                hs=float(x.get('home_score') or '')
                as_=float(x.get('away_score') or '')
            except:
                continue
            if gid:
                rows[gid]=(hs,as_)
    return rows

def try_float(v):
    try: return float(v)
    except: return None

def main():
    if not BOARD.exists() or not FINALS.exists():
        print("[FATAL] Missing board or finals CSV.", file=sys.stderr)
        sys.exit(1)

    finals = read_finals(FINALS)

    # Collect rows where we have both spread and model_line for finished games
    rows=[]
    with open(BOARD, newline='', encoding='utf-8') as f:
        r=csv.DictReader(f)
        for x in r:
            gid=(x.get('game_id') or x.get('msf_game_id') or '').strip()
            if not gid or gid not in finals:
                continue

            spread = try_float(x.get('vegas_line_home'))
            model  = try_float(x.get('model_line_home') or x.get('model_line_home_from_blend'))
            if spread is None or model is None:
                continue

            hs,as_ = finals[gid]
            margin = hs - as_  # home - away
            # Home covers if margin + spread > 0 ; push if exactly 0
            covered = 1 if (margin + spread) > 0 else (0 if (margin + spread) < 0 else 0.5)
            pick    = 'HOME' if model > spread else 'AWAY'
            correct = (pick=='HOME' and covered==1) or (pick=='AWAY' and covered==0)

            rows.append({
                'date': x.get('date',''),
                'away': x.get('away_team',''),
                'home': x.get('home_team',''),
                'spread_home': spread,
                'model_home': model,
                'final_margin': margin,
                'pick': pick,
                'covered': covered,
                'correct': correct
            })

    rows.sort(key=lambda r:(r['date'], r['home'], r['away']))
    n=len(rows); wins=sum(1 for r in rows if r['correct']); pushes=sum(1 for r in rows if r['covered']==0.5)
    acc=(wins/(n-pushes)) if (n-pushes)>0 else 0.0

    def fmt(x):
        return f"{x:.2f}" if isinstance(x,(int,float)) else str(x)

    # Render HTML (dark theme)
    hdr = """<!doctype html><meta charset="utf-8"><title>ATS Eval</title>
<style>
body{background:#111;color:#eee;font:14px/1.4 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Arial}
h1{margin:16px 12px}
table{width:98%;margin:8px auto;border-collapse:collapse}
th,td{padding:8px 10px;border-bottom:1px solid #222}
th{background:#181818;text-align:left}
tr:nth-child(even) td{background:#141414}
.ok{color:#7dff7d}.bad{color:#ff8a8a}
.mono{font-variant-numeric:tabular-nums}
</style>"""
    summary = f"<h1>ATS Evaluation</h1><p>Games: {n} | Wins: {wins} | Pushes: {pushes} | Accuracy (ex-push): {acc:.1%}</p>"
    tbl = ["<table><tr><th>Date</th><th>Away</th><th>Home</th><th class='mono'>Vegas (H)</th><th class='mono'>Model (H)</th><th class='mono'>Final Δ (H−A)</th><th>Pick</th><th>Covered?</th><th>Correct</th></tr>"]
    for r in rows:
        covered = "HOME" if r['covered']==1 else ("AWAY" if r['covered']==0 else "PUSH")
        ok = "ok" if r['correct'] else "bad"
        tbl.append(
            f"<tr><td>{r['date']}</td><td>{r['away']}</td><td>{r['home']}</td>"
            f"<td class='mono'>{fmt(r['spread_home'])}</td><td class='mono'>{fmt(r['model_home'])}</td>"
            f"<td class='mono'>{fmt(r['final_margin'])}</td><td>{r['pick']}</td>"
            f"<td>{covered}</td><td class='{ok}'>{'✓' if r['correct'] else '✗'}</td></tr>"
        )
    tbl.append("</table>")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(hdr+summary+"".join(tbl), encoding='utf-8')

    if n==0:
        print("[OK] No overlapping finished games yet; wrote empty ATS report.")
    else:
        print(f"[OK] ATS eval → {OUT} (rows={n}, wins={wins}, pushes={pushes}, acc={acc:.3f})")

if __name__ == "__main__":
    main()

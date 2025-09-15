#!/usr/bin/env python3
import argparse, csv, json, math, os
from pathlib import Path

def sigmoid(x): 
    return 1.0/(1.0+math.exp(-x))

def load_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--board",  default="out/model_board.csv")
    ap.add_argument("--finals", default="out/results/finals.csv")
    ap.add_argument("--cal",    default="out/calibration/model_line_calibration.json")
    ap.add_argument("--out",    default="reports/eval_ats.html")
    args = ap.parse_args()

    board = load_csv(args.board)
    finals = {r["game_id"]: (int(r["home_score"]), int(r["away_score"])) 
              for r in load_csv(args.finals) if r.get("home_score") and r.get("away_score")}
    if not board or not finals:
        print("[FATAL] Missing board or finals rows"); raise SystemExit(1)

    a,b = 0.0,1.0
    if Path(args.cal).exists():
        cal = json.load(open(args.cal, encoding="utf-8"))
        a,b = float(cal["a"]), float(cal["b"])

    rows_eval = []
    y_true, p_pred = [], []

    for r in board:
        gid = (r.get("msf_game_id") or "").strip()
        if gid not in finals: 
            continue

        try:
            vegas_line = float(r["vegas_line_home"])
            model_line = float(r["model_line_home"])
        except Exception:
            continue

        hs, as_ = finals[gid]
        margin = hs - as_

        # Outcome vs spread (push excluded)
        if margin == vegas_line:
            continue
        y_cover = 1 if margin > vegas_line else 0

        # Model cover probability using same logistic calibration, on the line delta
        # P(home covers) = sigmoid( a + b*(model_line - vegas_line) )
        p_cover = sigmoid(a + b*(model_line - vegas_line))

        # Model pick (home vs spread if edge positive)
        pick = "HOME" if (model_line - vegas_line) > 0 else "AWAY"

        rows_eval.append({
            "game_id": gid,
            "vegas_line_home": f"{vegas_line:+.1f}",
            "model_line_home": f"{model_line:+.2f}",
            "edge": f"{(model_line-vegas_line):+.2f}",
            "home_score": str(hs),
            "away_score": str(as_),
            "margin": f"{margin:+.0f}",
            "y_cover": "1" if y_cover==1 else "0",
            "p_cover": f"{p_cover:.6f}",
            "correct": "✓" if ((y_cover==1 and pick=='HOME') or (y_cover==0 and pick=='AWAY')) else "✗",
        })

        y_true.append(y_cover)
        p_pred.append(p_cover)

    if not rows_eval:
        print("[FATAL] No overlapping finished games to evaluate."); raise SystemExit(1)

    # Metrics
    import math
    n = len(y_true)
    brier = sum((p_pred[i]-y_true[i])**2 for i in range(n))/n
    eps = 1e-12
    logloss = -sum(y_true[i]*math.log(max(p_pred[i],eps)) + (1-y_true[i])*math.log(max(1-p_pred[i],eps)) for i in range(n))/n
    acc = sum(1 for i in range(n) if ( (p_pred[i] >= 0.5 and y_true[i]==1) or (p_pred[i] < 0.5 and y_true[i]==0) ))/n

    # HTML
    Path("reports").mkdir(parents=True, exist_ok=True)
    out = Path(args.out)
    def tr(cells, th=False):
        tag = "th" if th else "td"
        return "<tr>"+ "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>"

    header = ["Game ID","Vegas","Model","Edge","Home","Away","Margin","Cover(Y)","P(cover)","Correct"]
    body = "\n".join(tr([r["game_id"], r["vegas_line_home"], r["model_line_home"], r["edge"],
                         r["home_score"], r["away_score"], r["margin"], r["y_cover"], r["p_cover"], r["correct"]])
                     for r in rows_eval)

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>Week Evaluation (ATS)</title>
<style>
body{{font:16px/1.3 -apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Arial;color:#111}}
h1{{margin:8px 4px}}
table{{border-collapse:collapse;margin:8px}}
th,td{{border:1px solid #888;padding:4px 8px}}
th{{background:#eee}}
</style></head><body>
<h1>Week Evaluation (ATS)</h1>
<p>Games: {n} | Accuracy: {acc:.3f} | Brier: {brier:.6f} | Logloss: {logloss:.6f}</p>
<table>{tr(header, th=True)}{body}</table>
</body></html>"""
    out.write_text(html, encoding="utf-8")
    print(f"[OK] Wrote ATS evaluation: {out}")

if __name__ == "__main__":
    main()

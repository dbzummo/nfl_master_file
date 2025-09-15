#!/usr/bin/env python3
from __future__ import annotations
import os, json, csv, math
from collections import defaultdict
from typing import Dict, Any, List

# Expect predictions at out/week_predictions.csv with columns:
# game_id, home_team, away_team, p_home
# Expect finals at out/results/finals.csv with columns:
# game_id, home_score, away_score

def _read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _brier(p: float, y: int) -> float:
    return (p - y) ** 2

def _logloss(p: float, y: int) -> float:
    eps = 1e-15
    p = max(min(p, 1 - eps), eps)
    return -(y * math.log(p) + (1-y) * math.log(1-p))

def main() -> None:
    preds_path = "out/week_predictions.csv"
    finals_path = "out/results/finals.csv"
    if not os.path.exists(preds_path) or not os.path.exists(finals_path):
        print("[FATAL] Missing predictions or finals CSV")
        raise SystemExit(1)

    preds = {r["game_id"]: float(r["p_home"]) for r in _read_csv(preds_path)}
    rows = _read_csv(finals_path)

    # Determine week from data window if present; otherwise generic
    week_tag = os.environ.get("WEEK_TAG", "2025w02")

    per_game = []
    acc = 0
    brier_sum = 0.0
    logloss_sum = 0.0
    n = 0

    for r in rows:
        gid = r["game_id"]
        if gid not in preds:
            continue
        y = 1 if int(r["home_score"]) > int(r["away_score"]) else 0
        p = preds[gid]
        n += 1
        acc += 1 if ((p >= 0.5 and y == 1) or (p < 0.5 and y == 0)) else 0
        brier_sum += _brier(p, y)
        logloss_sum += _logloss(p, y)
        per_game.append({
            "game_id": gid, "p_home": p, "y": y,
            "correct": "✓" if ((p>=0.5) == (y==1)) else "✗"
        })

    if n == 0:
        print("[FATAL] No overlapping games between predictions and finals")
        raise SystemExit(1)

    metrics = {
        "games": n,
        "accuracy": acc / n,
        "brier": brier_sum / n,
        "logloss": logloss_sum / n,
        "week_tag": week_tag,
    }

    os.makedirs("out/eval", exist_ok=True)
    with open(f"out/eval/{week_tag}.json", "w", encoding="utf-8") as f:
        json.dump({"metrics": metrics, "per_game": per_game}, f, indent=2)

    # Minimal HTML
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/{week_tag}_eval.html", "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'></head><body><h1>Week Evaluation</h1>")
        f.write(f"<p>Games: {n} | Accuracy: {metrics['accuracy']:.3f} | "
                f"Brier: {metrics['brier']:.6f} | Logloss: {metrics['logloss']:.6f}</p>")
        f.write("<table border='1' cellpadding='4'><tr><th>Game ID</th><th>p_home</th><th>Correct</th></tr>")
        for g in per_game:
            f.write(f"<tr><td>{g['game_id']}</td><td>{g['p_home']:.3f}</td><td>{g['correct']}</td></tr>")
        f.write("</table></body></html>")
    print("[OK] Wrote evaluation:", f"reports/{week_tag}_eval.html")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations
import sys, os, math, traceback
import pandas as pd
from pathlib import Path

BOARD = Path("out/model_board.csv")
FINALS = Path("out/results/finals.csv")
OUT   = Path("reports/eval_ats.html")

def fatal_report(msg: str, detail: str = "") -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>ATS Eval — ERROR</title></head>
<body>
<h1>ATS Evaluation — ERROR</h1>
<p><b>{msg}</b></p>
<pre style="white-space:pre-wrap">{detail}</pre>
</body></html>"""
    OUT.write_text(html, encoding="utf-8")
    print(f"[FATAL] {msg}")
    return 1

def safe_float(x, default=None):
    try: return float(x)
    except: return default

def main() -> int:
    try:
        if not BOARD.exists():
            return fatal_report("Missing out/model_board.csv")
        if not FINALS.exists():
            return fatal_report("Missing out/results/finals.csv")

        # Read deterministically; we will normalize types ourselves.
        board = pd.read_csv(BOARD)
        finals = pd.read_csv(FINALS)

        # Hard requirement: finals has only game_id + scores; join must be on game_id.
        if "game_id" not in board.columns:
            return fatal_report("Board is missing 'game_id' column")
        if "game_id" not in finals.columns:
            return fatal_report("Finals is missing 'game_id' column")

        # Normalize dtypes to STRINGS on both sides to avoid int64/object mismatch.
        board["game_id"]  = board["game_id"].astype(str)
        finals["game_id"] = finals["game_id"].astype(str)

        # Choose spread source: prefer market (vegas_line_home) if present, else model_line_home.
        spread_col = None
        for c in ["vegas_line_home", "model_line_home", "model_line_home_adj"]:
            if c in board.columns:
                spread_col = c
                break
        if spread_col is None:
            return fatal_report("No spread-like column found on board (need one of vegas_line_home/model_line_home/model_line_home_adj)")

        # Optional probability to display
        pcol = None
        for c in ["p_home_blend", "p_home_model_adj", "p_home_model", "p_home"]:
            if c in board.columns:
                pcol = c
                break

        # Columns we need from board for the final table
        keep = ["game_id", spread_col]
        label_map = {spread_col: "spread_home"}
        if "date" in board.columns: keep.append("date")
        if "home_team" in board.columns: keep.append("home_team")
        if "away_team" in board.columns: keep.append("away_team")
        if pcol: 
            keep.append(pcol)
            label_map[pcol] = "p_home_hat"

        bcut = board[keep].rename(columns=label_map).copy()

        # Merge on game_id ONLY (finals lacks date/teams)
        m = finals.merge(bcut, on="game_id", how="left")

        # Compute ATS outcome:
        # Convention: positive spread_home means home is favored by that many.
        # Cover if (home_score - away_score) - spread_home > 0 by more than 0 (push == 0).
        m["home_score"] = pd.to_numeric(m["home_score"], errors="coerce")
        m["away_score"] = pd.to_numeric(m["away_score"], errors="coerce")
        m["spread_home"] = pd.to_numeric(m["spread_home"], errors="coerce")

        # sanity filter rows we can evaluate
        eval_mask = m["home_score"].notna() & m["away_score"].notna() & m["spread_home"].notna()
        eval_df = m.loc[eval_mask].copy()

        if eval_df.empty:
            return fatal_report("No evaluable rows after merge (scores/spread missing).")

        # pick: sign(spread_home) <= 0 -> pick home if p_home_hat>0.5? We keep it simple:
        # We'll judge by spread only: home covers if margin > spread; away covers if margin < spread; = -> push.
        margin = eval_df["home_score"] - eval_df["away_score"]
        diff = margin - eval_df["spread_home"]
        eval_df["ats_outcome"] = eval_df.apply(
            lambda r: "HOME cover" if (r["home_score"] - r["away_score"]) > r["spread_home"]
                      else ("AWAY cover" if (r["home_score"] - r["away_score"]) < r["spread_home"]
                            else "PUSH"), axis=1)

        # Define "pick" as the favorite against the spread (sign of spread). If spread == 0, pick none (push scenario).
        eval_df["pick"] = eval_df["spread_home"].apply(lambda s: "HOME -"+str(abs(s)) if s>0
                                                       else ("AWAY +"+str(abs(s)) if s<0 else "PICK=NONE"))

        # Correct if our pick matches the cover (excluding pushes from denominator).
        def correct_row(r):
            if r["ats_outcome"] == "PUSH": 
                return None
            if r["spread_home"] > 0 and r["ats_outcome"] == "HOME cover": 
                return True
            if r["spread_home"] < 0 and r["ats_outcome"] == "AWAY cover": 
                return True
            return False

        eval_df["correct"] = eval_df.apply(correct_row, axis=1)

        # Metrics
        total_rows = int(len(eval_df))
        non_push   = int(eval_df["ats_outcome"].ne("PUSH").sum())
        correct    = int(eval_df["correct"].sum(skipna=True))
        wr = (correct / non_push) if non_push > 0 else float("nan")

        # Render deterministic HTML
        def esc(s): 
            return ("" if pd.isna(s) else str(s)).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

        rows = []
        cols = ["date","away_team","home_team","spread_home","home_score","away_score","p_home_hat","pick","ats_outcome","correct"]
        for _, r in eval_df.iterrows():
            matchup = f"{esc(r.get('away_team',''))} @ {esc(r.get('home_team',''))}"
            rows.append(
                "<tr>" +
                f"<td>{esc(r.get('date',''))}</td>" +
                f"<td>{matchup}</td>" +
                f"<td>{esc(r.get('spread_home',''))}</td>" +
                f"<td>{esc(r.get('home_score',''))}-{esc(r.get('away_score',''))}</td>" +
                f"<td>{esc(r.get('p_home_hat',''))}</td>" +
                f"<td>{esc(r.get('pick',''))}</td>" +
                f"<td>{esc(r.get('ats_outcome',''))}</td>" +
                f"<td>{'' if r.get('correct') is None else ('✓' if r.get('correct') else '✗')}</td>" +
                "</tr>"
            )

        OUT.parent.mkdir(parents=True, exist_ok=True)
        html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>ATS Evaluation</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #ddd;padding:6px;text-align:center}}
th{{background:#f3f3f3}}
</style></head>
<body>
<h1>ATS Evaluation</h1>
<p>rows={total_rows} non_push={non_push} correct={correct} wr={wr:.3f if non_push>0 else float('nan')}</p>
<table><thead><tr>
<th>Date</th><th>Matchup</th><th>Spread(H)</th><th>Score(H-A)</th><th>p_home_hat</th><th>Pick</th><th>ATS Outcome</th><th>Correct</th>
</tr></thead><tbody>
{''.join(rows)}
</tbody></table>
</body></html>"""
        OUT.write_text(html, encoding="utf-8")
        print(f"[OK] ATS eval → {OUT} rows={total_rows} non_push={non_push} correct={correct} wr={wr:.3f if non_push>0 else float('nan')}")
        return 0

    except Exception as e:
        tb = traceback.format_exc()
        return fatal_report("Unhandled exception in ATS evaluator", tb)

if __name__ == "__main__":
    sys.exit(main())

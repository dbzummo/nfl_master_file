#!/usr/bin/env python3
import sys, math
from pathlib import Path
import pandas as pd

FIN = Path("out/results/finals.csv")
BRD = Path("out/model_board.csv")
OUT = Path("reports/eval_ats.html")

def pick(cols, df):
    for c in cols:
        if c in df.columns: return c
    return None

def main():
    if not FIN.exists():
        print("[FATAL] finals.csv missing", file=sys.stderr); sys.exit(2)
    if not BRD.exists():
        print("[FATAL] model_board.csv missing", file=sys.stderr); sys.exit(2)

    finals = pd.read_csv(FIN)
    board  = pd.read_csv(BRD)

    # Finals schema normalization
    date_col = pick(["date","game_date"], finals) or "date"
    home_col = pick(["home_team","home"], finals) or "home_team"
    away_col = pick(["away_team","away"], finals) or "away_team"
    hp_col   = pick(["home_score","home_pts","home_points"], finals)
    ap_col   = pick(["away_score","away_pts","away_points"], finals)

    if hp_col is None or ap_col is None:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(
            "<html><body><h1>ATS Evaluation</h1>"
            "<p>Insufficient columns in finals.csv to compute ATS (need home/away scores).</p>"
            "</body></html>", encoding="utf-8"
        )
        print("[OK] ATS eval → reports/eval_ats.html (insufficient columns notice)")
        return

    finals = finals.rename(columns={
        date_col:"date", home_col:"home_team", away_col:"away_team",
        hp_col:"home_pts", ap_col:"away_pts"
    })

    # Board schema normalization (only the needed bits)
    need = ["date","home_team","away_team","vegas_line_home","p_home_blend","p_home_model"]
    for c in need:
        if c not in board.columns: board[c] = None

    df = finals.merge(board[need], on=["date","home_team","away_team"], how="left")

    # Compute ATS
    df["vegas_line_home"] = pd.to_numeric(df["vegas_line_home"], errors="coerce").fillna(0.0)
    df["home_pts"] = pd.to_numeric(df["home_pts"], errors="coerce").fillna(0).astype(int)
    df["away_pts"] = pd.to_numeric(df["away_pts"], errors="coerce").fillna(0).astype(int)
    df["margin_home"] = df["home_pts"] - df["away_pts"]
    df["ats_val"] = df["margin_home"] + df["vegas_line_home"]  # >0 home cover, <0 away cover, 0 push

    def phat(row):
        for c in ("p_home_blend","p_home_model"):
            try:
                v = float(row.get(c)); 
                if math.isfinite(v): return min(max(v,0.0),1.0)
            except: pass
        return 0.5
    df["p_home_hat"] = df.apply(phat, axis=1)
    df["pick_home"]  = df["p_home_hat"] >= 0.5
    df["is_push"]    = (df["ats_val"] == 0)
    df["ats_home"]   = (df["ats_val"] > 0)
    df["correct"]    = (~df["is_push"]) & ((df["pick_home"] & df["ats_home"]) | (~df["pick_home"] & ~df["ats_home"]))

    n_total = int((~df["is_push"]).sum())
    n_corr  = int(df["correct"].sum())
    wr      = (n_corr / n_total) if n_total else 0.0

    # Minimal HTML (deterministic)
    rows=[]
    for _,r in df.iterrows():
        matchup=f"{r['away_team']} @ {r['home_team']}"
        rows.append(
            f"<tr><td>{r['date']}</td><td>{matchup}</td>"
            f"<td>{r['vegas_line_home']:+.1f}</td>"
            f"<td>{r['home_pts']}-{r['away_pts']}</td>"
            f"<td>{r['p_home_hat']:.3f}</td>"
            f"<td>{'HOME' if r['pick_home'] else 'AWAY'}</td>"
            f"<td>{'HOME' if r['ats_home'] else ('PUSH' if r['is_push'] else 'AWAY')}</td>"
            f"<td>{'✓' if r['correct'] else ('–' if r['is_push'] else '✗')}</td></tr>"
        )
    html = ("<!doctype html><html><head><meta charset='utf-8'><title>ATS Evaluation</title>"
            "<style>table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:4px 6px}</style>"
            "</head><body>"
            f"<h1>ATS Evaluation</h1><p><b>Non-push games:</b> {n_total} &nbsp; "
            f"<b>Correct:</b> {n_corr} &nbsp; <b>Win rate:</b> {wr:.3f}</p>"
            "<table><thead><tr><th>Date</th><th>Matchup</th><th>Spread(H)</th>"
            "<th>Score(H-A)</th><th>p_home_hat</th><th>Pick</th><th>ATS Outcome</th><th>Correct</th>"
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></body></html>")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(f"[OK] ATS eval → {OUT} games={len(df)} non_push={n_total} correct={n_corr} wr={wr:.3f}")

if __name__ == "__main__":
    main()

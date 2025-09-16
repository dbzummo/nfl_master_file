#!/usr/bin/env python3
"""
Deterministic ATS evaluator.

Prime-Directive behaviors:
- Auditable: prints join coverage & assumptions.
- Reproducible: no network; pure function of finals.csv + board.csv.
- Accurate: prefers game_id join; falls back to normalized (date, away@home).
- Fail-closed on IO; always emits reports/eval_ats.html if inputs exist.
"""
import sys, math, re
from pathlib import Path
import pandas as pd

FIN  = Path("out/results/finals.csv")
BRD  = Path("out/model_board.csv")
OUT  = Path("reports/eval_ats.html")

def pick(df, *cands):
    for c in cands:
        if c in df.columns: return c
    return None

def norm_team(x:str)->str:
    return (str(x) or "").strip().upper()

def norm_date(x):
    s = str(x).strip()
    if not s: return s
    # Allow 'YYYY-MM-DD' or 'YYYYMMDD'
    if re.fullmatch(r"\d{8}", s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s

def build_key(df, date_col, away_col, home_col):
    return df[date_col].map(norm_date) + "|" + df[away_col].map(norm_team) + "@"+ df[home_col].map(norm_team)

def safe_num(s, default=0.0):
    try:
        x = float(s)
        if math.isfinite(x): return x
    except Exception:
        pass
    return float(default)

def ensure_out_html(text):
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding="utf-8")

def main():
    if not FIN.exists():
        print("[FATAL] finals.csv missing", file=sys.stderr); sys.exit(2)
    if not BRD.exists():
        print("[FATAL] model_board.csv missing", file=sys.stderr); sys.exit(2)

    finals = pd.read_csv(FIN)
    board  = pd.read_csv(BRD)

    # Finals: pick columns
    f_date = pick(finals, "date","game_date","gamedate")
    f_home = pick(finals, "home_team","home","home_abbr","home_code")
    f_away = pick(finals, "away_team","away","away_abbr","away_code")
    f_hpts = pick(finals, "home_score","home_pts","home_points")
    f_apts = pick(finals, "away_score","away_pts","away_points")
    f_gid  = pick(finals, "game_id","msf_game_id","id","match_id")

    # Board: pick columns
    b_date = pick(board, "date","game_date")
    b_home = pick(board, "home_team","home")
    b_away = pick(board, "away_team","away")
    b_gid  = pick(board, "game_id","msf_game_id","id")
    b_sp   = pick(board, "vegas_line_home","line_home","spread_home")
    b_p1   = pick(board, "p_home_blend","p_home_model","p_home","p")
    # Sanity numeric
    if b_sp is None: 
        board["vegas_line_home"] = 0.0
        b_sp = "vegas_line_home"
    board[b_sp] = pd.to_numeric(board[b_sp], errors="coerce").fillna(0.0)

    # Require scores to compute ATS; if missing, still emit an explanatory report.
    have_scores = f_hpts is not None and f_apts is not None
    if have_scores:
        finals[f_hpts] = pd.to_numeric(finals[f_hpts], errors="coerce").fillna(0).astype(int)
        finals[f_apts] = pd.to_numeric(finals[f_apts], errors="coerce").fillna(0).astype(int)

    # Build joins: prefer game_id; else normalized (date|away@home)
    merged = None
    coverage = {"method": "", "rows": 0, "with_spread": 0}

    if f_gid and b_gid and finals[f_gid].notna().any() and board[b_gid].notna().any():
        merged = finals.merge(board[[b_gid,b_sp,b_p1] if b_p1 else [b_gid,b_sp]].rename(
            columns={b_gid:"__gid__", b_sp:"__sp__", (b_p1 or "dummy"):"__p__"}
        ), left_on=f_gid, right_on="__gid__", how="left")
        coverage["method"] = "game_id"
    else:
        if not (f_date and f_home and f_away and b_date and b_home and b_away):
            # Emit minimal report; we can't compute ATS without a join path.
            ensure_out_html(
                "<html><body><h1>ATS Evaluation</h1>"
                "<p>Cannot evaluate: missing join keys (no shared game_id and incomplete date/home/away columns).</p>"
                "</body></html>"
            )
            print("[OK] ATS eval → reports/eval_ats.html (no join path; explanatory report)")
            return
        finals["__key__"] = build_key(finals.rename(columns={f_date:"date",f_away:"away",f_home:"home"}),
                                      "date","away","home")
        board["__key__"]  = build_key(board.rename(columns={b_date:"date",b_away:"away",b_home:"home"}),
                                      "date","away","home")
        keep_cols = ["__key__", b_sp] + ([b_p1] if b_p1 else [])
        merged = finals.merge(board[keep_cols].rename(
            columns={b_sp:"__sp__", (b_p1 or "dummy"):"__p__"}
        ), on="__key__", how="left")
        coverage["method"] = "date+away@home"

    coverage["rows"] = len(merged)
    coverage["with_spread"] = int(merged["__sp__"].notna().sum())

    # Compute ATS if we have scores
    if have_scores:
        merged["margin_home"] = merged[f_hpts] - merged[f_apts]
        merged["ats_val"]     = merged["margin_home"] + merged["__sp__"].fillna(0.0)
        merged["is_push"]     = (merged["ats_val"] == 0)
        # pick prob for direction (home if >= .5)
        def phat(v):
            try:
                x = float(v)
                return min(max(x,0.0),1.0)
            except: return 0.5
        merged["p_home_hat"]  = merged["__p__"].map(phat) if "__p__" in merged else 0.5
        merged["pick_home"]   = merged["p_home_hat"] >= 0.5
        merged["ats_home"]    = merged["ats_val"] > 0
        merged["correct"]     = (~merged["is_push"]) & (
            (merged["pick_home"] & merged["ats_home"]) | (~merged["pick_home"] & ~merged["ats_home"])
        )
        non_push = int((~merged["is_push"]).sum())
        corr     = int(merged["correct"].sum())
        wr       = (corr / non_push) if non_push else 0.0
    else:
        non_push = corr = 0
        wr = 0.0

    # Emit deterministic HTML
    rows_html = []
    sample = merged.head(64)  # keep output small & deterministic
    # Try to render some identifiers regardless of column names
    def g(vc, *names, default=""):
        for n in names:
            if n in vc: return vc[n]
        return default
    for _, r in sample.iterrows():
        date = g(r, f_date, "date", "__key__").__str__()
        home = g(r, f_home, "home_team","home","HOME").__str__()
        away = g(r, f_away, "away_team","away","AWAY").__str__()
        sp   = safe_num(r.get("__sp__"), 0.0)
        hpts = r.get(f_hpts, "")
        apts = r.get(f_apts, "")
        ph   = r.get("p_home_hat", "")
        pk   = "HOME" if r.get("pick_home", False) else "AWAY"
        ats  = ("HOME" if r.get("ats_home", False) else ("PUSH" if r.get("is_push", False) else "AWAY")) if have_scores else ""
        ck   = ("✓" if r.get("correct", False) else ("–" if r.get("is_push", False) else "✗")) if have_scores else ""
        rows_html.append(
            f"<tr><td>{date}</td><td>{away} @ {home}</td><td>{sp:+.1f}</td>"
            f"<td>{hpts}-{apts}</td><td>{ph if ph=='' else f'{float(ph):.3f}'}</td>"
            f"<td>{pk if have_scores else ''}</td><td>{ats}</td><td>{ck}</td></tr>"
        )

    html = f"""<!doctype html><html><head><meta charset="utf-8">
<title>ATS Evaluation</title>
<style>table{{border-collapse:collapse}}td,th{{border:1px solid #ccc;padding:4px 6px;font:12px system-ui}}</style>
</head><body>
<h1>ATS Evaluation</h1>
<p><b>Join method:</b> {coverage['method']} &nbsp; <b>rows:</b> {coverage['rows']} &nbsp; <b>with spread:</b> {coverage['with_spread']}</p>
<p><b>Non-push games:</b> {non_push} &nbsp; <b>Correct:</b> {corr} &nbsp; <b>Win rate:</b> {wr:.3f}</p>
<table><thead><tr><th>Date</th><th>Matchup</th><th>Spread(H)</th><th>Score(H-A)</th>
<th>p_home_hat</th><th>Pick</th><th>ATS Outcome</th><th>Correct</th></tr></thead>
<tbody>
{''.join(rows_html)}
</tbody></table>
</body></html>
"""
    ensure_out_html(html)
    print(f"[OK] ATS eval → {OUT} rows={coverage['rows']} non_push={non_push} correct={corr} wr={wr:.3f}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import sys, json
from pathlib import Path
import pandas as pd

BOARD = Path("out/model_board.csv")
FINALS = Path("out/results/finals.csv")
OUTDIR = Path("reports"); OUTDIR.mkdir(parents=True, exist_ok=True)
OUTHTML = OUTDIR/"id_parity_audit.html"

def main()->int:
    if not BOARD.exists():
        write_html([], [], "Missing board file")
        print("[FATAL] Missing out/model_board.csv", file=sys.stderr)
        return 2
    if not FINALS.exists():
        write_html([], [], "Missing finals file")
        print("[FATAL] Missing out/results/finals.csv", file=sys.stderr)
        return 2

    b = pd.read_csv(BOARD, dtype=str)
    f = pd.read_csv(FINALS, dtype=str)

    if "game_id" not in b.columns:
        write_html([], [], "Board missing game_id")
        print("[FATAL] Board missing game_id", file=sys.stderr)
        return 2
    if "game_id" not in f.columns:
        write_html([], [], "Finals missing game_id")
        print("[FATAL] Finals missing game_id", file=sys.stderr)
        return 2

    b_ids = set(b["game_id"].dropna().astype(str))
    f_ids = set(f["game_id"].dropna().astype(str))

    miss_in_board = sorted(f_ids - b_ids)
    miss_in_finals = sorted(b_ids - f_ids)
    matched = sorted(b_ids & f_ids)

    write_html(miss_in_board, miss_in_finals, None, matched_count=len(matched), board_count=len(b_ids), finals_count=len(f_ids))

    if len(matched) == 0:
        print("[FATAL] Board↔Finals parity: zero matched game_id; check id_parity_audit.html", file=sys.stderr)
        return 1
    if miss_in_board or miss_in_finals:
        print(f"[FATAL] Board↔Finals parity: mismatches (in finals not in board={len(miss_in_board)}, in board not in finals={len(miss_in_finals)}); see reports/id_parity_audit.html", file=sys.stderr)
        return 1

    print(f"[OK] Board↔Finals parity OK; matches={len(matched)}")
    return 0

def write_html(miss_in_board, miss_in_finals, fatal_msg=None, matched_count=0, board_count=0, finals_count=0):
    def table(title, rows):
        if not rows:
            return f"<h3>{title}</h3><p>None</p>"
        head = "<thead><tr><th>game_id</th></tr></thead>"
        body = "<tbody>" + "".join(f"<tr><td>{x}</td></tr>" for x in rows[:1000]) + "</tbody>"
        return f"<h3>{title} (showing up to 1000)</h3><table>{head}{body}</table>"

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>ID Parity Audit</title>
<style>body{{font-family:system-ui,Arial,sans-serif}} table{{border-collapse:collapse}} th,td{{border:1px solid #ccc;padding:4px 8px}}</style>
</head><body>
<h2>ID Parity Audit</h2>
<p>board ids: {board_count} distinct; finals ids: {finals_count} distinct; matched: {matched_count}</p>
{"<p style='color:#b00'><strong>FATAL:</strong> "+fatal_msg+"</p>" if fatal_msg else ""}
{table("In finals but missing in board", miss_in_board)}
{table("In board but missing in finals", miss_in_finals)}
</body></html>"""
    OUTHTML.write_text(html, encoding="utf-8")
    print(f"[OK] wrote {OUTHTML}")

if __name__ == "__main__":
    sys.exit(main())

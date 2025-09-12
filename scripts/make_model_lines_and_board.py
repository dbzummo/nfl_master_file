#!/usr/bin/env python3
import json, math, pathlib, sys
import pandas as pd
from datetime import datetime
from pathlib import Path

# -------- paths --------
CAL_FILE   = pathlib.Path("out/calibration/model_line_calibration.json")
PRED_FILE  = pathlib.Path("out/week_predictions.csv")   # must include: date, away_team, home_team, p_home
ODDS_FILE  = pathlib.Path("out/odds/week_odds.csv")     # must include: date, away_team, home_team, spread (home perspective)
TEAM_INJ   = pathlib.Path("out/injuries/team_adjustments.csv")  # team, points_capped
PLAYER_IMP = pathlib.Path("out/injuries/player_impacts.csv")    # team, player, position, game_status, detail, points

OUT_CSV    = pathlib.Path("out/model_board.csv")
OUT_HTML   = pathlib.Path("reports/board_week.html")
CARDS_DIR  = pathlib.Path("reports/cards")

# -------- math helpers --------
def sigmoid(x): return 1.0/(1.0+math.exp(-x))
def logit(p):   return math.log(p/(1.0-p))

def prob_from_spread(a,b,spread):
    return sigmoid(a + b*spread)

def spread_from_prob(a,b,prob):
    # invert the mapping
    return (logit(prob) - a)/b

# -------- loading --------
def load_calibration():
    if not CAL_FILE.exists():
        print(f"[FATAL] Missing calibration at {CAL_FILE}", file=sys.stderr); sys.exit(2)
    obj = json.loads(CAL_FILE.read_text())
    a = float(obj["a"]); b = float(obj["b"])
    return a,b

def load_predictions():
    if not PRED_FILE.exists():
        print(f"[FATAL] Missing predictions at {PRED_FILE}", file=sys.stderr); sys.exit(2)
    df = pd.read_csv(PRED_FILE)
    # normalize
    for c in ("away_team","home_team"):
        df[c] = df[c].astype(str).str.upper().str.strip()
    # accept either string or datetime date column
    df["date"] = pd.to_datetime(df["date"]).dt.date
    # prefer p_home; if not present, try elo_exp_home fallback
    pcol = "p_home" if "p_home" in df.columns else ("elo_exp_home" if "elo_exp_home" in df.columns else None)
    if pcol is None:
        print("[FATAL] out/week_predictions.csv needs 'p_home' (or 'elo_exp_home')", file=sys.stderr); sys.exit(2)
    df = df[["date","away_team","home_team",pcol]].rename(columns={pcol:"p_home"})
    # clamp
    df["p_home"] = df["p_home"].clip(1e-6, 1-1e-6)
    return df

def load_odds():
    if not ODDS_FILE.exists():
        print(f"[FATAL] Missing odds at {ODDS_FILE}", file=sys.stderr); sys.exit(2)
    o = pd.read_csv(ODDS_FILE)
    for c in ("away_team","home_team"):
        o[c] = o[c].astype(str).str.upper().str.strip()
    o["date"] = pd.to_datetime(o["date"]).dt.date
    # required: spread (home perspective); totals optional
    need = {"date","away_team","home_team","spread"}
    if not need.issubset(o.columns):
        print(f"[FATAL] {ODDS_FILE} missing columns: {sorted(list(need - set(o.columns)))}", file=sys.stderr); sys.exit(2)
    o = o[["date","away_team","home_team","spread"] + ([ "total"] if "total" in o.columns else [])]
    return o

def load_inj_team():
    if TEAM_INJ.exists():
        t = pd.read_csv(TEAM_INJ)
        t["team"] = t["team"].astype(str).str.upper().str.strip()
        if "points_capped" not in t.columns:
            # older file name
            t["points_capped"] = t.get("points", 0.0)
        return t[["team","points_capped"]]
    # missing => zeros
    return pd.DataFrame({"team":[], "points_capped":[]})

def load_player_impacts():
    if PLAYER_IMP.exists():
        p = pd.read_csv(PLAYER_IMP)
        p["team"] = p["team"].astype(str).str.upper().str.strip()
        p["points"] = pd.to_numeric(p.get("points", 0.0), errors="coerce").fillna(0.0)
        return p
    return pd.DataFrame(columns=["team","player","position","game_status","detail","points"])

# -------- cards --------
CARD_CSS = """
body{font-family:-apple-system,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#0f1216;color:#e8ecef;margin:24px;}
h1{margin:0 0 8px 0;font-size:24px}
small{color:#a8b3bd}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin:16px 0 20px}
.card{background:#151a21;border:1px solid #2a3340;border-radius:8px;padding:14px}
.k{color:#8aa2b8}
.v{font-weight:600}
.table{width:100%;border-collapse:collapse;font-size:14px}
.table th,.table td{border-bottom:1px solid #233042;padding:8px 10px;text-align:left}
.table th{color:#a8b3bd;font-weight:600;background:#141922}
.badge{display:inline-block;padding:2px 8px;border-radius:99px;background:#1e2733;color:#cfe3ff;font-size:12px;margin-left:6px}
.pos{color:#67d087}.neg{color:#ff7b7b}
a{color:#9ecbff;text-decoration:none}
a:hover{text-decoration:underline}
"""

def card_slug(date, away, home):
    return f"{date}_{away}_at_{home}.html"

def fmt_spread(team, spread):
    # team code + signed number, home perspective
    s = f"{team} {spread:+.1f}"
    return s

def render_game_card(row, a, b, player_impacts):
    """
    row: series with date, away_team, home_team, spread, model_line, edge, p_market_home, p_model_home,
         inj_home_pts, inj_away_pts, inj_net, baseline_line
    """
    date = row["date"]
    away = row["away_team"]; home = row["home_team"]
    vegas_line = fmt_spread(home, row["spread"])
    model_line = fmt_spread(home, row["model_line"])
    edge = row["edge"]
    conf = abs(row["p_model_home"] - row["p_market_home"])*100.0

    # Top injury contributors
    ph = player_impacts[player_impacts["team"].eq(home)].sort_values("points", ascending=False).head(6)
    pa = player_impacts[player_impacts["team"].eq(away)].sort_values("points", ascending=False).head(6)

    def table_html(df):
        if df.empty:
            return "<p><i>No material injuries counted.</i></p>"
        cols = ["player","position","game_status","points","detail"]
        hdrs = ["Player","Pos","Status","Pts","Detail"]
        df2 = df[cols].copy()
        df2["points"] = df2["points"].map(lambda x: f"{x:+.1f}")
        return df2.to_html(index=False, classes="table", border=0, escape=True, header=True).replace(
            "<thead>", "<thead><tr>" + "".join([f"<th>{h}</th>" for h in []]) + "</tr>"
        )

    html = []
    html.append("<!doctype html><meta charset='utf-8'>")
    html.append(f"<title>{away} @ {home} — {date}</title>")
    html.append(f"<style>{CARD_CSS}</style>")
    html.append(f"<a href='../board_week.html'>&larr; Back to Board</a>")
    html.append(f"<h1>{away} @ {home} <span class='badge'>{date}</span></h1>")
    html.append("<div class='grid'>")

    # Summary
    html.append("<div class='card'>")
    html.append("<div class='k'>Vegas Line</div>")
    html.append(f"<div class='v' style='margin-bottom:8px'>{vegas_line}</div>")
    html.append("<div class='k'>Model Line</div>")
    html.append(f"<div class='v' style='margin-bottom:8px'>{model_line} "
                f"<span class='badge'>{'+' if edge>0 else ''}{edge:.1f} vs market</span></div>")
    html.append("<div class='k'>Probabilities</div>")
    html.append(f"<div class='v'>Market P(Home) {row['p_market_home']:.3f} · Model P(Home) {row['p_model_home']:.3f} "
                f"<span class='badge'>Confidence {conf:.1f}%</span></div>")
    html.append("</div>")

    # Decomposition
    html.append("<div class='card'>")
    html.append("<div class='k'>How this number was built</div>")
    html.append("<ul style='margin:8px 0 0 18px'>")
    html.append(f"<li>Calibrated mapping a={a:.4f}, b={b:.4f} converts probability ↔ spread.</li>")
    p0 = row['p_home'] if 'p_home' in row.index else row['p_model_home']
    html.append(f"<li>Baseline (no injuries): from P(Home)={p0:.3f} → "
                f"line {fmt_spread(home, row['baseline_line'])}.</li>")
    html.append(f"<li>Injury delta: Away {row['inj_away_pts']:.1f} − Home {row['inj_home_pts']:.1f} "
                f"= Net {row['inj_net']:+.1f} pts.</li>")
    html.append(f"<li>Final Model Line = Baseline + Net Inj = {fmt_spread(home, row['baseline_line'])} "
                f"{'+' if row['inj_net']>=0 else ''}{row['inj_net']:.1f} → {model_line}.</li>")
    html.append("</ul>")
    html.append("</div>")

    html.append("</div>")  # grid

    # Injuries
    html.append("<div class='grid'>")
    html.append(f"<div class='card'><div class='k'>Home injuries counted ({home})</div>{table_html(ph)}</div>")
    html.append(f"<div class='card'><div class='k'>Away injuries counted ({away})</div>{table_html(pa)}</div>")
    html.append("</div>")

    # Footer
    html.append("<div style='margin-top:14px;color:#8aa2b8'>")
    html.append("Notes: Vegas/Model lines are home spreads (e.g., 'GB −3.0'). "
                "Model probability uses the line after injury adjustments. "
                "Injury contributors come from out/injuries/player_impacts.csv (conservative top-N per group).")
    html.append("</div>")

    return "\n".join(html)

# -------- main board build --------
BOARD_CSS = """
body{font-family:-apple-system,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#0f1216;color:#e8ecef;margin:24px;}
h1{margin:0 0 8px 0}
small{color:#a8b3bd}
table{width:100%;border-collapse:collapse;margin-top:14px}
th,td{border-bottom:1px solid #233042;padding:8px 10px;text-align:left}
th{color:#a8b3bd;font-weight:600;background:#141922}
.badge{display:inline-block;padding:2px 8px;border-radius:99px;background:#1e2733;color:#cfe3ff;font-size:12px;margin-left:6px}
.pos{color:#67d087}.neg{color:#ff7b7b}
a{color:#9ecbff;text-decoration:none}
a:hover{text-decoration:underline}
"""

def main():
    a,b = load_calibration()
    preds  = load_predictions()
    odds   = load_odds()
    teams  = load_inj_team()
    pimps  = load_player_impacts()

    # join odds + predictions
    df = pd.merge(odds, preds, on=["date","away_team","home_team"], how="inner")

    # injury points (home/away, zeros if not found)
    home_inj = teams.rename(columns={"team":"home_team","points_capped":"inj_home_pts"})
    away_inj = teams.rename(columns={"team":"away_team","points_capped":"inj_away_pts"})
    df = pd.merge(df, home_inj, on="home_team", how="left")
    df = pd.merge(df, away_inj, on="away_team", how="left")
    df["inj_home_pts"] = df["inj_home_pts"].fillna(0.0)
    df["inj_away_pts"] = df["inj_away_pts"].fillna(0.0)
    df["inj_net"]      = df["inj_away_pts"] - df["inj_home_pts"]

    # probabilities
    df["p_market_home"] = df["spread"].map(lambda s: prob_from_spread(a,b,float(s)))
    # baseline line from p_home (no injuries)
    df["baseline_line"] = df["p_home"].map(lambda p: spread_from_prob(a,b,float(p)))
    # final model line = baseline + injury net
    df["model_line"]    = df["baseline_line"] + df["inj_net"]

    df["p_model_home"]  = df["model_line"].map(lambda s: prob_from_spread(a,b,float(s)))
    df["edge"]          = df["model_line"] - df["spread"]
    df["confidence"]    = (df["p_model_home"] - df["p_market_home"]).abs()*100.0

    # persist CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    tocsv = df[["date","away_team","home_team","spread","model_line","edge",
                "p_market_home","p_model_home","confidence",
                "inj_home_pts","inj_away_pts","inj_net","baseline_line"]].copy()
    tocsv.sort_values(["date","home_team","away_team"]).to_csv(OUT_CSV, index=False)
    print(f"[OK] wrote {OUT_CSV}  rows={len(df)}")

    # generate cards directory
    CARDS_DIR.mkdir(parents=True, exist_ok=True)

    # build HTML table with Vegas Line linked to card
    show = tocsv.copy()
    show["date"] = pd.to_datetime(show["date"]).dt.strftime("%Y-%m-%d")

    # link target
    def href(row):
        return f"cards/{card_slug(row['date'], row['away_team'], row['home_team'])}"

    # build linked "Vegas Line"
    show["Vegas Line"] = show.apply(lambda r: f"<a href='{href(r)}'>{r['home_team']} {r['spread']:+.1f}</a>", axis=1)
    show["Model Line"] = show.apply(lambda r: f"{r['home_team']} {r['model_line']:+.1f}", axis=1)
    show["P(Home) Mkt"]   = show["p_market_home"].map(lambda x: f"{x:.3f}")
    show["P(Home) Model"] = show["p_model_home"].map(lambda x: f"{x:.3f}")
    show["Confidence"]    = show["confidence"].map(lambda x: f"{x:.1f}%")
    show["Edge (pts)"]    = show["edge"].map(lambda x: f"<span class='{'pos' if x>0 else 'neg'}'>{x:+.1f}</span>")
    show["Home Inj Pts"]  = show["inj_home_pts"].map(lambda x: f"{x:.3f}".rstrip('0').rstrip('.') if x!=0 else "0")
    show["Away Inj Pts"]  = show["inj_away_pts"].map(lambda x: f"{x:.3f}".rstrip('0').rstrip('.') if x!=0 else "0")
    show["Net Inj (A-H)"] = show["inj_net"].map(lambda x: f"{x:+.3f}".rstrip('0').rstrip('.'))

    out_cols = ["date","away_team","home_team","Vegas Line","Model Line",
                "P(Home) Mkt","P(Home) Model","Confidence","Edge (pts)",
                "Home Inj Pts","Away Inj Pts","Net Inj (A-H)"]
    show = show[out_cols].rename(columns={"date":"Date","away_team":"Away","home_team":"Home"})

    # render main board
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    html = []
    html.append("<!doctype html><meta charset='utf-8'>")
    html.append("<title>Weekly Model Board</title>")
    html.append(f"<style>{BOARD_CSS}</style>")
    html.append("<h1>Week Board — Model vs Market</h1>")
    html.append("<small>Vegas vs Model lines (lines are HOME spreads). "
                "Confidence = |Model P(home) − Market P(home)|.</small>")
    html.append(show.to_html(index=False, escape=False))
    OUT_HTML.write_text("\n".join(html), encoding="utf-8")
    print("open reports/board_week.html")
    print(f"[OK] wrote {OUT_HTML}")

    # write cards
    for _, r in tocsv.iterrows():
        card_path = CARDS_DIR / card_slug(r["date"], r["away_team"], r["home_team"])
        card_html = render_game_card(r, a, b, pimps)
        card_path.write_text(card_html, encoding="utf-8")

    # one console example
    if tocsv.empty:
        print('[WARN] No games to display for current filters — wrote empty board.')
        Path('reports').mkdir(parents=True, exist_ok=True)
        with open('reports/board_week.html','w') as f:
            f.write('<html><body><h2>No games to display for current filters.</h2><p>(Board generated empty.)</p></body></html>')
        return
    # [EMPTY_GUARD_INSERTED]
    ex = tocsv.iloc[0]
    print(f"[DIAG] Example: {ex['away_team']} @ {ex['home_team']} | "
          f"spread={ex['spread']:+.1f} | model_line={ex['model_line']:+.1f} | "
          f"edge={ex['edge']:+.1f} | conf={abs(ex['p_model_home']-ex['p_market_home'])*100:.1f}%")

if __name__ == "__main__":
    main()


# === Post-run self-heal (ensures board is never left empty) ===
if __name__ == "__main__":
    try:
        import pandas as pd
        from pathlib import Path as _Path

        mb_path = _Path("out/model_board.csv")
        needs_heal = (not mb_path.exists())
        if not needs_heal:
            try:
                mb = pd.read_csv(mb_path)
                needs_heal = (mb.shape[0] == 0)
            except Exception:
                needs_heal = True

        if needs_heal:
            print("[INFO] Post-run self-heal: repopulating board from predictions (MSF week window).")
            pred_p = _Path("out/week_predictions.csv")
            msf_p  = _Path("out/msf_details/msf_week.csv")
            if pred_p.exists() and msf_p.exists():
                pred = pd.read_csv(pred_p)
                msf  = pd.read_csv(msf_p)

                # Normalize keys
                for c in ("date","away_team","home_team"):
                    if c in pred.columns: pred[c] = pred[c].astype(str)
                    if c in msf.columns:  msf[c]  = msf[c].astype(str)

                if "date" in pred.columns and "date" in msf.columns and len(msf) > 0:
                    dmin = pd.to_datetime(msf["date"]).min()
                    dmax = pd.to_datetime(msf["date"]).max()
                    mask = (pd.to_datetime(pred["date"]) >= dmin) & (pd.to_datetime(pred["date"]) <= dmax)
                    rec  = pred.loc[mask].copy()

                    # Ensure canonical p_home exists
                    if "p_home" not in rec.columns:
                        for c in ("p_home_cal_platt","p_home_cal_iso","p_home_raw","elo_exp_home"):
                            if c in rec.columns:
                                rec["p_home"] = rec[c].astype(float).clip(0,1)
                                break

                    keep = [c for c in ["date","away_team","home_team","p_home","msf_game_id"] if c in rec.columns]
                    if keep and len(rec) > 0:
                        rec[keep].sort_values("date").to_csv("out/model_board.csv", index=False)
                        print(f"[INFO] Post-run self-heal wrote out/model_board.csv rows={rec[keep].shape[0]}")
                    else:
                        print("[WARN] Post-run self-heal: nothing to write (check columns).")
            else:
                print("[WARN] Post-run self-heal: predictions/MSF files missing.")
    except Exception as _e:
        print("[WARN] Post-run self-heal failed:", _e)
# === End post-run self-heal ===


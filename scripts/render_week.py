#!/usr/bin/env python3
import os, glob, textwrap
import pandas as pd
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_OK = True
except Exception:
    PIL_OK = False

SRC = "out/predictions_week_calibrated_blend.csv"

def newest_report_dir():
    htmls = sorted(glob.glob("reports/*/weekly_report.html"))
    if not htmls:
        raise SystemExit("No reports/*/weekly_report.html found. Run publish_week.sh first.")
    return os.path.dirname(htmls[-1])

def pct(x): return f"{round(float(x)*100,1)}%"

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def favorite_note(row):
    p = float(row["home_win_prob"])
    fav = row["home_team"] if p >= 0.5 else row["away_team"]
    edge = abs(p-0.5)*100
    tilt = "leans strongly" if max(p,1-p) >= 0.65 else "leans"
    loc = "home" if p >= 0.5 else "away"
    return f"Model {tilt} toward {fav} ({loc}). Edge ~{edge:.1f} pts vs 50/50."

def draw_card(out_path, date, away, home, p_home, note):
    if not PIL_OK: return
    from PIL import Image, ImageDraw, ImageFont
    W,H = 1400,788
    img = Image.new("RGB",(W,H),(248,248,248))
    d = ImageDraw.Draw(img)
    try:
        font_h = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf",64)
        font_t = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf",40)
        font_s = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf",30)
    except:
        font_h=font_t=font_s=ImageFont.load_default()
    d.rectangle([0,0,W,110], fill=(20,20,20))
    d.text((40,24), f"{date} • NFL Week", fill="white", font=font_h)
    mid=W//2
    d.line([(mid,150),(mid,H-150)], fill=(200,200,200), width=4)
    def team(xc,name):
        tw=d.textlength(name,font=font_t)
        d.text((xc - tw/2, 430), name, fill=(30,30,30), font=font_t)
    team(mid-300, away); team(mid+300, home)
    ptxt=f"P(Home) {pct(p_home)}"
    pw=d.textlength(ptxt,font=font_h)
    x=mid - pw/2 - 30
    d.rounded_rectangle([x,520,x+pw+60,616], radius=16, fill=(34,139,230))
    d.text((mid - pw/2, 540), ptxt, fill="white", font=font_h)
    d.text((100,H-160), textwrap.fill(note,60), fill=(60,60,60), font=font_s)
    ensure_dir(os.path.dirname(out_path))
    img.save(out_path,"PNG")

def main():
    if not os.path.exists(SRC):
        raise SystemExit(f"Missing {SRC}. Build it first.")
    df = pd.read_csv(SRC)[["date","away_team","home_team","home_win_prob"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    out_dir = newest_report_dir()

    # write pretty table the HTML can use
    tbl = df.copy()
    tbl["P(Home)"] = (tbl["home_win_prob"].astype(float)*100).round(1).astype(str) + "%"
    tbl = tbl[["date","away_team","home_team","P(Home)"]]
    tbl.to_csv(os.path.join(out_dir, "week_table_v3.csv"), index=False)

    # generate simple game cards
    cards_dir = os.path.join(out_dir,"game_cards")
    for _,r in df.iterrows():
        note = favorite_note(r)
        fname = f"{r['date']}_{r['away_team'].replace(' ','_')}_at_{r['home_team'].replace(' ','_')}.png"
        draw_card(os.path.join(cards_dir,fname), r["date"], r["away_team"], r["home_team"], r["home_win_prob"], note)

    # make the HTML load v3 table (local replace)
    html = os.path.join(out_dir,"weekly_report.html")
    if os.path.exists(html):
        try:
            s=open(html,"r",encoding="utf-8").read()
            t=s.replace("week_table_v2.csv","week_table_v3.csv").replace("week_table.csv","week_table_v3.csv")
            if t!=s: open(html,"w",encoding="utf-8").write(t)
        except Exception as e:
            print("[warn] could not adjust HTML:", e)

    print(f"[render] table -> {os.path.join(out_dir,'week_table_v3.csv')}")
    print(f"[render] cards -> {cards_dir if PIL_OK else '(skipped, install pillow)'}")

if __name__ == "__main__":
    main()

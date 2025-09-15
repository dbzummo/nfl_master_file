import sys, os, runpy, traceback, pandas as pd, pathlib

OUT_DIR = pathlib.Path("out/odds")
OUT_FILE = OUT_DIR / "week_odds.csv"
GAMES = pathlib.Path("out/ingest/week_games.csv")

def write_stub_from_games(reason: str):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not GAMES.exists():
        print(f"[guard][stub] no {GAMES} to stub from; writing empty odds ({reason})")
        pd.DataFrame(columns=["date","msf_game_id","away_team","home_team","book","spread","total"]).to_csv(OUT_FILE, index=False)
        return
    g = pd.read_csv(GAMES)
    if g.empty:
        print(f"[guard][stub] {GAMES} empty; writing empty odds ({reason})")
        pd.DataFrame(columns=["date","msf_game_id","away_team","home_team","book","spread","total"]).to_csv(OUT_FILE, index=False)
        return
    # normalize columns
    a = g.get("away_team", g.get("away"))
    h = g.get("home_team", g.get("home"))
    out = pd.DataFrame({
        "date": pd.to_datetime(g.get("date", "" ).astype(str).str[:10], errors="coerce").dt.strftime("%Y-%m-%d"),
        "msf_game_id": g.get("msf_game_id", ""),
        "away_team": a.astype(str).str.upper() if a is not None else "",
        "home_team": h.astype(str).str.upper() if h is not None else "",
        "book": "Stub",
        "spread": 0.0,
        "total": 44.0,
    })
    out.to_csv(OUT_FILE, index=False)
    print(f"[guard][stub] wrote {OUT_FILE} rows={len(out)} ({reason})")

def main():
    # try the real script
    try:
        print("[guard] running scripts/fetch_odds.py …")
        runpy.run_path("scripts/fetch_odds.py", run_name="__main__")
    except SystemExit as e:
        # if the script intentionally exits, fall through to check artifact
        print(f"[guard] fetch_odds.py SystemExit code={e.code}")
    except Exception:
        print("\n==== fetch_odds.py TRACEBACK (caught by guard) ====\n")
        traceback.print_exc()
        print("\n===================================================\n")
        write_stub_from_games("exception in fetch_odds.py")
        return

    # if we got here without exception, ensure the file exists; if not, stub
    if not OUT_FILE.exists():
        write_stub_from_games("no output produced by fetch_odds.py")
    else:
        try:
            df = pd.read_csv(OUT_FILE)
            print(f"[guard] ok: {OUT_FILE} rows={len(df)}")
        except Exception:
            write_stub_from_games("output unreadable; rewrote stub")

if __name__ == "__main__":
    main()

import re, sys, pathlib

p = pathlib.Path("run_predictions.py")
s = p.read_text(encoding="utf-8")

# 1) Ensure import line exists
if "from injuries_fallbacks import derive_injuries_from_rosters" not in s:
    s = s.replace(
        "from run_monte_carlo import run_simulation",
        "from run_monte_carlo import run_simulation\nfrom injuries_fallbacks import derive_injuries_from_rosters"
    )

# 2) Replace STEP 5 block
pattern = re.compile(
    r'print\([^\n]*STEP\s*5[^\n]*\)\s*.*?(?=print\([^\n]*STEP\s*6)',
    re.DOTALL
)

replacement = """print("\\nSTEP 5: Fetching latest injury data (strict)...")
injuries = fetch_injured_players()
inj_ct = len(injuries) if isinstance(injuries, pd.DataFrame) else 0

if inj_ct == 0:
    # Live injuries empty: derive conservative unavailability from roster statuses (IR/PUP/NFI/Suspended).
    teams_in_play = _pick_teams_from_odds(odds_df)
    injuries = derive_injuries_from_rosters(teams_in_play)
    inj_ct = len(injuries)
    teams_ct = injuries["team_code"].nunique() if inj_ct else 0
    print(f"Roster-derived injuries: {inj_ct} rows across {teams_ct} teams.")

print(f"Found {inj_ct} records from injuries (live or fallback).")
"""

s2, n = pattern.subn(replacement, s)
if n == 0:
    print("Could not find STEP 5 block to replace.")
    sys.exit(1)

p.write_text(s2, encoding="utf-8")
print("✅ STEP 5 block replaced and import ensured.")
